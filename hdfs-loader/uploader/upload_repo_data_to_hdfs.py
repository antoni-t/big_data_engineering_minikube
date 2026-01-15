import os
import sys
import urllib.parse
from pathlib import Path

import requests


# --- Config -------------------------------------------------------------

HDFS_WEBHDFS_URL = os.environ["HDFS_WEBHDFS_URL"].rstrip("/")  # e.g. http://...:9870/webhdfs/v1
HDFS_USER = os.getenv("HDFS_USER", "hdfs")

DATA_ROOT = Path(os.getenv("DATA_ROOT", "/repo/data"))
AUTOBAHN_LOCAL = DATA_ROOT / "Alle-Autobahn-Daten" / "autobahn"
WEATHER_LOCAL = DATA_ROOT / "weather_data"

HDFS_AUTOBAHN_ROOT = os.getenv("HDFS_AUTOBAHN_ROOT", "/datalake/autobahn")
HDFS_WEATHER_ROOT = os.getenv("HDFS_WEATHER_ROOT", "/datalake/weather_hist")


# --- Helpers ------------------------------------------------------------

def _norm_hdfs_path(p: str) -> str:
    p = (p or "").strip()
    return "/" + p.lstrip("/")


def _join_hdfs(root: str, rel: str) -> str:
    """
    Join a root HDFS dir with a relative path, avoiding double slashes.
    """
    root_n = root.strip().rstrip("/")
    rel_n = rel.lstrip("/")
    return f"{root_n}/{rel_n}"

def webhdfs_url(hdfs_path: str, op: str, **params) -> str:
    """
    Build WebHDFS URL robustly:
      <HDFS_WEBHDFS_URL><normalized_path>?op=...&user.name=...
    Ensures no // after /webhdfs/v1.
    """
    qp = {"op": op, "user.name": HDFS_USER}
    qp.update(params)

    path = _norm_hdfs_path(hdfs_path)
    return f"{HDFS_WEBHDFS_URL}{path}?{urllib.parse.urlencode(qp)}"

# --- WebHDFS operations -------------------------------------------------

def hdfs_mkdirs(hdfs_path: str):
    url = webhdfs_url(hdfs_path, "MKDIRS")
    r = requests.put(url, timeout=60)
    r.raise_for_status()


def hdfs_put_bytes(hdfs_path: str, data: bytes, overwrite: bool = True):
    """
    WebHDFS CREATE typically returns 307 with Location header, then second PUT uploads data.
    Some setups may return 201 directly; we accept both.
    """
    url = webhdfs_url(hdfs_path, "CREATE", overwrite=str(overwrite).lower())

    r1 = requests.put(url, allow_redirects=False, timeout=60)

    # If it already created without redirect
    if r1.status_code == 201:
        return

    if r1.status_code != 307:
        raise RuntimeError(
            f"CREATE failed {r1.status_code} for {hdfs_path}: {r1.text[:400]}"
        )

    loc = r1.headers.get("Location")
    if not loc:
        raise RuntimeError("Missing Location header from WebHDFS CREATE")

    r2 = requests.put(loc, data=data, timeout=600)
    r2.raise_for_status()


# --- Upload logic --------------------------------------------------------

def upload_tree(local_root: Path, hdfs_root: str):
    if not local_root.exists():
        raise FileNotFoundError(f"Local path not found in cloned repo: {local_root}")

    files = [p for p in local_root.rglob("*.jsonl.gz") if p.is_file()]
    print(f"Found {len(files)} files under {local_root}")

    # normalize root once
    hdfs_root = _norm_hdfs_path(hdfs_root)

    # create root dir in HDFS
    hdfs_mkdirs(hdfs_root)

    for idx, f in enumerate(files, 1):
        rel = f.relative_to(local_root).as_posix()  # "YYYY/MM/DD/HH/file.jsonl.gz"
        target_file = _join_hdfs(hdfs_root, rel)

        # build parent dir robustly
        target_dir = "/" + "/".join(target_file.strip("/").split("/")[:-1])

        hdfs_mkdirs(target_dir)
        hdfs_put_bytes(target_file, f.read_bytes(), overwrite=True)

        if idx % 200 == 0:
            print(f"Uploaded {idx}/{len(files)}")

    print(f"Uploaded ALL: {local_root} -> {hdfs_root}")


def main() -> int:
    upload_tree(AUTOBAHN_LOCAL, HDFS_AUTOBAHN_ROOT)
    upload_tree(WEATHER_LOCAL, HDFS_WEATHER_ROOT)
    return 0


if __name__ == "__main__":
    sys.exit(main())