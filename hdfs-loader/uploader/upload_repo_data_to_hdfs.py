import os
import sys
import urllib.parse
from pathlib import Path
import requests

HDFS_WEBHDFS_URL = os.environ["HDFS_WEBHDFS_URL"].rstrip("/")  # http://...:9870/webhdfs/v1
HDFS_USER = os.getenv("HDFS_USER", "hdfs")

DATA_ROOT = Path(os.getenv("DATA_ROOT", "/repo/Data"))
AUTOBAHN_LOCAL = DATA_ROOT / "Alle-Autobahn-Daten" / "autobahn"
WEATHER_LOCAL  = DATA_ROOT / "weather_data"

HDFS_AUTOBAHN_ROOT = os.getenv("HDFS_AUTOBAHN_ROOT", "/datalake/autobahn")
HDFS_WEATHER_ROOT  = os.getenv("HDFS_WEATHER_ROOT", "/datalake/weather_hist")


def webhdfs_url(path: str, op: str, **params) -> str:
    qp = {"op": op, "user.name": HDFS_USER}
    qp.update(params)
    return f"{HDFS_WEBHDFS_URL}{path}?{urllib.parse.urlencode(qp)}"


def hdfs_mkdirs(path: str):
    r = requests.put(webhdfs_url(path, "MKDIRS"), timeout=30)
    r.raise_for_status()


def hdfs_put_bytes(hdfs_path: str, data: bytes, overwrite: bool = True):
    r1 = requests.put(
        webhdfs_url(hdfs_path, "CREATE", overwrite=str(overwrite).lower()),
        allow_redirects=False,
        timeout=30,
    )
    if r1.status_code not in (307, 201):
        raise RuntimeError(f"CREATE failed {r1.status_code}: {r1.text[:200]}")
    loc = r1.headers.get("Location")
    if not loc:
        raise RuntimeError("Missing Location header from WebHDFS CREATE")
    r2 = requests.put(loc, data=data, timeout=300)
    r2.raise_for_status()


def upload_tree(local_root: Path, hdfs_root: str):
    if not local_root.exists():
        raise FileNotFoundError(f"Local path not found in cloned repo: {local_root}")

    files = [p for p in local_root.rglob("*.jsonl.gz") if p.is_file()]
    print(f"Found {len(files)} files under {local_root}")

    hdfs_mkdirs(hdfs_root)

    for idx, f in enumerate(files, 1):
        rel = f.relative_to(local_root).as_posix()
        target_file = f"{hdfs_root}/{rel}"
        target_dir = "/" + "/".join(target_file.split("/")[:-1])
        hdfs_mkdirs(target_dir)
        hdfs_put_bytes(target_file, f.read_bytes(), overwrite=True)

        if idx % 200 == 0:
            print(f"Uploaded {idx}/{len(files)}")

    print(f"Uploaded ALL: {local_root} -> {hdfs_root}")


def main():
    upload_tree(AUTOBAHN_LOCAL, HDFS_AUTOBAHN_ROOT)
    upload_tree(WEATHER_LOCAL,  HDFS_WEATHER_ROOT)
    return 0


if __name__ == "__main__":
    sys.exit(main())
