#!/usr/bin/env python3
import os
import sys
import time
import urllib.parse
from pathlib import Path
from typing import Optional

import requests

######################################################################
# HDFS Initial Loader via WebHDFS (Kubernetes-tauglich)
#
# Zweck
#   Dieses Skript lädt historische, lokal im Repository vorhandene
#   Daten (Autobahn- und Wetterdaten im Format *.jsonl.gz) rekursiv
#   in ein Hadoop Distributed File System (HDFS), das über WebHDFS
#   erreichbar ist und in einem Kubernetes-Cluster betrieben wird.
#
# Kernfunktionalität
#   - Liest Konfiguration ausschließlich über Environment-Variablen
#     (u. a. WebHDFS-Endpoint, User, Zielpfade im Data Lake)
#   - Traversiert definierte lokale Verzeichnisse rekursiv
#   - Spiegelt die Verzeichnisstruktur 1:1 nach HDFS
#   - Legt benötigte HDFS-Verzeichnisse automatisch an (MKDIRS)
#   - Lädt Dateien über den zweistufigen WebHDFS-Workflow:
#       1) CREATE-Aufruf beim NameNode (HTTP 307 Redirect)
#       2) PUT der Datei zum DataNode
#
# Robustheit & Kubernetes-Spezifika
#   - Eigene HTTP-Request-Logik mit Retries und exponentiellem Backoff
#   - Behandlung typischer WebHDFS-Probleme in Kubernetes:
#       * Rewrite von DataNode-Pod-Hostnamen auf ein Service-DNS
#         (hdfs-datanode.default.svc.cluster.local)
#   - Konfigurierbare Timeouts für kurze (Metadata) und lange (Upload)
#     HTTP-Operationen
#
# Zielkontext
#   - Initiale Befüllung des HDFS-basierten Data Lakes (Bronze Layer)
#   - Einsatz als Kubernetes Job im Rahmen des
#     Big-Data-/Data-Engineering-Setups
#
# Ergebnis
#   - Autobahn-Daten → HDFS_AUTOBAHN_ROOT
#   - Wetter-Historie → HDFS_WEATHER_ROOT
#   - Struktur- und Dateiintegrität bleiben erhalten
######################################################################


# =========================
# ENV / CONFIG
# =========================
HDFS_WEBHDFS_URL = os.environ["HDFS_WEBHDFS_URL"].rstrip("/")  # e.g. http://hdfs-namenode...:9870/webhdfs/v1
HDFS_USER = os.getenv("HDFS_USER", "hdfs")

# This must be resolvable in Kubernetes DNS (Service name!)
HDFS_DATANODE_SERVICE = os.getenv("HDFS_DATANODE_SERVICE", "hdfs-datanode.default.svc.cluster.local")

DATA_ROOT = Path(os.getenv("DATA_ROOT", "/repo/data"))

AUTOBAHN_LOCAL = DATA_ROOT / "Alle-Autobahn-Daten" / "autobahn"
WEATHER_LOCAL = DATA_ROOT / "weather_data"

HDFS_AUTOBAHN_ROOT = os.getenv("HDFS_AUTOBAHN_ROOT", "/datalake/autobahn")
HDFS_WEATHER_ROOT = os.getenv("HDFS_WEATHER_ROOT", "/datalake/weather_hist")

HTTP_TIMEOUT_SHORT = int(os.getenv("HTTP_TIMEOUT_SHORT", "60"))
HTTP_TIMEOUT_LONG = int(os.getenv("HTTP_TIMEOUT_LONG", "600"))
RETRIES = int(os.getenv("HTTP_RETRIES", "6"))
BACKOFF_BASE_SEC = float(os.getenv("HTTP_BACKOFF_BASE_SEC", "0.5"))


# =========================
# HELPERS
# =========================
def normalize_hdfs_path(p: str) -> str:
    """Ensure exactly one leading slash, no double slashes inside."""
    p = (p or "").strip()
    if not p:
        return "/"
    # Prüfung, ob es eine URL ist (hat uns Probleme gemacht...)
    if "://" in p:
        raise ValueError(f"HDFS path must be a path, not a URL: {p}")
    # Pfade normalisieren
    while "//" in p:
        p = p.replace("//", "/")
    if not p.startswith("/"):
        p = "/" + p
    return p


def webhdfs_url(path: str, op: str, **params) -> str:
    path = normalize_hdfs_path(path)
    qp = {"op": op, "user.name": HDFS_USER}
    qp.update(params)
    return f"{HDFS_WEBHDFS_URL}{path}?{urllib.parse.urlencode(qp)}"

# HTTP request durchführen mit retries
def request_with_retries(method: str, url: str, *, allow_redirects: bool, timeout: int, **kwargs) -> requests.Response:
    last_exc: Optional[Exception] = None
    for attempt in range(1, RETRIES + 1):
        try:
            resp = requests.request(
                method,
                url,
                allow_redirects=allow_redirects,
                timeout=timeout,
                **kwargs,
            )
            return resp
        except Exception as e:
            last_exc = e
            sleep_s = min(BACKOFF_BASE_SEC * (2 ** (attempt - 1)), 10.0)
            print(f"[WARN] request failed (attempt {attempt}/{RETRIES}) {method} {url} -> {e}; sleeping {sleep_s:.1f}s")
            time.sleep(sleep_s)
    raise RuntimeError(f"HTTP request failed after {RETRIES} retries: {method} {url} ({last_exc})")


def hdfs_mkdirs(path: str):
    path = normalize_hdfs_path(path)
    url = webhdfs_url(path, "MKDIRS")
    r = request_with_retries("PUT", url, allow_redirects=True, timeout=HTTP_TIMEOUT_SHORT)
    if r.status_code >= 400:
        raise RuntimeError(f"MKDIRS failed {r.status_code} for {path}: {r.text[:300]}")


def rewrite_datanode_location(loc: str) -> str:
    """
    WebHDFS CREATE returns 307 Location to a DataNode host.
    In many k8s setups it's a Pod name that won't resolve.
    Rewrite host -> HDFS_DATANODE_SERVICE.
    """
    parsed = urllib.parse.urlparse(loc)
    if not parsed.scheme or not parsed.netloc:
        # unexpected, just return as-is
        return loc

    host = parsed.hostname or ""
    port = parsed.port

    # If Location already uses a service / resolvable name, keep it.
    # If it looks like a pod name (hdfs-datanode-<hash>...), rewrite.
    if host.startswith("hdfs-datanode-") or host.endswith(".pod.cluster.local"):
        new_netloc = f"{HDFS_DATANODE_SERVICE}:{port}" if port else HDFS_DATANODE_SERVICE
        parsed = parsed._replace(netloc=new_netloc)
        new_loc = urllib.parse.urlunparse(parsed)
        return new_loc

    # Sometimes it returns an IP. That's usually fine; leave it.
    return loc


def hdfs_put_bytes(hdfs_path: str, data: bytes, overwrite: bool = True):
    hdfs_path = normalize_hdfs_path(hdfs_path)

    # Step 1: ask NameNode to CREATE -> expect 307 redirect or sometimes 201.
    create_url = webhdfs_url(
        hdfs_path,
        "CREATE",
        overwrite=str(overwrite).lower(),
        createparent="true",
    )
    r1 = request_with_retries("PUT", create_url, allow_redirects=False, timeout=HTTP_TIMEOUT_SHORT)

    if r1.status_code == 201:
        # Rare, but acceptable
        return

    if r1.status_code != 307:
        raise RuntimeError(f"CREATE failed {r1.status_code} for {hdfs_path}: {r1.text[:300]}")

    loc = r1.headers.get("Location")
    if not loc:
        raise RuntimeError(f"Missing Location header from WebHDFS CREATE for {hdfs_path}")

    loc2 = rewrite_datanode_location(loc)

    # Step 2: upload bytes to DataNode (rewritten service hostname)
    r2 = request_with_retries("PUT", loc2, allow_redirects=False, timeout=HTTP_TIMEOUT_LONG, data=data)
    if r2.status_code >= 400:
        raise RuntimeError(
            f"UPLOAD failed {r2.status_code} for {hdfs_path}\n"
            f"  Location(orig): {loc}\n"
            f"  Location(used): {loc2}\n"
            f"  Body: {r2.text[:300]}"
        )


def upload_tree(local_root: Path, hdfs_root: str):
    hdfs_root = normalize_hdfs_path(hdfs_root)

    if not local_root.exists():
        raise FileNotFoundError(f"Local path not found in cloned repo: {local_root}")

    files = [p for p in local_root.rglob("*.jsonl.gz") if p.is_file()]
    print(f"Found {len(files)} files under {local_root}")

    # ensure root exists
    hdfs_mkdirs(hdfs_root)

    for idx, f in enumerate(files, 1):
        rel = f.relative_to(local_root).as_posix()
        target_file = normalize_hdfs_path(f"{hdfs_root}/{rel}")
        target_dir = normalize_hdfs_path("/".join(target_file.split("/")[:-1]) or "/")

        # make parent dir
        hdfs_mkdirs(target_dir)

        # upload
        hdfs_put_bytes(target_file, f.read_bytes(), overwrite=True)

        if idx % 200 == 0 or idx == len(files):
            print(f"Uploaded {idx}/{len(files)}")


def main() -> int:
    print(f"HDFS_WEBHDFS_URL={HDFS_WEBHDFS_URL}")
    print(f"HDFS_USER={HDFS_USER}")
    print(f"HDFS_DATANODE_SERVICE={HDFS_DATANODE_SERVICE}")
    print(f"DATA_ROOT={DATA_ROOT}")
    print(f"AUTOBAHN_LOCAL={AUTOBAHN_LOCAL}")
    print(f"WEATHER_LOCAL={WEATHER_LOCAL}")
    print(f"HDFS_AUTOBAHN_ROOT={HDFS_AUTOBAHN_ROOT}")
    print(f"HDFS_WEATHER_ROOT={HDFS_WEATHER_ROOT}")

    upload_tree(AUTOBAHN_LOCAL, HDFS_AUTOBAHN_ROOT)
    upload_tree(WEATHER_LOCAL, HDFS_WEATHER_ROOT)
    return 0


if __name__ == "__main__":
    sys.exit(main())
