"""
Upload landed Parquet files to a Databricks Unity Catalog Volume, then move
them to a local archive.

Run:  python uploader/upload_to_volume.py      (Airflow calls upload_all())

Flow per file:
    1. landing/payments/ingest_date=.../part-x.parquet
    2.   -> /Volumes/<catalog>/raw/files/landing/payments/ingest_date=.../part-x.parquet
    3.   -> landing_archive/payments/ingest_date=.../part-x.parquet   (local)
    4. then the ground-truth file for the DQ catch rate

WHY "UPLOAD THEN MOVE" (no manifest database):
    The landing folder itself is the to-do list: whatever is still in it hasn't
    been uploaded. A crash after upload but before the move just means the file
    is uploaded again to the SAME path with overwrite=True. Auto Loader doesn't
    re-ingest a path it already processed, and even if it did, silver dedupes
    on the Kafka event id. So the worst case is wasted bandwidth, never
    duplicated data.

WHY KEEP AN ARCHIVE (instead of deleting):
    Cheap insurance. If the Volume is wiped, we can re-upload everything.
    Production would set a retention policy (e.g. 30 days) on it.

WHY ONLY *.parquet (not .tmp):
    The consumer writes .tmp then renames. Ignoring .tmp means we never upload
    a half-written file.

Edge cases handled:
    - deterministic order (mtime, then path), so retries behave the same way
    - a file that disappears between listing and upload (another uploader run
      or a manual move) is skipped, not fatal
    - a failed upload stops the run and leaves that file (and later ones) in
      landing for the next run; files already archived stay archived
    - empty ground-truth file is not uploaded (Spark can't infer its schema)
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from payflow_common.connections import catalog, check_dbx_env  # noqa: E402

LANDING = Path(os.getenv("LANDING_DIR", ROOT / "landing"))
ARCHIVE = Path(os.getenv("ARCHIVE_DIR", ROOT / "landing_archive"))
GROUND_TRUTH = Path(os.getenv("GROUND_TRUTH", ROOT / "simulator" / "injected" / "bad_records.jsonl"))


def volume_root() -> str:
    return f"/Volumes/{catalog()}/raw/files"


def pending_files(landing: Path = LANDING) -> list[Path]:
    """Completed Parquet files waiting to be uploaded, oldest first (ties broken by path)."""
    out = []
    for p in landing.glob("*/ingest_date=*/*.parquet"):
        try:
            out.append((p.stat().st_mtime, str(p), p))
        except FileNotFoundError:
            continue  # moved by someone else while we were listing
    return [p for _, _, p in sorted(out)]


def upload_all(client=None, landing: Path = LANDING, archive: Path = ARCHIVE,
               ground_truth: Path = GROUND_TRUTH) -> int:
    """Upload every pending file. Returns how many files were uploaded."""
    if client is None:
        check_dbx_env()
        from databricks.sdk import WorkspaceClient
        client = WorkspaceClient()  # DATABRICKS_HOST / DATABRICKS_TOKEN

    files = pending_files(landing)
    t0, total_bytes, uploaded = time.monotonic(), 0, 0
    for p in files:
        rel = p.relative_to(landing).as_posix()
        try:
            size = p.stat().st_size
            with p.open("rb") as f:
                # Steps 1 -> 2. overwrite=True makes a crash-retry idempotent.
                client.files.upload(f"{volume_root()}/landing/{rel}", f, overwrite=True)
        except FileNotFoundError:
            continue
        total_bytes += size
        # Step 3: only after a successful upload.
        dest = archive / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        os.replace(p, dest)
        uploaded += 1

    # Step 4: ground truth for DQ catch-rate. Whole file, overwritten each time (small).
    if ground_truth.exists() and ground_truth.stat().st_size > 0:
        with ground_truth.open("rb") as f:
            client.files.upload(f"{volume_root()}/ground_truth/bad_records.jsonl", f, overwrite=True)

    print(f"uploaded {uploaded} files, {total_bytes / 1e6:.2f} MB in {time.monotonic() - t0:.1f}s")
    return uploaded


if __name__ == "__main__":
    upload_all()
