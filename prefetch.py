#!/usr/bin/env python3
"""并行预下载全部月度 parquet 到 cache/（与 pipeline.py 的 download 语义一致）。
先下完再扫描，避免单线程下载拖慢全程。用 6 路并发。"""
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pipeline import CACHE, HF_FILE, list_month_files, log, resumable_download  # noqa: E402


def fetch(item):
    path, size = item
    dest = os.path.join(CACHE, path.replace("/", "_"))
    if os.path.exists(dest) and (not size or os.path.getsize(dest) == size):
        return path, "cached"
    try:
        resumable_download(HF_FILE.format(path), dest, size,
                           tmp=dest + f".part{os.getpid()}")
        return path, "ok"
    except Exception as e:  # noqa: BLE001
        log(f"  FAILED {path}: {e}")
        return path, "FAILED"


def main():
    os.makedirs(CACHE, exist_ok=True)
    files = [(p, s) for p, s in list_month_files()
             if 2007 <= int(p[5:9]) <= 2026]
    log(f"prefetch: {len(files)} files, {sum(s for _, s in files)/1e9:.2f} GB")
    done = 0
    with ThreadPoolExecutor(max_workers=3) as ex:
        futs = [ex.submit(fetch, it) for it in files]
        for fu in as_completed(futs):
            path, status = fu.result()
            done += 1
            if status == "FAILED" or done % 20 == 0:
                log(f"  prefetch {done}/{len(files)} ({path}: {status})")
    log("prefetch done")


if __name__ == "__main__":
    main()
