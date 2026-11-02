#!/usr/bin/env python3
"""EzraTube 快速取数路径：HN 官方 Algolia 检索 API（与 HF 全量集同一 HN 语料）。

关键约束（实测）：Algolia 索引对单个查询只开放前 1000 条结果
（paginationLimitedTo），所以按「查询 × 年份」粗切片会丢掉大年份的绝大部分。
本脚本用自适应时间二分：某时间段 nbHits > 950 就对半切，直到每段可一次取完，
保证全量抽取。两个高精度查询 youtube.com / youtu.be，去重后用与
pipeline.scan 相同的正则精确过滤，产出同 schema 的 matches 文件。
断点续跑：state 记录已完成的（查询, 年份），重跑跳过；seen 集合防重复写。
parquet 全量管线（pipeline.py scan）仍是规范路径，本脚本是等价的快速路径。
"""
import html
import json
import os
import sys
import time
from datetime import datetime, timezone

import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pipeline import YT_RE, clean_text, log  # noqa: E402

BASE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(BASE, "matches_algolia.jsonl")
STATE = os.path.join(BASE, "algolia_state.json")
API = "https://hn.algolia.com/api/v1/search_by_date"
QUERIES = ["youtube.com", "youtu.be"]
YEARS = range(2007, 2027)
LIMIT = 950  # 单段可取上限（索引开放 1000 条，留余量）

SESS = requests.Session()
SEEN = set()
OUT_F = None
KEPT = 0


def fetch(params):
    for attempt in range(5):
        try:
            r = SESS.get(API, params=params, timeout=60)
            if r.status_code in (429, 500, 502, 503, 504):
                raise RuntimeError(f"http {r.status_code}")
            r.raise_for_status()
            return r.json()
        except Exception as e:  # noqa: BLE001
            if attempt == 4:
                raise
            log(f"  retry {attempt+1}: {e}")
            time.sleep(3 * (attempt + 1))


def nb_hits(q, a, b):
    d = fetch({"tags": "comment", "hitsPerPage": 0, "query": q,
               "numericFilters": f"created_at_i>={a},created_at_i<{b}"})
    return d["nbHits"]


def collect(q, a, b, depth=0):
    """抽取 [a,b) 内 query=q 的全部评论（自适应二分保证不撞 1000 条上限）。"""
    global KEPT
    n = nb_hits(q, a, b)
    if n == 0:
        return
    if n > LIMIT and (b - a) > 3600:
        mid = (a + b) // 2
        collect(q, a, mid, depth + 1)
        collect(q, mid, b, depth + 1)
        return
    page = 0
    while True:
        d = fetch({"tags": "comment", "hitsPerPage": 1000, "page": page,
                   "query": q,
                   "numericFilters": f"created_at_i>={a},created_at_i<{b}"})
        for h in d["hits"]:
            oid = int(h["objectID"])
            if oid in SEEN:
                continue
            raw = h.get("comment_text") or ""
            vids = list(dict.fromkeys(YT_RE.findall(html.unescape(raw))))
            if not vids:
                continue
            SEEN.add(oid)
            OUT_F.write(json.dumps({
                "hn_id": oid, "by": h.get("author"),
                "time": h.get("created_at_i"),
                "videos": vids, "text": clean_text(raw),
            }, ensure_ascii=False) + "\n")
            KEPT += 1
        OUT_F.flush()
        if page >= d["nbPages"] - 1 or d["nbPages"] == 0:
            break
        page += 1
        time.sleep(0.1)


def year_range(y):
    a = int(datetime(y, 1, 1, tzinfo=timezone.utc).timestamp())
    b = int(datetime(y + 1, 1, 1, tzinfo=timezone.utc).timestamp())
    return a, b


def main():
    global OUT_F
    state = {"done": []}
    if os.path.exists(STATE):
        state = json.load(open(STATE))
    if os.path.exists(OUT):
        with open(OUT, encoding="utf-8") as f:
            for line in f:
                SEEN.add(json.loads(line)["hn_id"])
    log(f"algolia extract v2 start: {len(SEEN)} already collected, "
        f"done slices: {state['done']}")
    OUT_F = open(OUT, "a", encoding="utf-8")
    for q in QUERIES:
        for y in YEARS:
            key = f"{q}|{y}"
            if key in state["done"]:
                continue
            a, b = year_range(y)
            before = KEPT
            collect(q, a, b)
            state["done"].append(key)
            json.dump(state, open(STATE, "w"))
            log(f"  slice {key}: +{KEPT - before} (kept total {KEPT}, "
                f"seen {len(SEEN)})")
    OUT_F.close()
    log(f"algolia extract v2 done: {len(SEEN)} unique comments -> {OUT}")


if __name__ == "__main__":
    main()
