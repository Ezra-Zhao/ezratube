#!/usr/bin/env python3
"""EzraTube 自家频道源：抓 Ezra 自己的两个 YouTube 频道官方 RSS（免 API key），
把各频道最新视频并入 data.json 的 own_channels 字段，供前端「我的频道」
置顶专栏渲染（两频道各占一栏）。

频道（Ezra 2026-11-03 13:40 确认共 2 个）：
- Ezra-Zhao（主频道）        UCOpe9VOkkdmuWcKNuA-M4vQ
- Ezra Daily 每日速报        UCsxU-VYZH9hMw1HVaPaS6KA（@EzraDaily777；
  ID 出处：yt-split/daily-schedule.json 台账明文 + 官方 RSS 回读频道名一致）

与 algolia_extract.py 同级：独立取数脚本，产物并入同一份 data.json，不另起
数据格式。单独运行可刷新自家频道部分；pipeline.py build 结束时也会自动调用
attach() 附加。单个频道抓取失败时保留 data.json 里的旧条目，不拖垮其他频道。

用法: ./.venv/bin/python mychannel.py
"""
import json
import os
import sys
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

BASE = os.path.dirname(os.path.abspath(__file__))
DATA_JSON = os.path.join(BASE, "data.json")

CHANNELS = [
    {"channel_id": "UCOpe9VOkkdmuWcKNuA-M4vQ", "note": "主频道"},
    {"channel_id": "UCsxU-VYZH9hMw1HVaPaS6KA", "note": "每日速报频道 · @EzraDaily777"},
]
MAX_VIDEOS = 15  # 官方 RSS 默认返回最新 15 条

NS = {
    "a": "http://www.w3.org/2005/Atom",
    "yt": "http://www.youtube.com/xml/schemas/2015",
    "m": "http://search.yahoo.com/mrss/",
}


def rss_url(channel_id):
    return f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"


def fetch_channel(channel_id, note=""):
    url = rss_url(channel_id)
    req = urllib.request.Request(url, headers={"User-Agent": "EzraTube/1.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        root = ET.fromstring(r.read())
    name = root.findtext("a:title", default="", namespaces=NS)
    videos = []
    for e in root.findall("a:entry", NS):
        vid = e.findtext("yt:videoId", namespaces=NS)
        if not vid:
            continue
        thumb_el = e.find(".//m:thumbnail", NS)
        thumb = (thumb_el.get("url") if thumb_el is not None
                 else f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg")
        published = e.findtext("a:published", default="", namespaces=NS)
        videos.append({
            "id": vid,
            "title": e.findtext("a:title", default="", namespaces=NS),
            "url": f"https://www.youtube.com/watch?v={vid}",
            "thumb": thumb,
            "published": published[:10],  # YYYY-MM-DD (UTC)
        })
    if not videos:
        raise RuntimeError(f"RSS returned no videos for {channel_id}")
    return {
        "name": name,
        "channel_id": channel_id,
        "channel_url": f"https://www.youtube.com/channel/{channel_id}",
        "feed_url": url,
        "note": note,
        "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "videos": videos[:MAX_VIDEOS],
    }


def attach(path=DATA_JSON):
    """把 own_channels 并入 data.json（其余字段原样保留），返回频道 dict 列表。"""
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    old = {c["channel_id"]: c for c in data.get("own_channels", [])}
    if "own_channel" in data:  # 迁移 v1 的单频道字段
        oc = data.pop("own_channel")
        old.setdefault(oc["channel_id"], oc)
    out = []
    for cfg in CHANNELS:
        cid = cfg["channel_id"]
        try:
            out.append(fetch_channel(cid, cfg.get("note", "")))
        except Exception:
            if cid in old:
                out.append(old[cid])
            else:
                raise
    data["own_channels"] = out
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=1)
    return out


def main():
    chans = attach()
    for oc in chans:
        print(f"own channel '{oc['name']}' ({oc['channel_id']}): "
              f"{len(oc['videos'])} videos -> {DATA_JSON}")
        for v in oc["videos"][:3]:
            print(f"  {v['published']}  {v['title']}  {v['url']}")
        if len(oc["videos"]) > 3:
            print(f"  ... 共 {len(oc['videos'])} 条")
    return 0


if __name__ == "__main__":
    sys.exit(main())
