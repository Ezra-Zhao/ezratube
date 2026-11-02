#!/usr/bin/env python3
"""EzraTube 数据管线（仿 Karpathy HackerTube 的筛法，省 token 版）。

数据源: Hugging Face `open-index/hacker-news`（HN 全量条目按月 parquet 分片，
        即 Karpathy 博客链接的同款数据集）。
阶段:
  scan     下载月度 parquet（带缓存）→ 流式扫描评论 → 规则过滤出含 YouTube
           链接的推荐评论 → matches.jsonl
  build    给评论打分 → 按视频聚合 → 取候选（默认上限 800）→ YouTube oEmbed
           补标题/作者（无 API key）→ 本地规则分主题 → data.json
  all      scan + build

全程零 LLM 调用：主题分类用本地关键词规则（v1 口径，见 README）。
可重跑：scan 有缓存会跳过已下载分片；build 读 matches.jsonl 重算。
"""
import argparse
import html
import json
import math
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import requests

BASE = os.path.dirname(os.path.abspath(__file__))
CACHE = os.path.join(BASE, "cache")
MATCHES = os.path.join(BASE, "matches.jsonl")
DATA_JSON = os.path.join(BASE, "data.json")
HF_API = "https://huggingface.co/api/datasets/open-index/hacker-news?blobs=true"
HF_FILE = "https://huggingface.co/datasets/open-index/hacker-news/resolve/main/{}"

MONTH_RE = re.compile(r"^data/(\d{4})/(\d{4}-\d{2})\.parquet$")
YT_RE = re.compile(
    r"(?:youtube\.com/(?:watch\?[^\s\"'<>]*v=|shorts/|embed/|v/|live/)|youtu\.be/)"
    r"([A-Za-z0-9_-]{11})"
)
TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")

POS_WORDS = [
    "recommend", "great", "excellent", "amazing", "fantastic", "superb",
    "best", "favorite", "favourite", "must watch", "worth watch", "worth it",
    "incredible", "brilliant", "awesome", "love this", "loved", "enjoyed",
    "gem", "classic", "fascinating", "outstanding", "wonderful", "superb",
    "insightful", "compelling", "riveting", "masterpiece", "phenomenal",
    "entertaining", "hilarious", "beautiful", "impressive", "solid talk",
    "great talk", "great video", "good talk", "nice talk", "interesting",
]
NEG_WORDS = [
    "boring", "terrible", "awful", "worst", "waste of time", "don't watch",
    "dont watch", "do not watch", "overrated", "clickbait", "misleading",
    "horrible", "dreadful", "painful to watch", "skip this", "not worth",
]
TALK_WORDS = ["video", "talk", "lecture", "watch", "presentation", "keynote",
              "documentary", "interview", "course", "tutorial"]

# 主题关键词规则（在 标题×2 + 评论全文 小写文本上计命中数，取最高者）
TOPICS = [
    ("ai", "AI 与机器学习", ["artificial intelligence", "machine learning", "deep learning",
        "neural network", "neural net", " llm", "gpt", "transformer", "chatgpt",
        "openai", "anthropic", "language model", "reinforcement learning",
        "computer vision", "tensorflow", "pytorch", "karpathy", "agi", " ai ",
        "data science", "chatbot", "diffusion", "nlp", "backpropagation", "dataset",
        "intelligent agent", "robotics"]),
    ("programming", "编程与软件工程", ["programming", "programmer", "python", "javascript",
        "typescript", "rust ", "golang", "compiler", "debugging", "code review",
        "software engineer", "refactor", "open source", "github", "functional programming",
        "object-oriented", "web development", "coding", "developer", "software",
        "framework", "emacs", "vim", "lisp", "haskell", "c++", "assembly",
        "kubernetes", "docker", "devops", " api ", "regex"]),
    ("cs", "计算机科学", ["computer science", "algorithm", "data structure",
        "operating system", "distributed system", "database", "networking",
        "cryptography", "kernel", "tcp", "operating systems", "unix", "linux",
        "encryption", "cpu", "gpu", "semiconductor", "hacking", "security research",
        "internet protocol", "browser engine"]),
    ("startup", "创业与商业", ["startup", "founder", "entrepreneur", "venture",
        "marketing", "management", "business", "leadership", "product management",
        "y combinator", "silicon valley", "ceo", "sales", "investor", "economy",
        "economic", "paul graham", "company culture", "hiring"]),
    ("science", "科学与太空", ["physics", "biology", "chemistry", "quantum", "nasa",
        "space", "astronomy", "evolution", "universe", "relativity", "particle",
        "climate", " neuroscience", "brain", "dna", "gene", "black hole", "planet",
        "health", "medical", "medicine", "nutrition", "cancer", "virus", "cell ",
        "earth", "ocean", "species", "fossil", "telescope", "mars", "rocket",
        "energy", "protein", "organism", "geology", "earthquake", "vaccine", "diet"]),
    ("math", "数学", ["mathematics", "calculus", "algebra", "geometry", "topology",
        "probability", "statistics", "theorem", "number theory", "fractal",
        "fourier", "linear algebra", " math ", "maths", "equation", "infinity",
        "prime number", "graph theory"]),
    ("history", "历史与社会", ["history", "historical", "ancient", "world war",
        "empire", "century", "archaeology", "civilization", "revolution", "cold war",
        "roman", "medieval", "kingdom", "war ii", "wwii", "ww2", "president",
        "soviet", "britain", "empire"]),
    ("music", "音乐", ["music", "guitar", "piano", "jazz", "concert", "song",
        "composer", "orchestra", "drums", "bass guitar", "violin", "live performance",
        "album", "singer", "hip hop", "beethoven", "mozart", "bach", "synth",
        "band ", "musician", "melody"]),
    ("film", "影视与动画", ["film", "movie", "cinema", "animation", "animator",
        "pixar", "documentary film", "director", "stop motion", "vfx", "special effects",
        "hollywood", "disney", "anime", "cartoon", "episode", "actor", "screenplay"]),
    ("games", "游戏", ["game design", "video game", "gaming", "speedrun", "minecraft",
        "nintendo", "game development", "gamedev", "zelda", "mario", "chess",
        "playstation", "xbox", "board game", " game ", "games "]),
    ("design", "设计与艺术", ["design", "typography", " ux", "user experience",
        "user interface", "graphic", "illustration", " architecture", "art history",
        "painting", "font", "logo design", "fashion", "drawing", "sketch", "artist"]),
    ("mind", "心理与思维", ["psychology", "philosophy", "thinking", "cognitive",
        "productivity", "decision", "happiness", "consciousness", "ethics", " stoic",
        "behavior", "habit", "meditation", "religion", "theology", "learning how",
        "memory", "sleep", "philosopher"]),
    ("life", "生活与技能", ["cooking", "woodworking", " diy", "repair", "gardening",
        "photography", "how to build", "craft", "workshop", "electronics project",
        "raspberry pi", "recipe", "cycling", "fitness", "exercise", "travel",
        "camera", "garden", "kitchen", "bicycle"]),
    ("talks", "演讲与访谈", [" ted ", "ted talk", "keynote", "interview",
        "lecture", "talk by", "speech", "fireside", "conference talk",
        "presentation by", "commencement", "podcast"]),
]
OTHER = ("other", "其他精选")


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def clean_text(raw):
    t = html.unescape(raw or "")
    t = TAG_RE.sub(" ", t)
    return WS_RE.sub(" ", t).strip()


# ---------------------------------------------------------------- scan
def list_month_files():
    r = requests.get(HF_API, timeout=60)
    r.raise_for_status()
    files = []
    for s in r.json().get("siblings", []):
        m = MONTH_RE.match(s.get("rfilename", ""))
        if m:
            files.append((s["rfilename"], s.get("size") or 0))
    files.sort()
    return files


def resumable_download(url, dest, size=0, tmp=None):
    """带 Range 断点续传的下载：连接中断时从已下字节处续，不从零重来。"""
    if os.path.exists(dest) and (not size or os.path.getsize(dest) == size):
        return dest
    tmp = tmp or (dest + ".part")
    for attempt in range(12):
        try:
            have = os.path.getsize(tmp) if os.path.exists(tmp) else 0
            if size and have >= size:
                break
            headers = {"Range": f"bytes={have}-"} if have else {}
            with requests.get(url, stream=True, timeout=180, headers=headers) as r:
                if have and r.status_code == 200:
                    have = 0  # 服务器不支持 Range，重下
                r.raise_for_status()
                mode = "ab" if have and r.status_code == 206 else "wb"
                with open(tmp, mode) as f:
                    for chunk in r.iter_content(1 << 20):
                        f.write(chunk)
            if not size or os.path.getsize(tmp) == size:
                break
        except Exception as e:  # noqa: BLE001
            log(f"  resume retry {attempt+1} {os.path.basename(dest)}: {e}")
            time.sleep(2)
    if os.path.exists(tmp) and (not size or os.path.getsize(tmp) == size):
        os.rename(tmp, dest)
        return dest
    raise RuntimeError(f"download incomplete: {dest}")


def download(path, size):
    dest = os.path.join(CACHE, path.replace("/", "_"))
    return resumable_download(HF_FILE.format(path), dest, size)


def scan(year_from, year_to):
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    os.makedirs(CACHE, exist_ok=True)
    files = [(p, s) for p, s in list_month_files()
             if year_from <= int(p[5:9]) <= year_to]
    log(f"scan: {len(files)} monthly files, "
        f"{sum(s for _, s in files)/1e9:.2f} GB total")
    total_rows = 0
    kept = 0
    with open(MATCHES, "w", encoding="utf-8") as out:
        for i, (path, size) in enumerate(files, 1):
            local = download(path, size)
            pf = pq.ParquetFile(local)
            n_file = 0
            for batch in pf.iter_batches(
                batch_size=65536,
                columns=["id", "type", "by", "time", "text", "deleted", "dead"],
            ):
                total_rows += batch.num_rows
                # 向量化预过滤（C++ 层）：评论类型、未删/未死、正文含 youtu
                mask = pc.and_(
                    pc.and_(
                        pc.equal(batch.column("type"), 2),
                        pc.equal(batch.column("deleted"), 0)),
                    pc.and_(
                        pc.equal(batch.column("dead"), 0),
                        pc.fill_null(
                            pc.match_substring(
                                pc.if_else(
                                    pc.is_null(batch.column("text")),
                                    "", batch.column("text")),
                                "youtu"),
                            False)),
                )
                hit = batch.filter(mask)
                for row in hit.to_pylist():
                    text = row["text"] or ""
                    vids = list(dict.fromkeys(YT_RE.findall(html.unescape(text))))
                    if not vids:
                        continue
                    ts = row["time"]
                    ts = int(ts.timestamp()) if hasattr(ts, "timestamp") else int(ts)
                    out.write(json.dumps({
                        "hn_id": row["id"], "by": row["by"], "time": ts,
                        "videos": vids, "text": clean_text(text),
                    }, ensure_ascii=False) + "\n")
                    n_file += 1
                    kept += 1
            log(f"  ({i}/{len(files)}) {path}: +{n_file} comments "
                f"(cum rows={total_rows/1e6:.1f}M, kept={kept})")
    log(f"scan done: rows={total_rows}, youtube-comments={kept} -> {MATCHES}")
    return kept


# ---------------------------------------------------------------- build
def comment_score(text):
    low = " " + text.lower() + " "
    pos = sum(1 for w in POS_WORDS if w in low)
    talk = sum(1 for w in TALK_WORDS if w in low)
    neg = sum(1 for w in NEG_WORDS if w in low)
    n = len(text)
    len_score = 2.0 if 120 <= n <= 700 else (1.0 if 60 <= n < 120 or 700 < n <= 1200 else 0.2)
    return pos * 2.0 + talk * 0.7 + len_score, pos, neg


def make_excerpt(text, limit=170):
    """从评论原文里截一段摘录，严格保证是原文子串（只做空白归一化）：
    优先取不含链接的、含推荐词的句子；句子含链接时只取链接之前的部分；
    都不行则取开头一段（在链接出现处截断）。"""
    t = WS_RE.sub(" ", text).strip()
    url_at = None
    m = YT_RE.search(t)
    if m:
        # 找到链接在原文中的大致位置（按域名出现处）
        for token in ("youtube.com", "youtu.be"):
            p = t.find(token)
            if p != -1 and (url_at is None or p < url_at):
                url_at = p
    sentences = re.split(r"(?<=[.!?])\s+", t)
    best, best_score = None, -1
    for s in sentences:
        s = s.strip()
        if "youtube.com" in s or "youtu.be" in s:
            cut = min([p for p in (s.find("youtube.com"), s.find("youtu.be"))
                       if p != -1])
            s = s[:cut].rstrip(" -–—:>(").strip()
        if len(s) < 25:
            continue
        sc = sum(1 for w in POS_WORDS if w in " " + s.lower() + " ")
        if sc > best_score and len(s) <= limit:
            best, best_score = s, sc
    if best:
        return best
    head = t if url_at is None else t[:url_at].rstrip(" -–—:>(")
    if len(head) <= limit:
        return head.strip()
    return head[:limit].rsplit(" ", 1)[0].rstrip(",;:") + "…"


def classify(title, full_text):
    hay = " " + (title + " " + title + " " + full_text).lower() + " "
    best_id, best_name, best_hits = OTHER[0], OTHER[1], 0
    for tid, tname, kws in TOPICS:
        hits = sum(1 for k in kws if k in hay)
        if hits > best_hits:
            best_id, best_name, best_hits = tid, tname, hits
    return best_id, best_name


def oembed(vid):
    try:
        r = requests.get(
            "https://www.youtube.com/oembed",
            params={"url": f"https://www.youtube.com/watch?v={vid}", "format": "json"},
            timeout=25,
        )
        if r.status_code != 200:
            return None
        d = r.json()
        return {"title": d.get("title", "").strip(),
                "author": d.get("author_name", "").strip()}
    except Exception:  # noqa: BLE001
        return None


def build(max_candidates, top_n, max_final=600):
    comments = []
    with open(MATCHES, encoding="utf-8") as f:
        for line in f:
            comments.append(json.loads(line))
    log(f"build: {len(comments)} youtube-link comments loaded")

    videos = {}
    for c in comments:
        score, pos, neg = comment_score(c["text"])
        if neg >= 1 and pos == 0:
            continue  # 纯负评不收
        if pos == 0 and score < 1.5:
            continue  # 没有任何推荐信号的弱评论不收
        for vid in c["videos"]:
            v = videos.setdefault(vid, {"recommenders": set(), "best": None})
            v["recommenders"].add(c["by"])
            if v["best"] is None or score > v["best"][0]:
                v["best"] = (score, c)
    ranked = []
    for vid, v in videos.items():
        n_rec = len(v["recommenders"])
        score, c = v["best"]
        year = datetime.fromtimestamp(c["time"], tz=timezone.utc).year
        recency = max(0, year - 2008) * 0.06
        total = score + 3.0 * math.log2(n_rec + 1) + recency
        ranked.append((total, vid, n_rec, c))
    ranked.sort(reverse=True)
    log(f"build: {len(ranked)} distinct videos after rule filter; "
        f"taking top {max_candidates} as labeling candidates")
    candidates = ranked[:max_candidates]

    log(f"build: oEmbed lookup for top {top_n} candidates ...")
    picked = candidates[:top_n]
    cache_path = os.path.join(BASE, "oembed_cache.json")
    ocache = {}
    if os.path.exists(cache_path):
        ocache = json.load(open(cache_path))
    todo = [t for t in picked if t[1] not in ocache]
    if todo:
        with ThreadPoolExecutor(max_workers=8) as ex:
            for t, meta in zip(todo, ex.map(lambda t: oembed(t[1]), todo)):
                ocache[t[1]] = meta
        json.dump(ocache, open(cache_path, "w"))
    metas = [ocache.get(t[1]) for t in picked]

    out_videos = []
    topic_counts = {}
    for (total, vid, n_rec, c), meta in zip(picked, metas):
        if not meta or not meta["title"]:
            continue
        excerpt = make_excerpt(c["text"])
        if len(excerpt) < 15:
            continue
        tid, tname = classify(meta["title"], c["text"])
        topic_counts[tname] = topic_counts.get(tname, 0) + 1
        out_videos.append({
            "id": vid,
            "title": meta["title"],
            "author": meta["author"],
            "url": f"https://www.youtube.com/watch?v={vid}",
            "thumb": f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg",
            "topic": tid,
            "topic_name": tname,
            "excerpt": excerpt,
            "hn_by": c["by"],
            "hn_id": c["hn_id"],
            "hn_url": f"https://news.ycombinator.com/item?id={c['hn_id']}",
            "hn_date": datetime.fromtimestamp(
                c["time"], tz=timezone.utc).strftime("%Y-%m-%d"),
            "n_rec": n_rec,
            "score": round(total, 2),
        })
    out_videos = out_videos[:max_final]
    topic_counts = {}
    for v in out_videos:
        topic_counts[v["topic_name"]] = topic_counts.get(v["topic_name"], 0) + 1
    topics_out = [{"id": tid, "name": tname, "count": topic_counts.get(tname, 0)}
                  for tid, tname, _ in TOPICS]
    topics_out.append({"id": OTHER[0], "name": OTHER[1],
                       "count": topic_counts.get(OTHER[1], 0)})
    topics_out = [t for t in topics_out if t["count"] > 0]
    data = {
        "site": "EzraTube",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "source": "Hacker News comments (open-index/hacker-news on Hugging Face)",
        "stats": {
            "comments_with_youtube": len(comments),
            "distinct_videos_filtered": len(ranked),
            "candidates_labeled": len(candidates),
            "videos_final": len(out_videos),
            "topics": len(topics_out),
        },
        "topics": topics_out,
        "videos": out_videos,
    }
    with open(DATA_JSON, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=1)
    log(f"build done: {len(out_videos)} videos, {len(topics_out)} topics -> {DATA_JSON}")
    for t in topics_out:
        log(f"  topic {t['name']}: {t['count']}")
    return data


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("stage", choices=["scan", "build", "all"])
    ap.add_argument("--year-from", type=int, default=2007)
    ap.add_argument("--year-to", type=int, default=2026)
    ap.add_argument("--max-candidates", type=int, default=800)
    ap.add_argument("--top-n", type=int, default=720)
    ap.add_argument("--max-final", type=int, default=600)
    args = ap.parse_args()
    if args.stage in ("scan", "all"):
        scan(args.year_from, args.year_to)
    if args.stage in ("build", "all"):
        build(args.max_candidates, args.top_n, args.max_final)


if __name__ == "__main__":
    main()
