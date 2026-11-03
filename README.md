# EzraTube v1

**EzraTube — Ezra Zhao's personal picks.** 600 videos, and not one of them came from YouTube's algorithm or an AI-generated list: every video was recommended by a real person in a Hacker News comment (2007–2026), and every card quotes the recommender's own words with a link to the original comment. Sifted from 261,835 video-linking HN comments with a rules-only pipeline — regex extraction, recommendation-signal scoring, keyword topics — with zero LLM calls. Inspired by Andrej Karpathy's HackerTube, built the cheap way.

🔗 **Live / 在线地址**：https://ezra-zhao.github.io/ezratube/ （GitHub Pages 永久地址，对外分享用这个）

> 数据与版权说明：推荐评论来自 Hacker News 公开评论存档，摘录为原文并附原评论链接；本站只做索引与跳转，**视频版权归原作者与 YouTube 创作者所有**。

给 Ezra 的个人版 HackerTube：不跟 YouTube 算法走，只收录 **Hacker News 历史评论里被真人推荐过的视频**。仿 Karpathy 的 HackerTube（2026-10-31）的做法，但管线按「先规则砍候选、零 LLM 烧量」的省钱版实现。

## 怎么打开

- 正式地址（GitHub Pages，永久）：https://ezra-zhao.github.io/ezratube/
- 公网窗口（serveo 隧道，临时备用，地址每次重开会变）：见交付报告里的当前 URL；重开窗口跑 `bash open-window.sh`（本地服务 8770 + 隧道，与教会系统的 8765 互不干扰）。
- 本地：`cd ~/workspace/ezratube && python3 -m http.server 8770` 后打开 http://127.0.0.1:8770/

## 界面功能

- 页面顶部有置顶专栏「我的频道」：Ezra 自己的两个 YouTube 频道（Ezra-Zhao 主频道、Ezra Daily 每日速报）各一栏、各最新 15 条上传，卡片带平实的「我的频道」小标识；数据来自 YouTube RSS（免 API key），与下方 HN 精选网格分开，不参与主题筛选。文案口径（Ezra 定）：只用平实标识，栏目说明一句话带过，不用自夸字眼
- 仿 YouTube 的深色视频网格：缩略图、标题、频道、主题、一句 HN 推荐语（带评论者与原评论链接）
- 顶部主题分类 chips（约 10–15 类）+ 搜索框（搜标题/频道/推荐语）
- 点击卡片 → 新标签在 YouTube 播放
- 被多人独立推荐的视频带「♥ N 人推荐」角标

## 数据来源与筛法

- **数据源**：Hacker News 全量评论语料（2007–2026），与 Karpathy 同源。两条等价取数路径：
  - **规范路径**：Hugging Face 数据集 `open-index/hacker-news`（Karpathy 博客原文链接的同款数据集，HN 全量条目按月 parquet 分片），由 `pipeline.py scan` 流式扫描。本机网络单流约 1.2 MB/s 且频繁断线，12.4 GB 全量靠断点续传慢慢下（`cache/` 缓存 + `prefetch.py` 预下载；本轮交付时缓存仍在后台补齐中，已下的分片足够随时续扫）。
  - **快速路径（v1 实际采用）**：HN 官方 Algolia 检索 API（`algolia_extract.py`）。两个高精度查询 `youtube.com` / `youtu.be`（抽样精确率 80% / 100%，再用与 scan 相同的正则做精确过滤）。实测坑：Algolia 对单个查询只开放前 1000 条结果（paginationLimitedTo），粗按年份切片会静默丢掉大年份九成数据；脚本用**自适应时间二分**（时间段命中数 >950 就对半切）保证全量抽取，逐年逐查询去重后产出 `matches_algolia.jsonl`（与 scan 同 schema）。两条路径是同一语料的不同取法，快速路径 2026-11-02 实测约 35 分钟抽完全量。
- **管线**（全程零 LLM 调用）：
  1. `scan`（parquet 路径）：流式扫描评论（pyarrow 向量化预过滤：评论类型、未删除/未标记 dead、正文含 `youtu`），正则提取 YouTube 视频 ID（watch / youtu.be / shorts / embed / live 形态），清洗 HTML 后落 `matches.jsonl`。
  2. `build`（`pipeline.py`，读 `matches.jsonl`）：
     - 评论打分：推荐词（recommend/great/excellent/best/must watch…）与谈视频的词加权，长度适中加分；含强负评词且无正评的评论直接剔除；无任何推荐信号的弱评论剔除。
     - 按视频聚合：独立推荐人数（对数加权）+ 最佳评论分 + 年代微调，排序取前 800 个候选（打标候选上限，硬性封顶）。
     - 标题/作者：走 YouTube 官方 oEmbed 接口补齐（无需 API key）；视频已删/私密导致 oEmbed 失败的直接淘汰——顺带保证链接可达。
     - 主题分类：本地关键词规则（标题×2 + 最佳评论全文）在 14 个预设主题上计命中取最高，无命中归「其他精选」。
     - 摘录：从最佳评论原文中截取句子（优先含推荐词的句子；句子含链接时只取链接前部分），严格保证是原文子串、不拼接、不改写、不编造，并标注 HN 评论者与日期、附原评论链接。已全量校验：600/600 条摘录逐字命中源评论。
- **数字**（2026-11-02 本轮实测，与 data.json 的 stats 一致）：Algolia 原始命中约 28.2 万条 → 正则精确过滤后 **261,835 条**带真实视频链接的评论 → 去重 **173,269 个视频**、经评论质量规则过滤后约 17.3 万 → 按聚合分取前 **800** 个候选（打标上限）→ oEmbed 验活（前 720 名，失败即淘汰）→ 最终入选 **600 个视频、15 个主题**（AI 与机器学习 41 / 编程与软件工程 138 / 计算机科学 19 / 创业与商业 34 / 科学与太空 103 / 数学 11 / 历史与社会 14 / 音乐 13 / 影视与动画 20 / 游戏 34 / 设计与艺术 27 / 心理与思维 13 / 生活与技能 3 / 演讲与访谈 30 / 其他精选 100）。
- **质量门实测**（2026-11-02）：抽查 10 个视频链接全部 200 可达、缩略图全部 200、标题取自 YouTube 官方 oEmbed 元数据（与视频天然一致）、600 条摘录全量逐字对源通过、另抽 3 条与 HN 官方 API 实时记录对版（评论者、内容一致）。

## 如何重跑管线

```bash
cd ~/workspace/ezratube
# 快速路径（推荐，约 35 分钟）：Algolia 抽取 → 覆盖 matches.jsonl → build
./.venv/bin/python algolia_extract.py
cp matches_algolia.jsonl matches.jsonl
./.venv/bin/python pipeline.py build    # 打分→聚合→oEmbed→分类→ data.json
# 规范路径（parquet 全量，慢）：下载(带缓存+断点续传)→扫描→build
./.venv/bin/python pipeline.py scan     # → matches.jsonl，已下分片会跳过
./.venv/bin/python pipeline.py build
# 或一把梭： ./.venv/bin/python pipeline.py all
# 只刷新自家频道（不重跑主站管线）： ./.venv/bin/python mychannel.py
```

- 参数：`--year-from/--year-to` 限定年份，`--max-candidates` 候选上限（默认 800），`--top-n` 做 oEmbed 的前 N 名（默认 720），`--max-final` 最终入选上限（默认 600）。
- `cache/` 是 parquet 分片缓存（约 12 GB），删掉不影响站点运行，只会让下次 scan 重新下载。
- `prefetch.py` 是可选的并行预下载器（3 路并发 + 断点续传），大批量重跑前先跑它更快。

## 已知限制（v1）

- 缩略图用 YouTube 官方图（hqdefault），仍是 UP 主上传的封面——Karpathy 的「视频 50% 处真实帧」反标题党手法未做（见 v2），避免为缩略图批量下载视频。
- 主题分类是关键词规则，会有错分（尤其跨界视频只归一个主类）；没有用 LLM 打标，是刻意的成本取舍。
- 评论摘录以英文为主（HN 社区语言），界面是中文，但内容不做翻译。
- 排序信号只用评论本身（推荐词、独立推荐人数、年代），没用 HN 故事分数（评论行里没有现成的父故事分数，v1 未做回查）。
- 数据是 HN 全量存档的静态快照，不含数据集导出之后的最新评论；增量更新靠重跑管线。
- 公网窗口走 serveo 免费隧道：首次打开有英文提示页需点 Continue；隧道地址每次重开都会变；VM 重启后窗口会断，跑 `open-window.sh` 恢复。

## v2 清单

1. **中段帧缩略图**：对入选视频抽 50% 处真实帧替换官方封面（Karpathy 原手法，反标题党）。
2. **接代学管线**：卡片加「让豆豆代看」入口，选中视频走现有转写→提炼稿→「开车听」音频管线，前端负责选片、后端负责代学交付。
3. **增源**：自家两个 YouTube 频道已接入（2026-11-03，「我的频道」置顶专栏，`mychannel.py`）；待续：Ezra 订阅的 YouTube 频道、X 书签、代学线已处理过的讲座清单，并入同一前端（多源信号合并排序）。
4. **LLM 打标复核**：对规则分类的低置信样本，用 review_pool 免费跨模型通道小批量复核（仍守候选上限，不大规模烧）。
5. **父故事分数回查**：用评论的 parent 链回查故事 score，给排序加权。
6. **ForYou 个性化**：仿原版，本地会话存观看/收藏状态做简单重排（无账号）。

## 文件清单

- `index.html` — 前端（纯静态，无构建）
- `data.json` — 站点数据（管线产出；含 `own_channels` 自家频道字段）
- `pipeline.py` — 管线（scan/build/all；build 结束自动附加自家频道）
- `mychannel.py` — 自家频道取数：抓两个频道（Ezra-Zhao `UCOpe9VOkkdmuWcKNuA-M4vQ`、Ezra Daily 每日速报 `UCsxU-VYZH9hMw1HVaPaS6KA`）的 YouTube RSS 并入 data.json；可单独运行刷新，单频道失败保留旧条目
- `algolia_extract.py` — Algolia 快速取数（自适应时间二分；状态存 `algolia_state.json`，可断点续跑）
- `prefetch.py` — 并行预下载（可选）
- `open-window.sh` — 起本地服务 + serveo 公网窗口（端口 8770）
- `matches.jsonl` — build 的输入（本轮 = Algolia 快速路径产物副本）
- `matches_algolia.jsonl` — Algolia 抽取的候选评论集（261,835 条）
- `matches_parquet_partial.jsonl` — parquet 路径的部分成果存档（2006–2011 年段 6,738 条，scan 中断时保存）
- `oembed_cache.json` — oEmbed 结果缓存（重跑 build 时免重复请求）
- `cache/` — parquet 分片缓存（可删）
