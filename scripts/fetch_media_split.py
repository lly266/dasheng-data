#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""大圣分媒体消耗拉取（api/2617 · 短剧×日×媒体）

用途：为 drama_roi.html「跨媒体选剧」视图准备分媒体消耗数据，支持自选日期时段。

接口事实（由 market.wuread.cn.har 实测确认）：
  POST https://market.wuread.cn/market-admin/adminNew/api/2617
    ?limit=&offset=&sort=consume&order=desc&pageNo=N&pageSize=1000
  body: pline_form=cltplay, data_merge=true, sdate/edate, media=1|2
  * 媒体由 body.media 决定，返回行的 media_name 恒为 null —— 必须按媒体分别请求，
    再由本脚本回填 media_name，前端才能按「头条 / 广点通」归类
  * 返回行粒度为 (book_id × cdate)，跨日窗口内行级 pre_roi 正常有值（无需按天拆分）
  * media=1 → 广点通（腾讯），media=2 → 头条
    依据：HAR 两组实测付费成本/退订率 112~155元/22~38% 与 49~60元/48~56%，
    分别对应 2611 全媒体数据里的「广点通 101元/34%」与「头条 69元/47%」

特性：Cookie(shiroCookie) 鉴权、失败重试 3 次、按窗口缓存、输出与 latest.json 同构

用法：
  python scripts/fetch_media_split.py                      # 默认近 7 天
  python scripts/fetch_media_split.py --sdate 2026-11-12 --edate 2026-11-18
  python scripts/fetch_media_split.py --media 2 --refresh   # 只拉头条且忽略缓存
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)  # verify=False 会刷屏告警

sys.path.insert(0, str(Path(__file__).resolve().parent))
import sync as S  # 复用字段裁剪与字典编码，保证输出与 data/latest.json 同构

API_2617 = "https://market.wuread.cn/market-admin/adminNew/api/2617"
CACHE_DIR = Path(".cache") / "2617"
OUT_FILE = Path("data") / "media_split.json"
DAYS_DEFAULT = 7
RETRY_DELAYS = (2, 5, 10)
MAX_PAGES = 200
PAGE_SIZE_DEFAULT = 1000

# 实测映射：media 参数值 → 媒体名（前端按 media_name 匹配 头条 / 广点通|腾讯）
MEDIA_MAP = {"2": "头条", "1": "广点通"}


def make_session(token: str) -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "Authorization": token,
        "Content-Type": "application/json;charset=UTF-8",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Origin": "https://market.wuread.cn",
        "Referer": "https://market.wuread.cn/newdz/home#/newdz/clientdashboard",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/146.0.0.0 Safari/537.36"
        ),
    })
    session.cookies.set("shiroCookie", token, domain="market.wuread.cn")
    return session


def build_payload(sdate: str, edate: str, media_code: str) -> str:
    """与前端页面实际发出的请求体一致（抓包含 book_age_type、无 is_new_book）。"""
    body = {
        "book_id": "", "agent_user_name": "", "pline_form": "cltplay", "app_name": "",
        "is_anime": "", "book_age_type": "", "is_today_up": "",
        "sdate": sdate, "edate": edate, "data_merge": True,
        "media": media_code, "consume_min": 0, "consume_max": 0,
        "os_type": "", "placement_mode": "", "article_type": "",
        "n_recharge_uv_min": "", "n_recharge_uv_max": "",
        "unsubscribe_rate_min": "", "unsubscribe_rate_max": "",
        "recharge_rate_min": "", "recharge_rate_max": "",
        "recharge_cost_min": "", "recharge_cost_max": "",
    }
    return json.dumps(body, ensure_ascii=False, separators=(",", ":"))


def post_with_retry(session: requests.Session, url: str, body: str, label: str):
    """失败按 2/5/10s 退避重试；全部失败返回 None（由调用方跳过该媒体）。"""
    last_err = "unknown"
    for attempt in range(len(RETRY_DELAYS) + 1):
        if attempt:
            time.sleep(RETRY_DELAYS[attempt - 1])
        try:
            resp = session.post(url, data=body.encode("utf-8"), timeout=120, verify=False)
            resp.raise_for_status()
            payload = resp.json()
            if "data" in payload or payload.get("code") in (0, 200, "0", "200"):
                return payload
            last_err = f"code={payload.get('code')} msg={payload.get('message') or payload.get('msg')}"
        except Exception as exc:  # noqa: BLE001 - 网络/JSON 异常一律重试
            last_err = repr(exc)
        print(f"      {label}: 第 {attempt + 1} 次请求失败（{last_err}），准备重试…", flush=True)
    print(f"      {label}: 重试耗尽，跳过（{last_err}）", flush=True)
    return None


def fetch_media(session: requests.Session, sdate: str, edate: str, media_code: str,
                page_size: int) -> list:
    """分页拉取某媒体在 [sdate, edate] 窗口内的 (短剧 × 日) 行，并回填 media_name。"""
    media_name = MEDIA_MAP.get(media_code, f"media{media_code}")
    body = build_payload(sdate, edate, media_code)
    rows, page = [], 1
    while page <= MAX_PAGES:
        url = (f"{API_2617}?limit=&offset=&sort=consume&order=desc"
               f"&pageNo={page}&pageSize={page_size}")
        payload = post_with_retry(session, url, body, f"{media_name} p{page}")
        if payload is None:
            break
        batch = payload.get("data") or []
        if isinstance(batch, dict):
            batch = batch.get("list") or batch.get("data") or []
        if not isinstance(batch, list) or not batch:
            break
        for row in batch:
            row["media_name"] = media_name          # ★ 后端不填，必须由抓取侧注入
        rows.extend(batch)
        print(f"      {media_name} p{page}: {len(batch)} 行（累计 {len(rows)}）", flush=True)
        if len(batch) < page_size:
            break
        page += 1
        time.sleep(0.3)
    return rows


def parse_date_range(args) -> tuple:
    cn_tz = timezone(timedelta(hours=8))
    today = datetime.now(cn_tz).date()
    sdate = (args.sdate or os.environ.get("SDATE") or "").strip()
    edate = (args.edate or os.environ.get("EDATE") or "").strip()
    try:
        end = datetime.strptime(edate, "%Y-%m-%d").date() if edate else today
        start = (datetime.strptime(sdate, "%Y-%m-%d").date() if sdate
                 else end - timedelta(days=max(1, args.days) - 1))
    except ValueError:
        print(f"[ERROR] 日期格式必须是 YYYY-MM-DD（收到 sdate={sdate!r} edate={edate!r}）")
        sys.exit(2)
    if start > end:
        print(f"[ERROR] 起始日期 {start} 晚于结束日期 {end}")
        sys.exit(2)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def main() -> int:
    ap = argparse.ArgumentParser(description="大圣分媒体消耗拉取（api/2617）")
    ap.add_argument("--sdate", help="起始日期 YYYY-MM-DD")
    ap.add_argument("--edate", help="结束日期 YYYY-MM-DD")
    ap.add_argument("--days", type=int, default=DAYS_DEFAULT, help="未给日期时取最近 N 天（含今天）")
    ap.add_argument("--media", choices=["1", "2", "all"], default="all",
                    help="1=广点通(腾讯) 2=头条 all=两者都拉（默认）")
    ap.add_argument("--refresh", action="store_true", help="忽略窗口缓存，强制重拉")
    ap.add_argument("--page-size", type=int, default=PAGE_SIZE_DEFAULT)
    ap.add_argument("--out", default=str(OUT_FILE))
    args = ap.parse_args()

    token = os.environ.get("DASHENG_TOKEN", "").strip()
    if not token:
        print("[ERROR] 未设置 DASHENG_TOKEN（大圣 shiroCookie）。")
        print("  本地：$env:DASHENG_TOKEN='...'；线上：GitHub repo → Settings → Secrets → Actions")
        return 1

    sdate, edate = parse_date_range(args)
    media_codes = ["2", "1"] if args.media == "all" else [args.media]
    print("=" * 60)
    print("  大圣分媒体消耗拉取（api/2617）")
    print(f"  日期窗口：{sdate} ~ {edate}")
    print(f"  媒体：{', '.join(f'{c}={MEDIA_MAP.get(c, c)}' for c in media_codes)}")
    print("=" * 60, flush=True)

    session = make_session(token)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    raw, cache_hit, failed = [], [], []
    for code in media_codes:
        cache_file = CACHE_DIR / f"{sdate}_{edate}_m{code}.json"
        rows = []
        if not args.refresh and cache_file.exists() and cache_file.stat().st_size > 2:
            try:
                rows = json.loads(cache_file.read_text(encoding="utf-8"))
                cache_hit.append(code)
                print(f"[缓存] {MEDIA_MAP.get(code, code)}: {len(rows)} 行", flush=True)
            except Exception:
                rows = []
        if not rows:
            rows = fetch_media(session, sdate, edate, code, args.page_size)
            if rows:
                cache_file.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")
            else:
                failed.append(MEDIA_MAP.get(code, code))
        raw.extend(rows)

    if not raw:
        print("[ERROR] 两个媒体都没有取到数据，Token 可能已过期。")
        print("  1. 登录 https://market.wuread.cn/market-admin/")
        print("  2. F12 → Application → Cookies → 复制 shiroCookie → 更新 DASHENG_TOKEN")
        return 1

    filtered = S.filter_records(raw)
    if not filtered:
        print("[ERROR] 有原始行但全部 consume<=0，无可输出数据。")
        return 1

    json_str = S.encode_dict(filtered)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json_str, encoding="utf-8")

    by_media = {}
    for row in filtered:
        media = str(row.get("media_name") or "(未填写)")
        try:
            consume = float(row.get("consume") or 0)
        except (TypeError, ValueError):
            consume = 0.0
        by_media[media] = by_media.get(media, 0.0) + consume

    size_mb = len(json_str.encode("utf-8")) / 1048576
    print("-" * 60)
    print(f"输出：{out_path}（{size_mb:.1f} MB，{len(filtered)} 行）")
    for media, consume in sorted(by_media.items(), key=lambda kv: -kv[1]):
        print(f"  媒体 {media}: 消耗 {consume:,.2f}")
    if cache_hit:
        print(f"  窗口缓存命中：{', '.join(MEDIA_MAP.get(c, c) for c in cache_hit)}")
    if failed:
        print(f"  [WARN] 以下媒体拉取失败：{failed}")
    print("完成。把该文件地址填进看板「数据源」即可查看「跨媒体选剧」视图。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
