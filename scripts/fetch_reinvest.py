#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""新媒体短剧「再投/追投」榜单拉取（ztdoc.wuread.cn · reinvest/list）

用途：为 drama_roi.html「跨媒体选剧」视图提供腾讯侧的「首日投放日期」（firstTfTime）
与榜单消耗/收入，用于和头条侧跑量情况做交叉判断。

接口事实（由 market.wuread.cn.har 实测确认）：
  GET https://ztdoc.wuread.cn/api/data/zt/reinvest/list
      ?source=tencent&page=1&pageSize=200
      &startDate=YYYY-MM-DD&endDate=YYYY-MM-DD
      &sortField=totalIncome&sortOrder=desc
      [&businessLines=新媒体短剧-付费]
  * 无鉴权，仅需 Referer: https://ztdoc.wuread.cn/reinvest
  * source=tencent 即「只看腾讯媒体」；startDate/endDate 过滤的是**首日投放日期**窗口
    实测：11-16~11-18 窗口 total=114；11-09~11-18 total=581
  * 响应：{total, page, pageSize, source, list:[{bookId, bookName, firstTfTime, ...}]}

用法：
  python scripts/fetch_reinvest.py                    # 近 7 天首投窗口
  python scripts/fetch_reinvest.py --sdate 2026-11-09 --edate 2026-11-18
  python scripts/fetch_reinvest.py --no-business-lines  # 不限业务线（total 更大）
"""

import argparse
import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

BASE_URL = "https://ztdoc.wuread.cn/api/data/zt/reinvest/list"
REFERER = "https://ztdoc.wuread.cn/reinvest"
OUT_FILE = Path("data") / "reinvest_tencent.json"
DAYS_DEFAULT = 7
PAGE_SIZE = 200
MAX_PAGES = 60
RETRY_DELAYS = (2, 5, 10)
BUSINESS_LINES = "新媒体短剧-付费"

# 前端用到的字段（其余丢弃，控制文件体积）
KEEP_FIELDS = (
    "bookId", "bookName", "firstTfTime", "totalConsume", "totalIncome",
    "zitouConsume", "zitouIncome", "zitouRoi", "fenxiaoIncome", "fenxiaoIncomeRatio",
    "consume14d", "consume30d", "income30d", "reinvestCategory", "isAnime",
    "comicType", "bookType", "cashMoney", "estdMoney",
)


def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Referer": REFERER,
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/146.0.0.0 Safari/537.36"
        ),
    })
    return session


def get_page(session: requests.Session, params: dict, page: int):
    """单页 GET，失败按 2/5/10s 退避重试；全失败返回 None。"""
    last_err = "unknown"
    query = dict(params, page=page, pageSize=PAGE_SIZE)
    for attempt in range(len(RETRY_DELAYS) + 1):
        if attempt:
            time.sleep(RETRY_DELAYS[attempt - 1])
        try:
            resp = session.get(BASE_URL, params=query, timeout=60)
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:  # noqa: BLE001
            last_err = repr(exc)
            print(f"      page{page}: 第 {attempt + 1} 次失败（{last_err}），准备重试…", flush=True)
    print(f"      page{page}: 重试耗尽，跳过（{last_err}）", flush=True)
    return None


def parse_date_range(args) -> tuple:
    cn_tz = timezone(timedelta(hours=8))
    today = datetime.now(cn_tz).date()
    sdate = (args.sdate or "").strip()
    edate = (args.edate or "").strip()
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
    ap = argparse.ArgumentParser(description="新媒体短剧榜单拉取（reinvest/list · 腾讯媒体）")
    ap.add_argument("--sdate", help="首日投放起始日期 YYYY-MM-DD")
    ap.add_argument("--edate", help="首日投放结束日期 YYYY-MM-DD")
    ap.add_argument("--days", type=int, default=DAYS_DEFAULT, help="未给日期时取最近 N 天")
    ap.add_argument("--all", action="store_true", help="拉全量榜单（不传 startDate/endDate，覆盖所有首投日期）")
    ap.add_argument("--no-business-lines", action="store_true", help="不限业务线（默认取「新媒体短剧-付费」）")
    ap.add_argument("--out", default=str(OUT_FILE))
    args = ap.parse_args()

    if args.all:
        sdate, edate = "", ""
        params = {
            "source": "tencent",                     # 只看腾讯媒体
            "sortField": "totalIncome",
            "sortOrder": "desc",
        }
    else:
        sdate, edate = parse_date_range(args)
        params = {
            "source": "tencent",                     # 只看腾讯媒体
            "startDate": sdate,
            "endDate": edate,
            "sortField": "totalIncome",
            "sortOrder": "desc",
        }
    if not args.no_business_lines:
        params["businessLines"] = BUSINESS_LINES

    print("=" * 60)
    print("  新媒体短剧榜单拉取（reinvest/list · source=tencent）")
    print(f"  首日投放窗口：{'全量（不限首投日期）' if args.all else sdate + ' ~ ' + edate}")
    print(f"  业务线：{'不限' if args.no_business_lines else BUSINESS_LINES}")
    print("=" * 60, flush=True)

    session = make_session()
    rows, total, page = [], 0, 1
    while page <= MAX_PAGES:
        payload = get_page(session, params, page)
        if not payload:
            break
        batch = payload.get("list") or []
        total = int(payload.get("total") or 0)
        if not batch:
            break
        rows.extend(batch)
        print(f"      page{page}: {len(batch)} 条（累计 {len(rows)}/{total}）", flush=True)
        if total and len(rows) >= total:
            break
        page += 1
        time.sleep(0.3)

    if not rows:
        print("[ERROR] 未取到任何榜单数据。")
        return 1

    slim = []
    for item in rows:
        row = {k: item.get(k) for k in KEEP_FIELDS if k in item}
        if row.get("bookId"):
            slim.append(row)

    cn_tz = timezone(timedelta(hours=8))
    out = {
        "fetched_at": datetime.now(cn_tz).strftime("%Y-%m-%dT%H:%M:%S+08:00"),
        "source": "tencent",
        "business_lines": "" if args.no_business_lines else BUSINESS_LINES,
        "sdate": sdate,
        "edate": edate,
        "total": total or len(slim),
        "count": len(slim),
        "rows": slim,
    }
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")

    size_kb = out_path.stat().st_size / 1024
    first_tf = {}
    for row in slim:
        ft = row.get("firstTfTime") or ""
        first_tf[ft] = first_tf.get(ft, 0) + 1
    print("-" * 60)
    print(f"输出：{out_path}（{size_kb:.0f} KB，{len(slim)} 条）")
    print("  首日投放日期分布：" + ", ".join(
        f"{k or '空'}={v}" for k, v in sorted(first_tf.items())[-8:]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
