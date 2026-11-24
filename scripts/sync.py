#!/usr/bin/env python3
"""
Dasheng data sync script for GitHub Actions.
Fetches data from Dasheng API, filters, dictionary-encodes, and writes to data/ directory.
"""

import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone

import requests

# ===== Config =====
API_URL_TPL = (
    "https://market.wuread.cn/market-admin/adminNew/api/2611"
    "?limit=&offset=&sort=&order=desc&pageNo={page}&pageSize={size}"
)
PAGE_SIZE = 50000
MAX_PAGES = 20
DAYS = 8
FIELDS = [
    "agent_user_name",
    "service_provider_name",
    "main_body",
    "placement_mode",
    "customer_id",
    "book_name",
    "os_type_desc",
    "app_name",
    "cdate",
    "consume",
    "n_recharge_uv",
    "unsubscribe_rate",
    "recharge_roi",
    "pre_roi",
    "is_new_book_format",
    "is_anime_desc",
    "media_name",
    "put_type",
]
SHORT_KEYS = list("abcdefghijklmnopqr")
DICT_KEYS = set("abcdefghiopqr")  # string fields → dictionary encoded
RAW_KEYS = set("jklmn")  # numeric fields → raw string values
SPLIT_THRESHOLD_MB = 12       # 超过即分片
PART_TARGET_MB = 8            # 单片目标大小（按字符近似，中文占 3 字节；实际约 9~11MB，远低于 jsDelivr 20MB 上限）
OUTPUT_DIR = "data"


def build_request_body(date_str: str) -> str:
    """Build the POST body matching sync_service_v3 format."""
    body = {
        "book_id": "", "app_name": "", "channel_code": "",
        "roi_h12_min": "", "roi_h12_max": "",
        "sdate": date_str, "edate": date_str,
        "data_merge": "false",
        "agent_user_name": "", "dept_name": "", "media": "", "os_type": "",
        "account": "", "account_name": "", "pline_form": "cltplay",
        "agent_name": "", "main_body": "", "service_provider_name": "",
        "isExport": False, "exportColumns": "",
        "consume_min": "", "consume_max": "",
        "is_new_book": "", "is_today_up": "", "placement_mode": "", "put_type": "",
        "unsubscribe_rate_min": "", "unsubscribe_rate_max": "",
        "is_anime": "", "customer_id": "",
        "roi_day_min": "", "roi_day_max": "",
        "recharge_cost_min": "", "recharge_cost_max": "",
        "n_r_uv_min": "", "n_r_uv_max": "",
        "n_recharge_uv_min": "", "n_recharge_uv_max": "",
        "subscribe_cost_min": "", "subscribe_cost_max": "",
        "pre_roi_min": "", "pre_roi_max": "",
    }
    return json.dumps(body, ensure_ascii=False, separators=(",", ":"))


# ===== Step 1: Fetch =====
def fetch_dasheng(token: str) -> list[dict]:
    """Fetch records from Dasheng API, day by day."""
    cn_tz = timezone(timedelta(hours=8))
    today = datetime.now(cn_tz).date()
    start = today - timedelta(days=DAYS - 1)

    session = requests.Session()
    session.headers.update({
        "Authorization": token,
        "Content-Type": "application/json;charset=UTF-8",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Origin": "https://market.wuread.cn",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/146.0.0.0 Safari/537.36"
        ),
    })
    session.cookies.set("shiroCookie", token, domain="market.wuread.cn")

    all_records = []
    s_str = start.strftime("%Y-%m-%d")
    e_str = today.strftime("%Y-%m-%d")
    print(f"[1/4] Fetching from Dasheng ({s_str} ~ {e_str}, {DAYS} days)...")

    for i in range(DAYS):
        dt = (start + timedelta(days=i)).strftime("%Y-%m-%d")
        body = build_request_body(dt)
        day_records = []
        seen = set()
        page = 1

        # 单日记录可能超过 pageSize=50000（实测 11-23 正好被截断），按页拉全
        while page <= MAX_PAGES:
            url = API_URL_TPL.format(page=page, size=PAGE_SIZE)
            records = None
            for attempt in range(2):
                try:
                    resp = session.post(url, data=body.encode("utf-8"), timeout=120)
                    resp.raise_for_status()
                    records = resp.json().get("data") or []
                    break
                except Exception as e:
                    if attempt == 0:
                        print(f"      {dt} p{page} : failed ({e}), retrying in 5s...")
                        time.sleep(5)
                    else:
                        print(f"      {dt} p{page} : retry failed ({e}), skipping")
            if records is None:
                break

            fresh = 0
            for r in records:
                key = json.dumps(r, sort_keys=True, ensure_ascii=False)
                if key in seen:
                    continue
                seen.add(key)
                day_records.append(r)
                fresh += 1
            print(f"      {dt} p{page} : {len(records)} rows / {fresh} new")

            # 不足一页，或接口忽略 pageNo 返回重复页（无新增）→ 停止，避免死循环
            if len(records) < PAGE_SIZE or fresh == 0:
                break
            page += 1
            time.sleep(1)

        all_records.extend(day_records)
        print(f"      {dt} : total {len(day_records)} records")
        if i < DAYS - 1:
            time.sleep(2)

    print(f"      Raw total: {len(all_records)} records")

    if not all_records:
        print("[ERROR] No data fetched. Token may have expired.")
        print("  1. Login to https://market.wuread.cn/market-admin/")
        print("  2. Press F12 -> Application -> Cookies")
        print("  3. Copy 'shiroCookie' value -> update DASHENG_TOKEN secret")
        return []

    return all_records


# ===== Step 2: Filter =====
def filter_records(records: list[dict]) -> list[dict]:
    """Keep only records with consume > 0 and trim to essential fields."""
    print("[2/4] Filtering records (consume > 0, trim fields)...", flush=True)
    filtered = []
    # Diagnostic: collect unique put_type values
    put_type_values = set()
    for r in records:
        pt = r.get("put_type", "")
        if pt is not None:
            put_type_values.add(str(pt))
    print(f"      [DIAG] put_type unique values: {sorted(put_type_values)}", flush=True)
    for r in records:
        try:
            consume = float(r.get("consume", 0) or 0)
        except (ValueError, TypeError):
            continue
        if consume > 0:
            slim = {}
            for field in FIELDS:
                slim[field] = r.get(field, "")
            filtered.append(slim)

    dropped = len(records) - len(filtered)
    print(f"      consume > 0 : {len(filtered)} kept, {dropped} dropped")
    return filtered


# ===== Step 3: Dictionary encode =====
def encode_dict(records: list[dict]) -> str:
    """Dictionary-encode records with short keys (a-p)."""
    print("[3/4] Dictionary encoding (short keys + integer indices)...")

    # Build field→short key mapping
    field_to_key = {FIELDS[i]: SHORT_KEYS[i] for i in range(len(FIELDS))}

    # Build dictionaries for string fields
    dicts: dict[str, dict[str, int]] = {k: {} for k in DICT_KEYS}
    for r in records:
        for field, key in field_to_key.items():
            if key in DICT_KEYS:
                val = str(r.get(field, ""))
                if val not in dicts[key]:
                    dicts[key][val] = len(dicts[key])

    # Build reverse lookup arrays
    dict_arrays: dict[str, list[str]] = {}
    for k in sorted(DICT_KEYS):
        arr = [""] * len(dicts[k])
        for val, idx in dicts[k].items():
            arr[idx] = val
        dict_arrays[k] = arr
        print(f"      dict[{k}]: {len(arr)} unique values")

    # Build output JSON manually for maximum compatibility with PS1 format
    parts = []

    # _m: key mapping
    m_items = ",".join(
        f'"{SHORT_KEYS[i]}":"{FIELDS[i]}"' for i in range(len(FIELDS))
    )
    parts.append(f'{{"_m":{{{m_items}}}')

    # _d: dictionaries
    d_items = []
    for k in sorted(DICT_KEYS):
        arr = dict_arrays[k]
        encoded_vals = []
        for v in arr:
            escaped = v.replace("\\", "\\\\").replace('"', '\\"')
            encoded_vals.append(f'"{escaped}"')
        d_items.append(f'"{k}":[{",".join(encoded_vals)}]')
    parts.append(f'"_d":{{{",".join(d_items)}}}')

    # data: records array
    data_items = []
    for r in records:
        row_parts = []
        for field, key in field_to_key.items():
            val = r.get(field, "")
            if key in DICT_KEYS:
                idx = dicts[key][str(val)]
                row_parts.append(f'"{key}":{idx}')
            else:
                val_str = str(val).replace("\\", "\\\\").replace('"', '\\"')
                row_parts.append(f'"{key}":"{val_str}"')
        data_items.append("{" + ",".join(row_parts) + "}")

    json_str = ",".join(parts) + ',"data":[' + ",".join(data_items) + "]}"

    size_mb = len(json_str.encode("utf-8")) / 1048576
    print(f"      Encoded to {size_mb:.1f} MB ({len(records)} records)")
    return json_str


# ===== Step 4: Write output =====
def write_output(json_str: str, record_count: int) -> None:
    """Write data files and meta.json to data/ directory."""
    print("[4/4] Writing output files...")
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    size_bytes = len(json_str.encode("utf-8"))
    size_mb = size_bytes / 1048576

    cn_tz = timezone(timedelta(hours=8))
    sync_time = datetime.now(cn_tz).strftime("%Y-%m-%dT%H:%M:%S+08:00")

    if size_mb <= SPLIT_THRESHOLD_MB:
        # Single file
        print(f"      Single file ({size_mb:.1f} MB)...")
        with open(os.path.join(OUTPUT_DIR, "latest.json"), "w", encoding="utf-8") as f:
            f.write(json_str)
        # Clean up old split files
        for fn in os.listdir(OUTPUT_DIR):
            if fn.startswith("part") and fn.endswith(".json"):
                os.remove(os.path.join(OUTPUT_DIR, fn))
                print(f"      Cleaned up old {fn}")

        meta = {
            "sync_time": sync_time,
            "record_count": record_count,
            "file_layout": "single",
            "files": ["latest.json"],
            "days": DAYS,
            "source": "dasheng-api",
        }
    else:
        # 动态分片：每片控制在 PART_TARGET_MB 以内（jsDelivr 单文件上限 20MB）
        print(f"      {size_mb:.1f} MB > {SPLIT_THRESHOLD_MB}MB, splitting...")
        data_start = json_str.index('"data":[') + 8
        data_end = json_str.rindex("]}")
        data_section = json_str[data_start:data_end]
        header = json_str[:data_start]  # 到 "data":[ 为止（含）

        target_chars = int(PART_TARGET_MB * 1048576)
        chunks = []
        pos = 0
        total = len(data_section)
        while pos < total:
            hard_end = min(total, pos + target_chars)
            if hard_end >= total:
                chunks.append(data_section[pos:])
                break
            cut = data_section.find("},{", hard_end)
            if cut < 0:
                cut = data_section.rfind("},{", pos, hard_end)
            if cut <= pos:
                cut = hard_end
            end = cut + 1              # 包含 '}'
            chunks.append(data_section[pos:end])
            pos = end + 1             # 跳过 ','

        files = []
        for idx, chunk in enumerate(chunks, 1):
            fn = f"part{idx}.json"
            content = header + chunk + "]}"
            with open(os.path.join(OUTPUT_DIR, fn), "w", encoding="utf-8") as f:
                f.write(content)
            files.append(fn)
            print(f"      {fn}: {len(content.encode('utf-8')) / 1048576:.1f} MB")

        # 清理上一轮遗留的多余分片与旧单文件
        for fn in os.listdir(OUTPUT_DIR):
            if fn.startswith("part") and fn.endswith(".json") and fn not in files:
                os.remove(os.path.join(OUTPUT_DIR, fn))
                print(f"      Cleaned up old {fn}")
        p = os.path.join(OUTPUT_DIR, "latest.json")
        if os.path.exists(p):
            os.remove(p)
            print("      Cleaned up old latest.json")

        meta = {
            "sync_time": sync_time,
            "record_count": record_count,
            "file_layout": "split",
            "files": files,
            "days": DAYS,
            "source": "dasheng-api",
        }

    # Write meta.json
    with open(os.path.join(OUTPUT_DIR, "meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, separators=(",", ":"))

    print(f"      meta.json written: {meta['file_layout']}, {record_count} records")
    print("      Done!")


# ===== Main =====
def main():
    token = os.environ.get("DASHENG_TOKEN", "").strip()
    if not token:
        print("[ERROR] DASHENG_TOKEN environment variable is not set.")
        print("  Set it in GitHub repo → Settings → Secrets → Actions")
        sys.exit(1)

    print("=" * 50)
    print("  Dasheng → GitHub Data Sync (Python)")
    print("=" * 50)
    print()

    records = fetch_dasheng(token)
    if not records:
        sys.exit(1)

    filtered = filter_records(records)
    if not filtered:
        print("[ERROR] No records with consumption found.")
        sys.exit(1)

    json_str = encode_dict(filtered)
    write_output(json_str, len(filtered))


if __name__ == "__main__":
    main()
