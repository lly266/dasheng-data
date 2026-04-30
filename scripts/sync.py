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
API_URL = (
    "https://market.wuread.cn/market-admin/adminNew/api/2611"
    "?limit=&offset=&sort=&order=desc&pageNo=1&pageSize=50000"
)
DAYS = 7
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
]
SHORT_KEYS = list("abcdefghijklmnop")
DICT_KEYS = set("abcdefghiop")  # string fields → dictionary encoded
RAW_KEYS = set("jklmn")  # numeric fields → raw string values
SPLIT_THRESHOLD_MB = 20
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

        for attempt in range(2):
            try:
                resp = session.post(API_URL, data=body.encode("utf-8"), timeout=120)
                resp.raise_for_status()
                data = resp.json()
                records = data.get("data") or []
                all_records.extend(records)
                print(f"      {dt} : {len(records)} records"
                      + (" (retry OK)" if attempt > 0 else ""))
                break
            except Exception as e:
                if attempt == 0:
                    print(f"      {dt} : failed ({e}), retrying in 5s...")
                    time.sleep(5)
                else:
                    print(f"      {dt} : retry failed ({e}), skipping")

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
    print("[2/4] Filtering records (consume > 0, trim fields)...")
    filtered = []
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
        for fn in ("part1.json", "part2.json"):
            p = os.path.join(OUTPUT_DIR, fn)
            if os.path.exists(p):
                os.remove(p)
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
        # Split into 2 files
        print(f"      {size_mb:.1f} MB > {SPLIT_THRESHOLD_MB}MB, splitting...")
        data_start = json_str.index('"data":[') + 8
        data_end = json_str.rindex("]}")
        data_section = json_str[data_start:data_end]

        mid = len(data_section) // 2
        split_pos = data_section.find("},{", mid)
        if split_pos < 0:
            split_pos = data_section.rfind("},{")
        split_pos += 1  # include the '}'

        header = json_str[:data_start]  # everything up to and including "data":[
        part1_data = data_section[:split_pos]
        part2_data = data_section[split_pos + 1:]  # skip the ','

        json1 = header + part1_data + "]}"
        json2 = header + part2_data + "]}"

        s1 = len(json1.encode("utf-8")) / 1048576
        s2 = len(json2.encode("utf-8")) / 1048576
        print(f"      Part1: {s1:.1f} MB, Part2: {s2:.1f} MB")

        with open(os.path.join(OUTPUT_DIR, "part1.json"), "w", encoding="utf-8") as f:
            f.write(json1)
        with open(os.path.join(OUTPUT_DIR, "part2.json"), "w", encoding="utf-8") as f:
            f.write(json2)
        # Clean up old single file
        p = os.path.join(OUTPUT_DIR, "latest.json")
        if os.path.exists(p):
            os.remove(p)
            print("      Cleaned up old latest.json")

        meta = {
            "sync_time": sync_time,
            "record_count": record_count,
            "file_layout": "split",
            "files": ["part1.json", "part2.json"],
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
