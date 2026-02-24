import csv
import json
import time
from typing import Any, Dict, List, Optional

import requests


URL = "https://sjjy.gsdep.cn/api/mall/index/products/list"

# ====== POST 请求体（按抓包）======
BASE_PAYLOAD = {
    "pageNo": 1,
    "pageSize": 12,
    "provinceCode": "620000",
    "areaCode": "",
    "orderBy": "order_count desc",
    "productName": ""
}

# ====== 请求头 ======
HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
    "content-type": "application/json",
    "origin": "https://sjjy.gsdep.cn",
    "referer": "https://sjjy.gsdep.cn/app/index.html",
    "sec-ch-ua": '"Not(A:Brand";v="8", "Chromium";v="144", "Microsoft Edge";v="144"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0"
    ),
}

# ====== Cookie（建议从浏览器复制最新的）======
COOKIE_STRING = (
    "eecc9d2f7258849f4308e2e98cde7ada=6185616f3de523a2145eb9f0b7bd06aa; "
    "server_name_session=237243c7eeb4cf55ffbdd5e8e906410c"
)


def cookie_str_to_dict(cookie_str: str) -> Dict[str, str]:
    cookies: Dict[str, str] = {}
    for part in cookie_str.split(";"):
        part = part.strip()
        if not part or "=" not in part:
            continue
        k, v = part.split("=", 1)
        cookies[k.strip()] = v.strip()
    return cookies


def flatten_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    """dict / list 字段转为 JSON 字符串，方便 CSV"""
    out: Dict[str, Any] = {}
    for k, v in rec.items():
        if isinstance(v, (dict, list)):
            out[k] = json.dumps(v, ensure_ascii=False)
        else:
            out[k] = v
    return out


def fetch_page(
    session: requests.Session,
    page_no: int,
    page_size: int,
    payload_overrides: Optional[Dict[str, Any]] = None,
    timeout: int = 20,
) -> Dict[str, Any]:
    payload = dict(BASE_PAYLOAD)
    payload["pageNo"] = page_no
    payload["pageSize"] = page_size
    if payload_overrides:
        payload.update(payload_overrides)

    resp = session.post(URL, json=payload, timeout=timeout)
    if resp.status_code != 200:
        raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:200]}")

    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(f"API error: {json.dumps(data, ensure_ascii=False)[:200]}")
    return data


def scrape_all(
    out_csv_path: str = "gsdep_products_all.csv",
    page_size: int = 12,
    sleep_sec: float = 0.3,
    payload_overrides: Optional[Dict[str, Any]] = None,
) -> int:
    session = requests.Session()
    session.headers.update(HEADERS)
    session.cookies.update(cookie_str_to_dict(COOKIE_STRING))

    all_rows: List[Dict[str, Any]] = []
    page_no = 1

    while True:
        data = fetch_page(
            session=session,
            page_no=page_no,
            page_size=page_size,
            payload_overrides=payload_overrides,
        )
        records = data.get("data", {}).get("records") or []
        if not records:
            break

        for rec in records:
            if isinstance(rec, dict):
                all_rows.append(flatten_record(rec))

        print(f"Fetched page {page_no}, got {len(records)} records, total {len(all_rows)}")

        page_no += 1
        time.sleep(sleep_sec)

    if not all_rows:
        raise RuntimeError("未获取到数据，可能 cookie 过期或参数不匹配")

    fieldnames = sorted({k for row in all_rows for k in row.keys()})
    with open(out_csv_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"Saved {len(all_rows)} rows to {out_csv_path}")
    return len(all_rows)


if __name__ == "__main__":
    # 示例：只抓某个关键词
    # payload_overrides = {"productName": "管理系统"}
    payload_overrides = None

    scrape_all(
        out_csv_path="gsdep_products_province620000.csv",
        page_size=12,
        sleep_sec=0.4,
        payload_overrides=payload_overrides,
    )
