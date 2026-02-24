import csv
import json
import time
from typing import Any, Dict, List, Optional

import requests


URL = "https://exchange.jsdataex.com/dc-detail/dc/getAllOnlineDcApiByDataNet"

# ====== POST 请求体（按抓包）======
BASE_PAYLOAD = {
    "pageNo": 1,
    "pageSize": 10,
    "categoryNames": "",
    "dataProductClassification": "",
    "productFrom": None,
    "displayStatusList": "100,104,105,113",
    "industryZoneName": "",
    "regionalZoneName": "",
    "specialZoneName": "",
}

# ====== 请求头（按抓包）======
HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
    "content-type": "application/json",
    "origin": "https://exchange.jsdataex.com",
    "referer": "https://exchange.jsdataex.com/trade-home/",
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

# 如果后续发现需要 cookie（有些站点需要登录态/风控），把浏览器 cookie 填这里
COOKIE_STRING = ""


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
    """dict/list 字段序列化，方便写 CSV"""
    out: Dict[str, Any] = {}
    for k, v in rec.items():
        if isinstance(v, (dict, list)):
            out[k] = json.dumps(v, ensure_ascii=False)
        else:
            out[k] = v
    return out


def post_page(
    session: requests.Session,
    page_no: int,
    page_size: int,
    payload_overrides: Optional[Dict[str, Any]] = None,
    timeout: int = 25,
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
    if data.get("code") != 200:
        raise RuntimeError(f"API error: {json.dumps(data, ensure_ascii=False)[:200]}")
    return data


def scrape_all(
    out_csv_path: str = "jsdataex_all_online_dc_api.csv",
    page_size: int = 10,
    sleep_sec: float = 0.25,
    max_pages: Optional[int] = None,
    payload_overrides: Optional[Dict[str, Any]] = None,
) -> int:
    session = requests.Session()
    session.headers.update(HEADERS)
    if COOKIE_STRING.strip():
        session.cookies.update(cookie_str_to_dict(COOKIE_STRING))

    # ====== 先拉第一页，获取 totalPage/totalCount ======
    first = post_page(session, 1, page_size, payload_overrides)
    data_block = first.get("data") or {}
    total_count = int(data_block.get("totalCount", 0) or 0)
    total_page = int(data_block.get("totalPage", 0) or 0)
    page_data = data_block.get("pageData") or []

    if not isinstance(page_data, list) or not page_data:
        raise RuntimeError("第一页 pageData 为空：可能参数不对/接口变更/需要 cookie")

    all_rows: List[Dict[str, Any]] = [flatten_record(x) for x in page_data if isinstance(x, dict)]

    print(f"TotalCount={total_count}, TotalPage={total_page}, pageSize={page_size}")
    print(f"Fetched page 1/{total_page}, got {len(page_data)} records, total {len(all_rows)}")

    # ====== 后续页 ======
    last_page = total_page if total_page > 0 else 10**9  # 兜底：如果没给 total_page 就一直拉到空
    if max_pages is not None:
        last_page = min(last_page, max_pages)

    for page_no in range(2, last_page + 1):
        resp = post_page(session, page_no, page_size, payload_overrides)
        block = resp.get("data") or {}
        records = block.get("pageData") or []

        if not records:
            print(f"Page {page_no} empty -> stop.")
            break

        all_rows.extend(flatten_record(x) for x in records if isinstance(x, dict))
        print(f"Fetched page {page_no}/{total_page if total_page else '?'} "
              f"got {len(records)} records, total {len(all_rows)}")

        # 如果知道 total_count 且抓够了，也可以提前停止
        if total_count and len(all_rows) >= total_count:
            break

        time.sleep(sleep_sec)

    # ====== 写 CSV ======
    fieldnames = sorted({k for row in all_rows for k in row.keys()})
    with open(out_csv_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"Saved {len(all_rows)} rows to {out_csv_path}")
    return len(all_rows)


if __name__ == "__main__":
    # 你可以覆盖过滤条件，例如只抓某个分类：
    # payload_overrides = {"categoryNames": "交通运输"}
    payload_overrides = None

    scrape_all(
        out_csv_path="jsdataex_displayStatus_100_104_105_113.csv",
        page_size=10,
        sleep_sec=0.3,
        max_pages=None,  # 想调试可改成 3
        payload_overrides=payload_overrides,
    )
