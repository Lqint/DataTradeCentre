import csv
import json
import time
from typing import Any, Dict, List, Optional

import requests


URL = "https://dex.jxggzyjy.cn/api/resource/searchProductByPage"

# ====== 请求体（按抓包）======
BASE_PAYLOAD = {
    "sort": "Default",
    "tags": [],
    "searchKey": "",
    "pageNo": 1,
    "pageSize": "9",  # 注意：抓包里 pageSize 是字符串
}

# ====== 请求头（按抓包）======
HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
    "content-type": "application/json",
    "origin": "https://dex.jxggzyjy.cn",
    "referer": "https://dex.jxggzyjy.cn/factorMarket",
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

# ====== Cookie（按抓包：一般是负载均衡路由；可选但建议带上）======
COOKIE_STRING = "lbinsertroute=d8741ebc991f909a85aa396d30233709"


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
    """
    list/dict 字段序列化为 JSON 字符串，方便落 CSV
    例如 productTag 是 list，resourceApiDTO 可能是 dict
    """
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
    page_size: str = "9",
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
    if data.get("code") != 0:
        raise RuntimeError(f"API error: {json.dumps(data, ensure_ascii=False)[:200]}")
    return data


def scrape_all(
    out_csv_path: str = "dex_jxggzyjy_products_all.csv",
    page_size: str = "9",
    sleep_sec: float = 0.3,
    max_pages: Optional[int] = None,
    payload_overrides: Optional[Dict[str, Any]] = None,
) -> int:
    session = requests.Session()
    session.headers.update(HEADERS)
    if COOKIE_STRING.strip():
        session.cookies.update(cookie_str_to_dict(COOKIE_STRING))

    # ====== 先拉第一页拿 totalPage ======
    first = post_page(session, page_no=1, page_size=page_size, payload_overrides=payload_overrides)
    block = first.get("data") or {}
    total_page = int(block.get("totalPage", 0) or 0)
    records = block.get("list") or []

    if not isinstance(records, list):
        raise RuntimeError("data.list 不是列表，接口返回结构可能变更")
    if not records:
        raise RuntimeError("第一页 list 为空：可能参数不对/需要登录态/被限流")

    all_rows: List[Dict[str, Any]] = [flatten_record(x) for x in records if isinstance(x, dict)]

    print(f"TotalPage={total_page}, pageSize={page_size}")
    print(f"Fetched page 1/{total_page}, got {len(records)} records, total {len(all_rows)}")

    last_page = total_page if total_page > 0 else 10**9
    if max_pages is not None:
        last_page = min(last_page, max_pages)

    # ====== 拉后续页 ======
    for page_no in range(2, last_page + 1):
        resp = post_page(session, page_no=page_no, page_size=page_size, payload_overrides=payload_overrides)
        block = resp.get("data") or {}
        records = block.get("list") or []

        if not records:
            print(f"Page {page_no} empty -> stop.")
            break

        all_rows.extend(flatten_record(x) for x in records if isinstance(x, dict))
        print(f"Fetched page {page_no}/{total_page if total_page else '?'} "
              f"got {len(records)} records, total {len(all_rows)}")

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
    # 你可以覆盖搜索条件，例如：
    # payload_overrides = {"searchKey": "车辆", "tags": []}
    payload_overrides = None

    scrape_all(
        out_csv_path="dex_jxggzyjy_factor_market_all.csv",
        page_size="9",
        sleep_sec=0.35,
        max_pages=None,   # 调试可改 3
        payload_overrides=payload_overrides,
    )
