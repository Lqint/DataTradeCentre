import csv
import json
import math
import time
from typing import Any, Dict, List, Optional

import requests


BASE_URL = "https://qddataops.com/application/dataProducts/DataProductsByCorName2"

# ====== 默认查询参数（按抓包）======
DEFAULT_PARAMS = {
    "productStatus": "2",
    "pageNo": "1",
    "pageSize": "12",
    "sortTime": "1",
    "sceneCategoryTypeList": "",
}

# ====== 请求头 ======
HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
    "cache-control": "no-cache",
    "connection": "keep-alive",
    "host": "qddataops.com",
    "referer": "https://qddataops.com/dataProducts",
    "sec-ch-ua": '"Not(A:Brand";v="8", "Chromium";v="144", "Microsoft Edge";v="144"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0",
}


def flatten_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    """dict/list 字段序列化，方便写 CSV"""
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
    params_overrides: Optional[Dict[str, Any]] = None,
    timeout: int = 20,
) -> Dict[str, Any]:
    params = dict(DEFAULT_PARAMS)
    params["pageNo"] = str(page_no)
    params["pageSize"] = str(page_size)
    if params_overrides:
        for k, v in params_overrides.items():
            params[k] = "" if v is None else str(v)

    resp = session.get(BASE_URL, params=params, timeout=timeout)
    if resp.status_code != 200:
        raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:200]}")

    data = resp.json()
    if data.get("status") is not True:
        raise RuntimeError(f"API error: {json.dumps(data, ensure_ascii=False)[:200]}")

    return data


def scrape_all(
    out_csv_path: str = "qddataops_data_products.csv",
    page_size: int = 12,
    sleep_sec: float = 0.25,
    params_overrides: Optional[Dict[str, Any]] = None,
) -> int:
    session = requests.Session()
    session.headers.update(HEADERS)

    all_rows: List[Dict[str, Any]] = []

    # ====== 先拉第一页，拿 total ======
    first = fetch_page(session, 1, page_size, params_overrides)
    data = first.get("data") or {}
    total = int(data.get("total", 0))
    records = data.get("list") or []

    if not records:
        raise RuntimeError("第一页无数据，接口可能变更")

    all_rows.extend(flatten_record(r) for r in records)
    total_pages = math.ceil(total / page_size)

    print(f"Total items: {total}, pages: {total_pages}")

    # ====== 后续页 ======
    for page_no in range(2, total_pages + 1):
        page_data = fetch_page(session, page_no, page_size, params_overrides)
        records = page_data.get("data", {}).get("list") or []
        if not records:
            break

        all_rows.extend(flatten_record(r) for r in records)
        print(f"Fetched page {page_no}/{total_pages}, total rows {len(all_rows)}")
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
    # 示例：只抓某个场景
    # params_overrides = {"sceneCategoryTypeList": "12"}
    params_overrides = None

    scrape_all(
        out_csv_path="qddataops_productStatus2_all.csv",
        page_size=12,
        sleep_sec=0.3,
        params_overrides=params_overrides,
    )
