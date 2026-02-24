import csv
import json
import time
from typing import Any, Dict, List, Optional

import requests


BASE = "https://trade.fjbdex.com/ltywpt-api/api/data-portal-center/portal/catentry/list"

# ====== 默认参数（按你抓包的 query）======
DEFAULT_PARAMS = {
    "labelId": "",
    "sceneId": "",
    "level": "",
    "sortType": "",
    "assetCategory": "",
    "dataType": "",
    "industryInvolved": "",
    "pageNo": "1",
    "pageSize": "10",
    "type": "3",
    "sort": "",
}

# ====== 模拟浏览器请求头 ======
HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
    "connection": "keep-alive",
    "host": "trade.fjbdex.com",
    "referer": "https://trade.fjbdex.com/ltywpt/standardized-products",
    "sec-ch-ua": '"Not(A:Brand";v="8", "Chromium";v="144", "Microsoft Edge";v="144"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0",
}

# ====== Cookie（可选）：如果你发现不带 cookie 会 401/403，就把浏览器 cookie 填进来 ======
COOKIE_STRING = (
    "Hm_lvt_504853aef787e95b0b5e3d553159ed32=1769167622,1769264143; "
    "Hm_lpvt_504853aef787e95b0b5e3d553159ed32=1769264143; "
    "HMACCOUNT=138539E7CD28F14F"
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

    resp = session.get(BASE, params=params, timeout=timeout)
    ct = resp.headers.get("content-type", "")

    if resp.status_code != 200:
        raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:200]}")
    if "application/json" not in ct:
        raise RuntimeError(f"Non-JSON response (content-type={ct}). Body head: {resp.text[:200]}")

    data = resp.json()
    if not isinstance(data, dict) or data.get("success") is not True:
        raise RuntimeError(f"Unexpected JSON: {json.dumps(data, ensure_ascii=False)[:300]}")
    return data


def flatten_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in rec.items():
        if isinstance(v, (dict, list)):
            out[k] = json.dumps(v, ensure_ascii=False)
        else:
            out[k] = v
    return out


def scrape_all(
    out_csv_path: str = "fjbdex_catentry_list.csv",
    page_size: int = 10,
    sleep_sec: float = 0.25,
    max_pages: Optional[int] = None,
    params_overrides: Optional[Dict[str, Any]] = None,
) -> int:
    session = requests.Session()
    session.headers.update(HEADERS)

    # cookie 不一定必须，但加上更像真实浏览器；同时 session 会自动接收 set-cookie
    if COOKIE_STRING.strip():
        session.cookies.update(cookie_str_to_dict(COOKIE_STRING))

    all_rows: List[Dict[str, Any]] = []
    page_no = 1

    while True:
        if max_pages is not None and page_no > max_pages:
            break

        data = fetch_page(
            session=session,
            page_no=page_no,
            page_size=page_size,
            params_overrides=params_overrides,
        )
        result = data.get("result") or {}
        records = result.get("records") or []

        if not records:
            break

        for rec in records:
            if isinstance(rec, dict):
                all_rows.append(flatten_record(rec))

        print(f"Fetched page {page_no}, got {len(records)} records, total {len(all_rows)}")

        page_no += 1
        time.sleep(sleep_sec)

    if not all_rows:
        raise RuntimeError("No data fetched. Possibly blocked or cookie expired / params mismatch.")

    fieldnames = sorted({k for row in all_rows for k in row.keys()})
    with open(out_csv_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"Saved {len(all_rows)} rows to {out_csv_path}")
    return len(all_rows)


if __name__ == "__main__":
    # 你可以按需覆盖查询参数，例如筛选 level=1 或 labelId=xxx
    params_overrides = None
    # 示例：
    # params_overrides = {"level": "1"}

    scrape_all(
        out_csv_path="fjbdex_catentry_type3_all.csv",
        page_size=10,
        sleep_sec=0.3,
        max_pages=None,
        params_overrides=params_overrides,
    )
