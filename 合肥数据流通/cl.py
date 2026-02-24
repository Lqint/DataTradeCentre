import csv
import json
import time
from typing import Any, Dict, List, Optional, Tuple

import requests


BASE_URL = "https://www.bigdatadex.com.cn/bdup/api/op/commodity/application/findAllPage"

# ====== 默认查询参数（按抓包）======
DEFAULT_PARAMS = {
    "searchName": "",
    "supplyAndDemand": "",
    "orderType": "0",
    "pageNo": "1",
    "pageSize": "8",
}

# ====== 请求头（按抓包尽量还原）======
HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
    "connection": "keep-alive",
    "host": "www.bigdatadex.com.cn",
    "referer": "https://www.bigdatadex.com.cn/dataApplication/list?dating=1",
    "sec-ch-ua": '"Not(A:Brand";v="8", "Chromium";v="144", "Microsoft Edge";v="144"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "tenant_id": "0",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0",
}

# ====== Cookie（非常关键：过期就需要你从浏览器复制最新的来替换）======
COOKIE_STRING = (
    "__jsluid_s=0ccb1515bde4469c6d9bb96b142e66d5; "
    "SECKEY_ABVK=nnRxSPImQ8RZsEHFhhI/sBRRHD9ub9JK8H432I+glAiu8g9jjq3rLmeFUube+s8813yjmGi3NddY101NWj11jg%3D%3D; "
    "BMAP_SECKEY=nnRxSPImQ8RZsEHFhhI_sBRRHD9ub9JK8H432I-glAiXPc0i_SCAh8lgExFnKn-0w26cAa-B_cUMHkoc1YHY9N65Rbbiuqpc_rfogWFcPqebxbM5Q-S41vBusTnhq1tQVU2dTO0yCLBJkZ3RCn87rw1mU5ckSkIppM-pkYqfl6P_-9JsmTPTvWfBsVJjBkf8s6yT80N2iS86vBiR81OY3Ub9KX88tk-8cabvmAUr2GY"
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


def ensure_json_response(resp: requests.Response) -> Dict[str, Any]:
    """保证返回 JSON；否则大概率是风控/验证页 HTML。"""
    ct = resp.headers.get("content-type", "")
    if resp.status_code != 200:
        raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:200]}")
    if "application/json" not in ct:
        raise RuntimeError(
            f"Non-JSON response (content-type={ct}). "
            f"可能被拦截/跳验证页。Body head: {resp.text[:200]}"
        )
    data = resp.json()
    if not isinstance(data, dict):
        raise RuntimeError(f"Unexpected JSON type: {type(data)}")
    if data.get("success") is not True:
        raise RuntimeError(f"API returned success!=true: {json.dumps(data, ensure_ascii=False)[:300]}")
    return data


def flatten_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    """把 dict/list 字段序列化为 JSON 字符串，方便写 CSV。"""
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
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    params = dict(DEFAULT_PARAMS)
    params["pageNo"] = str(page_no)
    params["pageSize"] = str(page_size)
    if params_overrides:
        for k, v in params_overrides.items():
            params[k] = "" if v is None else str(v)

    resp = session.get(BASE_URL, params=params, timeout=timeout)
    data = ensure_json_response(resp)

    result = data.get("result") or {}
    records = result.get("records") or []
    if not isinstance(records, list):
        raise RuntimeError("result.records is not a list")
    return records, result


def scrape_all(
    out_csv_path: str = "bigdatadex_findAllPage_all.csv",
    page_size: int = 8,
    sleep_sec: float = 0.3,
    start_page: int = 1,
    max_pages: Optional[int] = None,
    params_overrides: Optional[Dict[str, Any]] = None,
) -> int:
    session = requests.Session()
    session.headers.update(HEADERS)
    session.cookies.update(cookie_str_to_dict(COOKIE_STRING))

    all_rows: List[Dict[str, Any]] = []
    page_no = start_page

    # 如果接口提供 total/pages，我们尽量用它精确停止（有则用，无则用空页停止）
    total_pages: Optional[int] = None
    total_items: Optional[int] = None

    while True:
        if max_pages is not None and (page_no - start_page + 1) > max_pages:
            break
        if total_pages is not None and page_no > total_pages:
            break

        records, result_meta = fetch_page(
            session=session,
            page_no=page_no,
            page_size=page_size,
            params_overrides=params_overrides,
        )

        # 尝试读取常见分页字段（不同后端命名可能不同）
        for key in ["pages", "totalPages", "pageCount"]:
            if total_pages is None and isinstance(result_meta.get(key), int):
                total_pages = result_meta.get(key)

        for key in ["total", "totalCount", "count"]:
            if total_items is None and isinstance(result_meta.get(key), int):
                total_items = result_meta.get(key)

        if not records:
            break

        all_rows.extend(flatten_record(r) for r in records if isinstance(r, dict))

        msg = f"Fetched page {page_no}, got {len(records)} records, total {len(all_rows)}"
        if total_pages is not None:
            msg += f", pages={total_pages}"
        if total_items is not None:
            msg += f", total={total_items}"
        print(msg)

        page_no += 1
        time.sleep(sleep_sec)

        # 如果知道 total_items 且已抓够，也可以提前停止
        if total_items is not None and len(all_rows) >= total_items:
            break

    if not all_rows:
        raise RuntimeError(
            "No data fetched. 可能 cookie 过期/被风控拦截。"
            "请从浏览器 Network 里复制最新 Cookie 替换 COOKIE_STRING。"
        )

    fieldnames = sorted({k for row in all_rows for k in row.keys()})
    with open(out_csv_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"Saved {len(all_rows)} rows to {out_csv_path}")
    return len(all_rows)


if __name__ == "__main__":
    # 你可以覆盖参数，比如搜索关键字：
    # params_overrides = {"searchName": "语音"}
    params_overrides = None

    scrape_all(
        out_csv_path="bigdatadex_findAllPage_full.csv",
        page_size=8,
        sleep_sec=0.35,
        start_page=1,
        max_pages=None,
        params_overrides=params_overrides,
    )
