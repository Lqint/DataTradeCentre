import csv
import json
import time
from typing import Dict, Any, List, Optional

import requests


BASE = "https://s-gateway.hunandex.com"

LIST_API = f"{BASE}/api/data-portal-center/portal/catentry/newList"
DETAIL_API = f"{BASE}/api/data-portal-center/portal/applied/queryById"

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Origin": "https://trade.hunandex.com",
    "Referer": "https://trade.hunandex.com/",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0"
    ),
}


def safe_get(d: Dict[str, Any], path: List[str], default=None):
    cur = d
    for k in path:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def flatten_json(obj: Any, prefix: str = "", out: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """把嵌套 dict/list 拉平成一层，list 会转成 JSON 字符串。"""
    if out is None:
        out = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            key = f"{prefix}.{k}" if prefix else k
            flatten_json(v, key, out)
    elif isinstance(obj, list):
        out[prefix] = json.dumps(obj, ensure_ascii=False)
    else:
        out[prefix] = obj
    return out


def request_json(session: requests.Session, url: str, params: Dict[str, Any], timeout: int = 20) -> Dict[str, Any]:
    resp = session.get(url, params=params, headers=HEADERS, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def fetch_list_page(session: requests.Session, page_no: int, page_size: int = 12) -> List[Dict[str, Any]]:
    params = {
        "type": "",
        "sceneId": "",
        "labelId": "",
        "industryInvolved": "",
        "fwType": "",
        "tradeUserType": "",
        "sort": 1,
        "pageSize": page_size,
        "pageNo": page_no,
        "sortType": 0,
        "preference": 2,
    }
    data = request_json(session, LIST_API, params)
    if not data.get("success"):
        raise RuntimeError(f"List API failed: {data.get('message')} (code={data.get('code')})")
    records = safe_get(data, ["result", "records"], default=[])
    if not isinstance(records, list):
        records = []
    return records


def fetch_detail(session: requests.Session, product_id: str) -> Dict[str, Any]:
    params = {"id": product_id, "userId": "", "type": ""}
    data = request_json(session, DETAIL_API, params)
    if not data.get("success"):
        raise RuntimeError(f"Detail API failed for {product_id}: {data.get('message')} (code={data.get('code')})")
    detail = data.get("result") or {}
    if not isinstance(detail, dict):
        detail = {}
    return detail


def export_csv(rows: List[Dict[str, Any]], csv_path: str) -> None:
    # 收集所有字段名（拉平后的键）
    all_keys = set()
    for r in rows:
        all_keys.update(r.keys())
    fieldnames = sorted(all_keys)

    # UTF-8-SIG：Excel 打开中文不会乱码
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            writer.writerow(r)


def main(
    start_page: int = 1,
    end_page: int = 5,
    page_size: int = 12,
    sleep_seconds: float = 0.3,
    out_csv: str = "hunandex_products.csv",
):
    """
    start_page/end_page: 你想抓取的页码范围（包含两端）
    """
    session = requests.Session()

    seen_ids = set()
    out_rows: List[Dict[str, Any]] = []

    for page in range(start_page, end_page + 1):
        records = fetch_list_page(session, page_no=page, page_size=page_size)
        print(f"[list] page={page} records={len(records)}")

        if not records:
            break

        for item in records:
            pid = str(item.get("id", "")).strip()
            if not pid or pid in seen_ids:
                continue
            seen_ids.add(pid)

            # 详情
            try:
                detail = fetch_detail(session, pid)
            except Exception as e:
                print(f"[detail] id={pid} ERROR: {e}")
                continue

            # 合并：列表字段 + 详情字段（详情同名会覆盖列表）
            merged = {}
            merged.update(item if isinstance(item, dict) else {})
            merged.update(detail if isinstance(detail, dict) else {})

            flat = flatten_json(merged)
            out_rows.append(flat)

            time.sleep(sleep_seconds)

    export_csv(out_rows, out_csv)
    print(f"[done] rows={len(out_rows)} saved: {out_csv}")


if __name__ == "__main__":
    # 示例：抓 1~10 页
    main(start_page=1, end_page=10, page_size=12, sleep_seconds=0.2, out_csv="hunandex_products.csv")
