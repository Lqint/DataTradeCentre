import csv
import json
import time
from typing import Any, Dict, List, Optional

import requests


API_URL = "https://transaction.datadex.cn/api/resource/searchBy"

# 你抓包里看到的 tenantid
TENANT_ID = "shuchao_zongju"

# 站点页面（Referer）
REFERER = "https://transaction.datadex.cn/app/dataMarket"
ORIGIN = "https://transaction.datadex.cn"

# 如果接口需要登录态，把浏览器请求头里的 Cookie 原样贴到这里：
COOKIE = ""  # 例如: "SESSION=xxxx; token=yyyy; ..."

# 如果你抓包里有 Authorization / token 之类，也可以加：
AUTHORIZATION = ""  # 例如: "Bearer xxxxx"


def flatten(d: Dict[str, Any], parent_key: str = "", sep: str = ".") -> Dict[str, Any]:
    """把嵌套 dict 扁平化成一层，便于写 CSV"""
    items: Dict[str, Any] = {}
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.update(flatten(v, new_key, sep=sep))
        elif isinstance(v, list):
            # list 直接转 JSON 字符串保存（避免丢信息）
            items[new_key] = json.dumps(v, ensure_ascii=False)
        else:
            items[new_key] = v
    return items


def build_headers() -> Dict[str, str]:
    headers = {
        "accept": "application/json, text/plain, */*",
        "content-type": "application/json",
        "origin": ORIGIN,
        "referer": REFERER,
        "tenantid": TENANT_ID,
        "user-agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0"
        ),
    }
    if COOKIE:
        headers["cookie"] = COOKIE
    if AUTHORIZATION:
        headers["authorization"] = AUTHORIZATION
    return headers


def fetch_page(session: requests.Session, page_no: int, page_size: int = 12) -> Dict[str, Any]:
    payload = {
        "timestamp": int(time.time() * 1000),
        "index": 0,
        "saleFeesType": "",
        "sceneSecondTag": "",
        "sceneThirdTag": "",
        "paging": {"current": page_no, "size": page_size, "total": 0},
        "pageNo": page_no,
        "searchKey": "",
        "pageSize": page_size,
    }

    resp = session.post(API_URL, headers=build_headers(), json=payload, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return data


def crawl_all(output_csv: str = "datadex_resources.csv", page_size: int = 12, sleep_sec: float = 0.2):
    session = requests.Session()

    # 先拉第一页，拿 total / totalPage
    first = fetch_page(session, page_no=1, page_size=page_size)

    if first.get("code") != 0:
        raise RuntimeError(f"API 返回 code != 0: {first}")

    data = first.get("data") or {}
    total = data.get("total")
    total_page = data.get("totalPage")
    first_list = data.get("list") or []

    print(f"total={total}, totalPage={total_page}, pageSize={page_size}")

    # 收集所有行（扁平化）
    rows: List[Dict[str, Any]] = []
    for item in first_list:
        rows.append(flatten(item))

    # 逐页拉取剩余页
    for page in range(2, int(total_page) + 1):
        time.sleep(sleep_sec)
        try:
            j = fetch_page(session, page_no=page, page_size=page_size)
            if j.get("code") != 0:
                print(f"[WARN] page {page} code != 0: {j.get('code')}, raw={j}")
                continue

            lst = (j.get("data") or {}).get("list") or []
            for item in lst:
                rows.append(flatten(item))

            print(f"fetched page {page}/{total_page}, rows={len(rows)}")
        except requests.HTTPError as e:
            print(f"[ERROR] HTTPError on page {page}: {e}")
        except Exception as e:
            print(f"[ERROR] Exception on page {page}: {e}")

    # 动态表头（所有字段取并集）
    fieldnames = sorted({k for r in rows for k in r.keys()})

    with open(output_csv, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            writer.writerow(r)

    print(f"done. saved to {output_csv}. total rows={len(rows)}")


if __name__ == "__main__":
    crawl_all(output_csv="datadex_resources.csv", page_size=12, sleep_sec=0.2)
