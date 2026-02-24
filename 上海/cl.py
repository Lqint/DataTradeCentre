import csv
import json
import time
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


API_URL = "https://nidts.chinadep.com/dex-api/dex-es/query/all/search"


def build_session() -> requests.Session:
    """
    带重试策略的 session，避免偶发网络抖动导致中断。
    """
    session = requests.Session()

    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=0.8,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["POST"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def fetch_page(
    session: requests.Session,
    page_num: int,
    page_size: int = 10,
    filters: Optional[Dict[str, Any]] = None,
    cookie: Optional[str] = None,
    timeout: int = 30,
) -> Dict[str, Any]:
    """
    拉取单页数据，返回解析后的 JSON。
    """
    payload = {
        "pageNum": page_num,
        "pageSize": page_size,
        "operationSubject": "",
        "dataType": "",
        "serviceType": "",
        "zone": "",
        "sectorName": "",
        "aiTag": "",
        "search": "",
    }
    if filters:
        payload.update(filters)

    headers = {
        "accept": "application/json, text/plain, */*",
        "content-type": "application/json;charset=UTF-8",
        "origin": "https://nidts.chinadep.com",
        "referer": "https://nidts.chinadep.com/trading-market/product",
        "user-agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0"
        ),
    }
    if cookie:
        headers["cookie"] = cookie

    resp = session.post(API_URL, headers=headers, data=json.dumps(payload), timeout=timeout)
    # 有些站会返回 200 但 code != 200，所以这里两层判断
    resp.raise_for_status()
    data = resp.json()

    if not isinstance(data, dict) or data.get("code") != 200:
        raise RuntimeError(f"API返回异常：status={resp.status_code}, body={data}")

    return data


def normalize_item(item: Dict[str, Any]) -> Dict[str, Any]:
    """
    将每条记录里常见的 list 字段转成字符串，方便写入 CSV。
    你可以按需要增加/修改字段。
    """
    out = dict(item)

    # 这些字段在示例响应里是 list：aiTag/sectorName/zoneName
    for k in ["aiTag", "sectorName", "zoneName"]:
        v = out.get(k)
        if isinstance(v, list):
            out[k] = "|".join(str(x) for x in v)
        elif v is None:
            out[k] = ""
        else:
            out[k] = str(v)

    return out


def crawl_all_to_csv(
    output_csv: str = "nidts_all.csv",
    page_size: int = 10,
    filters: Optional[Dict[str, Any]] = None,
    cookie: Optional[str] = None,
    sleep_seconds: float = 0.2,
) -> None:
    session = build_session()

    # 先拉第 1 页拿总页数
    first = fetch_page(session, page_num=1, page_size=page_size, filters=filters, cookie=cookie)
    meta = first["data"]
    total_page = int(meta.get("totalPage", 1))
    total = int(meta.get("total", 0))

    print(f"总记录数: {total}, 总页数: {total_page}, pageSize: {page_size}")

    all_rows: List[Dict[str, Any]] = []

    def extract_list(page_json: Dict[str, Any]) -> List[Dict[str, Any]]:
        lst = page_json.get("data", {}).get("list", [])
        if not isinstance(lst, list):
            return []
        return [normalize_item(x) for x in lst if isinstance(x, dict)]

    # 第 1 页
    all_rows.extend(extract_list(first))

    # 后续页
    for p in range(2, total_page + 1):
        time.sleep(sleep_seconds)  # 温柔一点，避免触发限流
        page_json = fetch_page(session, page_num=p, page_size=page_size, filters=filters, cookie=cookie)
        rows = extract_list(page_json)
        all_rows.extend(rows)

        if p % 20 == 0 or p == total_page:
            print(f"已抓取: {p}/{total_page} 页，累计 {len(all_rows)} 条")

    if not all_rows:
        raise RuntimeError("未抓到任何数据：可能需要补充 cookie，或筛选条件导致无结果。")

    # 生成 CSV 表头：取所有记录的 key 并集，保证字段不丢
    fieldnames = sorted({k for r in all_rows for k in r.keys()})

    with open(output_csv, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"完成：共写入 {len(all_rows)} 条到 {output_csv}")


if __name__ == "__main__":
    # 如果你发现返回空/403/401，把浏览器抓包里的 cookie 字符串粘到这里
    COOKIE = None
    # COOKIE = "_pk_id.2.510f=...; Hm_lvt_...=...; ..."

    # 如果你需要筛选条件，可以在这里覆盖（键名必须和接口 payload 一致）
    FILTERS = {
        # "search": "",
        # "dataType": "01",
        # "serviceType": "已登记",
    }

    crawl_all_to_csv(
        output_csv="nidts_all.csv",
        page_size=10,
        filters=FILTERS,
        cookie=COOKIE,
        sleep_seconds=0.2,
    )
