import time
import random
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional

import requests
import pandas as pd


LIST_URL = "https://szj.hebei.gov.cn/hbjyzx/sjjyfwpt/hbdtc_custom/v1/api/open/product/list"
DETAIL_PREFIX = "https://szj.hebei.gov.cn/hbjyzx/sjjyfwpt/hbdtc_custom/v1/api/open/product/detail/"
HALL_REFERER = "https://szj.hebei.gov.cn/hbjyzx/sjjyfwpt/product-hall"


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "Accept": "application/json, text/plain, */*",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0",
        "Origin": "https://szj.hebei.gov.cn",
        "Referer": HALL_REFERER,
        "Cookie": "tenant_id=hbdtc",
    })
    # 可选：先访问一次页面，帮助拿到 JSESSIONID（很多站点需要）
    try:
        s.get(HALL_REFERER, timeout=20)
    except Exception:
        pass
    return s


def safe_json(resp: requests.Response) -> Dict[str, Any]:
    resp.raise_for_status()
    try:
        return resp.json()
    except Exception as e:
        raise RuntimeError(f"JSON parse failed: {e}; text={resp.text[:200]}") from e


def fetch_list_page(session: requests.Session, page_index: int, page_size: int) -> Dict[str, Any]:
    """
    你抓包显示 list 用 GET + query 参数，这里按 GET 实现。
    若你实际发现必须 POST，可在这里扩展（我之前给过 GET/POST 容错版本）。
    """
    params = {
        "pageIndex": page_index,
        "pageSize": page_size,
        "registrationDataName": "",
        "industryClassification": "",
        "dataProvisionMethod": "",
        "sortField": "listListingTime",
        "sjType": "",
    }
    resp = session.get(LIST_URL, params=params, timeout=30)
    return safe_json(resp)


def fetch_detail(session: requests.Session, uid: str) -> Dict[str, Any]:
    url = DETAIL_PREFIX + uid
    referer = f"https://szj.hebei.gov.cn/hbjyzx/sjjyfwpt/product-detail/{uid}"
    headers = {"Referer": referer}
    resp = session.get(url, headers=headers, timeout=30)
    return safe_json(resp)


def flatten(obj: Any, prefix: str = "", sep: str = ".") -> Dict[str, Any]:
    """
    将嵌套 dict/list 拍平成一层，便于保存 CSV。
    list 默认转成 JSON 字符串（避免列爆炸）。
    """
    out: Dict[str, Any] = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            key = f"{prefix}{sep}{k}" if prefix else str(k)
            out.update(flatten(v, key, sep=sep))
    elif isinstance(obj, list):
        # 列表直接 JSON 化，避免生成大量列
        out[prefix] = json.dumps(obj, ensure_ascii=False)
    else:
        out[prefix] = obj
    return out


def main():
    session = make_session()

    # 1) 全量抓取 list
    page_index = 1
    page_size = 100
    list_records: List[Dict[str, Any]] = []

    while True:
        js = fetch_list_page(session, page_index, page_size)
        data = js.get("data") or {}
        records = data.get("records") or []
        print(f"[LIST] page {page_index} -> {len(records)}")

        if not records:
            break

        list_records.extend(records)
        page_index += 1
        time.sleep(random.uniform(0.3, 1.0))

    if not list_records:
        print("List is empty. Check whether the API needs extra headers/cookies.")
        return

    # 从 list 里提取 uid
    uids = []
    for r in list_records:
        uid = r.get("uid")
        if uid:
            uids.append(uid)
    uids = list(dict.fromkeys(uids))  # 去重保持顺序
    print(f"Total uids: {len(uids)}")

    # 2) 逐个补齐 detail（可并发）
    # 并发数：政务站建议小一点，避免触发限流
    max_workers = 6

    details_map: Dict[str, Dict[str, Any]] = {}

    def worker(u: str) -> Optional[Dict[str, Any]]:
        # 每个请求轻微随机延迟，减少触发风控概率
        time.sleep(random.uniform(0.15, 0.5))
        js = fetch_detail(session, u)

        # 常见返回结构：{state/code, data: {...}}
        data = js.get("data")
        if data is None:
            # 仍然返回整包，方便你排查
            return {"uid": u, "_raw_detail": json.dumps(js, ensure_ascii=False)}
        # 把 uid 放回去，方便 join
        if isinstance(data, dict):
            data["uid"] = u
            return data
        return {"uid": u, "detail_data": json.dumps(data, ensure_ascii=False)}

    print(f"Fetching details with {max_workers} workers ...")
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(worker, u): u for u in uids}
        for i, fut in enumerate(as_completed(futures), 1):
            uid = futures[fut]
            try:
                d = fut.result()
                if d is not None:
                    details_map[uid] = d
            except Exception as e:
                details_map[uid] = {"uid": uid, "_detail_error": str(e)}
            if i % 20 == 0 or i == len(uids):
                print(f"[DETAIL] {i}/{len(uids)} done")

    # 3) 合并 list + detail
    merged_rows: List[Dict[str, Any]] = []
    for r in list_records:
        uid = r.get("uid")
        merged = dict(r)
        detail = details_map.get(uid, {})
        # 合并时避免覆盖 list 的关键字段：你也可以反过来覆盖
        for k, v in detail.items():
            if k not in merged:
                merged[k] = v
            else:
                merged[f"detail.{k}"] = v
        merged_rows.append(merged)

    # 4) 拍平成表格并导出 CSV
    flat_rows = []
    for row in merged_rows:
        flat_rows.append(flatten(row))

    df = pd.DataFrame(flat_rows)

    out_path = "hebei_products_full_with_detail.csv"
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"Saved: {out_path} (rows={len(df)}, cols={len(df.columns)})")


if __name__ == "__main__":
    main()
