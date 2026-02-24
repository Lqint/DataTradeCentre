import math
import time
import json
import random
from typing import Any, Dict, List, Optional

import requests
import pandas as pd


API_URL = "https://exchange.datadmz.com:30101/api/product/dataMarket/selectProductList"

# ====== 你可以按需修改这些筛选条件 ======
BASE_PAYLOAD = {
    "name": "",
    "listPlatform": "01",
    "pageNum": 1,          # 会被循环覆盖
    "pageSize": 20,        # 可调大一些，如 50/100（看服务端是否允许）
    "dataSource": [],
    "useIndustry": [],
    "dataProductType": [],
    "sourcePlatform": ["exchange", "2646e1cb091648d5b1652b410b4514b7"],
}

# ====== 可选：如果必须带 cookie 才能返回数据，把你的 cookie 字符串粘贴到这里 ======
COOKIE_STR: Optional[str] = None
# 例子：
# COOKIE_STR = "Hm_lvt_...=...; Hm_lpvt_...=...; HMACCOUNT=..."


def build_headers(cookie: Optional[str] = None) -> Dict[str, str]:
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json;charset=UTF-8",
        "Origin": "https://exchange.datadmz.com:30101",
        "Referer": "https://exchange.datadmz.com:30101/datadmz/tradeFloor/list?removeSideBar=true",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0"
        ),
    }
    if cookie:
        headers["Cookie"] = cookie
    return headers


def post_with_retry(
    session: requests.Session,
    url: str,
    payload: Dict[str, Any],
    headers: Dict[str, str],
    timeout: int = 30,
    max_retries: int = 5,
) -> Dict[str, Any]:
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            r = session.post(url, headers=headers, json=payload, timeout=timeout)
            # 一些站点会返回 200 但 body 不是 JSON；这里做下保护
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            # 指数退避 + 抖动
            sleep_s = min(2 ** attempt, 20) + random.random()
            print(f"[WARN] 请求失败 (attempt={attempt}/{max_retries}): {e}，{sleep_s:.1f}s 后重试")
            time.sleep(sleep_s)
    raise RuntimeError(f"多次重试仍失败: {last_err}")


def normalize_total(total_value: Any) -> int:
    # 示例里 total 是字符串 "860"
    if total_value is None:
        return 0
    if isinstance(total_value, (int, float)):
        return int(total_value)
    if isinstance(total_value, str):
        total_value = total_value.strip()
        if total_value.isdigit():
            return int(total_value)
    # 兜底：尝试转 int
    return int(total_value)


def fetch_all_to_csv(
    output_csv: str = "products_full.csv",
    page_size: int = 100,
    delay_range: tuple = (0.2, 0.6),  # 每页之间的随机延时，避免过快
) -> None:
    payload = dict(BASE_PAYLOAD)
    payload["pageSize"] = page_size

    headers = build_headers(COOKIE_STR)

    all_rows: List[Dict[str, Any]] = []

    with requests.Session() as session:
        # 先请求第 1 页，拿 total
        payload["pageNum"] = 1
        data = post_with_retry(session, API_URL, payload, headers=headers)

        total = normalize_total(data.get("total"))
        rows = data.get("rows") or []
        if not isinstance(rows, list):
            raise ValueError(f"响应 rows 不是 list：{type(rows)}")

        all_rows.extend(rows)
        if total == 0 and len(rows) == 0:
            # 常见原因：需要 cookie / 参数不对 / 被风控
            print("[WARN] total=0 且 rows 为空。若你确认站点有数据，请尝试把浏览器 cookie 填到 COOKIE_STR。")

        total_pages = max(1, math.ceil(total / page_size)) if total else 1
        print(f"[INFO] total={total}, pageSize={page_size}, 预计页数={total_pages}")

        # 从第 2 页开始拉取
        for page in range(2, total_pages + 1):
            payload["pageNum"] = page
            time.sleep(random.uniform(*delay_range))

            data = post_with_retry(session, API_URL, payload, headers=headers)
            rows = data.get("rows") or []
            if not isinstance(rows, list):
                raise ValueError(f"第 {page} 页响应 rows 不是 list：{type(rows)}")

            all_rows.extend(rows)
            print(f"[INFO] 第 {page}/{total_pages} 页：本页 {len(rows)} 条，累计 {len(all_rows)} 条")

            # 兜底：如果服务端 total 不靠谱，也可以根据空页提前结束
            if len(rows) == 0:
                print(f"[INFO] 第 {page} 页为空，提前结束。")
                break

    # JSON -> 表格
    df = pd.json_normalize(all_rows)

    # 去重（以 productId 为主；如果字段不存在，就按全行去重）
    if "productId" in df.columns:
        df = df.drop_duplicates(subset=["productId"])
    else:
        df = df.drop_duplicates()

    # 保存
    df.to_csv(output_csv, index=False, encoding="utf-8-sig")
    print(f"[DONE] 写入 {output_csv}，共 {len(df)} 条记录，列数 {df.shape[1]}")


if __name__ == "__main__":
    # 建议先用 100；若报错/超时，就降到 50 或 20
    fetch_all_to_csv(output_csv="products_full.csv", page_size=100)
