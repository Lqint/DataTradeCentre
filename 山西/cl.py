# -*- coding: utf-8 -*-
"""
全量爬取 https://www.sxdataex.com/api/resource/searchProductByPage
保存为 CSV（包含商品字段 + productTag 扁平化字段）

依赖：
    pip install requests pandas

用法：
    python crawl_sxdataex_products.py
"""

import json
import time
import random
from typing import Any, Dict, List, Tuple

import requests
import pandas as pd


API_URL = "https://www.sxdataex.com/api/resource/searchProductByPage"

# 你给的负载里 pageSize 是字符串 "12"，接口一般也能吃 int，这里统一用 int，避免奇怪类型问题
DEFAULT_PAYLOAD = {
    "sort": "Default",
    "tags": [],
    "searchKey": "",
    "pageNo": 1,
    "pageSize": 12,
}

HEADERS = {
    "accept": "application/json, text/plain, */*",
    "content-type": "application/json",
    "origin": "https://www.sxdataex.com",
    "referer": "https://www.sxdataex.com/lzportal/factorMarket",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0"
    ),
}

TIMEOUT = 20
MAX_RETRIES = 5


def request_page(session: requests.Session, payload: Dict[str, Any]) -> Dict[str, Any]:
    """请求单页数据，带重试。"""
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.post(
                API_URL,
                headers=HEADERS,
                data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
                timeout=TIMEOUT,
            )
            # 有些站会返回 200 但 code 非 0；也可能 4xx/5xx
            resp.raise_for_status()
            data = resp.json()
            return data
        except Exception as e:
            last_err = e
            # 指数退避 + 抖动
            sleep_s = min(2 ** attempt, 20) + random.uniform(0.2, 0.8)
            print(f"[WARN] 第{payload.get('pageNo')}页请求失败(第{attempt}次)：{e}，{sleep_s:.1f}s 后重试")
            time.sleep(sleep_s)
    raise RuntimeError(f"请求第{payload.get('pageNo')}页失败，已重试{MAX_RETRIES}次。最后错误：{last_err}")


def flatten_product_tags(item: Dict[str, Any]) -> Dict[str, Any]:
    """
    扁平化 productTag：
    - 输出 tags_domains / tags_industry / tags_sceneClassification 等（按 proCommonTagType 聚合）
    - 每个字段用 '|' 拼接 tagId（你示例里 tagId 是中文名）
    """
    tags = item.get("productTag") or []
    grouped: Dict[str, List[str]] = {}
    for t in tags:
        ttype = (t.get("proCommonTagType") or "").strip()
        tname = (t.get("tagId") or "").strip()
        if not ttype or not tname:
            continue
        grouped.setdefault(ttype, []).append(tname)

    # 去重但保序
    def uniq(seq: List[str]) -> List[str]:
        seen = set()
        out = []
        for x in seq:
            if x not in seen:
                seen.add(x)
                out.append(x)
        return out

    flat = {}
    for k, v in grouped.items():
        flat[f"tags_{k}"] = "|".join(uniq(v))

    flat["tags_all"] = "|".join(
        uniq([name for names in grouped.values() for name in names])
    )
    flat["productTag_count"] = len(tags)
    return flat


def normalize_item(item: Dict[str, Any]) -> Dict[str, Any]:
    """
    规范化单条记录：
    - 保留常用字段
    - 展开 tags
    - 额外保留原始 JSON（可选：太大可删）
    """
    base_fields = [
        "proResourceId",
        "proResourceName",
        "proResourceDesc",
        "proResourceType",
        "companyName",
        "companyLogo",
        "itemPrice",
        "views",
        "score",
        "payNum",
        "launchDate",
    ]
    row = {k: item.get(k) for k in base_fields}

    # 展开 tag 聚合字段
    row.update(flatten_product_tags(item))

    # 如你想保留更多字段，可以把 item 直接合并（不建议全塞，列会非常多）
    # row.update({f"raw_{k}": v for k, v in item.items() if k not in row})

    return row


def crawl_all(page_size: int = 12, sleep_range: Tuple[float, float] = (0.4, 1.2)) -> pd.DataFrame:
    """
    全量爬取：
    1) 先请求第1页拿 totalPage
    2) 遍历 1..totalPage
    """
    session = requests.Session()

    all_rows: List[Dict[str, Any]] = []

    # 第1页
    payload = dict(DEFAULT_PAYLOAD)
    payload["pageSize"] = page_size
    payload["pageNo"] = 1

    first = request_page(session, payload)

    if first.get("code") != 0:
        raise RuntimeError(f"接口返回 code != 0：{first}")

    data = first.get("data") or {}
    total_page = int(data.get("totalPage") or 0)
    current = int(data.get("current") or 1)
    lst = data.get("list") or []

    print(f"[INFO] 第{current}页 / 共{total_page}页，当前返回 {len(lst)} 条")
    for item in lst:
        all_rows.append(normalize_item(item))

    # 后续页
    for page in range(2, total_page + 1):
        payload["pageNo"] = page
        js = request_page(session, payload)
        if js.get("code") != 0:
            print(f"[WARN] 第{page}页 code != 0，跳过：{js.get('code')}")
            continue

        d = js.get("data") or {}
        lst = d.get("list") or []
        print(f"[INFO] 第{page}页 / 共{total_page}页，返回 {len(lst)} 条")
        for item in lst:
            all_rows.append(normalize_item(item))

        # 友好限速
        time.sleep(random.uniform(*sleep_range))

    df = pd.DataFrame(all_rows)

    # 去重（以 proResourceId 为主键）
    if "proResourceId" in df.columns:
        df = df.drop_duplicates(subset=["proResourceId"], keep="first")

    return df


def main():
    df = crawl_all(page_size=12)

    out_csv = "sxdataex_products_full.csv"
    # utf-8-sig 方便 Excel 直接打开不乱码
    df.to_csv(out_csv, index=False, encoding="utf-8-sig")

    print(f"[DONE] 共抓取 {len(df)} 条，已保存：{out_csv}")


if __name__ == "__main__":
    main()
