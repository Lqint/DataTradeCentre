# -*- coding: utf-8 -*-
"""
全量爬取 bbgdex productList 接口，保存 CSV
pip install requests pandas
"""

import math
import time
import random
import requests
import pandas as pd

BASE_URL = "https://www.bbgdex.com:9003/prod-api/td/front/home2/productList"

def fetch_page(session: requests.Session, page_num: int, page_size: int = 20, timeout: int = 20) -> dict:
    """
    拉取单页数据，返回 JSON（dict）
    """
    params = {
        "pageNum": page_num,
        "pageSize": page_size,
        # 下面这些参数你给的 URL 里是空值，保持一致即可
        "goodsName": "",
        "productTypeList": "",
        "productIndustryList": "",
        "productLabelList": "",
        "hotFlagSortDesc": "",
        "upAmallFlagSortDesc": "",
    }

    headers = {
        "Accept": "application/json, text/plain, */*",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0",
        "Referer": "https://www.bbgdex.com:9003/newDataMall/product/",
        # 如果接口需要 cookie / token，把它们加到这里：
        # "Cookie": "i18n_redirected=zh; ...",
        # "Authorization": "Bearer xxx",
    }

    resp = session.get(BASE_URL, params=params, headers=headers, timeout=timeout)
    resp.raise_for_status()

    data = resp.json()
    if not isinstance(data, dict) or "rows" not in data:
        raise ValueError(f"Unexpected response schema: {data}")

    return data


def crawl_all(page_size: int = 20, sleep_range=(0.2, 0.6), max_retries: int = 4) -> pd.DataFrame:
    """
    全量分页抓取，返回 DataFrame
    """
    session = requests.Session()

    # 先抓第一页拿 total
    first = fetch_page(session, page_num=1, page_size=page_size)
    total = int(first.get("total", 0))
    rows = first.get("rows", []) or []

    if total == 0:
        print("total=0，没有数据可抓取。")
        return pd.DataFrame()

    total_pages = math.ceil(total / page_size)
    print(f"total={total}, pageSize={page_size}, totalPages={total_pages}")

    all_rows = []
    all_rows.extend(rows)

    # 从第 2 页开始抓
    for page in range(2, total_pages + 1):
        for attempt in range(1, max_retries + 1):
            try:
                data = fetch_page(session, page_num=page, page_size=page_size)
                page_rows = data.get("rows", []) or []
                all_rows.extend(page_rows)

                print(f"[OK] page {page}/{total_pages} rows={len(page_rows)} collected={len(all_rows)}")
                break
            except Exception as e:
                if attempt == max_retries:
                    print(f"[FAIL] page {page} after {max_retries} retries: {e}")
                    raise
                backoff = (2 ** (attempt - 1)) + random.random()
                print(f"[RETRY] page {page} attempt={attempt}/{max_retries} err={e} sleep={backoff:.2f}s")
                time.sleep(backoff)

        # 轻微延迟，减少被风控概率
        time.sleep(random.uniform(*sleep_range))

    # 有些接口可能最后一页不足 pageSize；也可能 total 有变动
    # 这里尽量按实际抓到的为准
    df = pd.DataFrame(all_rows)
    print(f"done. collected rows={len(df)} (expected total={total})")
    return df


def main():
    df = crawl_all(page_size=20)
    if df.empty:
        return

    # 保存 CSV（utf-8-sig 兼容 Excel 直接打开不乱码）
    out_csv = "bbgdex_productList_all.csv"
    df.to_csv(out_csv, index=False, encoding="utf-8-sig")
    print(f"saved: {out_csv}  columns={len(df.columns)}")


if __name__ == "__main__":
    main()
