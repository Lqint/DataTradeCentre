# -*- coding: utf-8 -*-
"""
全量爬取 https://www.ahdexc.com/api/resource/searchProductByPage
并保存为 CSV

依赖:
  pip install requests pandas

用法:
  python crawl_ahdexc.py
"""

import time
import json
from typing import Any, Dict, List, Optional

import requests
import pandas as pd


API_URL = "https://www.ahdexc.com/api/resource/searchProductByPage"

# 你可以按需修改：pageSize 越大请求次数越少，但也可能触发风控
PAGE_SIZE = 150
SORT = "PayNum"

# 建议把浏览器里抓到的最新 cookie 粘贴到这里（尤其是 __jsluid_s 等可能会变）
COOKIE = (
    "__jsluid_s=6e465433347bb0f7b4c3b9782244077b; "
    "lbinsertroute=1d48ddd4ca08a248a500b0dcdc11e34c; "
    "Hm_lvt_886eeb4a32140acdf0c83410f051eb80=1769160267,1769259935; "
    "Hm_lpvt_886eeb4a32140acdf0c83410f051eb80=1769259935; "
    "HMACCOUNT=138539E7CD28F14F"
)

HEADERS = {
    "accept": "application/json, text/plain, */*",
    "content-type": "application/json",
    "origin": "https://www.ahdexc.com",
    "referer": "https://www.ahdexc.com/factorMarket",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0"
    ),
    "cookie": COOKIE,
}


def _flatten_product_tags(product_tag: Any) -> str:
    """
    将 productTag（list[dict]）压成字符串，便于写 CSV
    """
    if not product_tag:
        return ""
    if isinstance(product_tag, list):
        # 你示例里 tagId 看起来是中文标签名（如“数据类”）
        tags = []
        for t in product_tag:
            if isinstance(t, dict):
                tags.append(str(t.get("tagId") or t.get("proCommonTagId") or t.get("proTagId") or ""))
            else:
                tags.append(str(t))
        # 去空、去重、保序
        out = []
        seen = set()
        for x in tags:
            x = x.strip()
            if not x or x in seen:
                continue
            seen.add(x)
            out.append(x)
        return "|".join(out)
    return str(product_tag)


def _request_page(session: requests.Session, page_no: int) -> Dict[str, Any]:
    payload = {
        "sort": SORT,
        "tags": [],
        "searchKey": "",
        "resourceName": "",
        "pageNo": page_no,
        "pageSize": PAGE_SIZE,
    }

    resp = session.post(API_URL, headers=HEADERS, data=json.dumps(payload), timeout=60)
    # 常见：403/412/5xx 等
    resp.raise_for_status()

    data = resp.json()
    if not isinstance(data, dict):
        raise RuntimeError(f"Unexpected JSON: {type(data)}")

    # 业务 code 判断
    if data.get("code") != 0:
        raise RuntimeError(f"API returned code={data.get('code')}, msg={data.get('msg') or data.get('message')}")

    return data


def crawl_all(sleep_sec: float = 0.6, max_pages: Optional[int] = None) -> pd.DataFrame:
    """
    全量爬取，返回 DataFrame
    sleep_sec: 每页间隔，降低风控概率
    max_pages: 调试用，限制最多爬多少页（None 表示不限制）
    """
    rows: List[Dict[str, Any]] = []

    with requests.Session() as session:
        # 先请求第一页拿 totalPage/total
        first = _request_page(session, 1)
        d1 = first.get("data") or {}
        total = int(d1.get("total") or 0)
        total_page = int(d1.get("totalPage") or 0)
        size = int(d1.get("size") or PAGE_SIZE)

        print(f"[INFO] total={total}, totalPage={total_page}, pageSize(from server)={size}")

        def consume_list(lst: Any):
            if not lst:
                return
            if not isinstance(lst, list):
                raise RuntimeError(f"data.list is not a list: {type(lst)}")
            for item in lst:
                if not isinstance(item, dict):
                    continue

                # 复制一份，避免改原始结构
                r = dict(item)

                # 扁平化 productTag
                r["productTag_flat"] = _flatten_product_tags(r.get("productTag"))

                # 如果你想把整个 productTag 原样 JSON 丢进 CSV，也可以保留一列
                r["productTag_json"] = json.dumps(r.get("productTag", []), ensure_ascii=False)

                rows.append(r)

        consume_list(d1.get("list"))

        # 后续页
        pages = total_page if total_page > 0 else 1
        if max_pages is not None:
            pages = min(pages, max_pages)

        for page_no in range(2, pages + 1):
            time.sleep(sleep_sec)
            j = _request_page(session, page_no)
            d = j.get("data") or {}
            consume_list(d.get("list"))
            print(f"[INFO] fetched page {page_no}/{pages}, rows={len(rows)}")

    df = pd.DataFrame(rows)

    # 常见清理：把空列表/字典字段转成字符串，避免写 CSV 报错或变成乱格式
    for col in df.columns:
        # 若列里有 dict/list，转成 JSON 字符串
        if df[col].map(lambda x: isinstance(x, (dict, list))).any():
            df[col] = df[col].map(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (dict, list)) else x)

    return df


def main():
    df = crawl_all(sleep_sec=0.6)

    # 输出文件名可自定义
    out_csv = "ahdexc_products_full.csv"
    df.to_csv(out_csv, index=False, encoding="utf-8-sig")
    print(f"[DONE] saved: {out_csv}  (rows={len(df)}, cols={len(df.columns)})")


if __name__ == "__main__":
    main()
