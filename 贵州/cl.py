# -*- coding: utf-8 -*-
import csv
import json
import os
import random
import time
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


BASE_URL = "https://www.gzdex.com.cn/apaas/serviceapp/v3/servicemarket/dataShop/list"

def build_session() -> requests.Session:
    """
    带重试的 requests Session
    """
    session = requests.Session()

    retries = Retry(
        total=6,
        backoff_factor=0.8,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=10, pool_maxsize=10)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def fetch_page(
    session: requests.Session,
    params: Dict[str, Any],
    headers: Dict[str, str],
    timeout: int = 20,
) -> Dict[str, Any]:
    r = session.get(BASE_URL, params=params, headers=headers, timeout=timeout)
    # 有些 WAF 会返回 200 但不是 JSON，这里做个防护
    try:
        return r.json()
    except Exception:
        raise RuntimeError(f"Response is not JSON. status={r.status_code}, text(head)={r.text[:300]}")


def append_jsonl(path: str, items: List[Dict[str, Any]]) -> None:
    with open(path, "a", encoding="utf-8") as f:
        for it in items:
            f.write(json.dumps(it, ensure_ascii=False) + "\n")


def write_csv(path: str, all_items: List[Dict[str, Any]]) -> None:
    # 统一字段：取所有 item 的 key 并集（字段多时 CSV 会很宽）
    fieldnames = sorted({k for it in all_items for k in it.keys()})
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for it in all_items:
            w.writerow(it)


def main():
    # ====== 你给的参数（可按需改）======
    serviceType1 = 479
    serviceType2s = ""
    serviceType3s = ""
    orderBy = 0
    serviceName = ""
    ifTryOut = ""
    size = 10

    # 从第几页开始（断点续跑时可改）
    start_page = 1

    # 输出文件
    out_jsonl = "gzdex_dataShop_list.jsonl"
    out_csv = "gzdex_dataShop_list.csv"

    # 是否每页都写 JSONL（推荐，避免中途断）
    stream_jsonl = True

    # ====== 抓包头（可按需改/补全）======
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
        "referer": "https://www.gzdex.com.cn/market/list",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0",
        # 重要：有些站点不带 cookie 会被 WAF 拦，按你抓包填
        "cookie": "HWWAFSESID=93d7f8bc33387ec0b6; HWWAFSESTIME=1769268383735; cart_id=454e573e-eb7d-4078-9d28-02881e116482",
    }

    # ====== 断点续跑：如果 JSONL 已存在，可自动跳过已写入条数对应的页数（简单版）======
    # 更严谨可记录 last_page 到一个 state.json，这里给你一个简单可用的逻辑：
    if stream_jsonl and os.path.exists(out_jsonl) and start_page == 1:
        # 通过已写入行数估算已爬页数（每页 size 条，最后一页可能不足，估算只用于减少重复）
        with open(out_jsonl, "r", encoding="utf-8") as f:
            existing_lines = sum(1 for _ in f)
        if existing_lines > 0:
            start_page = max(1, existing_lines // size)
            print(f"[Resume] Detected {existing_lines} existing items in {out_jsonl}, set start_page={start_page}")

    session = build_session()

    all_items: List[Dict[str, Any]] = []
    page = start_page
    total = 0

    while True:
        params = {
            "serviceType1": serviceType1,
            "serviceType2s": serviceType2s,
            "serviceType3s": serviceType3s,
            "orderBy": orderBy,
            "serviceName": serviceName,
            "ifTryOut": ifTryOut,
            "page": page,
            "size": size,
        }

        data = fetch_page(session, params=params, headers=headers)

        # 兼容你示例：{"success":1,"data":[...]}
        success = data.get("success")
        items = data.get("data") or []

        # 如果遇到 success != 1 或返回结构异常，直接报出来方便你定位
        if success not in (1, True, "1"):
            # 有些接口失败仍可能返回 msg/code
            raise RuntimeError(f"API indicated failure at page={page}: {data}")

        if not isinstance(items, list):
            raise RuntimeError(f"Unexpected data type at page={page}: data['data'] is {type(items)}")

        if len(items) == 0:
            print(f"[Done] page={page} returned 0 items. Stop.")
            break

        total += len(items)
        print(f"[OK] page={page}, got={len(items)}, total={total}")

        # 写入（推荐每页写 JSONL，稳）
        if stream_jsonl:
            append_jsonl(out_jsonl, items)
        else:
            all_items.extend(items)

        # 友好限速：随机睡眠，降低触发风控概率
        time.sleep(random.uniform(0.5, 1.2))

        page += 1

    # 如果没做流式写，则这里统一写出
    if not stream_jsonl:
        append_jsonl(out_jsonl, all_items)

    # 为了写 CSV，我们需要把所有数据读出来（若流式写了）
    if stream_jsonl:
        all_items = []
        with open(out_jsonl, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    all_items.append(json.loads(line))

    write_csv(out_csv, all_items)

    print(f"\nSaved:\n- {out_jsonl} ({len(all_items)} items)\n- {out_csv}\n")


if __name__ == "__main__":
    main()
