import math
import time
import json
import random
import requests
import pandas as pd

API_URL = "https://www.nmdex.cn/nm/register/product/list"

# 你抓包里的 tokenp（如果失效，换成浏览器最新抓到的）
TOKENP = "9VOHVNPLPbDNSifV5VXeJPy1nOCQyhldjMgM8UeSLJg="

HEADERS = {
    "accept": "application/json, text/plain, */*",
    "content-type": "application/json;charset=UTF-8",
    "origin": "https://www.nmdex.cn",
    "referer": "https://www.nmdex.cn/",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0",
    "tokenp": TOKENP,
    "userid": "no_data",
    # 抓包里是 undefined，这里不传也行；某些后端会检查字段存在，你也可以保留：
    # "authorization": "undefined",
}

def safe_json(resp: requests.Response):
    """尽量稳地解析 JSON，失败时抛出更清晰的错误。"""
    try:
        return resp.json()
    except Exception:
        raise RuntimeError(f"Response not JSON. status={resp.status_code}, text={resp.text[:300]}")

def fetch_page(session: requests.Session, app_scene_type: int, page_num: int, page_size: int = 100, desc_type: str = "2"):
    payload = {
        "pageNum": page_num,
        "pageSize": page_size,
        "productOnShelves": "1",
        "descType": desc_type,
        "appSceneType": str(app_scene_type),
    }
    r = session.post(API_URL, headers=HEADERS, json=payload, timeout=30)
    r.raise_for_status()
    j = safe_json(r)
    # 你示例里 status=0 代表成功
    if j.get("status") != 0:
        raise RuntimeError(f"API returned non-zero status: {j}")
    return j["data"]

def flatten_record(rec: dict) -> dict:
    """把 productInfo/productDetail 扁平化到同一行，列名加前缀避免冲突。"""
    out = {}
    pi = (rec or {}).get("productInfo") or {}
    pd_ = (rec or {}).get("productDetail") or {}

    for k, v in pi.items():
        out[f"productInfo_{k}"] = v
    for k, v in pd_.items():
        out[f"productDetail_{k}"] = v

    # 如果某些字段是 JSON 字符串（如 '["1","2"]'），可以选择转成更可读的形式
    # 这里做一个温和处理：能 parse 就转成 Python list 再 dump 成字符串（更规范）
    for key in list(out.keys()):
        val = out[key]
        if isinstance(val, str) and val.startswith("[") and val.endswith("]"):
            try:
                parsed = json.loads(val)
                out[key] = json.dumps(parsed, ensure_ascii=False)
            except Exception:
                pass
    return out

def crawl_all(app_scene_types=range(1, 13), page_size=100, sleep_range=(0.2, 0.6), max_pages_safety=10000):
    rows = []
    seen_ids = set()

    with requests.Session() as s:
        for t in app_scene_types:
            print(f"\n=== appSceneType={t} ===")
            # 先请求第一页拿 count
            data1 = fetch_page(s, t, page_num=1, page_size=page_size)
            count = int(data1.get("count") or 0)
            records = data1.get("records") or []
            total_pages = math.ceil(count / page_size) if page_size > 0 else 0

            print(f"count={count}, page_size={page_size}, total_pages={total_pages}")

            def consume(recs):
                nonlocal rows
                for rec in recs:
                    flat = flatten_record(rec)
                    pid = flat.get("productInfo_productId") or flat.get("productDetail_productId")
                    # 去重：避免不同 appSceneType 或分页重复
                    if pid and pid in seen_ids:
                        continue
                    if pid:
                        seen_ids.add(pid)
                    rows.append(flat)

            consume(records)

            # 后续分页
            for page in range(2, min(total_pages, max_pages_safety) + 1):
                # 轻微随机延迟，减少被限流概率
                time.sleep(random.uniform(*sleep_range))
                data = fetch_page(s, t, page_num=page, page_size=page_size)
                recs = data.get("records") or []
                consume(recs)
                print(f"  page {page}/{total_pages}: +{len(recs)} (total_rows={len(rows)})")

    return rows

def main():
    rows = crawl_all(app_scene_types=range(1, 13), page_size=100)
    if not rows:
        print("No data fetched.")
        return

    df = pd.DataFrame(rows)

    # 输出文件名
    out_csv = "nmdex_products_full.csv"

    # utf-8-sig 方便 Excel 直接打开不乱码
    df.to_csv(out_csv, index=False, encoding="utf-8-sig")
    print(f"\nSaved: {out_csv}")
    print(f"Rows: {len(df)}, Cols: {len(df.columns)}")

if __name__ == "__main__":
    main()
