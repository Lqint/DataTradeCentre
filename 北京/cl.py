import time
import json
import requests
import pandas as pd

URL = "https://webs.bjidex.com/api/dstp/data-asset-server/dataProduct/deal/list"

# 你抓包里看到的必要请求头（尽量保持最小可用）
HEADERS = {
    "accept": "application/json, text/plain, */*",
    "content-type": "application/json",
    "origin": "https://webs.bjidex.com",
    "referer": "https://webs.bjidex.com/sys-bsc-home/",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0",
    # 你给的这俩很关键（网关/应用标识）
    "cityos-application-code": "icos-bip",
    "x-os-system-code": "icos-bip",
}

# 如果接口需要登录态/风控校验，可能还需要 Cookie 或 authToken
# 把你浏览器里复制的 Cookie 粘到这里（若不需要可留空）
COOKIE = "Hm_lvt_b7cf45c70d25b46632335bfbff44b232=1769157933,1769158231,1769167537,1769228019; HMACCOUNT=138539E7CD28F14F; Hm_lpvt_b7cf45c70d25b46632335bfbff44b232=1769246142; e66c8c38-7461-4bae-89f2-739df527c691=WyIzNDQ1ODQ0MTIyIl0"  # 例如: "Hm_lvt_xxx=...; e66c8c38-...=..."

def fetch_page(page_num: int, page_size: int, order_query_key="new_query_key", supply_flag=1):
    payload = {
        "pageNum": page_num,
        "pageSize": page_size,
        "timestamp": int(time.time() * 1000),
        "orderQueryKey": order_query_key,
        "supplyFlag": supply_flag,
        "viewCode": 1,
    }

    headers = dict(HEADERS)
    if COOKIE:
        headers["cookie"] = COOKIE

    resp = requests.post(URL, headers=headers, data=json.dumps(payload), timeout=30)
    resp.raise_for_status()
    data = resp.json()

    if data.get("code") != 200:
        raise RuntimeError(f"API返回异常: {data.get('code')} {data.get('message')}")

    lst = (((data or {}).get("data") or {}).get("list")) or []
    return lst

def normalize_record(rec: dict):
    """把一些嵌套/列表字段转成更适合 CSV 的形式"""
    rec = dict(rec)

    # 关键词列表 => 用 | 连接
    if isinstance(rec.get("productKeywords"), list):
        rec["productKeywords"] = "|".join([str(x) for x in rec["productKeywords"]])

    # 你也可以在这里做更多清洗，比如去HTML标签等
    return rec

def crawl_all(page_size=50, sleep_sec=0.3, max_pages=10000):
    all_rows = []
    page_num = 1

    while page_num <= max_pages:
        lst = fetch_page(page_num, page_size)
        if not lst:
            break

        all_rows.extend([normalize_record(x) for x in lst])

        # 如果这一页数量不足 page_size，通常说明到末尾了
        if len(lst) < page_size:
            break

        page_num += 1
        time.sleep(sleep_sec)

    return all_rows

def main():
    rows = crawl_all(page_size=50, sleep_sec=0.3)
    if not rows:
        print("没有抓到数据。若接口要求登录/鉴权，请在脚本里填 COOKIE 或相关 token。")
        return

    df = pd.DataFrame(rows)

    # 输出 CSV（utf-8-sig 方便 Excel 打开不乱码）
    out_csv = "data_products.csv"
    df.to_csv(out_csv, index=False, encoding="utf-8-sig")
    print(f"抓取完成：{len(df)} 条，已保存到 {out_csv}")

if __name__ == "__main__":
    main()
