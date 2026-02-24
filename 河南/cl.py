import time
import random
import csv
from typing import Any, Dict, List, Optional

import requests


API_URL = "https://market.zzbdex.com/data-deal-admin/frontDeskHomePage/frontPageProduct"
BASE_ORIGIN = "https://market.zzbdex.com"
REFERER = "https://market.zzbdex.com/trade/product"

# 你提供的负载模板（可按需改筛选条件）
BASE_PAYLOAD = {
    "pageNum": 1,
    "pageSize": 100,  # 建议调大减少请求次数（如 200/500），太大可能被限流
    "sceneCode": "",
    "tags": "",
    "siteCode": "",
    "regionalZone": "",
    "productType": "",
    "dataType": "",
    "orderType": 0,
    "orderByComprehensive": 1,
}

# 如果站点需要 cookie 才能访问，把浏览器里复制到这里（可留空）
COOKIE = ""  # 例如："Hm_lvt_xxx=...; HWWAFSESID=...; ..."


def build_headers() -> Dict[str, str]:
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Origin": BASE_ORIGIN,
        "Referer": REFERER,
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
    }
    if COOKIE:
        headers["Cookie"] = COOKIE
    return headers


def request_page(session: requests.Session, payload: Dict[str, Any], retries: int = 5) -> Dict[str, Any]:
    last_err: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            resp = session.post(API_URL, json=payload, headers=build_headers(), timeout=20)
            # 常见反爬/鉴权失败提示
            if resp.status_code in (401, 403):
                raise RuntimeError(f"HTTP {resp.status_code}：可能需要登录/鉴权/正确Cookie")
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            last_err = e
            sleep_s = min(2 ** attempt, 20) + random.uniform(0, 0.8)
            print(f"[WARN] 第{attempt}次请求失败：{e}，{sleep_s:.1f}s 后重试...")
            time.sleep(sleep_s)
    raise RuntimeError(f"请求失败，已重试 {retries} 次，最后错误：{last_err}")


def normalize_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    """把 record 扁平化/补全字段（例如 picture 拼成完整URL）"""
    out = dict(rec)

    pic = out.get("picture")
    if pic and isinstance(pic, str) and not pic.startswith("http"):
        # 常见情况：返回相对路径
        out["picture_url"] = f"{BASE_ORIGIN}/{pic.lstrip('/')}"
    else:
        out["picture_url"] = pic

    return out


def save_csv(path: str, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        print("[INFO] 没有数据可写入。")
        return

    # 统一列名（取所有键的并集）
    fieldnames = []
    seen = set()
    for r in rows:
        for k in r.keys():
            if k not in seen:
                seen.add(k)
                fieldnames.append(k)

    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)

    print(f"[OK] 已写入 {len(rows)} 行 -> {path}")


def crawl_all(output_csv: str = "frontPageProduct_all.csv") -> None:
    session = requests.Session()

    page = 1
    page_size = int(BASE_PAYLOAD.get("pageSize", 100))
    all_rows: List[Dict[str, Any]] = []

    while True:
        payload = dict(BASE_PAYLOAD)
        payload["pageNum"] = page

        data = request_page(session, payload)
        if data.get("code") != 0:
            raise RuntimeError(f"接口返回异常：code={data.get('code')}, msg={data.get('msg')}")

        records = (((data.get("data") or {}).get("records")) or [])
        print(f"[INFO] page={page}, records={len(records)}")

        if not records:
            break

        for rec in records:
            all_rows.append(normalize_record(rec))

        # 如果不足一页，通常意味着到末尾
        if len(records) < page_size:
            break

        page += 1
        # 温和降速，避免触发风控
        time.sleep(random.uniform(0.3, 0.9))

    save_csv(output_csv, all_rows)


if __name__ == "__main__":
    crawl_all("frontPageProduct_all.csv")
