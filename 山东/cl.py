import csv
import json
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Iterable

import requests


BASE_URL = "https://www.sddep.com/server/product/noauth/sjjygoods/noauthList"
DEFAULT_SIZE = 9

# 你可以按需改：请求间隔、超时、最大重试
SLEEP_SECONDS = 0.6
TIMEOUT = 15
MAX_RETRIES = 4


@dataclass
class CrawlConfig:
    size: int = DEFAULT_SIZE
    goodsType: str = ""          # 接口允许空
    goodsClassify: str = ""      # 接口允许空
    plat_sources: Iterable[int] = (0, 1)
    out_prefix: str = "sddep_goods"


def _make_session() -> requests.Session:
    s = requests.Session()
    # 这些 header 基本足够（不强依赖 cookie）
    s.headers.update({
        "accept": "application/json, text/plain, */*",
        "user-agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "referer": "https://www.sddep.com/gjdt",
        "origin": "https://www.sddep.com",
    })
    return s


def _request_json(session: requests.Session, params: Dict[str, Any]) -> Dict[str, Any]:
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(BASE_URL, params=params, timeout=TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            last_err = e
            # 简单退避
            time.sleep(min(2.0, 0.5 * attempt))
    raise RuntimeError(f"请求失败，params={params}, err={last_err!r}")


def _extract_list(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    你的示例里是 payload["list"]。
    为了兼容可能的结构变化，这里做几个候选。
    """
    if isinstance(payload.get("list"), list):
        return payload["list"]

    data = payload.get("data")
    if isinstance(data, dict):
        if isinstance(data.get("list"), list):
            return data["list"]
        if isinstance(data.get("records"), list):
            return data["records"]

    # 兜底：找第一个 list 值
    for k, v in payload.items():
        if isinstance(v, list):
            return v
    return []


def _detect_total_pages(payload: Dict[str, Any], size: int) -> Optional[int]:
    """
    尝试从常见字段推断总页数：total/records/pages等。
    推断不到返回 None。
    """
    candidates = []

    # 顶层
    for key in ("pages", "totalPages", "pageCount"):
        v = payload.get(key)
        if isinstance(v, int) and v > 0:
            return v

    # data层
    data = payload.get("data")
    if isinstance(data, dict):
        for key in ("pages", "totalPages", "pageCount"):
            v = data.get(key)
            if isinstance(v, int) and v > 0:
                return v
        for key in ("total", "totalCount", "records", "recordCount"):
            v = data.get(key)
            if isinstance(v, int) and v >= 0:
                candidates.append(v)

    # 顶层 total
    for key in ("total", "totalCount", "records", "recordCount"):
        v = payload.get(key)
        if isinstance(v, int) and v >= 0:
            candidates.append(v)

    if candidates:
        total = max(candidates)
        pages = (total + size - 1) // size
        return pages if pages > 0 else 1

    return None


def crawl_one_platsource(cfg: CrawlConfig, plat_source: int) -> List[Dict[str, Any]]:
    session = _make_session()

    all_items: List[Dict[str, Any]] = []
    page = 1
    total_pages = None

    while True:
        params = {
            "page": page,
            "size": cfg.size,
            "goodsType": cfg.goodsType,
            "goodsClassify": cfg.goodsClassify,
            "platSource": plat_source,
        }

        payload = _request_json(session, params=params)

        # 可选：检查 status/message
        status = payload.get("status")
        if status not in (200, None):
            raise RuntimeError(f"接口返回非200 status={status}, payload={payload}")

        items = _extract_list(payload)
        if total_pages is None:
            total_pages = _detect_total_pages(payload, cfg.size)

        if not items:
            # 没数据就结束
            break

        all_items.extend(items)

        # 结束条件1：已知总页数
        if isinstance(total_pages, int) and page >= total_pages:
            break

        # 结束条件2：返回数量不足一页（常见分页结束信号）
        if len(items) < cfg.size:
            break

        page += 1
        time.sleep(SLEEP_SECONDS)

    return all_items


def save_jsonl(path: str, rows: List[Dict[str, Any]]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def save_csv(path: str, rows: List[Dict[str, Any]], fieldnames: Optional[List[str]] = None) -> None:
    if not rows:
        # 写一个空文件也行
        with open(path, "w", encoding="utf-8", newline="") as f:
            f.write("")
        return

    # 自动字段：取所有key并集（稳定排序：先常见字段，再其余）
    if fieldnames is None:
        common = [
            "id", "goodsName", "goodsType", "goodsClassify", "amount", "uploadTime",
            "goodsImageUrl", "click", "website", "isCharge", "supplierName",
            "goodsValue", "platSource", "platformName", "prefectureId", "pushSource"
        ]
        keys = set()
        for r in rows:
            keys.update(r.keys())
        rest = sorted([k for k in keys if k not in common])
        fieldnames = [k for k in common if k in keys] + rest

    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            # list/dict 之类转成 json 字符串，避免 CSV 乱套
            row = {}
            for k in fieldnames:
                v = r.get(k)
                if isinstance(v, (list, dict)):
                    row[k] = json.dumps(v, ensure_ascii=False)
                else:
                    row[k] = v
            w.writerow(row)


def main():
    cfg = CrawlConfig()

    merged: List[Dict[str, Any]] = []
    for ps in cfg.plat_sources:
        items = crawl_one_platsource(cfg, ps)
        print(f"platSource={ps} 抓取数量: {len(items)}")
        merged.extend(items)

        jsonl_path = f"{cfg.out_prefix}_platSource{ps}.jsonl"
        csv_path = f"{cfg.out_prefix}_platSource{ps}.csv"
        save_jsonl(jsonl_path, items)
        save_csv(csv_path, items)
        print(f"已保存: {jsonl_path}, {csv_path}")

    # 去重（按 id）
    uniq = {}
    for r in merged:
        rid = r.get("id")
        uniq[rid if rid is not None else json.dumps(r, ensure_ascii=False)] = r
    merged_uniq = list(uniq.values())

    save_jsonl(f"{cfg.out_prefix}_ALL_UNIQ.jsonl", merged_uniq)
    save_csv(f"{cfg.out_prefix}_ALL_UNIQ.csv", merged_uniq)
    print(f"合并去重后总数: {len(merged_uniq)}")
    print("已保存: "
          f"{cfg.out_prefix}_ALL_UNIQ.jsonl, {cfg.out_prefix}_ALL_UNIQ.csv")


if __name__ == "__main__":
    main()
