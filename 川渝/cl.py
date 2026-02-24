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

API_URL = "https://trade-operator.westdex.com.cn/dop/market/goods/list"

OUT_JSONL = "westdex_goods_list.jsonl"
OUT_CSV = "westdex_goods_list.csv"
CHECKPOINT = "westdex_checkpoint.json"
ERR_LOG = "westdex_errors.log"


def log_err(msg: str) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    with open(ERR_LOG, "a", encoding="utf-8") as f:
        f.write(f"[{ts}] {msg}\n")


def build_session() -> requests.Session:
    session = requests.Session()

    # urllib3 的重试：只负责连接/部分状态码重试（更底层的）
    retries = Retry(
        total=4,                  # 底层重试别太多，业务层我们还会再控
        connect=4,
        read=4,
        backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["POST"],
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=10, pool_maxsize=10)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def safe_get_result(resp: Dict[str, Any]) -> List[Dict[str, Any]]:
    data = resp.get("data") or {}
    inner = data.get("data") or {}
    result = inner.get("result") or []
    if not isinstance(result, list):
        raise RuntimeError(f"Unexpected result type: {type(result)}; resp keys={list(resp.keys())}")
    return result


def append_jsonl(path: str, items: List[Dict[str, Any]]) -> None:
    with open(path, "a", encoding="utf-8") as f:
        for it in items:
            f.write(json.dumps(it, ensure_ascii=False) + "\n")


def write_csv(path: str, all_items: List[Dict[str, Any]]) -> None:
    fieldnames = sorted({k for it in all_items for k in it.keys()})
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for it in all_items:
            w.writerow(it)


def load_checkpoint() -> int:
    if not os.path.exists(CHECKPOINT):
        return 1
    try:
        with open(CHECKPOINT, "r", encoding="utf-8") as f:
            obj = json.load(f)
        p = int(obj.get("page", 1))
        return max(1, p)
    except Exception:
        return 1


def save_checkpoint(page: int) -> None:
    with open(CHECKPOINT, "w", encoding="utf-8") as f:
        json.dump({"page": page, "ts": time.time()}, f, ensure_ascii=False, indent=2)


def is_json_response(r: requests.Response) -> bool:
    ctype = (r.headers.get("Content-Type") or "").lower()
    return "application/json" in ctype or "application/json;" in ctype


def post_page_robust(
    session: requests.Session,
    payload: Dict[str, Any],
    headers: Dict[str, str],
    timeout: int = 25,
    max_attempts: int = 8,
) -> Optional[Dict[str, Any]]:
    """
    返回：
      - 成功：dict(JSON)
      - 多次失败：None（让主循环决定怎么处理：跳过/退出）
    """
    for attempt in range(1, max_attempts + 1):
        try:
            # 每次请求轻微抖动，降低固定节奏被识别概率
            time.sleep(random.uniform(0.2, 0.9))

            r = session.post(API_URL, json=payload, headers=headers, timeout=timeout)

            status = r.status_code
            head = (r.text or "")[:180].replace("\n", " ").replace("\r", " ")

            # 403/401：通常是风控/鉴权，不要 json()，先退避
            if status in (401, 403):
                log_err(f"HTTP {status} attempt={attempt} payload.page={payload.get('pageNum')} head={head}")

                # 逐步加长等待，带随机（指数退避 + 抖动）
                wait = min(120, (2 ** attempt) + random.uniform(1.0, 5.0))
                time.sleep(wait)

                # 有些 WAF 会针对连接/会话特征，失败几次后重建 session
                if attempt in (3, 5):
                    try:
                        session.close()
                    except Exception:
                        pass
                    session = build_session()
                continue

            # 429：被限流，尊重 Retry-After 或退避
            if status == 429:
                ra = r.headers.get("Retry-After")
                if ra and ra.isdigit():
                    wait = int(ra) + random.uniform(0.5, 2.0)
                else:
                    wait = min(90, (1.8 ** attempt) + random.uniform(1.0, 4.0))
                log_err(f"HTTP 429 attempt={attempt} wait={wait:.1f}s page={payload.get('pageNum')}")
                time.sleep(wait)
                continue

            # 其它非 2xx：记录并重试
            if status < 200 or status >= 300:
                log_err(f"HTTP {status} attempt={attempt} page={payload.get('pageNum')} head={head}")
                time.sleep(min(60, (1.6 ** attempt) + random.uniform(0.5, 2.0)))
                continue

            # 2xx 但不是 JSON：可能返回了 HTML（例如风控页），也按失败处理
            if not is_json_response(r):
                log_err(f"Non-JSON 2xx attempt={attempt} page={payload.get('pageNum')} ctype={r.headers.get('Content-Type')} head={head}")
                time.sleep(min(60, (1.6 ** attempt) + random.uniform(0.5, 2.0)))
                continue

            # 解析 JSON（这里仍可能 JSONDecodeError）
            try:
                data = r.json()
            except Exception as e:
                log_err(f"JSONDecodeError attempt={attempt} page={payload.get('pageNum')} err={repr(e)} head={head}")
                time.sleep(min(60, (1.6 ** attempt) + random.uniform(0.5, 2.0)))
                continue

            # 业务 code 判断：失败也可以重试几次（有时网关抖）
            code = data.get("code")
            if code != 10000:
                log_err(f"API code!=10000 attempt={attempt} page={payload.get('pageNum')} code={code} msg={data.get('message')}")
                time.sleep(min(60, (1.4 ** attempt) + random.uniform(0.5, 2.0)))
                continue

            return data

        except (requests.Timeout, requests.ConnectionError) as e:
            log_err(f"NetworkError attempt={attempt} page={payload.get('pageNum')} err={repr(e)}")
            time.sleep(min(60, (1.6 ** attempt) + random.uniform(0.5, 2.0)))
            continue

        except Exception as e:
            # 未知错误也别直接崩，记录一下再重试
            log_err(f"UnknownError attempt={attempt} page={payload.get('pageNum')} err={repr(e)}")
            time.sleep(min(60, (1.6 ** attempt) + random.uniform(0.5, 2.0)))
            continue

    return None


def main():
    base_payload = {
        "goodsType": "",
        "dataIndustryCategory": "",
        "dataZone": "",
        "dataDomain": "",
        "orderField": "publishTime",
        "sortOrder": "desc",
        "keyword": "",
        "pageNum": 1,
        "pageSize": 50,
    }

    headers = {
        "accept": "application/json, text/plain, */*",
        "content-type": "application/json",
        "origin": "https://westdex.com.cn",
        "referer": "https://westdex.com.cn/",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
        "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",

        # 如果接口需要登录态，把你抓包里的值填进来
        # "token": "xxxxx",
        # "authorization": "Bearer xxxxx",
    }

    # 从 checkpoint 断点续跑
    page = load_checkpoint()
    print(f"[Start] from page={page}")

    session = build_session()

    # 去重：如果你要“跨断点去重”，需要从 jsonl 读一遍 goodsId
    seen_goods_ids = set()
    if os.path.exists(OUT_JSONL):
        try:
            with open(OUT_JSONL, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                        gid = obj.get("goodsId")
                        if gid is not None:
                            seen_goods_ids.add(gid)
                    except Exception:
                        continue
            print(f"[Resume] loaded seen_goods_ids={len(seen_goods_ids)}")
        except Exception as e:
            log_err(f"Failed to load existing jsonl for dedupe: {repr(e)}")

    total_new = 0

    while True:
        payload = dict(base_payload)
        payload["pageNum"] = page

        resp = post_page_robust(session, payload=payload, headers=headers)

        # 多次失败：不直接崩，保存 checkpoint 后退出，方便你下次继续跑
        if resp is None:
            save_checkpoint(page)
            print(f"[Stop] page={page} failed too many times. Check {ERR_LOG} and resume later.")
            break

        items = safe_get_result(resp)
        if not items:
            print(f"[Done] page={page} returned 0 result. Stop.")
            # 到尾页了也保存一下
            save_checkpoint(page)
            break

        new_items = []
        for it in items:
            gid = it.get("goodsId")
            if gid is None:
                new_items.append(it)
                continue
            if gid in seen_goods_ids:
                continue
            seen_goods_ids.add(gid)
            new_items.append(it)

        if new_items:
            append_jsonl(OUT_JSONL, new_items)
            total_new += len(new_items)

        print(f"[OK] page={page}, got={len(items)}, new={len(new_items)}, total_new={total_new}")

        # 成功拿到这一页后再保存 checkpoint（保证断点更准确）
        save_checkpoint(page + 1)

        # 主循环再加一次更慢的限速（整体节奏更“人”）
        time.sleep(random.uniform(0.8, 2.2))
        page += 1

    # 写 CSV
    all_items: List[Dict[str, Any]] = []
    if os.path.exists(OUT_JSONL):
        with open(OUT_JSONL, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        all_items.append(json.loads(line))
                    except Exception:
                        continue

    write_csv(OUT_CSV, all_items)
    print(f"\nSaved:\n- {OUT_JSONL} ({len(all_items)} items)\n- {OUT_CSV}\n- checkpoint: {CHECKPOINT}\n- errors: {ERR_LOG}\n")


if __name__ == "__main__":
    main()
