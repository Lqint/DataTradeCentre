import csv
import json
import os
import random
import time
from typing import Any, Dict, Iterable, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


BASE = "https://www.bigdatadex.com.cn/bdup/api/op/commodity/application/findAllPage"

DEFAULT_PARAMS = {
    "searchName": "",
    "supplyAndDemand": "",
    "orderType": "0",
    "pageNo": "1",
    "pageSize": "8",
}

HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
    "connection": "keep-alive",
    "referer": "https://www.bigdatadex.com.cn/dataApplication/list?dating=1",
    "sec-ch-ua": '"Not(A:Brand";v="8", "Chromium";v="144", "Microsoft Edge";v="144"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "tenant_id": "0",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0",
}

COOKIE_STRING = (
    "__jsluid_s=0ccb1515bde4469c6d9bb96b142e66d5; "
    "SECKEY_ABVK=nnRxSPImQ8RZsEHFhhI/sBRRHD9ub9JK8H432I+glAiu8g9jjq3rLmeFUube+s8813yjmGi3NddY101NWj11jg%3D%3D; "
    "BMAP_SECKEY=nnRxSPImQ8RZsEHFhhI_sBRRHD9ub9JK8H432I-glAiXPc0i_SCAh8lgExFnKn-0w26cAa-B_cUMHkoc1YHY9N65Rbbiuqpc_rfogWFcPqebxbM5Q-S41vBusTnhq1tQVU2dTO0yCLBJkZ3RCn87rw1mU5ckSkIppM-pkYqfl6P_-9JsmTPTvWfBsVJjBkf8s6yT80N2iS86vBiR81OY3Ub9KX88tk-8cabvmAUr2GY"
)

# 断点续跑的进度文件
PROGRESS_PATH = "bigdatadex_progress.json"


def cookie_str_to_dict(cookie_str: str) -> Dict[str, str]:
    cookies: Dict[str, str] = {}
    for part in cookie_str.split(";"):
        part = part.strip()
        if not part or "=" not in part:
            continue
        k, v = part.split("=", 1)
        cookies[k.strip()] = v.strip()
    return cookies


def build_session() -> requests.Session:
    """
    更稳的 Session：
    - 连接池复用
    - 针对常见临时失败做自动重试（429/5xx/连接问题等）
    """
    s = requests.Session()
    s.headers.update(HEADERS)
    s.cookies.update(cookie_str_to_dict(COOKIE_STRING))

    retry = Retry(
        total=6,
        connect=6,
        read=6,
        status=6,
        backoff_factor=0.6,  # 指数退避因子（配合抖动更稳）
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,  # 我们自己判断 status_code
        respect_retry_after_header=True,
    )

    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


def is_likely_blocked(resp: requests.Response) -> Tuple[bool, str]:
    """
    粗略识别：被拦截/风控验证页/登录失效等
    """
    ct = (resp.headers.get("content-type") or "").lower()
    text_head = (resp.text or "")[:300].lower()

    # 非 JSON 或者出现典型 HTML 片段
    if "application/json" not in ct:
        if "<html" in text_head or "<!doctype html" in text_head:
            return True, f"Non-JSON HTML response (ct={ct})"
        return True, f"Non-JSON response (ct={ct})"

    # JSON 但内容像风控提示
    try:
        data = resp.json()
    except Exception:
        return True, "JSON parse failed"

    # 你原始接口约定 success=True 才正常
    if isinstance(data, dict) and data.get("success") is False:
        # 有些站会返回 msg/code 表示未登录/风控
        msg = str(data.get("msg") or data.get("message") or "")[:120]
        return True, f"API success=false, msg={msg}"

    return False, ""


def fetch_page(
    session: requests.Session,
    page_no: int,
    page_size: int,
    params_overrides: Optional[Dict[str, Any]] = None,
    timeout: Tuple[int, int] = (6, 25),  # (connect timeout, read timeout)
) -> Dict[str, Any]:
    params = dict(DEFAULT_PARAMS)
    params["pageNo"] = str(page_no)
    params["pageSize"] = str(page_size)
    if params_overrides:
        params.update({k: str(v) for k, v in params_overrides.items()})

    resp = session.get(BASE, params=params, timeout=timeout)

    # 这里不依赖 urllib3 的 raise_for_status，我们自己做更细判断
    if resp.status_code != 200:
        # 429/5xx 可能已经被重试过，仍失败就抛出明确错误
        raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:200]}")

    blocked, reason = is_likely_blocked(resp)
    if blocked:
        raise RuntimeError(f"Likely blocked/cookie expired: {reason}. Body head: {resp.text[:200]}")

    data = resp.json()
    if not isinstance(data, dict) or data.get("success") is not True:
        raise RuntimeError(f"Unexpected JSON: {json.dumps(data, ensure_ascii=False)[:300]}")

    return data


def flatten_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in rec.items():
        if isinstance(v, (dict, list)):
            out[k] = json.dumps(v, ensure_ascii=False)
        else:
            out[k] = v
    return out


def jitter_sleep(base_sec: float) -> None:
    """
    抖动：避免固定间隔更像机器人
    """
    # base 的 +/- 40%，并叠加 0~0.2 秒随机
    sec = max(0.0, base_sec * random.uniform(0.6, 1.4) + random.uniform(0, 0.2))
    time.sleep(sec)


def load_progress(path: str) -> int:
    if not os.path.exists(path):
        return 1
    try:
        with open(path, "r", encoding="utf-8") as f:
            obj = json.load(f)
        page_no = int(obj.get("next_page_no", 1))
        return max(1, page_no)
    except Exception:
        return 1


def save_progress(path: str, next_page_no: int) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({"next_page_no": next_page_no, "ts": int(time.time())}, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def iter_records(
    session: requests.Session,
    start_page: int,
    page_size: int,
    sleep_sec: float,
    max_pages: Optional[int],
    params_overrides: Optional[Dict[str, Any]],
) -> Iterable[Dict[str, Any]]:
    """
    迭代产出 records，并且更精准控制停止条件：
    - 如果返回 result.pages / result.total 等字段，就用它判断是否结束
    - 否则 fallback：records 为空就停
    """
    page_no = start_page
    seen_empty = 0

    while True:
        if max_pages is not None and (page_no - start_page + 1) > max_pages:
            return

        # 手动再加一层“应用级重试”，针对风控/偶发非 JSON 这种情况
        # （注意：如果 cookie 过期，这里重试也没用，会更快暴露问题）
        last_err = None
        for attempt in range(1, 4):
            try:
                data = fetch_page(session, page_no, page_size, params_overrides=params_overrides)
                last_err = None
                break
            except Exception as e:
                last_err = e
                # 指数退避 + 抖动
                backoff = min(20.0, (2 ** (attempt - 1)) * 1.2)
                time.sleep(backoff + random.uniform(0, 0.6))
        if last_err is not None:
            raise last_err

        result = data.get("result") or {}
        records = result.get("records") or []

        # 尝试读取更精准的分页信息
        pages = result.get("pages")  # 常见：总页数
        total = result.get("total")  # 常见：总条数
        current = result.get("current") or result.get("pageNo")  # 有些接口叫 current
        size = result.get("size") or result.get("pageSize")

        if not records:
            seen_empty += 1
            # 连续两次空页再停，避免偶发抖动/短暂异常导致提前结束
            if seen_empty >= 2:
                return
        else:
            seen_empty = 0
            for rec in records:
                if isinstance(rec, dict):
                    yield flatten_record(rec)

        # 进度：写入“下一页”
        save_progress(PROGRESS_PATH, page_no + 1)

        # 如果 pages 可用，就用 pages 停止（更可靠）
        try:
            if pages is not None and int(pages) > 0 and page_no >= int(pages):
                return
        except Exception:
            pass

        page_no += 1
        jitter_sleep(sleep_sec)


def scrape_all(
    out_csv_path: str = "bigdatadex_records.csv",
    page_size: int = 8,
    sleep_sec: float = 0.35,
    max_pages: Optional[int] = None,
    params_overrides: Optional[Dict[str, Any]] = None,
    resume: bool = True,
) -> int:
    session = build_session()

    start_page = load_progress(PROGRESS_PATH) if resume else 1
    print(f"[INFO] start_page={start_page}, page_size={page_size}, resume={resume}")

    # 边抓边写：更稳、更省内存
    wrote_header = False
    total_rows = 0
    fieldnames: Optional[list] = None

    file_exists = os.path.exists(out_csv_path) and os.path.getsize(out_csv_path) > 0
    if file_exists and not resume:
        raise RuntimeError(f"{out_csv_path} already exists. Set resume=True or change output path.")

    mode = "a" if (resume and file_exists) else "w"
    with open(out_csv_path, mode, newline="", encoding="utf-8-sig") as f:
        writer = None

        for row in iter_records(
            session=session,
            start_page=start_page,
            page_size=page_size,
            sleep_sec=sleep_sec,
            max_pages=max_pages,
            params_overrides=params_overrides,
        ):
            # 第一次拿到 row 时确定表头
            if writer is None:
                # 如果是续跑且文件存在：我们无法安全读取已有 header（为简单起见）
                # 方案：用“当前row的keys”作为 header，后续新增字段会丢失。
                # 如果你想 100% 保留新增字段，需要先读一遍旧CSV的header再合并。
                fieldnames = sorted(row.keys())
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")

                if mode == "w":
                    writer.writeheader()
                    wrote_header = True
                else:
                    # append 时默认不再写 header
                    wrote_header = False

            writer.writerow(row)
            total_rows += 1

            # 每写一些就 flush，减少中途挂掉导致丢数据
            if total_rows % 50 == 0:
                f.flush()
                print(f"[INFO] wrote {total_rows} rows so far...")

    if total_rows == 0:
        raise RuntimeError("No data fetched. Possibly blocked or cookie expired.")

    print(f"[DONE] saved {total_rows} rows to {out_csv_path} (header_written={wrote_header})")
    return total_rows


if __name__ == "__main__":
    params_overrides = None
    # params_overrides = {"searchName": "语音"}

    scrape_all(
        out_csv_path="bigdatadex_findAllPage.csv",
        page_size=8,
        sleep_sec=0.35,
        max_pages=None,
        params_overrides=params_overrides,
        resume=True,  # 支持断点续跑
    )
