import asyncio
import json
import os
import random
import re
from pathlib import Path
from typing import Optional, Dict, Any

import pandas as pd
import requests
import cv2
from paddleocr import PaddleOCR
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError


# ========= 配置 =========
IN_CSV = "products.csv"
OUT_CSV = "products_with_date.csv"

OUT_JL = "products_with_date.jl"          # 增量写入
CHECKPOINT = "checkpoint_detail.json"     # 断点
CERT_DIR = Path("cert_imgs")              # 证书图保存目录

CONCURRENCY = 2                           # 并发数（太大容易不稳）
HEADLESS = True

# 裁剪右下角比例（识别右下角日期）
CROP_Y_START = 0.65
CROP_X_START = 0.55

# 重试
TRIES_NAV = 3
TRIES_OCR = 2


# ========= OCR 初始化（全局一次）=========
ocr = PaddleOCR(
    lang="ch",
    use_textline_orientation=True
)



# ========= 通用工具 =========
async def with_retry(coro_fn, *, tries=3, base_delay=1.0, jitter=0.3, name="op"):
    last_err = None
    for i in range(tries):
        try:
            return await coro_fn()
        except Exception as e:
            last_err = e
            delay = base_delay * (2 ** i) + random.random() * jitter
            print(f"[retry] {name} ({i+1}/{tries}) {type(e).__name__}: {e} -> sleep {delay:.1f}s")
            await asyncio.sleep(delay)
    raise last_err


def load_checkpoint() -> Dict[str, Any]:
    if os.path.exists(CHECKPOINT):
        with open(CHECKPOINT, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"done": {}}   # done: {row_index: "date or null"}


def save_checkpoint(done: Dict[str, Any]):
    with open(CHECKPOINT, "w", encoding="utf-8") as f:
        json.dump({"done": done}, f, ensure_ascii=False, indent=2)


def append_jsonl(record: Dict[str, Any]):
    with open(OUT_JL, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def download_image(url: str, save_path: Path, timeout=25):
    save_path.parent.mkdir(parents=True, exist_ok=True)
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    save_path.write_bytes(r.content)


def normalize_date(s: str) -> str:
    """
    规范化日期输出（尽量保持原样，但清理空格）
    """
    return re.sub(r"\s+", "", s)


def extract_date_from_certificate(img_path: Path) -> Optional[str]:
    """
    对证书图右下角进行 OCR 并提取日期。
    """
    img = cv2.imread(str(img_path))
    if img is None:
        return None

    h, w, _ = img.shape
    crop = img[int(h * CROP_Y_START): h, int(w * CROP_X_START): w]

    result = ocr.ocr(crop, cls=True)
    if not result:
        return None

    texts = []
    for line in result:
        for box in line:
            texts.append(box[1][0])
    text = " ".join(texts)

    patterns = [
        r"\d{4}年\d{1,2}月\d{1,2}日",
        r"\d{4}-\d{1,2}-\d{1,2}",
        r"\d{4}\.\d{1,2}\.\d{1,2}",
        r"\d{4}/\d{1,2}/\d{1,2}",
    ]
    for p in patterns:
        m = re.search(p, text)
        if m:
            return normalize_date(m.group())

    return None


async def get_cert_img_url(page) -> Optional[str]:
    """
    在详情页中获取证书图片 src。
    根据你给的示例：<img class="el-image__inner" ...>
    这里做得更稳：
      - 优先找 class=el-image__inner
      - 若有多张，优先选包含 'productCertificate' 的
    """
    # 等页面至少渲染一些图片出来
    await page.wait_for_load_state("domcontentloaded")
    try:
        await page.wait_for_load_state("networkidle", timeout=20000)
    except PlaywrightTimeoutError:
        pass

    imgs = page.locator("img.el-image__inner")
    try:
        await imgs.first.wait_for(timeout=20000)
    except PlaywrightTimeoutError:
        return None

    n = await imgs.count()
    if n == 0:
        return None

    # 先挑包含 productCertificate 的
    for i in range(n):
        src = await imgs.nth(i).get_attribute("src")
        if src and "productCertificate" in src:
            return src

    # 否则就用第一张
    src0 = await imgs.first.get_attribute("src")
    return src0


async def process_one(page, row_idx: int, detail_url: str) -> Dict[str, Any]:
    """
    处理一个产品：打开详情页 -> 拿证书图片 -> 下载 -> OCR -> 返回日期
    """
    if not detail_url or not isinstance(detail_url, str):
        return {"row_idx": row_idx, "detail_url": detail_url, "register_date": None, "error": "no_detail_url"}

    async def _nav():
        await page.goto(detail_url, wait_until="domcontentloaded", timeout=30000)
    await with_retry(_nav, tries=TRIES_NAV, name="goto_detail")

    img_url = await with_retry(lambda: get_cert_img_url(page), tries=TRIES_NAV, name="get_cert_img_url")
    if not img_url:
        return {"row_idx": row_idx, "detail_url": detail_url, "register_date": None, "error": "no_cert_img"}

    # 下载证书图
    cert_path = CERT_DIR / f"{row_idx}.png"
    try:
        download_image(img_url, cert_path)
    except Exception as e:
        return {"row_idx": row_idx, "detail_url": detail_url, "register_date": None, "error": f"download_fail:{e}"}

    # OCR（可重试：有时偶发识别空）
    date = None
    last_err = None
    for _ in range(TRIES_OCR):
        try:
            date = extract_date_from_certificate(cert_path)
            if date:
                break
        except Exception as e:
            last_err = e
        await asyncio.sleep(0.3)

    if not date:
        return {"row_idx": row_idx, "detail_url": detail_url, "register_date": None,
                "error": f"ocr_no_date{'' if not last_err else ':'+str(last_err)}"}

    return {"row_idx": row_idx, "detail_url": detail_url, "register_date": date, "error": None}


async def worker(name: str, browser, queue: asyncio.Queue, results: Dict[int, Any], done_map: Dict[str, Any]):
    context = await browser.new_context(viewport={"width": 1400, "height": 900}, locale="zh-CN")
    page = await context.new_page()

    while True:
        item = await queue.get()
        if item is None:
            queue.task_done()
            break

        row_idx, detail_url = item
        try:
            res = await process_one(page, row_idx, detail_url)
        except Exception as e:
            res = {"row_idx": row_idx, "detail_url": detail_url, "register_date": None, "error": f"fatal:{e}"}

        # 记录
        results[row_idx] = res
        done_map[str(row_idx)] = res.get("register_date")

        # 增量落盘（断了也有）
        append_jsonl(res)
        save_checkpoint(done_map)

        print(f"[{name}] row={row_idx} date={res.get('register_date')} err={res.get('error')}")
        await asyncio.sleep(random.uniform(0.2, 0.8))  # 小限速更稳
        queue.task_done()

    await context.close()


async def main():
    if not os.path.exists(IN_CSV):
        raise FileNotFoundError(f"Input CSV not found: {IN_CSV}")

    df = pd.read_csv(IN_CSV)
    if "detail_url" not in df.columns:
        raise ValueError("CSV must contain a 'detail_url' column")

    ck = load_checkpoint()
    done_map: Dict[str, Any] = ck.get("done", {})  # { "row_idx": "date or null" }

    # 准备任务队列：只处理还没 done 的
    queue: asyncio.Queue = asyncio.Queue()
    total = 0
    skipped = 0
    for idx, row in df.iterrows():
        if str(idx) in done_map:
            skipped += 1
            continue
        total += 1
        queue.put_nowait((idx, row.get("detail_url")))

    print(f"Total rows: {len(df)} | To process: {total} | Skipped by checkpoint: {skipped}")

    # 若全部都 done，直接合并输出
    if total == 0:
        print("Nothing to do. Writing OUT_CSV from checkpoint...")
        if "register_date" not in df.columns:
            df["register_date"] = None
        for k, v in done_map.items():
            df.at[int(k), "register_date"] = v
        df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
        print(f"Saved: {OUT_CSV}")
        return

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS)

        results: Dict[int, Any] = {}
        workers = []
        for i in range(CONCURRENCY):
            workers.append(asyncio.create_task(worker(f"w{i+1}", browser, queue, results, done_map)))

        # 等队列完成
        await queue.join()

        # 停 worker
        for _ in range(CONCURRENCY):
            queue.put_nowait(None)
        await asyncio.gather(*workers)

        await browser.close()

    # 回写 register_date 到 df
    if "register_date" not in df.columns:
        df["register_date"] = None
    for k, v in done_map.items():
        try:
            df.at[int(k), "register_date"] = v
        except Exception:
            pass

    df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    print(f"Saved: {OUT_CSV}")
    print(f"Checkpoint: {CHECKPOINT}")
    print(f"Incremental log: {OUT_JL}")
    print(f"Cert images dir: {CERT_DIR.resolve()}")


if __name__ == "__main__":
    asyncio.run(main())
