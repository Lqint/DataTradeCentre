import asyncio
import json
import os
import random
from urllib.parse import urljoin

import pandas as pd
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError


START_URL = "https://dex.hubei-data.com/product/list"

OUT_JSON = "products.json"
OUT_CSV = "products.csv"

# 增量与断点文件
OUT_JL = "products.jl"          # jsonlines，每抓到一条就追加，断了也不丢
CHECKPOINT = "checkpoint.json" # 记录爬到第几页、已见过哪些key


# -----------------------
# 小工具：重试包装
# -----------------------
async def with_retry(coro_fn, *, tries=4, base_delay=1.0, jitter=0.3, name="op"):
    """
    coro_fn: 一个无参 async function
    """
    last_err = None
    for i in range(tries):
        try:
            return await coro_fn()
        except Exception as e:
            last_err = e
            delay = base_delay * (2 ** i) + random.random() * jitter
            print(f"[retry] {name} failed ({i+1}/{tries}): {type(e).__name__}: {e}  -> sleep {delay:.1f}s")
            await asyncio.sleep(delay)
    raise last_err


def load_checkpoint():
    if os.path.exists(CHECKPOINT):
        with open(CHECKPOINT, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"page_index": 1, "seen_keys": []}


def save_checkpoint(page_index, seen_keys):
    tmp = {"page_index": page_index, "seen_keys": list(seen_keys)}
    with open(CHECKPOINT, "w", encoding="utf-8") as f:
        json.dump(tmp, f, ensure_ascii=False, indent=2)


def append_jsonl(item):
    with open(OUT_JL, "a", encoding="utf-8") as f:
        f.write(json.dumps(item, ensure_ascii=False) + "\n")


async def safe_text(locator, default=None):
    try:
        if await locator.count() == 0:
            return default
        t = await locator.first.inner_text()
        return t.strip() if t else default
    except Exception:
        return default


async def safe_attr(locator, attr, default=None):
    try:
        if await locator.count() == 0:
            return default
        v = await locator.first.get_attribute(attr)
        return v if v else default
    except Exception:
        return default


# -----------------------
# 抽取当前页产品列表
# -----------------------
async def extract_products_on_page(page, base_url: str):
    # 等列表出现（加 retry）
    async def _wait():
        await page.wait_for_selector("div.market-product", timeout=20000)
    await with_retry(_wait, name="wait_list")

    cards = page.locator("div.market-product")
    count = await cards.count()

    items = []
    for i in range(count):
        card = cards.nth(i)

        # 单卡片防崩：任何字段异常都不影响其他卡
        try:
            href = await safe_attr(card.locator('a[href^="/product/detail/"]'), "href")
            detail_url = urljoin(base_url, href) if href else None

            title = await safe_text(card.locator(".content-title .title"))
            org = await safe_text(card.locator(".market-product-footer .product-org"))
            price = await safe_text(card.locator(".market-product-footer .content-price_num"))

            # 交付方式 / 应用场景
            delivery = None
            scenarios = None
            rate_loc = card.locator(".market-product-content .content-rate")
            rate_count = await rate_loc.count()
            for r in range(rate_count):
                line = (await rate_loc.nth(r).inner_text()).strip()
                if "交付方式" in line:
                    delivery = line.replace("交付方式：", "").strip()
                elif "应用场景" in line:
                    scenarios = line.replace("应用场景：", "").strip()

            # 标签
            tags = []
            tag_loc = card.locator(".header-tab")
            tag_count = await tag_loc.count()
            for t in range(tag_count):
                tag_text = (await tag_loc.nth(t).inner_text()).strip()
                if tag_text:
                    tags.append(tag_text)

            cert_img = await safe_attr(card.locator(".market-product-header img"), "src")

            items.append(
                {
                    "title": title,
                    "detail_url": detail_url,
                    "org": org,
                    "price": price,
                    "delivery": delivery,
                    "scenarios": scenarios,
                    "tags": tags,
                    "cert_img": cert_img,
                }
            )
        except Exception as e:
            print(f"[warn] card parse failed on index={i}: {type(e).__name__}: {e}")
            continue

    return items


# -----------------------
# 更稳的翻页
# -----------------------
async def goto_next_page(page):
    next_btn = page.locator("button.btn-next")
    if await next_btn.count() == 0:
        return False

    disabled_attr = await next_btn.get_attribute("disabled")
    aria_disabled = await next_btn.get_attribute("aria-disabled")
    class_name = (await next_btn.get_attribute("class")) or ""

    if disabled_attr is not None:
        return False
    if aria_disabled in ("true", "disabled"):
        return False
    if "is-disabled" in class_name or "disabled" in class_name:
        return False

    # 记录翻页前：第一条 detail_url + title（双保险）
    first_href = await safe_attr(page.locator('div.market-product a[href^="/product/detail/"]').first, "href")
    first_title = await safe_text(page.locator("div.market-product .content-title .title").first)

    async def _click_and_wait():
        await next_btn.click()

        # 先等 DOM 变化 / 网络稳定
        try:
            await page.wait_for_load_state("domcontentloaded", timeout=20000)
        except PlaywrightTimeoutError:
            pass
        try:
            await page.wait_for_load_state("networkidle", timeout=20000)
        except PlaywrightTimeoutError:
            pass

        # 等列表“确实换了”
        async def changed():
            new_href = await safe_attr(page.locator('div.market-product a[href^="/product/detail/"]').first, "href")
            new_title = await safe_text(page.locator("div.market-product .content-title .title").first)
            # 有任何一个变化就算翻页成功
            return (new_href and new_href != first_href) or (new_title and new_title != first_title)

        await page.wait_for_function(
            "() => document.querySelectorAll('div.market-product').length > 0",
            timeout=20000
        )

        # 再用 python 侧轮询确认变化（更稳）
        for _ in range(40):
            if await changed():
                return
            await asyncio.sleep(0.25)

        raise PlaywrightTimeoutError("next page content not changed")

    try:
        await with_retry(_click_and_wait, tries=3, base_delay=1.0, name="next_page")
        return True
    except Exception:
        return False


# -----------------------
# 主逻辑：支持断点续爬 + 增量落盘
# -----------------------
async def main():
    ck = load_checkpoint()
    page_index = ck.get("page_index", 1)
    seen = set(ck.get("seen_keys", []))

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(viewport={"width": 1400, "height": 900}, locale="zh-CN")
        page = await context.new_page()

        # goto 加 retry
        async def _goto():
            await page.goto(START_URL, wait_until="domcontentloaded", timeout=30000)
            try:
                await page.wait_for_load_state("networkidle", timeout=20000)
            except PlaywrightTimeoutError:
                pass

        await with_retry(_goto, name="goto_start")

        base_url = page.url

        # 如果要真正“跳到断点页”，需要页面支持输入页码/参数。
        # 这里默认只能从第一页开始点 next，所以：如果 page_index>1，则先翻到断点页。
        if page_index > 1:
            print(f"[resume] need fast-forward to page {page_index} ...")
            cur = 1
            while cur < page_index:
                ok = await goto_next_page(page)
                if not ok:
                    print("[resume] cannot reach checkpoint page, stop.")
                    await browser.close()
                    return
                cur += 1

        # 开始抓
        while True:
            items = await extract_products_on_page(page, base_url)
            print(f"Page {page_index}: {len(items)} items")

            # 增量写入 + 去重
            new_cnt = 0
            for x in items:
                key = x.get("detail_url") or (x.get("title"), x.get("org"))
                if not key or key in seen:
                    continue
                seen.add(key)
                append_jsonl(x)
                new_cnt += 1

            print(f"  + new: {new_cnt}, seen: {len(seen)}")

            # 保存断点：下一页页码（意味着当前页完成）
            save_checkpoint(page_index + 1, seen)

            ok = await goto_next_page(page)
            if not ok:
                break
            page_index += 1

        # 汇总 jsonl -> json/csv（最后再做一次整合）
        all_rows = []
        if os.path.exists(OUT_JL):
            with open(OUT_JL, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        all_rows.append(json.loads(line))

        # 最终去重（防止意外重复写入）
        final_seen = set()
        dedup = []
        for x in all_rows:
            key = x.get("detail_url") or (x.get("title"), x.get("org"))
            if key in final_seen:
                continue
            final_seen.add(key)
            dedup.append(x)

        with open(OUT_JSON, "w", encoding="utf-8") as f:
            json.dump(dedup, f, ensure_ascii=False, indent=2)

        df = pd.DataFrame(dedup)
        df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")

        print(f"Saved: {OUT_JSON}, {OUT_CSV}  total={len(dedup)}")
        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
