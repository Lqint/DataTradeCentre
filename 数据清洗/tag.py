import os
import json
import time
from typing import Dict, Any, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from openai import OpenAI
from tqdm import tqdm


TARGET_CATEGORIES = [
    "政务/公共服务",
    "金融/征信",
    "交通/出行",
    "工业/制造/能源",
    "医疗/健康",
    "零售/消费",
    "位置/地图/时空",
    "互联网/内容/媒体",
    "环境/气象",
    "其他（不可分类的）",
]

SYSTEM_PROMPT = f"""
你是一个严谨的中文数据产品分类器。请将输入的“产品名称、类别_原始、产品介绍”映射到以下一个且仅一个类别（必须完全匹配枚举）：
{json.dumps(TARGET_CATEGORIES, ensure_ascii=False)}

你必须使用 json 输出（只输出合法 JSON，不要输出额外文字），并严格遵循下面的 JSON 结构：
{{
  "mapped_category": "政务/公共服务|金融/征信|交通/出行|工业/制造/能源|医疗/健康|零售/消费|位置/地图/时空|互联网/内容/媒体|环境/气象|其他（不可分类的）",
  "confidence": 0.0,
  "reason": "不超过60字的中文理由"
}}

规则：
1) 如果信息不足或无法明确归类，输出“其他（不可分类的）”
2) confidence 取 0~1 的小数
3) 只能输出 JSON（json）
""".strip()


def build_user_prompt(product_name: str, raw_category: str, intro: str) -> str:
    return f"""
请根据下列字段完成分类映射，并按要求输出 json：

产品名称：{product_name or ""}
类别_原始：{raw_category or ""}
产品介绍：{intro or ""}
""".strip()


def safe_get_json(content: str) -> Optional[Dict[str, Any]]:
    try:
        data = json.loads(content)
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def normalize_category(cat: str) -> str:
    cat = (cat or "").strip()
    return cat if cat in TARGET_CATEGORIES else "其他（不可分类的）"


def is_retryable_error(err: Exception) -> bool:
    """
    简单判断：429/限速、超时、临时网络问题等都重试。
    不同 SDK 的异常类型可能不同，这里用字符串兜底。
    """
    s = str(err).lower()
    retry_keywords = [
        "429", "rate limit", "too many requests",
        "timeout", "timed out",
        "temporarily", "temporary",
        "connection", "network",
        "server", "502", "503", "504",
        "empty content", "invalid json",
    ]
    return any(k in s for k in retry_keywords)


def call_deepseek_json(
    client: OpenAI,
    product_name: str,
    raw_category: str,
    intro: str,
    model: str = "deepseek-chat",
    max_retries: int = 5,
) -> Dict[str, Any]:
    user_prompt = build_user_prompt(product_name, raw_category, intro)
    last_err: Optional[Exception] = None

    for attempt in range(1, max_retries + 1):
        try:
            resp = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt},
                ],
                response_format={"type": "json_object"},
                temperature=0.1,
                max_tokens=256,
            )

            content = (resp.choices[0].message.content or "").strip()
            if not content:
                raise ValueError("empty content")

            data = safe_get_json(content)
            if not data:
                raise ValueError(f"invalid json: {content[:160]}")

            mapped = normalize_category(str(data.get("mapped_category", "")))
            reason = str(data.get("reason", ""))[:60]

            try:
                confidence = float(data.get("confidence", 0.0))
            except Exception:
                confidence = 0.0
            confidence = max(0.0, min(1.0, confidence))

            return {
                "mapped_category": mapped,
                "confidence": confidence,
                "reason": reason,
                "raw_json": content,
                "error": "",
            }

        except Exception as e:
            last_err = e
            if attempt < max_retries and is_retryable_error(e):
                # 指数退避 + 少量抖动
                sleep_s = min(8.0, 0.6 * (2 ** (attempt - 1)))
                time.sleep(sleep_s + (attempt * 0.05))
                continue
            break

    return {
        "mapped_category": "其他（不可分类的）",
        "confidence": 0.0,
        "reason": f"调用/解析失败：{str(last_err)[:40]}",
        "raw_json": "",
        "error": str(last_err)[:200] if last_err else "unknown",
    }


def worker_task(
    idx: int,
    product_name: str,
    raw_category: str,
    intro: str,
    model: str,
) -> Tuple[int, Dict[str, Any]]:
    """
    线程任务：每个线程各自创建 client（更稳，避免共享连接状态问题）
    """
    api_key = os.getenv("DEEPSEEK_API_KEY")
    if not api_key:
        raise RuntimeError("请先设置环境变量 DEEPSEEK_API_KEY")

    client = OpenAI(api_key=api_key, base_url="https://api.deepseek.com")
    result = call_deepseek_json(
        client=client,
        product_name=product_name,
        raw_category=raw_category,
        intro=intro,
        model=model,
    )
    return idx, result


def main(
    input_xlsx: str,
    output_xlsx: str,
    col_product_name: str = "产品名称",
    col_raw_category: str = "类别_原始",
    col_intro: str = "产品介绍",
    new_col_name: str = "映射类别",
    model: str = "deepseek-chat",
    max_workers: int = 12,
    chunksize_submit: int = 500,
):
    api_key = os.getenv("DEEPSEEK_API_KEY")
    if not api_key:
        raise RuntimeError("请先设置环境变量 DEEPSEEK_API_KEY")

    df = pd.read_excel(input_xlsx)

    for c in [col_product_name, col_raw_category, col_intro]:
        if c not in df.columns:
            raise ValueError(f"缺少列：{c}，当前表头：{list(df.columns)}")

    n = len(df)
    mapped = [""] * n
    conf = [0.0] * n
    reason = [""] * n
    err = [""] * n

    # 为了更好的“进度显示体验”：
    # - 以“完成的 future 数量”更新进度条，而不是 iterrows。
    # - 单独显示成功/失败数量和最近一个分类结果。
    pbar = tqdm(total=n, desc="Classifying", dynamic_ncols=True, mininterval=0.2)

    success_cnt = 0
    fail_cnt = 0

    # 分批提交任务，避免一次性创建过多 future 占用内存
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = []

        def submit_range(start: int, end: int):
            for idx in range(start, end):
                row = df.iloc[idx]
                product_name = str(row.get(col_product_name, "") or "")
                raw_category = str(row.get(col_raw_category, "") or "")
                intro = str(row.get(col_intro, "") or "")
                futures.append(
                    ex.submit(worker_task, idx, product_name, raw_category, intro, model)
                )

        # 分批提交
        for start in range(0, n, chunksize_submit):
            end = min(n, start + chunksize_submit)
            submit_range(start, end)

            # 处理这一批 futures（也可以不分批处理，这里是为了内存稳定）
            for fut in as_completed(futures):
                idx, res = fut.result()

                mapped[idx] = res["mapped_category"]
                conf[idx] = res["confidence"]
                reason[idx] = res["reason"]
                err[idx] = res.get("error", "")

                if err[idx]:
                    fail_cnt += 1
                else:
                    success_cnt += 1

                pbar.update(1)
                # 进度条右侧信息（更直观）
                pbar.set_postfix_str(
                    f"ok={success_cnt} fail={fail_cnt} last={mapped[idx]} {conf[idx]:.2f}"
                )

            futures.clear()

    pbar.close()

    df[new_col_name] = mapped
    df[new_col_name + "_置信度"] = conf
    df[new_col_name + "_理由"] = reason
    df[new_col_name + "_错误"] = err

    df.to_excel(output_xlsx, index=False)
    print(f"已输出：{output_xlsx}")


if __name__ == "__main__":
    main(
        input_xlsx="datacentre_data.xlsx",
        output_xlsx="datacentre_data_with_category.xlsx",
        new_col_name="映射为",
        model="deepseek-chat",
        max_workers=12,          # 并发线程数：建议 8~16 之间试
        chunksize_submit=500,    # 大表时分批提交，内存更稳
    )
