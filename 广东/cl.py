import os
import re
import json
import csv
from typing import Any, Dict, List, Tuple, Optional
from datetime import datetime, timezone, timedelta


# ====== 你只需要改这里 ======
INPUT_DIR = r"spgl"  # 你的JSON文件夹
OUTPUT_CSV = "szdex_products_友好字段.csv"
# ============================

# 时间转换：按北京时间（UTC+8）
TZ_CN = timezone(timedelta(hours=8))


def read_json_file(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.loads(f.read().strip())


def list_target_files(folder: str) -> List[str]:
    files = []
    for name in os.listdir(folder):
        if name.startswith("pageQuerySjspList"):
            files.append(os.path.join(folder, name))

    # 排序：pageQuerySjspList 在最前，然后 [1],[2]...
    def sort_key(p: str) -> Tuple[int, int]:
        base = os.path.basename(p)
        m = re.search(r"\[(\d+)\]", base)
        if m:
            return (1, int(m.group(1)))
        return (0, 0)

    files.sort(key=sort_key)
    return files


def ms_to_datetime_str(ms: Any) -> str:
    """
    毫秒时间戳 -> YYYY-MM-DD HH:MM:SS（北京时间）
    兼容：None / "" / 数字字符串
    """
    if ms in (None, ""):
        return ""
    try:
        ms_int = int(ms)
        dt = datetime.fromtimestamp(ms_int / 1000, tz=TZ_CN)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        # 万一字段不是时间戳，就原样返回
        return str(ms)


def norm(item: Dict[str, Any], source_file: str) -> Dict[str, Any]:
    """
    输出：中文友好字段名
    """
    return {
        "产品ID": item.get("id", ""),
        "产品编号": item.get("spSn", ""),
        "产品名称": item.get("spMc", ""),
        "发布方企业": item.get("fbfQyMc", ""),

        "产品类型": item.get("spsjlxMc", ""),      # 数据产品/数据服务/数据工具
        "产品分类": item.get("spsjlxFlMc", ""),    # 分类（有时同上）
        "交付方式": item.get("jfxtMc", ""),        # 人工交付/数据库表/文件类交付等

        "应用领域": item.get("yylyMcs", "") or "", # 可能为 null
        "应用场景": item.get("yycjMcs", "") or "",

        "销售价格": item.get("xsjg", ""),
        "价格单位": item.get("xsgg", ""),

        "产品描述": item.get("spms", ""),

        "列表图路径": item.get("lbtDz", ""),

        "创建时间": ms_to_datetime_str(item.get("cjsj")),
        "上架时间": ms_to_datetime_str(item.get("spsjSj")),

        "产品状态码": item.get("spzt", ""),
        "是否严选": item.get("sfSsYp", ""),   # Y/N
        "是否有效": item.get("sfyx", ""),     # Y/N

        "来源文件": source_file,

        # 如你需要追溯原字段结构，可以保留这个（不想要就删掉）
        "原始字段列表": ",".join(sorted(item.keys())),
    }


def dedup_key(r: Dict[str, Any]) -> str:
    # 以“产品ID”去重最稳
    k = str(r.get("产品ID", "")).strip()
    if k:
        return k
    # 兜底用编号
    return str(r.get("产品编号", "")).strip()


def main():
    files = list_target_files(INPUT_DIR)
    if not files:
        raise SystemExit(f"没找到 pageQuerySjspList* 文件，目录：{INPUT_DIR}")

    rows_out: List[Dict[str, Any]] = []
    seen = set()

    for fp in files:
        base = os.path.basename(fp)
        try:
            j = read_json_file(fp)
        except Exception as e:
            print(f"[SKIP] {base} JSON解析失败：{e}")
            continue

        if not isinstance(j, dict) or not isinstance(j.get("data"), dict):
            print(f"[WARN] {base} 找不到 data 对象，跳过")
            continue

        data = j["data"]
        rows = data.get("rows")
        if not isinstance(rows, list):
            print(f"[WARN] {base} 找不到 data.rows 列表，跳过")
            continue

        added = 0
        for it in rows:
            if not isinstance(it, dict):
                continue
            r = norm(it, source_file=base)
            k = dedup_key(r)
            if not k or k in seen:
                continue
            seen.add(k)
            rows_out.append(r)
            added += 1

        print(f"[OK] {base}: 读取 {len(rows)} 条，新增 {added} 条")

    if not rows_out:
        raise SystemExit("没有解析到任何产品条目，请检查文件内容是否与示例一致。")

    # 固定 CSV 列顺序（中文友好）
    fieldnames = [
        "产品ID", "产品编号", "产品名称", "发布方企业",
        "产品类型", "产品分类", "交付方式",
        "应用领域", "应用场景",
        "销售价格", "价格单位",
        "产品描述",
        "列表图路径",
        "创建时间", "上架时间",
        "产品状态码", "是否严选", "是否有效",
        "来源文件",
        "原始字段列表",
    ]

    with open(OUTPUT_CSV, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows_out)

    print(f"[DONE] 去重后共 {len(rows_out)} 条")
    print(f"[SAVED] {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
