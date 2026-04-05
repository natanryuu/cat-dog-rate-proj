#!/usr/bin/env python3
"""
fetch_vet_count.py — 台北市 12 行政區動物醫院數 (截面)
======================================================

資料來源: 台北市資料大平臺「臺北市動物醫院一覽表」
  dataset_id: 01bcb5ee-7c18-41fa-86d4-4e75daee1f94
  https://data.taipei/dataset/detail?id=01bcb5ee-7c18-41fa-86d4-4e75daee1f94

邏輯
----
1. 拉取所有動物醫院逐筆資料
2. 從「行政區」欄或地址欄辨識所屬區
3. 聚合到區級計數

Output: data/vet_count_cross.csv
"""

import io
from pathlib import Path

import pandas as pd
import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ────────────────────────────────────────────────────────────
TAIPEI_DISTRICTS = [
    "松山區", "信義區", "大安區", "中山區", "中正區", "大同區",
    "萬華區", "文山區", "南港區", "內湖區", "士林區", "北投區",
]

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

DATASET_ID = "01bcb5ee-7c18-41fa-86d4-4e75daee1f94"
RESOURCE_ID = "40d79051-1839-4d00-855f-be88f1e06caf"
CSV_URL = (f"https://data.taipei/api/dataset/{DATASET_ID}"
           f"/resource/{RESOURCE_ID}/download")


# ────────────────────────────────────────────────────────────
def fetch_all_vets() -> list:
    """直接下載 CSV 並轉為 list of dict。"""
    print(f"  下載 CSV ...", end=" ", flush=True)
    try:
        r = requests.get(CSV_URL, timeout=30, verify=False)
        r.raise_for_status()
        # data.taipei CSV 通常為 UTF-8 with BOM
        df = pd.read_csv(io.StringIO(r.content.decode("utf-8-sig")))
        print(f"{len(df)} 筆")
        return df.to_dict(orient="records")
    except Exception as e:
        print(f"\n  ⚠ 下載失敗: {e}")
        return []


def probe_fields(records: list):
    """印出前 3 筆的所有欄位。"""
    if not records:
        print("  無資料可 probe")
        return
    print(f"\n  === PROBE: 共 {len(records)} 筆 ===")
    for i, rec in enumerate(records[:3]):
        print(f"\n  --- 第 {i+1} 筆 ---")
        for k, v in rec.items():
            print(f"    {k:30s} = {v}")
    print()


def process_vets(records: list) -> pd.DataFrame:
    """聚合逐筆動物醫院 → 12 區計數。"""
    df = pd.DataFrame(records)
    print(f"  全部欄位: {list(df.columns)}")

    # ── 辨識行政區欄位 ──
    dist_col = _find_district_col(df)

    # ── 辨識地址欄位 (備用) ──
    addr_col = _find_addr_col(df)

    if dist_col:
        print(f"  → 使用行政區欄位: {dist_col}")
        return _count_by_district_col(df, dist_col)
    elif addr_col:
        print(f"  → 行政區欄位不明，改用地址欄位: {addr_col}")
        return _count_by_address(df, addr_col)
    else:
        print("  ⚠ 無法辨識行政區或地址欄位，請手動檢查")
        return pd.DataFrame()


def _find_district_col(df: pd.DataFrame) -> str | None:
    """找含有 12 區名稱的欄位。"""
    candidates = ["行政區", "district", "District", "DISTRICT",
                  "區域", "area", "Area"]
    for c in candidates:
        if c in df.columns:
            return c
    # 模糊: 找值含完整區名（如「中正區」）且能匹配多個台北區名的欄位
    for c in df.columns:
        vals = df[c].dropna().astype(str)
        match_count = sum(
            vals.str.contains(d, regex=False).any()
            for d in TAIPEI_DISTRICTS
        )
        if match_count >= 8:  # 至少 8 區有資料
            return c
    return None


def _find_addr_col(df: pd.DataFrame) -> str | None:
    """找地址欄位。"""
    candidates = ["地址", "Address", "address", "ADDR", "addr",
                  "醫院地址", "院址"]
    for c in candidates:
        if c in df.columns:
            return c
    for c in df.columns:
        vals = df[c].dropna().astype(str).head(10)
        if vals.str.contains("臺北市|台北市|路|街|巷").any():
            return c
    return None


def _count_by_district_col(df, dist_col) -> pd.DataFrame:
    """依行政區欄位直接 groupby 計數。"""
    rows = []
    for dist in TAIPEI_DISTRICTS:
        dist_short = dist.replace("區", "")
        mask = df[dist_col].astype(str).str.contains(
            f"({dist}|{dist_short}$)", regex=True)
        rows.append({
            "district": dist,
            "vet_count": int(mask.sum()),
        })
    result = pd.DataFrame(rows)
    # 驗證: 加總 vs 原始筆數
    total_matched = result["vet_count"].sum()
    print(f"  → 匹配 {total_matched}/{len(df)} 筆 "
          f"({'OK' if total_matched == len(df) else '有遺漏，可能含非台北市資料'})")
    return result


def _count_by_address(df, addr_col) -> pd.DataFrame:
    """從地址解析行政區後計數。"""
    rows = []
    for dist in TAIPEI_DISTRICTS:
        mask = df[addr_col].astype(str).str.contains(dist, na=False)
        rows.append({
            "district": dist,
            "vet_count": int(mask.sum()),
        })
    result = pd.DataFrame(rows)
    total_matched = result["vet_count"].sum()
    print(f"  → 地址匹配 {total_matched}/{len(df)} 筆")
    return result


def main():
    print("🏥 台北市動物醫院數")
    print("─" * 50)

    records = fetch_all_vets()
    if not records:
        print("\n  ❌ 無資料回傳。請確認 dataset_id 是否仍有效:")
        print(f"     https://data.taipei/dataset/detail?id={DATASET_ID}")
        return

    probe_fields(records)

    result = process_vets(records)
    if result.empty:
        return

    outpath = DATA_DIR / "vet_count_cross.csv"
    result.to_csv(outpath, index=False, encoding="utf-8-sig")
    print(f"\n  ✅ 已存檔: {outpath}")
    print(result.to_string(index=False))


if __name__ == "__main__":
    main()
