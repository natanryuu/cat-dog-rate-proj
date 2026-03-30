"""
04_merge_panel.py
=================
合併所有清理後的資料，組成最終 panel data
並執行基本診斷（缺失、平衡性、描述統計、相關矩陣）

執行前請確認 data/clean/ 裡有：
  - pets_raw.csv       （來自 01_fetch_pets.py，注意這個還在 raw 但已清理）
  - household_clean.csv（來自 02_fetch_household.py）
  - housing_clean.csv  （來自 03_fetch_housing.py）
"""

import pandas as pd
import numpy as np
import os

CLEAN_DIR  = "data/clean"
OUTPUT_DIR = "data/clean"
os.makedirs(OUTPUT_DIR, exist_ok=True)

TAIPEI_DISTRICTS = [
    "松山區", "信義區", "大安區", "中山區", "中正區",
    "大同區", "萬華區", "文山區", "南港區", "內湖區",
    "士林區", "北投區"
]
TARGET_YEARS = list(range(2015, 2024))


# ── 讀入各子資料 ─────────────────────────────────────────────────
def load_data():
    pets = pd.read_csv("data/raw/pets_raw.csv", encoding="utf-8-sig")
    hh   = pd.read_csv(f"{CLEAN_DIR}/household_clean.csv",  encoding="utf-8-sig")
    hs   = pd.read_csv(f"{CLEAN_DIR}/housing_clean.csv",    encoding="utf-8-sig")
    return pets, hh, hs


# ── 建立完整的 district × year 索引（確保 balanced panel）────────
def make_balanced_skeleton() -> pd.DataFrame:
    idx = pd.MultiIndex.from_product(
        [TAIPEI_DISTRICTS, TARGET_YEARS],
        names=["district", "year"]
    )
    return pd.DataFrame(index=idx).reset_index()


# ── 合併 ──────────────────────────────────────────────────────────
def merge_all(pets, hh, hs) -> pd.DataFrame:
    skeleton = make_balanced_skeleton()

    # 選取需要的欄位
    pets_sel = pets[["district", "year", "cat_count", "dog_count", "cat_dog_ratio"]]
    hh_sel   = hh[["district", "year", "single_hh_ratio", "marriage_rate"]
                  + (["population"] if "population" in hh.columns else [])]
    hs_sel   = hs[["district", "year", "housing_size"]]

    panel = (
        skeleton
        .merge(pets_sel, on=["district", "year"], how="left")
        .merge(hh_sel,   on=["district", "year"], how="left")
        .merge(hs_sel,   on=["district", "year"], how="left")
    )

    # 建立 district 編號（固定效果需要）
    district_map = {d: i for i, d in enumerate(TAIPEI_DISTRICTS, 1)}
    panel["district_id"] = panel["district"].map(district_map)

    return panel


# ── 診斷 ──────────────────────────────────────────────────────────
def diagnostics(panel: pd.DataFrame):
    print("\n" + "="*50)
    print("Panel Data 診斷報告")
    print("="*50)

    print(f"\n【1】尺寸：{panel.shape}（預期 108 × N欄）")
    print(f"     - 觀測值：{len(panel)}")
    print(f"     - 行政區：{panel['district'].nunique()}")
    print(f"     - 年份：{sorted(panel['year'].unique())}")

    print("\n【2】缺失值統計：")
    missing = panel.isnull().sum()
    missing_pct = (missing / len(panel) * 100).round(1)
    miss_df = pd.DataFrame({"缺失數": missing, "缺失%": missing_pct})
    print(miss_df[miss_df["缺失數"] > 0].to_string())
    if miss_df["缺失數"].sum() == 0:
        print("     ✓ 無缺失值")

    print("\n【3】描述統計（主要變數）：")
    key_vars = ["cat_dog_ratio", "housing_size", "single_hh_ratio", "marriage_rate"]
    key_vars = [v for v in key_vars if v in panel.columns]
    print(panel[key_vars].describe().round(3).to_string())

    print("\n【4】相關矩陣（依變數 vs 自變數）：")
    if len(key_vars) >= 2:
        print(panel[key_vars].corr().round(3).to_string())

    print("\n【5】各區觀測筆數（應各為 9）：")
    counts = panel.groupby("district").size()
    unbalanced = counts[counts != 9]
    if unbalanced.empty:
        print("     ✓ Balanced panel（每區 9 年）")
    else:
        print(f"     [警告] 以下區不平衡：")
        print(unbalanced.to_string())

    print("\n【6】依變數（貓犬比）各區時間趨勢：")
    if "cat_dog_ratio" in panel.columns:
        pivot = panel.pivot(index="district", columns="year", values="cat_dog_ratio")
        print(pivot.round(2).to_string())

    print("\n" + "="*50)


# ── Stata / R 格式輸出（備用）────────────────────────────────────
def export_for_r_stata(panel: pd.DataFrame):
    """
    匯出適合 R（plm 套件）或 Stata（xtset）的格式
    """
    # 確保欄位順序整齊
    col_order = [
        "district_id", "district", "year",
        "cat_dog_ratio", "cat_count", "dog_count",
        "housing_size", "single_hh_ratio", "marriage_rate",
    ] + [c for c in panel.columns if c not in [
        "district_id", "district", "year",
        "cat_dog_ratio", "cat_count", "dog_count",
        "housing_size", "single_hh_ratio", "marriage_rate",
    ]]

    col_order = [c for c in col_order if c in panel.columns]
    out = panel[col_order].copy()

    path_csv = f"{OUTPUT_DIR}/panel_final.csv"
    out.to_csv(path_csv, index=False, encoding="utf-8-sig")
    print(f"\n✓ 最終 panel 儲存：{path_csv}")

    # 也存一份 Excel（方便老師看）
    path_xlsx = f"{OUTPUT_DIR}/panel_final.xlsx"
    with pd.ExcelWriter(path_xlsx, engine="openpyxl") as writer:
        out.to_excel(writer, sheet_name="panel", index=False)
        # 另一個 sheet 放 pivot（貓犬比）
        if "cat_dog_ratio" in out.columns:
            pivot = out.pivot(index="district", columns="year", values="cat_dog_ratio")
            pivot.round(3).to_excel(writer, sheet_name="cat_dog_ratio_pivot")
    print(f"✓ Excel 版本儲存：{path_xlsx}")

    return out


# ── 主流程 ────────────────────────────────────────────────────────
def main():
    print("=== 04_merge_panel.py 開始 ===\n")

    try:
        pets, hh, hs = load_data()
    except FileNotFoundError as e:
        print(f"[錯誤] 找不到資料檔：{e}")
        print("請先執行 01、02、03 腳本")
        return

    print(f"讀入資料：")
    print(f"  寵物登記：{len(pets)} 筆")
    print(f"  戶政資料：{len(hh)} 筆")
    print(f"  住宅坪數：{len(hs)} 筆")

    panel = merge_all(pets, hh, hs)
    diagnostics(panel)
    final = export_for_r_stata(panel)

    print("\n── 最終 panel 前 12 筆 ──")
    print(final.head(12).to_string())
    print("\n完成！可以開始跑固定效果模型了 🎉")


if __name__ == "__main__":
    main()
