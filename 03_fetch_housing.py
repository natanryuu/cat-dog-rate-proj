"""
03_fetch_housing.py
===================
從內政部實價登錄取得各區住宅平均坪數（housing_size）

【資料來源選項】

選項 A（推薦）：內政部不動產交易實際資訊（CSV 批次下載）
  下載網址：https://plvr.land.moi.gov.tw/DownloadOpenData
  → 選「台北市」→ 每季 CSV
  → 存放到 data/raw/realestate/ 資料夾

選項 B：內政部開放資料 API（限最近兩年）
  https://www.land.moi.gov.tw/chhtml/content/182

選項 C（備用）：台北市住宅存量統計（有時有坪數分布）
  https://pip.moi.gov.tw/V3/E/SCRE0201.aspx

【欄位說明】
  我們用「建物坪數」（建坪，不含車位）的各區年度中位數或平均數
  代理「住宅大小」這個自變數
"""

import pandas as pd
import os
import glob

RAW_DIR   = "data/raw/realestate"
CLEAN_DIR = "data/clean"
os.makedirs(CLEAN_DIR, exist_ok=True)

TAIPEI_DISTRICTS = [
    "松山區", "信義區", "大安區", "中山區", "中正區",
    "大同區", "萬華區", "文山區", "南港區", "內湖區",
    "士林區", "北投區"
]

TARGET_YEARS = list(range(2015, 2024))

# 實價登錄 CSV 的關鍵欄位（實際欄位名稱以下載版為準）
COL_MAP = {
    "鄉鎮市區": "district",
    "建物移轉總面積平方公尺": "building_area_m2",
    "交易年月日": "trade_date",
    "主要用途": "main_use",
    "主要建材": "main_material",
    "總價元": "total_price",
}

SQFT_TO_PING = 1 / 3.305785   # 1 坪 = 3.305785 平方公尺


# ── 讀取並清理單一 CSV ────────────────────────────────────────────
def process_csv(filepath: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(filepath, encoding="utf-8", low_memory=False)
    except UnicodeDecodeError:
        df = pd.read_csv(filepath, encoding="big5", low_memory=False)

    df.columns = df.columns.str.strip()

    # 重新命名欄位
    rename = {k: v for k, v in COL_MAP.items() if k in df.columns}
    df = df.rename(columns=rename)

    required = ["district", "building_area_m2", "trade_date"]
    if not all(c in df.columns for c in required):
        print(f"  [跳過] 缺少必要欄位：{filepath}")
        return pd.DataFrame()

    # 只留住宅（主要用途含「住」）
    if "main_use" in df.columns:
        df = df[df["main_use"].astype(str).str.contains("住", na=False)]

    # 只留台北市 12 區
    df["district"] = df["district"].astype(str).str.strip()
    df = df[df["district"].isin(TAIPEI_DISTRICTS)].copy()

    # 面積轉坪
    df["building_area_m2"] = pd.to_numeric(df["building_area_m2"], errors="coerce")
    df["building_ping"]    = df["building_area_m2"] * SQFT_TO_PING

    # 排除異常值（< 5 坪或 > 500 坪）
    df = df[(df["building_ping"] >= 5) & (df["building_ping"] <= 500)]

    # 取年份
    # 實價登錄日期格式：民國 1040101 → 取前 3 碼 + 1911
    df["trade_date"] = df["trade_date"].astype(str).str.strip()
    df["year"] = df["trade_date"].str[:3].apply(
        lambda x: int(x) + 1911 if x.isdigit() and len(x) == 3 else None
    )
    df = df[df["year"].isin(TARGET_YEARS)].copy()

    return df[["district", "year", "building_ping", "total_price"]
              if "total_price" in df.columns
              else ["district", "year", "building_ping"]]


# ── 聚合為各區各年 ────────────────────────────────────────────────
def aggregate_by_district_year(df: pd.DataFrame) -> pd.DataFrame:
    agg = (
        df.groupby(["district", "year"])["building_ping"]
        .agg(
            housing_size_mean="mean",
            housing_size_median="median",
            housing_size_n="count"
        )
        .reset_index()
    )
    # 主要自變數用中位數（對離群值比較穩健）
    agg["housing_size"] = agg["housing_size_median"]
    return agg


# ── 主流程 ────────────────────────────────────────────────────────
def main():
    print("=== 03_fetch_housing.py 開始 ===\n")

    csv_files = glob.glob(f"{RAW_DIR}/**/*.csv", recursive=True)
    csv_files += glob.glob(f"{RAW_DIR}/*.csv")

    if not csv_files:
        print(f"[錯誤] 在 {RAW_DIR} 找不到 CSV 檔案")
        print("請到 https://plvr.land.moi.gov.tw/DownloadOpenData 下載台北市各季資料")
        return

    print(f"找到 {len(csv_files)} 個 CSV 檔案，開始處理...")

    frames = []
    for f in sorted(csv_files):
        print(f"  處理：{os.path.basename(f)}")
        df = process_csv(f)
        if not df.empty:
            frames.append(df)

    if not frames:
        print("[錯誤] 所有檔案處理失敗")
        return

    all_data = pd.concat(frames, ignore_index=True)
    print(f"\n合計：{len(all_data):,} 筆住宅交易")

    # 聚合
    panel = aggregate_by_district_year(all_data)
    panel = panel.sort_values(["district", "year"]).reset_index(drop=True)

    # 診斷：應有 108 筆
    print(f"\n── 聚合後：{len(panel)} 筆（預期 108）──")
    missing = 108 - len(panel)
    if missing > 0:
        print(f"  [警告] 缺少 {missing} 筆（某些區/年交易量可能為零）")

    print("\n── 各區樣本數（確認是否有稀疏問題）──")
    print(panel.groupby("district")["housing_size_n"].sum().sort_values())

    out_path = f"{CLEAN_DIR}/housing_clean.csv"
    panel.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"\n✓ 儲存：{out_path}")
    print(panel.head(12).to_string())


if __name__ == "__main__":
    main()
