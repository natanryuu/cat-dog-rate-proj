"""
台北市12行政區 房屋型態比例 (2014-2025)
資料來源：內政部不動產交易實價查詢服務網（實價登錄）
https://plvr.land.moi.gov.tw/

建物型態分類：公寓、華廈、住宅大樓、套房、透天厝、其他
"""

import pandas as pd
import requests
import zipfile
import io
import os
import time
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")

# ============================================================
# 設定
# ============================================================
OUTPUT_DIR = Path("data_raw/building_type")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# 台北市 12 行政區
TAIPEI_DISTRICTS = [
    "中正區", "大同區", "中山區", "松山區", "大安區", "萬華區",
    "信義區", "士林區", "北投區", "內湖區", "南港區", "文山區"
]

# 建物型態標準化對照（實價登錄原始欄位 → 簡化分類）
BUILDING_TYPE_MAP = {
    "公寓(5樓含以下無電梯)": "公寓",
    "華廈(10層含以下有電梯)": "華廈",
    "住宅大樓(11層含以上有電梯)": "住宅大樓",
    "套房(1房1廳1衛)": "套房",
    "透天厝": "透天厝",
    # 其餘歸類為「其他」
}

# 年度 → 民國年
def ad_to_roc(year: int) -> int:
    return year - 1911

# ============================================================
# Step 1: 下載實價登錄資料（按季）
# ============================================================
def download_season_data(roc_year: int, quarter: int, data_dir: Path) -> Path | None:
    """
    從實價登錄下載指定季度的 CSV 壓縮檔，解壓後回傳資料夾路徑。
    檔案命名：{roc_year}S{quarter}
    """
    season_tag = f"{roc_year}S{quarter}"
    season_dir = data_dir / season_tag

    if season_dir.exists() and any(season_dir.glob("*.csv")):
        print(f"  [快取] {season_tag} 已存在，跳過下載")
        return season_dir

    # 內政部實價登錄 Open Data 下載 URL
    url = (
        f"https://plvr.land.moi.gov.tw/DownloadSeason"
        f"?season={season_tag}&type=zip&fileName=lvr_landcsv.zip"
    )

    try:
        print(f"  [下載] {season_tag} ...", end=" ")
        resp = requests.get(url, timeout=30, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })
        resp.raise_for_status()

        season_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            zf.extractall(season_dir)
        print("OK")
        return season_dir

    except Exception as e:
        print(f"失敗 ({e})")
        return None


def download_all(start_year=2014, end_year=2025) -> Path:
    """下載所有年度 × 季度的資料，若該季資料夾已存在則跳過"""
    data_dir = OUTPUT_DIR  # 季度資料夾直接放在 data_raw/building_type/ 下

    for year in range(start_year, end_year + 1):
        roc = ad_to_roc(year)
        print(f"\n▶ {year}年 (民國{roc}年)")
        for q in range(1, 5):
            download_season_data(roc, q, data_dir)
            time.sleep(1)  # 禮貌性延遲

    return data_dir


# ============================================================
# Step 2: 讀取 & 合併 CSV
# ============================================================
def read_one_season(season_dir: Path, roc_year: int) -> pd.DataFrame:
    """
    讀取單一季度資料夾中的台北市不動產買賣 CSV。
    台北市代碼為 'a'，買賣檔案名通常為 a_lvr_land_a.csv
    """
    # 可能的檔名格式（歷年略有不同）
    candidates = [
        "a_lvr_land_a.csv",       # 標準
        "A_LVR_LAND_A.csv",       # 大寫
        "a_lvr_land_a.CSV",
    ]

    target = None
    for c in candidates:
        p = season_dir / c
        if p.exists():
            target = p
            break

    if target is None:
        # 嘗試找任何以 a_ 開頭的 land 檔案
        matches = list(season_dir.glob("[aA]_lvr_land_[aA].*"))
        if matches:
            target = matches[0]

    if target is None:
        return pd.DataFrame()

    try:
        # 實價登錄 CSV 前兩列通常是中英文欄位名，取中文列為 header
        df = pd.read_csv(target, encoding="utf-8-sig", low_memory=False)

        # 如果第一列是英文欄位名，跳過它
        if df.iloc[0].astype(str).str.contains("district|city", case=False).any():
            df = df.iloc[1:].reset_index(drop=True)

        return df
    except Exception as e:
        print(f"    讀取失敗 {target}: {e}")
        return pd.DataFrame()


def load_all_data(data_dir: Path, start_year=2014, end_year=2025) -> pd.DataFrame:
    """合併所有季度資料"""
    frames = []

    for year in range(start_year, end_year + 1):
        roc = ad_to_roc(year)
        for q in range(1, 5):
            season_tag = f"{roc}S{q}"
            season_dir = data_dir / season_tag
            if not season_dir.exists():
                continue

            df = read_one_season(season_dir, roc)
            if df.empty:
                continue

            df["_year"] = year
            df["_quarter"] = q
            frames.append(df)

    if not frames:
        raise ValueError("沒有成功讀取任何資料，請確認下載是否成功")

    combined = pd.concat(frames, ignore_index=True)
    print(f"\n✅ 共讀取 {len(combined):,} 筆交易紀錄")
    return combined


# ============================================================
# Step 3: 清理 & 計算房屋型態比例
# ============================================================
def standardize_building_type(raw: str) -> str:
    """將原始建物型態對應到簡化分類"""
    if pd.isna(raw):
        return "其他"
    raw = str(raw).strip()
    return BUILDING_TYPE_MAP.get(raw, "其他")


def compute_housing_type_proportions(df: pd.DataFrame) -> pd.DataFrame:
    """
    計算每年每區各建物型態的交易比例。
    回傳長格式 DataFrame: year, district, building_type, count, proportion
    """
    # 找出正確的欄位名稱（歷年 CSV 欄位名可能不同）
    district_col = None
    type_col = None

    for col in df.columns:
        col_str = str(col).strip()
        if "鄉鎮市區" in col_str:
            district_col = col
        elif "建物型態" in col_str:
            type_col = col

    if district_col is None or type_col is None:
        raise ValueError(
            f"找不到必要欄位。現有欄位: {list(df.columns)}\n"
            f"district_col={district_col}, type_col={type_col}"
        )

    print(f"  使用欄位: 區={district_col}, 建物型態={type_col}")

    # 篩選台北市 12 區
    df = df[df[district_col].isin(TAIPEI_DISTRICTS)].copy()
    print(f"  篩選後筆數: {len(df):,}")

    # 標準化建物型態
    df["building_type"] = df[type_col].apply(standardize_building_type)

    # 計算各 (年, 區, 型態) 的交易筆數
    grouped = (
        df.groupby(["_year", district_col, "building_type"])
        .size()
        .reset_index(name="count")
    )
    grouped.rename(columns={district_col: "district", "_year": "year"}, inplace=True)

    # 計算比例
    totals = grouped.groupby(["year", "district"])["count"].transform("sum")
    grouped["proportion"] = (grouped["count"] / totals).round(4)

    return grouped.sort_values(["year", "district", "building_type"]).reset_index(drop=True)


# ============================================================
# Step 4: 輸出寬表 (pivot)
# ============================================================
def make_pivot_table(long_df: pd.DataFrame) -> pd.DataFrame:
    """
    轉換成寬格式：index=(year, district), columns=building_type, values=proportion
    """
    pivot = long_df.pivot_table(
        index=["year", "district"],
        columns="building_type",
        values="proportion",
        fill_value=0
    )

    # 確保型態順序
    desired_order = ["公寓", "華廈", "住宅大樓", "套房", "透天厝", "其他"]
    existing = [c for c in desired_order if c in pivot.columns]
    pivot = pivot[existing]

    pivot.columns.name = None
    return pivot.reset_index()


# ============================================================
# Step 5: 輸出摘要統計
# ============================================================
def print_summary(pivot_df: pd.DataFrame):
    """印出摘要"""
    print("\n" + "=" * 70)
    print("📊 台北市12行政區 房屋型態交易比例 (2014-2025)")
    print("=" * 70)

    # 每區跨年平均
    type_cols = [c for c in pivot_df.columns if c not in ["year", "district"]]
    avg_by_district = pivot_df.groupby("district")[type_cols].mean().round(4)
    print("\n▼ 各區平均比例 (跨年)")
    print(avg_by_district.to_string())

    # 每年全市平均
    avg_by_year = pivot_df.groupby("year")[type_cols].mean().round(4)
    print("\n▼ 全市年度平均比例")
    print(avg_by_year.to_string())


# ============================================================
# Main
# ============================================================
def main():
    START_YEAR = 2014
    END_YEAR = 2025

    print("=" * 70)
    print("台北市12行政區 房屋型態比例 資料蒐集程式")
    print(f"年度範圍: {START_YEAR} - {END_YEAR}")
    print("資料來源: 內政部不動產交易實價查詢服務網")
    print("=" * 70)

    # 1. 下載
    print("\n[Step 1] 下載實價登錄資料...")
    data_dir = download_all(START_YEAR, END_YEAR)

    # 2. 讀取
    print("\n[Step 2] 讀取並合併 CSV...")
    raw_df = load_all_data(data_dir, START_YEAR, END_YEAR)

    # 3. 計算比例
    print("\n[Step 3] 計算各區房屋型態比例...")
    long_df = compute_housing_type_proportions(raw_df)

    # 4. 寬表
    pivot_df = make_pivot_table(long_df)

    # 5. 輸出
    # 長格式
    long_path = OUTPUT_DIR / "taipei_housing_type_long.csv"
    long_df.to_csv(long_path, index=False, encoding="utf-8-sig")
    print(f"\n✅ 長格式已存: {long_path}")

    # 寬格式
    wide_path = OUTPUT_DIR / "taipei_housing_type_wide.csv"
    pivot_df.to_csv(wide_path, index=False, encoding="utf-8-sig")
    print(f"✅ 寬格式已存: {wide_path}")

    # 摘要
    print_summary(pivot_df)

    # Panel data 格式 (適合迴歸分析)
    panel_path = OUTPUT_DIR / "taipei_housing_type_panel.csv"
    panel = pivot_df.copy()
    panel.to_csv(panel_path, index=False, encoding="utf-8-sig")
    print(f"\n✅ Panel 格式已存: {panel_path}")
    print(f"   維度: {panel.shape[0]} 列 × {panel.shape[1]} 欄")
    print(f"   預期: {len(TAIPEI_DISTRICTS)} 區 × {END_YEAR - START_YEAR + 1} 年 = {len(TAIPEI_DISTRICTS) * (END_YEAR - START_YEAR + 1)} 列")

    return pivot_df


if __name__ == "__main__":
    df = main()
