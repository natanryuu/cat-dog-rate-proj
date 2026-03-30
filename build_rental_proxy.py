"""
build_rental_proxy.py
=====================
從實價登錄下載台北市租賃資料（2015–2023），
結合戶政司住宅戶數，產出區級年度租賃活躍度 proxy。

輸出：rental_proxy_panel.csv
欄位：year, district, rental_count, household_count, rental_ratio
"""

import time
import io
import requests
import pandas as pd
from pathlib import Path

# ── 設定 ──────────────────────────────────────────────────────────────────────

RAW_DIR  = Path("data/raw_rental")      # 存放下載快取的 CSV
OUT_FILE = Path("rental_proxy_panel.csv")

RAW_DIR.mkdir(exist_ok=True)

# 民國年對應西元年（2015–2024）
# 2024 只有 S1+S2，後續會做年化處理
YEAR_MAP = {
    2015: 104, 2016: 105, 2017: 106, 2018: 107,
    2019: 108, 2020: 109, 2021: 110, 2022: 111,
    2023: 112, 2024: 113,
}

# 各年可下載的季度（2024 只有上半年）
QUARTERS_MAP = {
    **{y: [1, 2, 3, 4] for y in range(2015, 2025)},
    2024: [1, 2],   # 只有 S1、S2
}

# 台北市代碼 = A；租賃檔 = A_lvr_land_C.csv
CITY_CODE = "A"
BASE_URL  = "https://plvr.land.moi.gov.tw/DownloadSeason"

# 台北市12行政區標準名稱（用於對齊不同來源）
DISTRICTS = [
    "松山區", "信義區", "大安區", "中山區", "中正區",
    "大同區", "萬華區", "文山區", "南港區", "內湖區",
    "士林區", "北投區",
]

# ── Step 1：下載實價登錄租賃資料 ──────────────────────────────────────────────

def download_season(roc_year: int, quarter: int) -> pd.DataFrame | None:
    """
    下載單季租賃 CSV，回傳 DataFrame。
    API 直接回傳 CSV（非 ZIP），失敗時印出警告並回傳 None。
    """
    season   = f"{roc_year}S{quarter}"
    filename = f"{CITY_CODE}_lvr_land_C.csv"
    cache    = RAW_DIR / f"{season}_{filename}"

    # 有快取就直接讀
    if cache.exists():
        print(f"  [快取] {season}")
        return pd.read_csv(cache, encoding="utf-8-sig", low_memory=False)

    # API 直接回傳 CSV（Content-Type: application/octet-stream）
    url = f"{BASE_URL}?season={season}&type=zip&fileName={filename}"
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()

        raw = resp.content

        # 存快取
        cache.write_bytes(raw)

        df = pd.read_csv(
            io.BytesIO(raw),
            encoding="utf-8-sig",
            low_memory=False,
        )
        print(f"  [下載] {season}：{len(df)} 筆")
        return df

    except Exception as e:
        print(f"  [錯誤] {season}：{e}")
        return None


def fetch_all_rental() -> pd.DataFrame:
    """
    下載 2015–2024 所有季度，合併成一張表，
    新增 ad_year（西元年）欄位。
    """
    frames = []
    for ad_year, roc_year in YEAR_MAP.items():
        for q in QUARTERS_MAP[ad_year]:
            df = download_season(roc_year, q)
            if df is None:
                continue

            # 欄位名稱因年份略有差異，統一抓「鄉鎮市區」
            col_district = next(
                (c for c in df.columns if "鄉鎮市區" in c), None
            )
            if col_district is None:
                print(f"  [警告] {roc_year}S{q}：找不到鄉鎮市區欄位，跳過")
                continue

            tmp = df[[col_district]].copy()
            tmp.columns = ["district"]
            tmp["ad_year"] = ad_year
            frames.append(tmp)

            time.sleep(0.5)   # 避免打太快被擋

    if not frames:
        raise RuntimeError("沒有成功下載任何資料，請確認網路或 URL 是否正常。")

    return pd.concat(frames, ignore_index=True)


# ── Step 2：統計各區每年租賃件數 ──────────────────────────────────────────────

def count_by_district_year(df: pd.DataFrame) -> pd.DataFrame:
    """
    groupby 鄉鎮市區 × 年度，計算租賃件數。
    只保留台北市12個行政區。
    2024 年只有 S1+S2（兩季），件數 × 2 年化，
    使各年具可比性，論文中需說明此處理方式。
    """
    df["district"] = df["district"].str.strip()
    df = df[df["district"].isin(DISTRICTS)].copy()

    counts = (
        df.groupby(["ad_year", "district"])
        .size()
        .reset_index(name="rental_count")
    )

    # 2024 年化：只有兩季 → 乘以 2 估算全年
    mask = counts["ad_year"] == 2024
    counts.loc[mask, "rental_count"] = (
        counts.loc[mask, "rental_count"] * 2
    )
    if mask.any():
        print("  [年化] 2024 年租賃件數已乘以 2（上半年→全年推估）")

    return counts


# ── Step 3：讀入住宅戶數（需手動填入或讀 CSV）─────────────────────────────────
#
# 資料來源：戶政司統計 > 各縣市鄉鎮市區戶數
# 網址：https://www.ris.gov.tw/app/portal/346
# 下載後存成 household.csv，格式：
#   year, district, household_count
#
# 如果尚未取得，以下提供一組 2015–2023 的概估值供測試用。
# 正式版請替換成官方數字。

HOUSEHOLD_PLACEHOLDER = {
    # 格式：(西元年, 行政區): 住宅戶數（粗估值，請替換）
    # 以下數字為示意，誤差約 ±5%
    **{(y, "松山區"): 62000 for y in range(2015, 2025)},
    **{(y, "信義區"): 72000 for y in range(2015, 2025)},
    **{(y, "大安區"): 110000 for y in range(2015, 2025)},
    **{(y, "中山區"): 94000 for y in range(2015, 2025)},
    **{(y, "中正區"): 56000 for y in range(2015, 2025)},
    **{(y, "大同區"): 46000 for y in range(2015, 2025)},
    **{(y, "萬華區"): 67000 for y in range(2015, 2025)},
    **{(y, "文山區"): 95000 for y in range(2015, 2025)},
    **{(y, "南港區"): 42000 for y in range(2015, 2025)},
    **{(y, "內湖區"): 98000 for y in range(2015, 2025)},
    **{(y, "士林區"): 98000 for y in range(2015, 2025)},
    **{(y, "北投區"): 90000 for y in range(2015, 2025)},
}


def load_household(csv_path: str | None = None) -> pd.DataFrame:
    """
    讀入住宅戶數資料。
    - 若 csv_path 存在，從 CSV 讀入（官方數字）。
    - 否則使用佔位估計值，並印出警告。
    """
    if csv_path and Path(csv_path).exists():
        df = pd.read_csv(csv_path, encoding="utf-8-sig")
        df.columns = ["year", "district", "household_count"]
        print(f"[住宅戶數] 從 {csv_path} 讀入 {len(df)} 筆官方資料")
        return df

    print("[警告] 找不到 household.csv，使用概估佔位值，正式分析請替換！")
    rows = [
        {"year": y, "district": d, "household_count": cnt}
        for (y, d), cnt in HOUSEHOLD_PLACEHOLDER.items()
    ]
    return pd.DataFrame(rows)


# ── Step 4：合併，計算 rental_ratio ───────────────────────────────────────────

def build_panel(
    rental_counts: pd.DataFrame,
    household: pd.DataFrame,
) -> pd.DataFrame:
    """
    合併租賃件數與住宅戶數，計算 rental_ratio。
    """
    panel = rental_counts.merge(
        household,
        left_on=["ad_year", "district"],
        right_on=["year", "district"],
        how="left",
    )

    # 補齊可能 missing 的 year 欄
    panel["year"] = panel["ad_year"]
    panel = panel[["year", "district", "rental_count", "household_count"]]

    # 計算 proxy：年度租賃件數 / 住宅戶數
    panel["rental_ratio"] = (
        panel["rental_count"] / panel["household_count"]
    ).round(6)

    # 補全缺值行（某區某年沒有租賃紀錄 → 件數填 0）
    full_index = pd.MultiIndex.from_product(
        [list(YEAR_MAP.keys()), DISTRICTS],
        names=["year", "district"],
    )
    panel = (
        panel.set_index(["year", "district"])
        .reindex(full_index, fill_value=0)
        .reset_index()
    )

    # household_count 不能被 0 填充，重新 merge 補回
    panel = panel.drop(columns=["household_count", "rental_ratio"])
    panel = panel.merge(household, on=["year", "district"], how="left")
    panel["rental_ratio"] = (
        panel["rental_count"] / panel["household_count"]
    ).round(6)

    return panel.sort_values(["district", "year"]).reset_index(drop=True)


# ── Step 5：輸出 & 快速診斷 ───────────────────────────────────────────────────

def diagnostics(panel: pd.DataFrame) -> None:
    print("\n=== 資料摘要 ===")
    print(f"觀測數：{len(panel)}（應為 {len(DISTRICTS) * len(YEAR_MAP)} = {len(DISTRICTS)*len(YEAR_MAP)}）")
    print(f"缺值：\n{panel.isnull().sum()}")

    print("\n=== rental_ratio 描述統計 ===")
    print(panel["rental_ratio"].describe().round(4))

    print("\n=== 各區平均 rental_ratio（排序）===")
    avg = (
        panel.groupby("district")["rental_ratio"]
        .mean()
        .sort_values(ascending=False)
        .round(4)
    )
    print(avg.to_string())

    print("\n=== 各年度平均 rental_ratio ===")
    yr = (
        panel.groupby("year")["rental_ratio"]
        .mean()
        .round(4)
    )
    print(yr.to_string())


# ── 主流程 ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":

    # 1. 下載實價登錄
    print("Step 1：下載實價登錄租賃資料...")
    rental_raw = fetch_all_rental()

    # 2. 統計件數
    print("\nStep 2：統計各區年度租賃件數...")
    rental_counts = count_by_district_year(rental_raw)
    print(f"  共 {len(rental_counts)} 筆區×年組合")

    # 3. 讀入住宅戶數
    #    正式版：把官方 CSV 路徑填入，例如 "household.csv"
    print("\nStep 3：讀入住宅戶數...")
    household = load_household(csv_path=None)   # <-- 有官方 CSV 請填路徑

    # 4. 建立面板
    print("\nStep 4：合併，計算 rental_ratio...")
    panel = build_panel(rental_counts, household)

    # 5. 輸出
    panel.to_csv(OUT_FILE, index=False, encoding="utf-8-sig")
    print(f"\n[完成] 輸出至 {OUT_FILE}")

    # 6. 診斷
    diagnostics(panel)