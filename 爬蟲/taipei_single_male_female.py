"""
戶政司 Open API - ODRP019「戶數、人口數按戶別及性別」
拉取臺北市 12 行政區（里級），民國 104～114 年資料
輸出：data_raw/odrp019_taipei_104_114.csv

注意：
  - 104~105 年需要另外從 data_raw/single/opendata{104,105}Y010.csv 匯入（API 無此年度）
  - 113 年起 API 欄位名稱從英文改為中文，且 COUNTY 參數失效，
    需抓取全國資料後自行篩選臺北市
"""

import requests
import pandas as pd
import time
import sys

# ============================================================
# 設定
# ============================================================
BASE_URL = "https://www.ris.gov.tw/rs-opendata/api/v1/datastore/ODRP019"
COUNTY = "臺北市"
YEARS = list(range(104, 115))  # 104~114（民國）
OUTPUT_CSV = "data/taipei_single_male_female.csv"

# 104~105 年 API 無資料，需從本機 CSV 匯入
LOCAL_CSV_MAP = {
    104: "data_raw/single/opendata104Y010.csv",
    105: "data_raw/single/opendata105Y010.csv",
}

# 臺北市 12 行政區（用來驗證資料完整性）
TAIPEI_DISTRICTS = [
    "松山區", "信義區", "大安區", "中山區", "中正區", "大同區",
    "萬華區", "文山區", "南港區", "內湖區", "士林區", "北投區",
]

# 113 年起 API 回傳中文欄位，需對應回英文欄位名
COL_MAP_ZH_TO_EN = {
    "統計年": "statistic_yyy",
    "區域別": "site_id",
    "村里名稱": "village",
    "共同生活戶_戶數": "household_ordinary_total",
    "共同事業戶_戶數": "household_business_total",
    "單獨生活戶_戶數": "household_single_total",
    "共同生活戶_男": "household_ordinary_m",
    "共同事業戶_男": "household_business_m",
    "單獨生活戶_男": "household_single_m",
    "共同生活戶_女": "household_ordinary_f",
    "共同事業戶_女": "household_business_f",
    "單獨生活戶_女": "household_single_f",
}


def fetch_year(year: int) -> list[dict]:
    """抓取指定年度、臺北市的所有分頁資料。
    先嘗試帶 COUNTY 參數；若查無資料則改抓全國再篩選。
    """
    all_records = []
    page = 1

    # 第一次嘗試：帶 COUNTY 參數
    url = f"{BASE_URL}/{year}"
    try:
        resp = requests.get(url, params={"COUNTY": COUNTY, "PAGE": "1"}, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except (requests.exceptions.RequestException, ValueError) as e:
        print(f"  [X] {year} 年請求失敗: {e}")
        return []

    code = data.get("responseCode", "")
    use_county_param = code in ("OD-0100", "OD-0101-S")

    if use_county_param:
        # COUNTY 參數有效，正常分頁抓取
        records = data.get("responseData", [])
        total_page = int(data.get("totalPage", "1"))
        total_size = data.get("totalDataSize", "?")
        print(f"  -> {year} 年 第 1 頁 (COUNTY 模式) "
              f"取得 {len(records)} 筆（共 {total_size} 筆 / {total_page} 頁）")
        all_records.extend(records)

        for p in range(2, total_page + 1):
            time.sleep(0.3)
            try:
                resp = requests.get(url, params={"COUNTY": COUNTY, "PAGE": str(p)}, timeout=30)
                resp.raise_for_status()
                data = resp.json()
            except (requests.exceptions.RequestException, ValueError) as e:
                print(f"  [X] {year} 年第 {p} 頁請求失敗: {e}")
                break
            records = data.get("responseData", [])
            print(f"  -> {year} 年 第 {p}/{total_page} 頁 取得 {len(records)} 筆")
            all_records.extend(records)
    else:
        # COUNTY 參數失效（如 113 年起），抓全國再篩選
        print(f"  -> {year} 年 COUNTY 參數無效，改抓全國資料篩選...")
        all_national = []
        page = 1
        while True:
            try:
                resp = requests.get(url, params={"PAGE": str(page)}, timeout=30)
                resp.raise_for_status()
                data = resp.json()
            except (requests.exceptions.RequestException, ValueError) as e:
                print(f"  [X] {year} 年第 {page} 頁請求失敗: {e}")
                break

            if data.get("responseCode", "") not in ("OD-0100", "OD-0101-S"):
                print(f"  [X] {year} 年 API 回應: {data.get('responseCode')} "
                      f"- {data.get('responseMessage', '')}")
                break

            records = data.get("responseData", [])
            total_page = int(data.get("totalPage", "1"))
            all_national.extend(records)
            print(f"  -> {year} 年 第 {page}/{total_page} 頁 取得 {len(records)} 筆")

            if page >= total_page:
                break
            page += 1
            time.sleep(0.3)

        # 判斷區域欄位名稱（中文或英文）並篩選臺北市
        if all_national:
            first = all_national[0]
            if "區域別" in first:
                all_records = [r for r in all_national if COUNTY in r.get("區域別", "")]
            elif "site_id" in first:
                all_records = [r for r in all_national if COUNTY in r.get("site_id", "")]
            else:
                all_records = all_national
        print(f"  -> 篩選臺北市後：{len(all_records)} 筆")

    return all_records


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """將中文欄位名統一轉為英文欄位名。"""
    # 如果有中文欄位，進行轉換
    if "區域別" in df.columns or "統計年" in df.columns:
        df = df.rename(columns=COL_MAP_ZH_TO_EN)
        # 移除多餘的中文欄位（如「區域別代碼」）
        extra_cols = [c for c in df.columns if c not in COL_MAP_ZH_TO_EN.values()
                      and c not in ("district_code", "year_ad")]
        if "區域別代碼" in df.columns:
            df["district_code"] = df["區域別代碼"]
        df = df.drop(columns=[c for c in extra_cols if c in df.columns], errors="ignore")
    return df


def load_local_csv(year: int) -> pd.DataFrame:
    """從本機 CSV 讀取指定年度資料（API 無此年度）。"""
    path = LOCAL_CSV_MAP.get(year)
    if not path:
        print(f"  [!] 無本機檔案對應 {year} 年")
        return pd.DataFrame()
    try:
        df = pd.read_csv(path, encoding="utf-8-sig", dtype=str)
    except FileNotFoundError:
        print(f"  [!] 找不到 {path}，跳過 {year} 年")
        return pd.DataFrame()

    df = df.rename(columns=COL_MAP_ZH_TO_EN)
    df = df[df["site_id"].str.startswith(COUNTY)].copy()

    # 移除多餘欄位
    keep = list(COL_MAP_ZH_TO_EN.values())
    df = df[[c for c in keep if c in df.columns]]

    print(f"  -> 從本機檔案讀取 {year} 年臺北市：{len(df)} 筆")
    return df


def main():
    print("=" * 60)
    print("ODRP019 fetch script")
    print(f"range: {COUNTY}, {YEARS[0]}~{YEARS[-1]}")
    print("=" * 60)

    frames = []

    for year in YEARS:
        print(f"\n[{year}] ({year + 1911})")

        if year in LOCAL_CSV_MAP:
            df_year = load_local_csv(year)
        else:
            records = fetch_year(year)
            if not records:
                continue
            df_year = pd.DataFrame(records)

        # 統一欄位名稱
        df_year = normalize_columns(df_year)

        # 確保 statistic_yyy 正確
        df_year["statistic_yyy"] = str(year)

        frames.append(df_year)
        time.sleep(0.5)

    if not frames:
        print("\n[!] No data fetched.")
        sys.exit(1)

    # --------------------------------------------------------
    # 合併 & 整理
    # --------------------------------------------------------
    df = pd.concat(frames, ignore_index=True)

    # 補齊欄位
    if "district_code" not in df.columns:
        df["district_code"] = ""
    df["year_ad"] = df["statistic_yyy"].astype(int) + 1911

    # 統一欄位順序
    col_order = [
        "statistic_yyy", "site_id", "village",
        "household_ordinary_total", "household_business_total", "household_single_total",
        "household_ordinary_m", "household_business_m", "household_single_m",
        "household_ordinary_f", "household_business_f", "household_single_f",
        "district_code", "year_ad",
    ]
    df = df[[c for c in col_order if c in df.columns]]

    # 排序
    df["_sort"] = df["statistic_yyy"].astype(int)
    df = df.sort_values(["_sort", "site_id", "village"]).drop(columns=["_sort"])

    # --------------------------------------------------------
    # 輸出
    # --------------------------------------------------------
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")

    print("\n" + "=" * 60)
    years_found = sorted(df["statistic_yyy"].astype(int).unique())
    print(f"Done! {len(df)} rows, years: {years_found}")
    for y in years_found:
        n = len(df[df["statistic_yyy"].astype(int) == y])
        print(f"  {y}: {n} rows")
    print(f"Output: {OUTPUT_CSV}")
    print("=" * 60)

    # 驗證行政區完整性
    if "site_id" in df.columns:
        districts = df["site_id"].str.replace(COUNTY, "", regex=False).unique()
        missing = [d for d in TAIPEI_DISTRICTS if d not in districts]
        if missing:
            print(f"\n[!] Missing districts: {missing}")
        else:
            print(f"\nAll 12 districts present.")


if __name__ == "__main__":
    main()
