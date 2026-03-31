"""
戶政司 Open API - ODRP019「戶數、人口數按戶別及性別」
拉取臺北市 12 行政區，民國 106～113 年資料
輸出：odrp019_taipei_106_113.csv
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
YEARS = list(range(106, 114))  # 106~113（民國）
OUTPUT_CSV = "data_raw/odrp019_taipei_106_113.csv"

# 臺北市 12 行政區（用來驗證資料完整性）
TAIPEI_DISTRICTS = [
    "松山區", "信義區", "大安區", "中山區", "中正區", "大同區",
    "萬華區", "文山區", "南港區", "內湖區", "士林區", "北投區",
]


def fetch_year(year: int) -> list[dict]:
    """抓取指定年度、臺北市的所有分頁資料"""
    all_records = []
    page = 1

    while True:
        url = f"{BASE_URL}/{year}"
        params = {"COUNTY": COUNTY, "PAGE": str(page)}

        print(f"  → 抓取 {year} 年 第 {page} 頁 ...", end=" ")

        try:
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except requests.exceptions.RequestException as e:
            print(f"❌ 請求失敗: {e}")
            break
        except ValueError:
            print("❌ 回傳非 JSON")
            break

        code = data.get("responseCode", "")
        records = data.get("responseData", [])
        total_page = int(data.get("totalPage", "1"))
        total_size = data.get("totalDataSize", "?")

        # OD-0101-S = 處理完成（成功）；OD-0102-S = 查無資料
        if code not in ("OD-0100", "OD-0101-S"):
            print(f"❌ API 回應碼: {code} - {data.get('responseMessage', '')}")
            break

        print(f"✔ 取得 {len(records)} 筆（共 {total_size} 筆 / {total_page} 頁）")

        # 每筆加上年度欄位
        for rec in records:
            rec["statistic_yyy"] = str(year)
        all_records.extend(records)

        if page >= total_page:
            break
        page += 1
        time.sleep(0.3)  # 避免打太快被擋

    return all_records


def main():
    print("=" * 60)
    print(f"開始抓取 ODRP019（戶數、人口數按戶別及性別）")
    print(f"範圍：{COUNTY}，民國 {YEARS[0]}～{YEARS[-1]} 年")
    print("=" * 60)

    all_data = []

    for year in YEARS:
        print(f"\n📅 民國 {year} 年（西元 {year + 1911}）")
        records = fetch_year(year)
        all_data.extend(records)
        time.sleep(0.5)

    if not all_data:
        print("\n⚠️  沒有抓到任何資料，請確認網路連線及 API 是否正常。")
        sys.exit(1)

    # --------------------------------------------------------
    # 轉成 DataFrame
    # --------------------------------------------------------
    df = pd.DataFrame(all_data)

    # 加上西元年欄位
    if "statistic_yyy" in df.columns:
        df["year_ad"] = df["statistic_yyy"].astype(int) + 1911

    # --------------------------------------------------------
    # 篩選：只留行政區層級（排除村里明細及縣市小計）
    # --------------------------------------------------------
    # API 回傳可能包含村里級資料，我們只要「鄉鎮市區」級別
    # 判斷方式：site_id（區名）不為空，且 village 為空或為該區小計
    if "village" in df.columns:
        # 只保留 village 為空（即行政區小計列）的資料
        df_district = df[
            (df["village"].isna()) | (df["village"].str.strip() == "")
        ].copy()
    else:
        df_district = df.copy()

    # 如果上面篩不到，退而求其次保留所有資料讓使用者自行篩選
    if df_district.empty:
        print("\n⚠️  無法自動篩選行政區層級，保留全部資料供手動篩選。")
        df_district = df.copy()

    # --------------------------------------------------------
    # 輸出
    # --------------------------------------------------------
    df_district.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")

    print("\n" + "=" * 60)
    print(f"✅ 完成！共 {len(df_district)} 筆資料")
    print(f"📄 已輸出：{OUTPUT_CSV}")
    print("=" * 60)

    # 快速預覽
    print("\n📊 欄位清單：")
    for col in df_district.columns:
        print(f"   - {col}")

    print(f"\n📊 前 5 筆預覽：")
    print(df_district.head().to_string(index=False))

    # 驗證行政區完整性
    if "site_id" in df_district.columns:
        site_col = "site_id"
    elif "town" in df_district.columns:
        site_col = "town"
    else:
        site_col = None

    if site_col:
        districts_found = df_district[site_col].dropna().unique()
        print(f"\n📊 涵蓋行政區（{len(districts_found)} 個）：")
        for d in sorted(districts_found):
            mark = "✔" if d in TAIPEI_DISTRICTS else "  "
            print(f"   {mark} {d}")


if __name__ == "__main__":
    main()
