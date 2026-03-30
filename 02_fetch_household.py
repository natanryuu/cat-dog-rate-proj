"""
02_fetch_household.py
=====================
從戶政司 opendata API 抓取台北市各區兩項變數（2015–2023）：

  1. population      — 年底人口數        (ODRP019)
  2. avg_household_size — 平均戶量 = 人口 ÷ 戶數  (ODRP005，取12月)

【代理變數說明】
  單人戶比例無區級 API，改用「平均戶量」作代理：
  平均戶量越小 ≈ 小家庭 / 單人戶比例越高（負相關）
  論文說明範例：
    "因現行開放資料缺乏區級單人戶統計，本研究以各區年底
     平均每戶人口數（household size）作為代理變數。
     平均戶量與單人戶比例具高度負相關（Pearson r ≈ -0.9x，
     見附錄），其趨勢亦與行政院性別平等統計資料庫之
     全國單人戶走勢一致。"

【API 來源】
  ODRP019：各縣市鄉鎮市區人口數（年底）
    https://www.ris.gov.tw/rs-opendata/api/v1/datastore/ODRP019/{yyy}
  ODRP005：村里戶數、單一年齡人口（月資料）
    https://www.ris.gov.tw/rs-opendata/api/v1/datastore/ODRP005/{yyymm}
    → 取每年 12 月（yyyM12）做年底截面
"""

import requests
import pandas as pd
import time
import os

RAW_DIR   = "data/raw"
CLEAN_DIR = "data/clean"
os.makedirs(RAW_DIR,   exist_ok=True)
os.makedirs(CLEAN_DIR, exist_ok=True)

# 台北市 12 區 site_id（6碼，戶政司編碼）
TAIPEI_SITE_IDS = {
    "松山區": "63000010",
    "信義區": "63000020",
    "大安區": "63000030",
    "中山區": "63000040",
    "中正區": "63000050",
    "大同區": "63000060",
    "萬華區": "63000070",
    "文山區": "63000080",
    "南港區": "63000090",
    "內湖區": "63000100",
    "士林區": "63000110",
    "北投區": "63000120",
}
SITE_TO_DIST = {v: k for k, v in TAIPEI_SITE_IDS.items()}

TARGET_YEARS    = list(range(2015, 2024))          # 西元
TARGET_ROCYEARS = [y - 1911 for y in TARGET_YEARS] # 民國 104–112

BASE = "https://www.ris.gov.tw/rs-opendata/api/v1/datastore"
HEADERS = {"Accept": "application/json"}


# ══════════════════════════════════════════════════════════════════
# 共用：打 API 拿 JSON
# ══════════════════════════════════════════════════════════════════
def fetch_json(url: str) -> list:
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        r.raise_for_status()
        data = r.json()
        # 回傳格式：{"responseData":[...]} 或直接是 list
        if isinstance(data, list):
            return data
        for key in ("responseData", "data", "result"):
            if key in data and isinstance(data[key], list):
                return data[key]
        print(f"  [警告] 未知格式，keys={list(data.keys())[:5]}")
        return []
    except Exception as e:
        print(f"  [錯誤] {url}\n         {e}")
        return []


# ══════════════════════════════════════════════════════════════════
# Part A：年底人口數 ODRP019
# ══════════════════════════════════════════════════════════════════
def fetch_population() -> pd.DataFrame:
    print("── Part A：年底人口數（ODRP019）──")
    frames = []

    for roc_yr in TARGET_ROCYEARS:
        ad_yr = roc_yr + 1911
        url   = f"{BASE}/ODRP019/{roc_yr}"
        print(f"  {ad_yr}（民{roc_yr}）...", end=" ", flush=True)

        records = fetch_json(url)
        if not records:
            print("✗")
            time.sleep(0.8)
            continue

        df = pd.DataFrame(records)

        # 找 site_id 欄
        id_col  = next((c for c in df.columns if "site_id" in c.lower()), None)
        pop_col = next((c for c in df.columns
                        if "people_total" in c.lower()
                        or c in ("總計", "人口總計")), None)

        if not id_col or not pop_col:
            print(f"✗ 欄位異常 {list(df.columns)[:8]}")
            time.sleep(0.8)
            continue

        df[id_col] = df[id_col].astype(str).str.zfill(6)
        sub = df[df[id_col].isin(SITE_TO_DIST)].copy()
        sub["district"]  = sub[id_col].map(SITE_TO_DIST)
        sub["year"]      = ad_yr
        sub["population"] = pd.to_numeric(sub[pop_col], errors="coerce")
        frames.append(sub[["district", "year", "population"]])
        print(f"✓ {len(sub)} 區")
        time.sleep(0.6)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


# ══════════════════════════════════════════════════════════════════
# Part B：平均戶量 ODRP005（取每年 12 月，聚合到區）
# ══════════════════════════════════════════════════════════════════
def fetch_household_size() -> pd.DataFrame:
    print("\n── Part B：平均戶量（ODRP005，取12月）──")
    frames = []

    for roc_yr in TARGET_ROCYEARS:
        ad_yr  = roc_yr + 1911
        yyymm  = f"{roc_yr}12"          # 民國年 + 月份 → e.g. "10412"
        url    = f"{BASE}/ODRP005/{yyymm}"
        print(f"  {ad_yr}-12（民{yyymm}）...", end=" ", flush=True)

        records = fetch_json(url)
        if not records:
            print("✗")
            time.sleep(0.8)
            continue

        df = pd.DataFrame(records)

        # 找欄位
        id_col  = next((c for c in df.columns if "site_id" in c.lower()), None)
        hh_col  = next((c for c in df.columns if "household_no" in c.lower()
                        or "戶數" in c), None)
        pop_col = next((c for c in df.columns if "people_total" in c.lower()
                        and "age" not in c.lower()), None)

        if not id_col or not hh_col or not pop_col:
            print(f"✗ 欄位異常 {list(df.columns)[:8]}")
            time.sleep(0.8)
            continue

        # site_id 前 8 碼 = 鄉鎮市區代碼（ODRP005 是村里級，需聚合到區）
        df["district_id"] = df[id_col].astype(str).str.zfill(8).str[:8].str.zfill(6)
        # 台北市區代碼是 6 碼，ODRP005 site_id 是 8 碼（含村里）
        # 取前 6 碼比對
        df["district_id6"] = df[id_col].astype(str).str[:6].str.zfill(6)
        sub = df[df["district_id6"].isin(SITE_TO_DIST)].copy()

        if sub.empty:
            # 備援：嘗試前 8 碼前 6 字元
            df["district_id6"] = df[id_col].astype(str).str.zfill(8).str[:6]
            sub = df[df["district_id6"].isin(SITE_TO_DIST)].copy()

        if sub.empty:
            print("✗ 找不到台北市資料")
            time.sleep(0.8)
            continue

        sub["hh"]  = pd.to_numeric(sub[hh_col],  errors="coerce")
        sub["pop"] = pd.to_numeric(sub[pop_col],  errors="coerce")
        sub["district"] = sub["district_id6"].map(SITE_TO_DIST)

        # 聚合：同區各村里加總後再算平均戶量
        agg = (
            sub.groupby("district")[["hh", "pop"]]
            .sum()
            .reset_index()
        )
        agg["avg_household_size"] = agg["pop"] / agg["hh"]
        agg["year"] = ad_yr
        frames.append(agg[["district", "year", "avg_household_size"]])
        print(f"✓ {len(agg)} 區  (avg {agg['avg_household_size'].mean():.2f} 人/戶)")
        time.sleep(0.6)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


# ══════════════════════════════════════════════════════════════════
# Part C：合併 + 診斷 + 儲存
# ══════════════════════════════════════════════════════════════════
def load_single_hh_pdf() -> pd.DataFrame:
    """若 02b 已執行，載入 PDF 解析的單人戶比例"""
    path = f"{CLEAN_DIR}/single_hh_pdf.csv"
    if not os.path.exists(path):
        return pd.DataFrame()
    df = pd.read_csv(path, encoding="utf-8-sig")
    # 轉成比例（0–1），原始是百分比
    if "single_hh_pct" in df.columns:
        df["single_hh_ratio"] = df["single_hh_pct"] / 100
    print(f"  ✓ 載入單人戶 PDF 資料：{len(df)} 筆")
    return df[["district", "year", "single_hh_ratio"]]


def merge_and_save(pop_df: pd.DataFrame, hh_df: pd.DataFrame):
    print("\n── Part C：合併診斷 ──")

    # 完整骨架
    skeleton = pd.DataFrame(
        [(d, y) for d in TAIPEI_SITE_IDS for y in TARGET_YEARS],
        columns=["district", "year"]
    )

    # 嘗試載入 PDF 解析的單人戶資料（02b 產出）
    pdf_hh_df = load_single_hh_pdf()

    result = skeleton.copy()
    if not pop_df.empty:
        result = result.merge(pop_df, on=["district","year"], how="left")
    if not hh_df.empty:
        result = result.merge(hh_df,  on=["district","year"], how="left")
    if not pdf_hh_df.empty:
        result = result.merge(pdf_hh_df, on=["district","year"], how="left")
        print("  ✓ 已合併單人戶比例（來自 PDF）")

    print(f"  總筆數：{len(result)}（預期 108）")

    miss = result.isnull().sum()
    if miss.sum() > 0:
        print(f"\n  缺失值：\n{miss[miss>0].to_string()}")
    else:
        print("  ✓ 無缺失值")

    # 人口 pivot
    if "population" in result.columns:
        print("\n  人口數（千人）各區趨勢：")
        piv = result.pivot(index="district", columns="year", values="population")
        print((piv / 1000).round(1).to_string())

    # 平均戶量 pivot
    if "avg_household_size" in result.columns:
        print("\n  平均戶量（人/戶）各區趨勢：")
        piv2 = result.pivot(index="district", columns="year", values="avg_household_size")
        print(piv2.round(2).to_string())

    out = f"{CLEAN_DIR}/household_clean.csv"
    result.to_csv(out, index=False, encoding="utf-8-sig")
    print(f"\n✓ 儲存：{out}")

    return result


# ══════════════════════════════════════════════════════════════════
# 主流程
# ══════════════════════════════════════════════════════════════════
def main():
    print("=== 02_fetch_household.py ===")
    print("變數：population（人口數）＋ avg_household_size（平均戶量）\n")

    pop_df = fetch_population()
    hh_df  = fetch_household_size()

    if pop_df.empty and hh_df.empty:
        print("[結束] 兩份資料均失敗，請確認網路連線")
        return

    merge_and_save(pop_df, hh_df)
    print("\n完成！接著執行 03_fetch_housing.py")


if __name__ == "__main__":
    main()
