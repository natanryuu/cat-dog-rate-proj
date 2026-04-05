"""
fetch_migration_v2.py — 台北市 12 區淨遷移率
================================================
data.taipei 的遷徙資料是按年度分檔的 CSV（Big5 編碼），
不是 API 查詢格式。本腳本直接下載 CSV 檔案。

已知的 resource ID：
  109年:     23b57598-2301-498a-848b-1e79aa4be3b7
  110年:     789a5fb4-34d0-4812-b59f-4e70a1771a08
  111年起迄今: 120906e8-cebe-475d-bf84-a0b815eec3a2

104–108 年（2015–2019）可能需要手動從 data.taipei 頁面下載。
腳本會先抓能抓的，再告訴你缺什麼。

Output: data/migration_panel_orgin.csv
"""

import requests
import pandas as pd
import io
import time
from pathlib import Path

OUTPUT_DIR = Path("data")
OUTPUT_DIR.mkdir(exist_ok=True)

DISTRICTS = [
    "松山區", "信義區", "大安區", "中山區", "中正區", "大同區",
    "萬華區", "文山區", "南港區", "內湖區", "士林區", "北投區",
]

# ── 已知的 resource 下載 URL ──
DOWNLOAD_BASE = "https://data.taipei/api/frontstage/tpeod/dataset/resource.download"
RESOURCES = {
    "111年起迄今": "120906e8-cebe-475d-bf84-a0b815eec3a2",
    "110年":       "789a5fb4-34d0-4812-b59f-4e70a1771a08",
    "109年":       "23b57598-2301-498a-848b-1e79aa4be3b7",
}

# 如果你在 data.taipei 頁面找到更多 resource ID，加在這裡：
# "108年": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
# "107年": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
# ...


def download_csv(name, rid):
    """下載一個 CSV 檔案（Big5 編碼）。"""
    url = f"{DOWNLOAD_BASE}?rid={rid}"
    print(f"  📡 {name} ...", end=" ")

    try:
        resp = requests.get(url, timeout=30, verify=False)
        resp.raise_for_status()

        # data.taipei CSV 是 Big5 編碼
        for enc in ["big5", "cp950", "utf-8-sig", "utf-8"]:
            try:
                df = pd.read_csv(io.BytesIO(resp.content), encoding=enc)
                if len(df.columns) > 3:
                    print(f"✅ {len(df)} rows ({enc})")
                    return df
            except:
                continue

        print("❌ 無法解碼")
        return None

    except Exception as e:
        print(f"❌ {e}")
        return None


def process_monthly_df(df):
    """處理月資料 DataFrame，標準化欄位名。"""
    # 印欄位
    cols = [c.strip() for c in df.columns]
    df.columns = cols

    # 常見欄位名對應
    col_map = {}
    for c in cols:
        cl = c.strip()
        if "年" in cl and "月" not in cl:
            col_map["year_raw"] = c
        elif "月" in cl:
            col_map["month"] = c
        elif "區" in cl or "行政" in cl:
            col_map["district_raw"] = c
        elif "遷入" in cl:
            col_map["move_in"] = c
        elif "遷出" in cl:
            col_map["move_out"] = c
        elif "出生" in cl:
            col_map["birth"] = c
        elif "死亡" in cl:
            col_map["death"] = c

    if not all(k in col_map for k in ["year_raw", "district_raw", "move_in", "move_out"]):
        print(f"    ⚠️ 欄位偵測不完整: {col_map}")
        print(f"    實際欄位: {cols}")
        return None

    # 重命名
    rename = {col_map[k]: k for k in col_map}
    sub = df.rename(columns=rename)

    # 數值清洗
    for col in ["move_in", "move_out", "birth", "death"]:
        if col in sub.columns:
            sub[col] = pd.to_numeric(
                sub[col].astype(str).str.replace(",", "").str.strip(),
                errors="coerce"
            )

    # 年份：民國年 → 西元年
    sub["year_num"] = pd.to_numeric(
        sub["year_raw"].astype(str).str.extract(r"(\d+)")[0],
        errors="coerce"
    )
    sub["year"] = sub["year_num"].apply(
        lambda y: int(y + 1911) if pd.notna(y) and y < 200 else (int(y) if pd.notna(y) else None)
    )

    # 行政區清洗（去空格、去「臺北市」前綴）
    sub["district"] = sub["district_raw"].astype(str).str.strip()
    for d in DISTRICTS:
        sub.loc[sub["district"].str.contains(d, na=False), "district"] = d

    # 篩選
    sub = sub[sub["district"].isin(DISTRICTS)]
    sub = sub[sub["year"].between(2015, 2025)]

    return sub


def main():
    print("🚚 台北市各區淨遷移率")
    print("─" * 50)

    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # Step 1: 下載所有已知 resource
    all_monthly = []
    for name, rid in RESOURCES.items():
        df = download_csv(name, rid)
        if df is not None:
            processed = process_monthly_df(df)
            if processed is not None:
                all_monthly.append(processed)
        time.sleep(1)

    # 如果有手動放在 data/ 裡的 CSV 也一起讀
    manual_csvs = list(OUTPUT_DIR.glob("raw_migration_*.csv"))
    for fpath in manual_csvs:
        print(f"  📂 讀取手動檔案: {fpath.name} ...", end=" ")
        for enc in ["big5", "cp950", "utf-8-sig"]:
            try:
                df = pd.read_csv(fpath, encoding=enc)
                processed = process_monthly_df(df)
                if processed is not None:
                    all_monthly.append(processed)
                    print(f"✅ {len(processed)} rows")
                break
            except:
                continue

    if not all_monthly:
        print("\n  ❌ 沒有成功取得任何資料")
        return

    # Step 2: 合併月資料
    combined = pd.concat(all_monthly, ignore_index=True)
    combined = combined.drop_duplicates(subset=["district", "year", "month"] if "month" in combined.columns else ["district", "year"])
    print(f"\n  📊 合併月資料: {len(combined)} rows")
    print(f"     年份: {sorted(combined['year'].dropna().unique().astype(int))}")

    # Step 3: 聚合到年
    agg_cols = {"move_in": "sum", "move_out": "sum"}
    if "birth" in combined.columns:
        agg_cols["birth"] = "sum"
    if "death" in combined.columns:
        agg_cols["death"] = "sum"

    annual = combined.groupby(["district", "year"]).agg(agg_cols).reset_index()
    annual["net_migration"] = annual["move_in"] - annual["move_out"]
    annual["year"] = annual["year"].astype(int)

    # 年中人口（從你已有的 age_district_panel 取）
    age_path = Path("data/age_district_panel.csv")  # 或上層目錄
    if not age_path.exists():
        # 嘗試其他路徑
        for p in ["age_district_panel.csv", "../age_district_panel.csv",
                   "data/age_district_panel.csv"]:
            if Path(p).exists():
                age_path = Path(p)
                break

    if age_path.exists():
        age = pd.read_csv(age_path, encoding="utf-8-sig")
        year_col = "ad_year" if "ad_year" in age.columns else "year"
        pop_map = age.set_index(["district", year_col])["total_pop"].to_dict()
        annual["total_pop"] = annual.apply(
            lambda r: pop_map.get((r["district"], r["year"]), None), axis=1
        )
        annual["IV_mig"] = (annual["net_migration"] / annual["total_pop"] * 1000).round(2)
        print(f"     ✅ 用 age_district_panel.csv 的人口數計算千分率")
    else:
        print(f"     ⚠️ 找不到人口數資料，IV_mig 用淨遷移人數代替")
        annual["IV_mig"] = annual["net_migration"]

    # 篩選 2015–2024
    annual = annual[annual["year"].between(2015, 2024)].sort_values(["district", "year"])

    # Step 4: 輸出
    out_path = OUTPUT_DIR / "migration_panel.csv"
    annual.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"\n  💾 {out_path} ({len(annual)} rows)")

    # 覆蓋率
    print(f"\n  === 覆蓋率 ===")
    missing_years = set()
    for d in sorted(DISTRICTS):
        years = sorted(annual[annual["district"] == d]["year"].unique())
        n = len(years)
        status = "✅" if n == 10 else f"⚠️ {n}/10"
        if n < 10:
            missing = set(range(2015, 2025)) - set(years)
            missing_years.update(missing)
            status += f" 缺 {sorted(missing)}"
        print(f"    {d}: {status}")

    if missing_years:
        print(f"""
  ─────────────────────────────────────────
  ⚠️ 缺少 {sorted(missing_years)} 年的資料

  解決方式：
  1. 前往 https://data.taipei/dataset/detail?id=65d72874-4dd1-409c-8cfa-82011f001d77
  2. 往下滑，看有沒有 104–108 年的 CSV 檔案
  3. 如果有 → 下載後放到 data/ 資料夾，命名為 raw_migration_104.csv 等
  4. 重新執行本腳本，會自動讀取

  或者：
  - 去「臺北市統計年報」找年度遷徙統計
    https://dbas.gov.taipei/
  - 搜尋「遷入遷出」→ 下載區級年度資料
  - 手動整理成 CSV 放到 data/ 裡
        """)
    else:
        print(f"\n  ✅ 完整 {len(annual)} 筆！")

    if len(annual) > 0:
        print(f"""
  ─────────────────────────────────────────
  合併指令:
    mig = pd.read_csv("data/migration_panel.csv")
    panel = panel.merge(mig[["district","year","IV_mig"]],
                        on=["district","year"], how="left")
        """)


if __name__ == "__main__":
    main()
