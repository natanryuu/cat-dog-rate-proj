"""
fetch_migration_ods.py — 從 data_raw/in_and_out/ 的 ODS 檔整理台北市各行政區遷入遷出
=============================================================================
資料來源：data_raw/in_and_out/
  - 遷入：90起迄今遷入 (1).ods   (民國90~115年，單一檔案含所有年度)
  - 遷出：104遷出.ods ~ 110遷出.ods (各年度單獨檔案)
         111起迄今遷出.ods       (民國111~115年)

Output: data/migration_panel.csv
"""

import pandas as pd
import re
from pathlib import Path

RAW_DIR = Path("data_raw/in_and_out")
OUTPUT_DIR = Path("data")
OUTPUT_DIR.mkdir(exist_ok=True)

DISTRICTS = ["松山", "信義", "大安", "中山", "中正", "大同",
             "萬華", "文山", "南港", "內湖", "士林", "北投"]

MONTHS = [f"{i}月" for i in range(1, 13)]


def parse_year_block(df, start_row):
    """解析一個年度的資料區塊 (title row + 空行 + header + 合計 + 12區 + 註)。
    回傳 (year_roc, records list, next_start_row)"""

    title = str(df.iloc[start_row, 0]).strip()
    m = re.search(r"(\d+)年", title)
    if not m:
        return None, [], start_row + 1
    year_roc = int(m.group(1))

    # 資料列從 title+3 開始 (title, 空行, header, 合計行開始)
    data_start = start_row + 3
    records = []

    for i in range(data_start, min(data_start + 13, len(df))):
        district = str(df.iloc[i, 0]).strip() if pd.notna(df.iloc[i, 0]) else ""
        if district in DISTRICTS:
            row_data = {"district": district + "區", "year": year_roc + 1911}
            # 合計在 col 1
            total = df.iloc[i, 1]
            row_data["annual_total"] = pd.to_numeric(total, errors="coerce")
            # 月份在 col 2~13
            for mi, month in enumerate(MONTHS):
                val = df.iloc[i, 2 + mi]
                row_data[month] = pd.to_numeric(val, errors="coerce")
            records.append(row_data)

    return year_roc, records, data_start + 14


def parse_ods_file(filepath):
    """解析一個 ODS 檔案，回傳所有年度的 records。"""
    df = pd.read_excel(filepath, engine="odf", header=None)
    all_records = []
    i = 0
    while i < len(df):
        val = str(df.iloc[i, 0]) if pd.notna(df.iloc[i, 0]) else ""
        if "臺北市" in val and "年" in val and "行政區" in val:
            year_roc, records, next_i = parse_year_block(df, i)
            if records:
                all_records.extend(records)
            i = next_i
        else:
            i += 1
    return all_records


def main():
    print("整理台北市各行政區遷入遷出資料")
    print("=" * 50)

    # ── 遷入 ──
    move_in_file = RAW_DIR / "90起迄今遷入 (1).ods"
    print(f"\n讀取遷入: {move_in_file.name}")
    in_records = parse_ods_file(move_in_file)
    df_in = pd.DataFrame(in_records)
    df_in_annual = df_in[["district", "year", "annual_total"]].rename(
        columns={"annual_total": "move_in"}
    )
    years_in = sorted(df_in["year"].unique())
    print(f"  {len(df_in_annual)} 筆, 年份: {years_in[0]}~{years_in[-1]}")

    # ── 遷出 ──
    out_records = []

    # 單年度檔案 (104~110)
    for f in sorted(RAW_DIR.glob("*遷出.ods")):
        if "起迄今" in f.name:
            continue
        print(f"讀取遷出: {f.name}")
        records = parse_ods_file(f)
        out_records.extend(records)
        print(f"  {len(records)} 筆")

    # 111起迄今
    combined_out = RAW_DIR / "111起迄今遷出.ods"
    if combined_out.exists():
        print(f"讀取遷出: {combined_out.name}")
        records = parse_ods_file(combined_out)
        out_records.extend(records)
        print(f"  {len(records)} 筆")

    df_out = pd.DataFrame(out_records)
    df_out_annual = df_out[["district", "year", "annual_total"]].rename(
        columns={"annual_total": "move_out"}
    )
    years_out = sorted(df_out["year"].unique())
    print(f"\n遷出合計: {len(df_out_annual)} 筆, 年份: {years_out[0]}~{years_out[-1]}")

    # ── 合併遷入遷出 ──
    merged = df_in_annual.merge(df_out_annual, on=["district", "year"], how="outer")
    merged["net_migration"] = merged["move_in"] - merged["move_out"]
    merged = merged.sort_values(["district", "year"]).reset_index(drop=True)

    # ── 嘗試加入人口數計算千分率 ──
    age_path = OUTPUT_DIR / "age_district_panel.csv"
    if age_path.exists():
        age = pd.read_csv(age_path, encoding="utf-8-sig")
        year_col = "ad_year" if "ad_year" in age.columns else "year"
        pop_map = age.set_index(["district", year_col])["total_pop"].to_dict()
        merged["total_pop"] = merged.apply(
            lambda r: pop_map.get((r["district"], r["year"]), None), axis=1
        )
        merged["net_migration_rate_per_1000"] = (
            merged["net_migration"] / merged["total_pop"] * 1000
        ).round(2)
        print("\n已用 age_district_panel.csv 的人口數計算淨遷移千分率")
    else:
        print("\n找不到 age_district_panel.csv，跳過千分率計算")

    # ── 篩選有遷入+遷出的年份 ──
    complete = merged.dropna(subset=["move_in", "move_out"])

    # ── 輸出 ──
    out_path = OUTPUT_DIR / "migration_panel.csv"
    complete.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"\n輸出: {out_path} ({len(complete)} 筆)")

    # ── 覆蓋率 ──
    print(f"\n{'district':<8} {'years':>5}  range")
    print("-" * 40)
    for d in sorted(complete["district"].unique()):
        sub = complete[complete["district"] == d]
        yrs = sorted(sub["year"].unique())
        print(f"{d:<8} {len(yrs):>5}  {int(yrs[0])}~{int(yrs[-1])}")

    # ── 預覽 ──
    print(f"\n預覽 (前10筆):")
    print(complete.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
