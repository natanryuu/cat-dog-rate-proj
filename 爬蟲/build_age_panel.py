"""
從 data_raw/age/ 下的 ODS 檔（各年度臺北市人口按性別年齡分）
彙整出行政區級的幼年人口比、老年人口比 panel。
輸出：data_raw/age/age_district_panel.csv
"""

import pandas as pd
import glob
import re
import sys

INPUT_DIR = "data_raw/age"
OUTPUT_CSV = "data/age_district_panel.csv"

TAIPEI_DISTRICTS = [
    "松山區", "信義區", "大安區", "中山區", "中正區", "大同區",
    "萬華區", "文山區", "南港區", "內湖區", "士林區", "北投區",
]


def process_ods(filepath: str) -> list[dict]:
    """讀取單一 ODS 檔，回傳 12 個行政區的幼年 / 老年人口資料。"""
    # 從檔名取得民國年
    basename = filepath.replace("\\", "/").split("/")[-1]
    match = re.search(r"(\d+)年", basename)
    if not match:
        print(f"  [!] 無法解析年度: {basename}")
        return []
    roc_year = int(match.group(1))
    ad_year = roc_year + 1911

    df = pd.read_excel(filepath, engine="odf", header=None)

    # col 0: 區域別, col 1: 性別, col 2: 總計, col 3: 合計_0~4歲
    # 65+ 小計欄位位置（每組 6 欄：1 小計 + 5 逐歲）
    # col 81: 合計_65~69, col 87: 合計_70~74, col 93: 合計_75~79
    # col 99: 合計_80~84, col 105: 合計_85~89, col 111: 合計_90~94
    # col 117: 合計_95~99, col 123: 100歲以上
    COLS_65_PLUS = [81, 87, 93, 99, 105, 111, 117, 123]

    records = []
    for _, row in df.iterrows():
        area = str(row.iloc[0]).strip().replace(" ", "")
        sex = str(row.iloc[1]).strip()

        # 只取「計」（合計行），跳過男/女
        if sex != "計":
            continue

        # 只取 12 行政區
        if area not in TAIPEI_DISTRICTS:
            continue

        total_pop = int(row.iloc[2])
        pop_0_4 = int(row.iloc[3])
        pop_65_plus = sum(int(row.iloc[c]) for c in COLS_65_PLUS)

        records.append({
            "ad_year": ad_year,
            "roc_year": roc_year,
            "district": area,
            "total_pop": total_pop,
            "pop_0_4": pop_0_4,
            "ratio_0_4": round(pop_0_4 / total_pop, 6),
            "pop_65_plus": pop_65_plus,
            "ratio_65_plus": round(pop_65_plus / total_pop, 6),
        })

    return records


def main():
    ods_files = sorted(glob.glob(f"{INPUT_DIR}/*.ods"))
    print(f"Found {len(ods_files)} ODS files")

    all_records = []
    for f in ods_files:
        records = process_ods(f)
        print(f"  {f.split('/')[-1].split(chr(92))[-1]}: {len(records)} districts")
        all_records.extend(records)

    df = pd.DataFrame(all_records)
    df = df.sort_values(["ad_year", "district"]).reset_index(drop=True)
    df.to_csv(OUTPUT_CSV, index=False)

    years = sorted(df["ad_year"].unique())
    print(f"\nDone! {len(df)} rows, years: {years}")
    print(f"Output: {OUTPUT_CSV}")

    # 驗證完整性
    for y in years:
        n = len(df[df["ad_year"] == y])
        if n != 12:
            print(f"  [!] {y}: only {n} districts")


if __name__ == "__main__":
    main()
