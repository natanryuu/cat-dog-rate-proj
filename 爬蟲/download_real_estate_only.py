"""
獨立下載實價登錄 — 只抓租賃，民國 114 年（2025）四季
用法：python download_real_estate_only.py
"""

import requests
import os
import zipfile
import time

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DIR = os.path.join(BASE_DIR, 'data_raw')

def download_rent_114():
    rent_dir = os.path.join(RAW_DIR, 'rent')
    zip_dir = os.path.join(RAW_DIR, 'zip_cache')
    os.makedirs(rent_dir, exist_ok=True)
    os.makedirs(zip_dir, exist_ok=True)

    base_url = "https://plvr.land.moi.gov.tw/DownloadSeason"

    # 只抓民國 114 年 4 季
    seasons = [f"114S{s}" for s in range(1, 5)]

    print("=" * 60)
    print("  下載實價登錄（租賃）— 民國 114 年")
    print(f"  共 {len(seasons)} 個季度：{seasons[0]} ~ {seasons[-1]}")
    print("=" * 60)

    success = 0

    for season in seasons:
        zip_path = os.path.join(zip_dir, f"{season}.zip")
        rent_file = os.path.join(rent_dir, f"{season}_a_lvr_land_c.csv")

        if os.path.exists(rent_file):
            print(f"  [跳過] {season} 已存在")
            success += 1
            continue

        url = f"{base_url}?season={season}&type=zip&fileName=lvr_landcsv.zip"
        print(f"  [下載] {season}...", end=" ", flush=True)

        try:
            resp = requests.get(url, timeout=60)
            if resp.status_code != 200 or len(resp.content) < 1000:
                print(f"❌ HTTP {resp.status_code} 或檔案太小")
                continue

            with open(zip_path, 'wb') as f:
                f.write(resp.content)

            with zipfile.ZipFile(zip_path, 'r') as zf:
                names = zf.namelist()
                rent_csv = [n for n in names if 'a_lvr_land_c' in n.lower() and n.endswith('.csv')]
                if rent_csv:
                    with zf.open(rent_csv[0]) as src, open(rent_file, 'wb') as dst:
                        dst.write(src.read())
                    success += 1
                    print("✅")
                else:
                    print("❌ ZIP 內找不到租賃 CSV")

        except Exception as e:
            print(f"❌ {e}")

        time.sleep(2)

    print(f"\n  租賃 CSV: {success}/{len(seasons)} 個季度")


if __name__ == '__main__':
    download_rent_114()
