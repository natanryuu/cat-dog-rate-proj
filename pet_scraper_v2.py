"""
寵物登記管理資訊網 爬蟲 v3
修正: pet.gov.tw SSL 憑證缺少 Subject Key Identifier → verify=False
"""

import requests
import json
import time
import csv
import urllib3

# 關閉 InsecureRequestWarning 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_URL = "https://www.pet.gov.tw/PetsMap/Handler_ENRF/MapApiTown.ashx"
YEARS = range(2014 , 2025)

TAIPEI_DISTRICTS = {
    "V01": "松山區", "V02": "信義區", "V03": "大安區", "V04": "中山區",
    "V05": "中正區", "V06": "大同區", "V07": "萬華區", "V08": "文山區",
    "V09": "南港區", "V10": "內湖區", "V11": "士林區", "V12": "北投區",
}

OUTPUT_CSV = "taipei_pet_registration_2014_2025.csv"


def fetch_year(year: int, session: requests.Session) -> list:
    add_with = {
        "ANIMAL": "0",
        "SPAY": "2",
        "SIRE": "0",
        "PETSEX": "2",
        "Color": "G",
        "ST": f"{year}/01/01",
        "ED": f"{year}/12/31",
        "Addr": "",
        "inpType": "Addr",
    }

    params = {
        "Area": "0",
        "iAddWith": json.dumps(add_with, ensure_ascii=False),
    }

    headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Origin": "https://www.pet.gov.tw",
        "Referer": "https://www.pet.gov.tw/PetsMap/PetsMap.aspx",
        "X-Requested-With": "XMLHttpRequest",
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
    }

    try:
        resp = session.post(
            BASE_URL,
            params=params,
            headers=headers,
            timeout=30,
            verify=False,  # ← pet.gov.tw 憑證有問題，跳過驗證
        )
        resp.raise_for_status()
        data = resp.json()

        if isinstance(data, list):
            return data
        if isinstance(data, dict) and "d" in data:
            inner = data["d"]
            return json.loads(inner) if isinstance(inner, str) else inner

    except requests.exceptions.HTTPError:
        print(f"  ❌ HTTP {resp.status_code}: {resp.text[:200]}")
    except Exception as e:
        print(f"  ❌ 錯誤: {e}")

    return []


def main():
    session = requests.Session()
    all_rows = []

    print("=" * 65)
    print("  寵物登記管理資訊網 — 台北市各區犬貓登記數 爬蟲")
    print("=" * 65)

    for year in YEARS:
        print(f"\n📅 {year} 年 ...")
        records = fetch_year(year, session)

        if not records:
            print(f"  ⚠️  無資料或請求失敗")
            continue

        taipei = sorted(
            [r for r in records if r.get("CountyID") == "V"],
            key=lambda x: x.get("TownID", ""),
        )

        print(f"  全國 {len(records)} 區 → 台北市 {len(taipei)} 區")

        for r in taipei:
            town_id = r.get("TownID", "")
            row = {
                "year": year,
                "town_id": town_id,
                "town_name": r.get("TownName", TAIPEI_DISTRICTS.get(town_id, "")),
                "dog_count": r.get("cntD", 0),
                "cat_count": r.get("cntC", 0),
                "total_count": r.get("cnt", 0),
                "house_count": r.get("HouseCnt", 0),
            }
            all_rows.append(row)
            print(f"    {row['town_name']:5s}  "
                  f"🐕 {row['dog_count']:>5d}  "
                  f"🐈 {row['cat_count']:>5d}  "
                  f"🏠 {row['house_count']:>5d}")

        time.sleep(2)

    # ── 寫入 CSV ──
    if all_rows:
        fieldnames = [
            "year", "town_id", "town_name",
            "dog_count", "cat_count", "total_count", "house_count",
        ]
        with open(OUTPUT_CSV, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_rows)

        total_dogs = sum(r["dog_count"] for r in all_rows)
        total_cats = sum(r["cat_count"] for r in all_rows)
        print(f"\n{'=' * 65}")
        print(f"  ✅ 完成！{len(all_rows)} 筆已存入 {OUTPUT_CSV}")
        print(f"  累計犬: {total_dogs:,}  貓: {total_cats:,}")
        print(f"{'=' * 65}")
    else:
        print("\n❌ 沒有抓到任何資料")


if __name__ == "__main__":
    main()
