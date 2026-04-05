#!/usr/bin/env python3
"""
fetch_park_area.py — 台北市 12 行政區公園綠地面積比 (截面)
==========================================================

資料來源: 公園走透透臺北新花漾 API
  https://parks.gov.taipei/parks/api/

邏輯
----
1. 拉取所有公園逐筆資料 (含面積 + 座標)
2. 用地址/名稱文字比對 + 座標最近鄰歸區
3. 聚合到區級: 公園數、公園總面積
4. park_area_pct = 公園總面積(km²) / 區面積(km²)

Output: data/park_area_cross.csv
"""

from pathlib import Path

import pandas as pd
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ────────────────────────────────────────────────────────────
TAIPEI_DISTRICTS = [
    "松山區", "信義區", "大安區", "中山區", "中正區", "大同區",
    "萬華區", "文山區", "南港區", "內湖區", "士林區", "北投區",
]

# 台北市各區面積 (km²) — 來源: 臺北市政府民政局
DISTRICT_AREA_KM2 = {
    "松山區":  9.2878,  "信義區": 11.2077,
    "大安區": 11.3614,  "中山區": 13.6821,
    "中正區":  7.6071,  "大同區":  5.6815,
    "萬華區":  8.8522,  "文山區": 31.5090,
    "南港區": 21.8424,  "內湖區": 31.5787,
    "士林區": 62.3682,  "北投區": 56.8216,
}

# 各區行政中心概略座標 (用於最近鄰歸區)
DISTRICT_CENTERS = {
    "松山區": (25.0497, 121.5578), "信義區": (25.0327, 121.5719),
    "大安區": (25.0264, 121.5434), "中山區": (25.0648, 121.5330),
    "中正區": (25.0324, 121.5185), "大同區": (25.0631, 121.5133),
    "萬華區": (25.0342, 121.4998), "文山區": (24.9897, 121.5704),
    "南港區": (25.0550, 121.6066), "內湖區": (25.0838, 121.5888),
    "士林區": (25.0928, 121.5248), "北投區": (25.1319, 121.5012),
}

# 管理單位 → 行政區 (確定歸屬者)
UNIT_TO_DISTRICT = {
    "松山區公所": "松山區", "信義區公所": "信義區", "大安區公所": "大安區",
    "中山區公所": "中山區", "中正區公所": "中正區", "大同區公所": "大同區",
    "萬華區公所": "萬華區", "文山區公所": "文山區", "南港區公所": "南港區",
    "內湖區公所": "內湖區", "士林區公所": "士林區", "北投區公所": "北投區",
}

PARKS_API = "https://parks.gov.taipei/parks/api/"
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)


# ────────────────────────────────────────────────────────────
def fetch_all_parks() -> list:
    """從公園走透透 API 拉取所有公園資料。"""
    print("  拉取 parks.gov.taipei ...", end=" ", flush=True)
    r = requests.get(PARKS_API, timeout=30, verify=False)
    r.raise_for_status()
    parks = r.json()
    print(f"{len(parks)} 筆")
    return parks


def _nearest_district(lat: float, lng: float) -> str:
    """以座標距離找最近的行政區中心。"""
    best, min_d = None, float("inf")
    for d, (clat, clng) in DISTRICT_CENTERS.items():
        dist = (lat - clat) ** 2 + (lng - clng) ** 2
        if dist < min_d:
            min_d, best = dist, d
    return best


def assign_district(park: dict) -> str | None:
    """依序用地址文字、管理單位、座標最近鄰判斷行政區。"""
    text = park.get("pm_location", "") + park.get("pm_name", "")

    # 1. 地址/名稱含完整區名
    for d in TAIPEI_DISTRICTS:
        if d in text:
            return d

    # 2. 管理單位直接對應
    unit = park.get("pm_unit", "")
    if unit in UNIT_TO_DISTRICT:
        return UNIT_TO_DISTRICT[unit]

    # 3. 座標最近鄰
    lat = float(park.get("pm_Latitude", 0) or 0)
    lng = float(park.get("pm_Longitude", 0) or 0)
    if lat > 24 and lng > 121:
        return _nearest_district(lat, lng)

    return None


def process_parks(parks: list) -> pd.DataFrame:
    """聚合逐筆公園 → 12 區統計。"""
    buckets: dict[str, list] = {d: [] for d in TAIPEI_DISTRICTS}
    skipped = 0

    for p in parks:
        d = assign_district(p)
        if d:
            buckets[d].append(p)
        else:
            skipped += 1

    if skipped:
        print(f"  ⚠ {skipped} 筆無法歸區")

    rows = []
    for dist in TAIPEI_DISTRICTS:
        ps = buckets[dist]
        park_m2 = sum(float(p.get("pm_LandPublicArea", 0) or 0) for p in ps)
        park_km2 = park_m2 / 1_000_000
        dist_km2 = DISTRICT_AREA_KM2[dist]

        rows.append({
            "district": dist,
            "park_count": len(ps),
            "park_area_m2": round(park_m2, 2),
            "park_area_km2": round(park_km2, 6),
            "district_area_km2": dist_km2,
            "park_area_pct": round(park_km2 / dist_km2, 6),
        })

    return pd.DataFrame(rows)


def main():
    print("🌳 台北市公園綠地面積比")
    print("─" * 50)

    parks = fetch_all_parks()
    if not parks:
        print("  ❌ 無資料回傳")
        return

    print(f"  第一筆欄位: {list(parks[0].keys())}")

    result = process_parks(parks)
    if result.empty:
        return

    outpath = DATA_DIR / "park_area_cross.csv"
    result.to_csv(outpath, index=False, encoding="utf-8-sig")
    print(f"\n  ✅ 已存檔: {outpath}  ({len(result)} 區)")
    print(result.to_string(index=False))


if __name__ == "__main__":
    main()
