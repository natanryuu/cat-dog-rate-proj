"""
05_fetch_petgov.py
==================
從「寵物登記管理資訊網」逆向 API，取得台北市各區
逐年（2015–2023）貓狗登記數量。

目標端點（AJAX handlers）：
  MapApiCity.ashx  → 縣市層級（取台北市 CountyID）
  MapApiTown.ashx  → 區層級（指定 CountyID 後取 12 區數字）

編碼流程（與瀏覽器 JS 完全一致）：
  JSON.stringify → encodeURI → btoa → encodeURI
"""

import base64
import json
import time
import pandas as pd
import requests
from urllib.parse import quote

# ── 設定 ──────────────────────────────────────────────────────────
BASE_URL    = "https://www.pet.gov.tw/PetsMap/Handler_ENRF"
OUTPUT_CSV  = "data/raw/petgov_raw.csv"

TARGET_YEARS = list(range(2015, 2026))   # 2015–2025

TAIPEI_DISTRICTS = {
    "松山區", "信義區", "大安區", "中山區", "中正區",
    "大同區", "萬華區", "文山區", "南港區", "內湖區",
    "士林區", "北投區",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer":    "https://www.pet.gov.tw/PetsMap/PetsMap.aspx",
    "X-Requested-With": "XMLHttpRequest",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)


def init_session():
    """先造訪主頁面，讓伺服器設定 session cookie"""
    resp = SESSION.get(
        "https://www.pet.gov.tw/PetsMap/PetsMap.aspx", timeout=30
    )
    resp.raise_for_status()
    print(f"  session 初始化完成，cookies: {dict(SESSION.cookies)}")


# ── 編碼工具 ───────────────────────────────────────────────────────
# JavaScript encodeURI 不編碼以下字元（RFC 3986 保留 + 常用符號）
_JS_SAFE = "~@#$&()*!+=:;,.?/'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_."

def _encode_uri(s: str) -> str:
    """模擬 JS encodeURI()"""
    return quote(s, safe=_JS_SAFE)


def build_iAddWith(st: str = "", ed: str = "", animal: str = "0") -> str:
    """
    建立 iAddWith 查詢字串。
    animal: "0"=犬, "1"=貓, "2"=全部（實測結果，與網站 JS 註解不同）
    st/ed:  日期字串，格式 YYYY/MM/DD（空字串 = 不限日期）
    """
    obj = {
        "ANIMAL":  animal,
        "SPAY":    "0",      # 0=全部
        "SIRE":    "0",
        "PETSEX":  "2",
        "Color":   "G",
        "ST":      st,
        "ED":      ed,
        "Addr":    "",
        "inpType": "Addr",
    }
    step1 = json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
    step2 = _encode_uri(step1)                          # encodeURI
    step3 = base64.b64encode(step2.encode()).decode()   # btoa
    step4 = _encode_uri(step3)                          # encodeURI again
    return step4


# ── API 呼叫 ───────────────────────────────────────────────────────
def get_taipei_county_id(iAddWith: str) -> str | None:
    """向 MapApiCity.ashx 查詢，回傳台北市的 CountyID"""
    url  = f"{BASE_URL}/MapApiCity.ashx?Area=0&iAddWith={iAddWith}"
    resp = SESSION.post(url, timeout=30)
    resp.raise_for_status()

    data = resp.json()
    if not isinstance(data, list):
        data = [data]

    for city in data:
        name = str(city.get("CountyName", ""))
        if "臺北市" in name or "台北市" in name:
            return str(city.get("CountyID", ""))

    print(f"  [警告] 找不到台北市，回傳：{data[:2]}")
    return None


def build_iAddWith_raw(st: str = "", ed: str = "", animal: str = "0") -> str:
    """
    MapApiTown.ashx 用的格式：
    localStorage 存的是原始 JSON 字串（未 Base64），直接拼接到 URL。
    瀏覽器不會自動 URL encode URL 中的特殊字元，所以伺服器收到的是原始 JSON。
    animal: "0"=犬, "1"=貓, "2"=全部（實測結果，與網站 JS 註解不同）
    """
    obj = {
        "ANIMAL":  animal,
        "SPAY":    "0",
        "SIRE":    "0",
        "PETSEX":  "2",
        "Color":   "G",
        "ST":      st,
        "ED":      ed,
        "Addr":    "",
        "inpType": "Addr",
    }
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def get_town_data(iAddWith_raw: str) -> list[dict]:
    """
    MapApiTown.ashx?Area=0&iAddWith={raw_json}
    GetTown() 不帶 City 參數，回傳全台所有鄉鎮；我們再過濾台北12區。
    """
    url  = f"{BASE_URL}/MapApiTown.ashx?Area=0&iAddWith={iAddWith_raw}"
    resp = SESSION.post(url, timeout=30)
    resp.raise_for_status()

    data = resp.json()
    return data if isinstance(data, list) else [data]


# ── 解析單年資料 ───────────────────────────────────────────────────
def parse_year(year: int) -> pd.DataFrame:
    st = f"{year}/01/01"
    ed = f"{year}/12/31"

    iAddWith_raw = build_iAddWith_raw(st=st, ed=ed)
    towns = get_town_data(iAddWith_raw)

    rows = []
    for t in towns:
        if t.get("CountyID") != "V":
            continue
        name = str(t.get("TownName", t.get("AreaName", ""))).strip()
        if name not in TAIPEI_DISTRICTS:
            continue

        cat_count = (t.get("cntC") or t.get("CatCnt") or t.get("cat_cnt") or 0)
        dog_count = (t.get("cntD") or t.get("DogCnt") or t.get("dog_cnt") or 0)
        total     = (t.get("cnt")  or t.get("Cnt")   or t.get("total")   or 0)

        rows.append({
            "year":      year,
            "district":  name,
            "cat_count": int(cat_count),
            "dog_count": int(dog_count),
            "total":     int(total),
        })

    return pd.DataFrame(rows)


# ── 備援：若 API 不分貓狗，分開查詢 ──────────────────────────────
def parse_year_split(year: int) -> pd.DataFrame:
    """
    實測 API 動物代碼（與網站 JS 註解不同）：
      ANIMAL="0" → 犬，ANIMAL="1" → 貓，ANIMAL="2" → 全部
    分別查詢犬與貓，各取 cnt 欄位。
    """
    st = f"{year}/01/01"
    ed = f"{year}/12/31"

    results = {}

    for animal_code, key in [("0", "dog_count"), ("1", "cat_count")]:
        iAddWith_raw = build_iAddWith_raw(st=st, ed=ed, animal=animal_code)
        towns = get_town_data(iAddWith_raw)
        for t in towns:
            if t.get("CountyID") != "V":   # V = 臺北市
                continue
            name = str(t.get("TownName", "")).strip()
            if name not in TAIPEI_DISTRICTS:
                continue
            results.setdefault(name, {"dog_count": 0, "cat_count": 0})
            results[name][key] = int(t.get("cnt") or 0)

        time.sleep(0.5)

    rows = [{"year": year, "district": d, **v} for d, v in results.items()]
    return pd.DataFrame(rows)


# ── 主流程 ─────────────────────────────────────────────────────────
def main():
    import os
    os.makedirs("data/raw", exist_ok=True)

    print("=== 05_fetch_petgov.py 開始 ===\n")
    init_session()
    all_frames = []

    for year in TARGET_YEARS:
        print(f"查詢 {year} 年...", end=" ", flush=True)

        try:
            df = parse_year_split(year)

            if df.empty:
                print("→ 空資料，跳過")
            else:
                all_frames.append(df)
                print(f"→ {len(df)} 區，貓={df['cat_count'].sum():,}，狗={df['dog_count'].sum():,}")

        except Exception as e:
            print(f"→ 錯誤：{e}")

        time.sleep(1.0)   # 禮貌性延遲（兩次查詢已在 parse_year_split 內有 0.5s）

    if not all_frames:
        print("\n[錯誤] 無任何資料，請確認 API 端點與編碼格式")
        print("建議用瀏覽器開 DevTools → Network，手動抓一次真實請求來比對")
        return

    panel = pd.concat(all_frames, ignore_index=True)
    panel["cat_dog_ratio"] = panel["cat_count"] / panel["dog_count"].replace(0, float("nan"))
    panel = panel.sort_values(["district", "year"]).reset_index(drop=True)

    # 計算年增率（YoY %）：各區依年份排序後，與前一年比較
    panel["cat_yoy"] = (
        panel.groupby("district")["cat_count"]
        .pct_change()
        .mul(100)
        .round(2)
    )
    panel["dog_yoy"] = (
        panel.groupby("district")["dog_count"]
        .pct_change()
        .mul(100)
        .round(2)
    )

    panel.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    print(f"\n✓ 儲存：{OUTPUT_CSV}（{len(panel)} 筆）")
    print(f"  預期 {12 * len(TARGET_YEARS)} 筆（12區 × {len(TARGET_YEARS)}年），實際 {len(panel)} 筆")

    # 快速診斷
    print("\n── 各區各年貓狗比（pivot）──")
    pivot = panel.pivot(index="district", columns="year", values="cat_dog_ratio").round(3)
    print(pivot.to_string())

    print("\n── 貓登記年增率 %（pivot）──")
    pivot_cat = panel.pivot(index="district", columns="year", values="cat_yoy")
    print(pivot_cat.to_string())

    print("\n── 犬登記年增率 %（pivot）──")
    pivot_dog = panel.pivot(index="district", columns="year", values="dog_yoy")
    print(pivot_dog.to_string())


# ── 除錯工具：印出一次真實的原始回應 ─────────────────────────────
def debug_raw_response(year: int = 2022):
    """執行一次查詢，印出完整 JSON，方便確認欄位名稱"""
    init_session()
    st, ed = f"{year}/01/01", f"{year}/12/31"
    iAddWith  = build_iAddWith(st=st, ed=ed)
    county_id = get_taipei_county_id(iAddWith)
    print(f"臺北市 CountyID = {county_id!r}")

    iAddWith_raw = build_iAddWith_raw(st=st, ed=ed)
    url = f"{BASE_URL}/MapApiTown.ashx?Area=0&iAddWith={iAddWith_raw}"
    print(f"\n請求 URL：\n{url}\n")
    resp = SESSION.post(url, timeout=30)
    print(f"HTTP status : {resp.status_code}")
    print(f"Content-Type: {resp.headers.get('Content-Type', '(無)')}")
    print(f"回應長度    : {len(resp.text)} bytes")
    print(f"回應前 500 字：\n{resp.text[:500]!r}")
    if resp.headers.get("Content-Type", "").startswith("application/json"):
        data = resp.json()
        taipei = [t for t in data if str(t.get("TownName", "")).strip() in TAIPEI_DISTRICTS]
        print(f"\n台北市各區資料（{len(taipei)} 筆）：")
        for t in taipei[:3]:
            print(json.dumps(t, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    import sys
    if "--debug" in sys.argv:
        # debug_raw_response()
        # 用法：python 05_fetch_petgov.py --debug [年份]
        # 例如：python 05_fetch_petgov.py --debug 2005
        yr = int(sys.argv[2]) if len(sys.argv) > 2 else 2022
        debug_raw_response(yr)
    elif "--probe" in sys.argv:
        # 快速探測多個舊年份，看哪年開始有台北市資料
        init_session()
        for yr in [2000, 2003, 2005, 2008, 2010, 2012, 2014]:
            raw = build_iAddWith_raw(f"{yr}/01/01", f"{yr}/12/31", "1")
            towns = get_town_data(raw)
            tpe = [t for t in towns if t.get("CountyID") == "V"]
            total = sum(t.get("cnt", 0) for t in tpe)
            print(f"{yr} 年：台北市 {len(tpe)} 區，犬登記總計 {total:,} 筆")
            time.sleep(0.8)
    else:
        main()
