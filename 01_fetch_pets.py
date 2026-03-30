"""
01_fetch_pets.py
================
從「寵物登記管理資訊網」(www.pet.gov.tw) 爬取台北市各區
逐年貓狗登記數（2015–2023）

【資料來源】
  https://www.pet.gov.tw/Web/O302.aspx
  - 支援「查詢區間」過濾（以登記日期為基準）
  - 右側面板顯示各行政區數據

【策略】
  網站是 Vue.js SPA，資料由後端 API 提供。
  本腳本：
  Step 1 → 找出 API endpoint（用 requests.Session 模擬瀏覽器）
  Step 2 → 逐年（2015–2023）× 台北市各區 送查詢
  Step 3 → 整理成 panel CSV

【注意】
  若網站改版或 API 有變動，請執行 debug_api() 重新找 endpoint。
"""

import requests
import pandas as pd
import json
import time
import os
from datetime import date

OUTPUT_DIR = "data/raw"
os.makedirs(OUTPUT_DIR, exist_ok=True)

BASE_URL = "https://www.pet.gov.tw"

# 台北市 12 行政區及對應代碼（需確認網站實際使用的代碼）
# 先用名稱，待 API 確認後補上數字代碼
TAIPEI_DISTRICTS = {
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

TARGET_YEARS = list(range(2015, 2024))


# ── 工具：建立帶 headers 的 Session ──────────────────────────────
def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Referer": f"{BASE_URL}/Web/O302.aspx",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
    })
    return s


# ── Step 1：嘗試已知的可能 API 路徑 ─────────────────────────────
CANDIDATE_ENDPOINTS = [
    "/api/O302/GetStatData",
    "/api/O302/GetTownData",
    "/Api/O302",
    "/WebService/O302.asmx",
    "/api/Map/GetRegistCount",
    "/api/Stat/GetByArea",
]

def probe_api(session: requests.Session) -> str | None:
    """
    嘗試各候選 endpoint，回傳第一個成功的路徑
    """
    test_payload = {
        "CountyCode": "63",          # 台北市
        "TownCode":   "63000010",    # 松山區
        "StartDate":  "2015-01-01",
        "EndDate":    "2015-12-31",
        "AnimalType": "",            # 空 = 全部；或 "1"=犬, "2"=貓
    }

    for path in CANDIDATE_ENDPOINTS:
        url = BASE_URL + path
        try:
            r = session.post(url, json=test_payload, timeout=10)
            if r.status_code == 200 and r.headers.get("Content-Type", "").startswith("application/json"):
                print(f"✓ 找到 API endpoint：{url}")
                return url
        except Exception:
            pass

        try:
            r = session.get(url, params=test_payload, timeout=10)
            if r.status_code == 200 and r.headers.get("Content-Type", "").startswith("application/json"):
                print(f"✓ 找到 API endpoint（GET）：{url}")
                return url
        except Exception:
            pass

    return None


# ── Step 2：查詢單一年份 × 單一行政區 ────────────────────────────
def query_one(session: requests.Session, api_url: str,
              district_name: str, district_code: str,
              year: int) -> dict | None:
    payload = {
        "CountyCode": "63",
        "TownCode":   district_code,
        "StartDate":  f"{year}-01-01",
        "EndDate":    f"{year}-12-31",
    }
    try:
        r = session.post(api_url, json=payload, timeout=15)
        r.raise_for_status()
        data = r.json()
        return data
    except Exception as e:
        print(f"  [錯誤] {district_name} {year}：{e}")
        return None


# ── Step 3：解析回傳資料（待確認欄位後調整）────────────────────
def parse_response(raw: dict, district: str, year: int) -> dict | None:
    """
    回傳欄位可能是：
      {"DogCount": 123, "CatCount": 456, ...}
    或陣列格式，需實際測試後確認。
    本函式先嘗試常見欄位名稱，找不到就印出讓使用者手動確認。
    """
    if raw is None:
        return None

    # 展平（有時包在 "data" 或 "result" 裡）
    if isinstance(raw, dict):
        for key in ("data", "result", "Data", "Result"):
            if key in raw:
                raw = raw[key]
                break

    if isinstance(raw, list):
        raw = raw[0] if raw else {}

    dog = None
    cat = None

    # 嘗試多種可能的欄位名稱
    for k in raw:
        kl = k.lower()
        if "dog" in kl or "犬" in kl or kl in ("a", "dogcount"):
            dog = raw[k]
        if "cat" in kl or "貓" in kl or kl in ("b", "catcount"):
            cat = raw[k]

    if dog is None or cat is None:
        print(f"  [未知格式] {district} {year}，原始資料：{json.dumps(raw, ensure_ascii=False)[:200]}")
        return None

    try:
        dog_n = int(str(dog).replace(",", ""))
        cat_n = int(str(cat).replace(",", ""))
    except ValueError:
        return None

    return {
        "district":      district,
        "year":          year,
        "dog_count":     dog_n,
        "cat_count":     cat_n,
        "cat_dog_ratio": cat_n / dog_n if dog_n > 0 else None,
    }


# ── Debug 輔助：印出實際 API 回傳內容 ────────────────────────────
def debug_api():
    """
    執行這個函式可以看到網站實際回傳的 JSON 結構，
    供你手動調整 parse_response() 的欄位名稱。
    """
    session = make_session()
    # 先拿首頁 cookie
    session.get(f"{BASE_URL}/Web/O302.aspx", timeout=15)

    api_url = probe_api(session)
    if api_url is None:
        print("[debug] 所有候選 endpoint 都失敗")
        print("建議：用瀏覽器開 DevTools → Network → 篩選 XHR/Fetch")
        print("在 O302 頁面操作一次查詢，找出實際呼叫的 URL 和 payload")
        return

    payload = {
        "CountyCode": "63",
        "TownCode": "63000010",
        "StartDate": "2020-01-01",
        "EndDate": "2020-12-31",
    }
    r = session.post(api_url, json=payload)
    print(f"\n狀態碼：{r.status_code}")
    print(f"回傳內容：\n{json.dumps(r.json(), ensure_ascii=False, indent=2)[:1000]}")


# ── 主流程 ────────────────────────────────────────────────────────
def main():
    print("=== 01_fetch_pets.py 開始 ===\n")
    session = make_session()

    # 先拿首頁 session / cookie
    print("初始化 session...")
    session.get(f"{BASE_URL}/Web/O302.aspx", timeout=15)

    # 找 API endpoint
    api_url = probe_api(session)
    if api_url is None:
        print("\n[失敗] 找不到 API endpoint")
        print("請執行 debug_api() 或用瀏覽器 DevTools 手動確認 URL")
        print("找到後更新 CANDIDATE_ENDPOINTS 清單")
        return

    rows = []
    total = len(TAIPEI_DISTRICTS) * len(TARGET_YEARS)
    count = 0

    for dist_name, dist_code in TAIPEI_DISTRICTS.items():
        for year in TARGET_YEARS:
            count += 1
            print(f"[{count}/{total}] {dist_name} {year}...", end=" ")

            raw = query_one(session, api_url, dist_name, dist_code, year)
            row = parse_response(raw, dist_name, year)

            if row:
                rows.append(row)
                print(f"貓{row['cat_count']} 犬{row['dog_count']} 比{row['cat_dog_ratio']:.3f}")
            else:
                print("失敗")

            time.sleep(0.8)   # 避免過快

    if not rows:
        print("\n[結果] 無成功資料，請執行 debug_api() 排查")
        return

    df = pd.DataFrame(rows).sort_values(["district", "year"]).reset_index(drop=True)
    out_path = f"{OUTPUT_DIR}/pets_raw.csv"
    df.to_csv(out_path, index=False, encoding="utf-8-sig")

    print(f"\n✓ 完成！儲存：{out_path}（{len(df)} 筆，預期 108）")
    print("\n── 貓犬比 pivot ──")
    print(df.pivot(index="district", columns="year", values="cat_dog_ratio").round(3).to_string())


if __name__ == "__main__":
    # 正式執行：main()
    # 除錯模式：debug_api()
    main()
    # debug_api()