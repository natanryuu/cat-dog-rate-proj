"""
fetch_pet_registration.py
=========================
從「寵物登記管理資訊網」(pet.gov.tw) 的歸戶統計 API
爬取台北市 12 個行政區的逐年犬貓登記數（2015–2024），
整理成 panel data CSV。

【資料來源】
  https://www.pet.gov.tw/Web/O302.aspx
  API endpoint: POST /Handler/PostData.ashx
  Method: O302C_2（區級明細）

【技術細節】
  - Python 3.14 的 ssl 模組與 pet.gov.tw 的 TLS 不相容，
    因此本腳本使用 subprocess 呼叫系統 curl 繞過 SSL 限制
  - API 有速率限制（查詢間隔過短會回 "查詢間隔過短，請稍待片刻"),
    每次請求間隔 3 秒

【輸出】
  data_raw/pet/pet_registration_panel.csv
  欄位：ad_year, roc_year, district, species, registered_count

【驗證】
  每個年度 × 物種應有 12 筆（台北市 12 區），不足會印警告

【執行】
  python fetch_pet_registration.py
"""

import subprocess
import os
import sys
import time
import json
import csv
import re

# ── 設定 ──────────────────────────────────────────────────────────

OUTPUT_DIR = "data_raw/pet"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "pet_registration_panel.csv")

API_URL = "https://www.pet.gov.tw/Handler/PostData.ashx"

COUNTY_ID = "V"  # 台北市

# Animal radio: "0"=狗, "1"=貓
SPECIES_MAP = {"狗": "0", "貓": "1"}

TARGET_YEARS = list(range(2015, 2025))  # 2015 ~ 2024（西元）

# 台北市 12 行政區（用於驗證）
TAIPEI_DISTRICTS = [
    "松山區", "信義區", "大安區", "中山區", "中正區", "大同區",
    "萬華區", "文山區", "南港區", "內湖區", "士林區", "北投區",
]

SLEEP_SEC = 3  # API 有速率限制，間隔 3 秒

# ── API 欄位對照 ─────────────────────────────────────────────────
# fld01 = 登記單位數
# fld02 = 登記數(A)
# fld03 = 除戶數(B)
# fld04 = 變更數(D)
# fld05 = 轉讓數(C)
# fld06 = 絕育數(E)
# fld07, fld08, fld10 = 其他統計欄位


# ── curl 封裝 ─────────────────────────────────────────────────────

def curl_post(url: str, form_data: dict) -> str:
    """用系統 curl 發 POST 請求（application/x-www-form-urlencoded）。"""
    cmd = [
        "curl", "-s", "-k",
        "-X", "POST",
        "-H", "Content-Type: application/x-www-form-urlencoded",
        "-H", "User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
              "AppleWebKit/537.36 (KHTML, like Gecko) "
              "Chrome/124.0.0.0 Safari/537.36",
        "-H", "Referer: https://www.pet.gov.tw/Web/O302.aspx",
    ]
    for k, v in form_data.items():
        cmd += ["--data-urlencode", f"{k}={v}"]
    cmd.append(url)

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if result.returncode != 0:
        raise RuntimeError(f"curl failed: {result.stderr}")
    return result.stdout


# ── 查詢 API ─────────────────────────────────────────────────────

def fetch_one(year: int, animal_code: str, max_retries: int = 3) -> list[dict]:
    """
    查詢一個年度 × 物種的台北市各區資料。

    回傳 list of dict，每筆包含 AreaID, AreaName, fld01~fld10。
    若失敗（rate limit 或錯誤），會自動重試。
    """
    sdate = f"{year}/01/01"
    edate = f"{year}/12/31"

    param_obj = {
        "SDATE": sdate,
        "EDATE": edate,
        "Animal": animal_code,
        "CountyID": COUNTY_ID,
    }

    form_data = {
        "Method": "O302C_2",
        "Param": json.dumps(param_obj, ensure_ascii=False),
    }

    for attempt in range(1, max_retries + 1):
        try:
            raw = curl_post(API_URL, form_data)
            resp = json.loads(raw)
        except (json.JSONDecodeError, RuntimeError) as e:
            print(f"  [錯誤] attempt {attempt}: {e}")
            if attempt < max_retries:
                time.sleep(SLEEP_SEC * 2)
            continue

        if not resp.get("Success"):
            msg = resp.get("Message", "")
            if "間隔" in msg and attempt < max_retries:
                wait = SLEEP_SEC * (attempt + 1)
                print(f"  [rate limit] 等 {wait}s...", end=" ", flush=True)
                time.sleep(wait)
                continue
            elif "查無資料" in msg:
                return []
            else:
                print(f"  [API 錯誤] {msg}")
                return []

        # Success
        try:
            data = json.loads(resp["Message"])
            return data if isinstance(data, list) else []
        except (json.JSONDecodeError, KeyError) as e:
            print(f"  [解析錯誤] {e}")
            return []

    return []


# ── 從 AreaName 取出行政區名 ──────────────────────────────────────

def parse_district(area_name: str) -> str:
    """
    AreaName 格式為 "103大同區"（前面有郵遞區號），
    移除前面的數字前綴。
    """
    return re.sub(r"^\d+", "", area_name).strip()


# ── 主流程 ────────────────────────────────────────────────────────

def main():
    print("=== fetch_pet_registration.py 開始 ===")
    print(f"目標：台北市 12 區 × {len(TARGET_YEARS)} 年 × 2 物種")
    print(f"預估時間：{len(TARGET_YEARS) * len(SPECIES_MAP) * SLEEP_SEC // 60 + 1} 分鐘\n")
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    all_rows = []
    total_combos = len(TARGET_YEARS) * len(SPECIES_MAP)
    count = 0

    for year in TARGET_YEARS:
        roc_year = year - 1911
        for species_name, animal_code in SPECIES_MAP.items():
            count += 1
            print(f"[{count:2d}/{total_combos}] {year}(民{roc_year}) {species_name}...",
                  end=" ", flush=True)

            data = fetch_one(year, animal_code)

            if not data:
                print("無資料")
                time.sleep(SLEEP_SEC)
                continue

            year_count = 0
            for item in data:
                district = parse_district(item.get("AreaName", ""))

                if district not in TAIPEI_DISTRICTS:
                    continue

                try:
                    reg_count = int(item.get("fld02", 0))
                except (ValueError, TypeError):
                    reg_count = 0

                all_rows.append({
                    "ad_year": year,
                    "roc_year": roc_year,
                    "district": district,
                    "species": species_name,
                    "registered_count": reg_count,
                })
                year_count += 1

            print(f"取得 {year_count} 區（API 回傳 {len(data)} 筆）")
            time.sleep(SLEEP_SEC)

    # ── 驗證 & 輸出 ──────────────────────────────────────────────
    print(f"\n{'='*50}")
    print("驗證與輸出\n")

    if not all_rows:
        print("[結果] 無成功資料！")
        return

    # 完整性檢查：每個 year × species 應有 12 筆
    print("── 完整性檢查 ──")
    has_warning = False
    for year in TARGET_YEARS:
        roc_year = year - 1911
        for species_name in SPECIES_MAP:
            n = sum(
                1 for r in all_rows
                if r["ad_year"] == year and r["species"] == species_name
            )
            if n != 12:
                print(f"  ⚠ {year}(民{roc_year}) {species_name}：{n}/12 筆")
                has_warning = True
    if not has_warning:
        print("  ✓ 全部年度 × 物種皆為 12 筆")

    # 排序 & 寫入 CSV
    fieldnames = ["ad_year", "roc_year", "district", "species", "registered_count"]
    all_rows.sort(key=lambda r: (r["ad_year"], r["species"], r["district"]))

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)

    total_expected = len(TARGET_YEARS) * len(SPECIES_MAP) * 12  # 240
    print(f"\n✓ 完成！儲存：{OUTPUT_FILE}")
    print(f"  共 {len(all_rows)} 筆（預期 {total_expected}）")

    # 簡易摘要
    print("\n── 各年度登記數摘要（狗） ──")
    dog_rows = [r for r in all_rows if r["species"] == "狗"]
    for year in TARGET_YEARS:
        yr_total = sum(r["registered_count"] for r in dog_rows if r["ad_year"] == year)
        print(f"  {year}: {yr_total:,}")


if __name__ == "__main__":
    main()
