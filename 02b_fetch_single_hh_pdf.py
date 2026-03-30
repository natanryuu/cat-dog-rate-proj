"""
02b_fetch_single_hh_pdf.py
==========================
自動下載台北市主計處「家庭收支訪問調查報告」104–112年 PDF
並解析「表2 臺北市家戶規模結構概況」取出各區單人戶比例

【依賴套件】
  pip install requests pdfplumber pandas

【執行】
  python 02b_fetch_single_hh_pdf.py

【產出】
  data/raw/pdf/       ← 各年份 PDF
  data/clean/single_hh_pdf.csv  ← 12區 × 9年 單人戶比例
"""

import requests
import pdfplumber
import pandas as pd
import os
import time
import re

PDF_DIR   = "data/raw/pdf"
CLEAN_DIR = "data/clean"
os.makedirs(PDF_DIR,   exist_ok=True)
os.makedirs(CLEAN_DIR, exist_ok=True)

# ── 104–112年 PDF 直連 URL ────────────────────────────────────────
PDF_URLS = {
    2015: "https://www-ws.gov.taipei/Download.ashx?u=LzAwMS9VcGxvYWQvMzY3L3JlbGZpbGUvNDU2NzIvNzg5MDIzMi8zOWUyYjNhNC00NGNiLTRmMDMtOWY2Mi03YzU3NGY5NDUwNjYucGRm&n=MTA05bm06Ie65YyX5biC5a625bqt5pS25pSv6Kiq5ZWP6Kq%2f5p%2bl5aCx5ZGKLnBkZg%3d%3d&icon=.pdf",
    2016: "https://www-ws.gov.taipei/Download.ashx?u=LzAwMS9VcGxvYWQvMzY3L3JlbGZpbGUvNDU2NzIvNzg5MDIzMy8yOThhN2VhNS1iNmRjLTRlZmEtODgwMi00ZTZmNjgyZTMwMmMucGRm&n=MTA15bm06Ie65YyX5biC5a625bqt5pS25pSv6Kiq5ZWP6Kq%2f5p%2bl5aCx5ZGKLnBkZg%3d%3d&icon=.pdf",
    2017: "https://www-ws.gov.taipei/Download.ashx?u=LzAwMS9VcGxvYWQvMzY3L3JlbGZpbGUvNDU2NzIvNzk0MDA4NC81N2VkMTc1My1iZDBhLTRhNDQtOWM1Zi03ZjNjZGMzY2M3MWIucGRm&n=MTA25bm06Ie65YyX5biC5a625bqt5pS25pSv6Kiq5ZWP6Kq%2f5p%2bl5aCx5ZGKLnBkZg%3d%3d&icon=.pdf",
    2018: "https://www-ws.gov.taipei/Download.ashx?u=LzAwMS9VcGxvYWQvMzY3L3JlbGZpbGUvNDU2NzIvODEwMDE1Ni9kOTc1OGQ5ZC00YzA3LTQ1NTEtYjg3Zi0yMGFiZTY1MWE2YzMucGRm&n=MTA35bm06Ie65YyX5biC5a625bqt5pS25pSv6Kiq5ZWP6Kq%2f5p%2bl5aCx5ZGKLnBkZg%3d%3d&icon=.pdf",
    2019: "https://www-ws.gov.taipei/Download.ashx?u=LzAwMS9VcGxvYWQvMzY3L3JlbGZpbGUvNDU2NzIvODI2OTc0OS8yNDllNzYwMC04OTRmLTQ5YTEtYmVkNi01MjUzZTY4OGYxM2QucGRm&n=MTA45bm06Ie65YyX5biC5a625bqt5pS25pSv6Kiq5ZWP6Kq%2f5p%2bl5aCx5ZGKLnBkZg%3d%3d&icon=.pdf",
    2020: "https://www-ws.gov.taipei/Download.ashx?u=LzAwMS9VcGxvYWQvMzY3L3JlbGZpbGUvNDU2NzIvODQ3MTQxNy8wNjFmMDZlMy1iZjE1LTQzODYtOWNhYi1iMWM1YzdiNzBkZDMucGRm&n=MTA55bm06Ie65YyX5biC5a625bqt5pS25pSv6Kiq5ZWP6Kq%2f5p%2bl5aCx5ZGKLnBkZg%3d%3d&icon=.pdf",
    2021: "https://www-ws.gov.taipei/Download.ashx?u=LzAwMS9VcGxvYWQvMzY3L3JlbGZpbGUvNDU2NzIvODg4NjE2Ny81MDdhNTE5ZC1hNjNmLTQ4MmEtOGFjNy1lZjI5NjJlYmZlYTgucGRm&n=MTEw5bm06Ie65YyX5biC5a625bqt5pS25pSv6Kiq5ZWP6Kq%2f5p%2bl5aCx5ZGKLnBkZg%3d%3d&icon=.pdf",
    2022: "https://www-ws.gov.taipei/Download.ashx?u=LzAwMS9VcGxvYWQvMzY3L3JlbGZpbGUvNDU2NzIvOTA2NTE2NC9jMWMwNzFmOS05ZDA4LTQ3OTMtYmRmMC04ZTYwYWNhNjU5ODUucGRm&n=5Li76KiI6JmVLTExMS0yMDIy5bm06Ie65YyX5biC5a625bqt5pS25pSv6Kiq5ZWP6Kq%2f5p%2bl5aCx5ZGKLnBkZg%3d%3d&icon=.pdf",
    2023: "https://www-ws.gov.taipei/Download.ashx?u=LzAwMS9VcGxvYWQvMzY3L3JlbGZpbGUvNDU2NzIvOTMwMjQ1Ny8xZjdjM2EzNS01NzQyLTRjY2ItYjc3ZC0zMDM1ODc4N2Q5ZDcucGRm&n=6Ie65YyX5biCMTEy5bm05a625bqt5pS25pSv6Kiq5ZWP6Kq%2f5p%2bl5aCx5ZGKLnBkZg%3d%3d&icon=.pdf",
}

TAIPEI_DISTRICTS = [
    "松山區", "信義區", "大安區", "中山區", "中正區",
    "大同區", "萬華區", "文山區", "南港區", "內湖區",
    "士林區", "北投區",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://dbas.gov.taipei/",
}


# ── Step 1：下載 PDF ──────────────────────────────────────────────
def download_pdf(year: int, url: str) -> str | None:
    out_path = f"{PDF_DIR}/{year}.pdf"
    if os.path.exists(out_path):
        print(f"  {year}: 已存在，略過下載")
        return out_path

    try:
        r = requests.get(url, headers=HEADERS, timeout=60, stream=True)
        r.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(8192):
                f.write(chunk)
        size_kb = os.path.getsize(out_path) // 1024
        print(f"  {year}: ✓ 下載完成（{size_kb} KB）")
        return out_path
    except Exception as e:
        print(f"  {year}: ✗ 下載失敗 → {e}")
        return None


# ── Step 2：解析 PDF 找「表2」單人戶比例 ─────────────────────────
def parse_pdf(pdf_path: str, year: int) -> list[dict]:
    """
    掃描 PDF 每頁，找到含「家戶規模」或「1人」的表格
    擷取 12 個行政區各自的 1人戶（單人戶）百分比

    表格結構（參考你截圖的 109年版）：
      行別  | 戶量 | 總計  | 1人  | 2人  | ...
      全市  | 3.01 | 100   | 8.75 | ...
      松山區| 2.77 | 100   | 9.16 | ...
    """
    rows = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text() or ""

                # 快速篩：這頁有沒有「家戶規模」或「1人」相關字眼
                if "家戶規模" not in text and "1人" not in text and "單人" not in text:
                    continue

                # 嘗試用 extract_table 取結構化資料
                tables = page.extract_tables()
                for table in tables:
                    found = _parse_table(table, year)
                    if found:
                        rows.extend(found)

                # 如果 extract_table 沒抓到，改用文字解析備援
                if not rows:
                    found = _parse_text(text, year)
                    if found:
                        rows.extend(found)

                if rows:
                    break   # 找到就停

    except Exception as e:
        print(f"    [解析錯誤] {pdf_path}: {e}")

    return rows


def _parse_table(table: list, year: int) -> list[dict]:
    """從 pdfplumber 的結構化表格找單人戶欄"""
    if not table or len(table) < 3:
        return []

    # 找標題列（含「1人」或「單人」）
    header_idx = None
    col_1p     = None
    for i, row in enumerate(table):
        row_str = " ".join(str(c or "") for c in row)
        if "1人" in row_str or "單人" in row_str:
            header_idx = i
            # 找「1人」在哪欄
            for j, cell in enumerate(row):
                if cell and ("1人" in str(cell) or "單人" in str(cell)):
                    col_1p = j
                    break
            break

    if header_idx is None or col_1p is None:
        return []

    results = []
    for row in table[header_idx + 1:]:
        if not row or len(row) <= col_1p:
            continue
        district = str(row[0] or "").strip()
        if district not in TAIPEI_DISTRICTS:
            continue
        val_str = str(row[col_1p] or "").strip().replace(",", "")
        try:
            val = float(val_str)
            results.append({
                "district": district,
                "year": year,
                "single_hh_pct": val,   # 單人戶佔總戶數 %
            })
        except ValueError:
            pass

    return results


def _parse_text(text: str, year: int) -> list[dict]:
    """文字備援解析：逐行找行政區名稱 + 數字"""
    results = []
    lines = text.split("\n")

    # 先找「1人」欄在第幾個數字位置（看標題行）
    col_pos = None
    for line in lines:
        if "1人" in line or "單人" in line:
            nums_before = len(re.findall(r'\d+\.?\d*', line.split("1人")[0]))
            col_pos = nums_before
            break

    if col_pos is None:
        return []

    for line in lines:
        district = next((d for d in TAIPEI_DISTRICTS if d in line), None)
        if not district:
            continue
        nums = re.findall(r'\d+\.?\d+', line)
        if len(nums) > col_pos:
            try:
                val = float(nums[col_pos])
                if 0 < val < 100:
                    results.append({
                        "district": district,
                        "year": year,
                        "single_hh_pct": val,
                    })
            except (ValueError, IndexError):
                pass

    return results


# ── 主流程 ────────────────────────────────────────────────────────
def main():
    print("=== 02b_fetch_single_hh_pdf.py ===\n")
    print("目標：台北市家庭收支訪問調查 104–112年 → 各區單人戶比例\n")

    all_rows = []

    for year, url in sorted(PDF_URLS.items()):
        print(f"【{year}年】")
        pdf_path = download_pdf(year, url)
        if not pdf_path:
            print(f"  → 跳過（下載失敗）")
            time.sleep(1)
            continue

        rows = parse_pdf(pdf_path, year)
        if rows:
            all_rows.extend(rows)
            dists = [r["district"] for r in rows]
            print(f"  → 解析到 {len(rows)} 區：{dists}")
        else:
            print(f"  → ⚠️  自動解析失敗，請手動確認 PDF 格式")
            print(f"     PDF 位置：{pdf_path}")

        time.sleep(1.2)

    if not all_rows:
        print("\n[結果] 無資料，可能需要手動從 PDF 複製數字")
        print("請開啟 data/raw/pdf/ 下的 PDF，找「表2」的 1人 欄位")
        print("手動建立 data/raw/single_hh_manual.csv：")
        print("  欄位：district,year,single_hh_pct")
        return

    df = pd.DataFrame(all_rows).drop_duplicates(["district","year"])
    df = df.sort_values(["district","year"]).reset_index(drop=True)

    # 診斷
    total = len(df)
    print(f"\n── 解析結果：{total} 筆（預期 108）──")
    missing = 108 - total
    if missing > 0:
        print(f"⚠️  缺少 {missing} 筆，可能需要手動補充")

    # pivot 預覽
    if total > 0:
        piv = df.pivot(index="district", columns="year", values="single_hh_pct")
        print("\n單人戶比例（%）：")
        print(piv.round(2).to_string())

    out = f"{CLEAN_DIR}/single_hh_pdf.csv"
    df.to_csv(out, index=False, encoding="utf-8-sig")
    print(f"\n✓ 儲存：{out}")
    print("接著執行 02_fetch_household.py 合併人口 + 單人戶資料")


if __name__ == "__main__":
    main()
