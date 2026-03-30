"""
591 租屋網 - 台北市各行政區寵物友善出租比例爬蟲
confirmed API 版本（2026-03-30 DevTools 驗證）

執行方式：
    python 591_pet_scraper.py            # 爬一次當下快照
    python 591_pet_scraper.py wayback    # 查 Wayback 歷史可用性

⚠️  僅供學術研究，每次請求間隔 3 秒，請勿商業使用。
"""

import requests
import re
import pandas as pd
import time
from datetime import datetime

# ────────────────────────────────────────────────────
#  1. 常數（全部已由 DevTools 驗證）
# ────────────────────────────────────────────────────

BASE_URL = "https://rent.591.com.tw/list"
TAIPEI_REGION_ID = 1

# section_id 從 response items.sectionid 欄位確認
DISTRICT_IDS = {
    "中正區":  1,
    "大同區":  2,
    "中山區":  3,
    "松山區":  4,
    "大安區":  5,
    "萬華區":  6,
    "信義區":  7,
    "士林區":  8,
    "北投區":  9,
    "內湖區": 10,
    "南港區": 11,
    "文山區": 12,
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer": "https://rent.591.com.tw/",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-TW,zh;q=0.9",
}

SLEEP_SEC = 3


# ────────────────────────────────────────────────────
#  2. session 初始化
# ────────────────────────────────────────────────────

def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)
    resp = session.get("https://rent.591.com.tw/", timeout=10)
    resp.raise_for_status()

    # 從首頁 HTML 中擷取 CSRF token
    match = re.search(r'<meta\s+name="csrf-token"\s+content="([^"]+)"', resp.text)
    if match:
        csrf_token = match.group(1)
        session.headers["X-CSRF-TOKEN"] = csrf_token
        print(f"[init] CSRF token: {csrf_token[:20]}...")
    else:
        print("[init] WARN: 未找到 CSRF token, API 請求可能失敗")

    print(f"[init] session OK, cookies: {list(session.cookies.keys())}")
    time.sleep(SLEEP_SEC)
    return session


# ────────────────────────────────────────────────────
#  3. 核心：打一次 API，只取 data.total
# ────────────────────────────────────────────────────

def fetch_total(session, section_id: int, pet_only: bool = False) -> int:
    """
    只需讀第一頁的 data.total（已確認為字串型別）。
    全台北 + other=pet 時 total="3534"，不需翻頁。
    """
    params = {
        "timestamp": int(datetime.now().timestamp() * 1000),
        "region": TAIPEI_REGION_ID,
        "section": section_id,
    }
    if pet_only:
        params["other"] = "pet"   # ✅ 已由 GA beacon 確認

    resp = session.get(BASE_URL, params=params, timeout=15)
    resp.raise_for_status()

    if not resp.text.strip():
        raise ValueError("空回應 (empty body)")

    try:
        data = resp.json()
    except Exception:
        print(f"    ⚠️  非 JSON 回應 (status={resp.status_code}), 前 200 字: {resp.text[:200]}")
        raise

    try:
        return int(data["data"]["total"])   # ✅ 路徑已確認
    except (KeyError, TypeError, ValueError) as e:
        print(f"    ⚠️  解析失敗: {e}, raw={data.get('data', {}).get('total')}")
        return 0


# ────────────────────────────────────────────────────
#  4. 主流程：一次跑完 12 區
# ────────────────────────────────────────────────────

def crawl_snapshot(output_path: str = "591_pet_snapshot.csv") -> pd.DataFrame:
    session = build_session()
    today = datetime.today().strftime("%Y-%m-%d")
    records = []

    for district, sec_id in DISTRICT_IDS.items():
        print(f"\n[{district}] section={sec_id}")
        try:
            total = fetch_total(session, sec_id, pet_only=False)
            time.sleep(SLEEP_SEC)
            pet   = fetch_total(session, sec_id, pet_only=True)
            time.sleep(SLEEP_SEC)

            ratio = round(pet / total, 4) if total > 0 else None
            pct   = f"{ratio:.1%}" if ratio else "N/A"
            print(f"  全部={total}  寵物友善={pet}  比例={pct}")

        except Exception as e:
            print(f"  ⚠️  {e}")
            total, pet, ratio = None, None, None

        records.append({
            "crawl_date": today,
            "district": district,
            "total_listings": total,
            "pet_listings": pet,
            "pet_ratio": ratio,
        })

    df_new = pd.DataFrame(records)

    try:
        df_old = pd.read_csv(output_path, encoding="utf-8-sig")
        df_all = pd.concat([df_old, df_new], ignore_index=True)
    except FileNotFoundError:
        df_all = df_new

    df_all.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"\n✅ 存至 {output_path}（共 {len(df_all)} 筆）")
    return df_new


# ────────────────────────────────────────────────────
#  5. Wayback Machine 歷史快照調查
# ────────────────────────────────────────────────────

def check_wayback(years=None):
    if years is None:
        years = list(range(2015, 2025))

    print("=== Wayback Machine 快照調查 ===")
    for yr in years:
        params = {
            "url": "rent.591.com.tw/list*",
            "output": "json",
            "from": f"{yr}0101",
            "to": f"{yr}1231",
            "limit": 3,
            "fl": "timestamp,statuscode",
            "filter": "statuscode:200",
            "collapse": "timestamp:6",
        }
        try:
            resp = requests.get(
                "https://web.archive.org/cdx/search/cdx",
                params=params, timeout=20
            )
            rows = resp.json()
            cnt = max(0, len(rows) - 1)
            print(f"  {yr}: {'✅' if cnt > 0 else '❌'}  {cnt} 筆快照")
        except Exception as e:
            print(f"  {yr}: 查詢失敗 ({e})")
        time.sleep(1)


# ────────────────────────────────────────────────────
#  6. 入口
# ────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    mode = sys.argv[1] if len(sys.argv) > 1 else "snapshot"

    if mode == "snapshot":
        crawl_snapshot("591_pet_snapshot.csv")
    elif mode == "wayback":
        check_wayback()
    else:
        print("用法：python 591_pet_scraper.py [snapshot|wayback]")
