#!/usr/bin/env python3
"""
fetch_edu_mig.py — 台北市 12 行政區: 教育程度
=============================================

變數
----
1. edu_ratio      大專以上學歷比       ODRP020 (yearly, API 106-114)

⚠ 104-105 年 (2015-2016) 戶政司 API 無資料，教育程度改從本地 CSV 讀取
  (data_raw/education/opendata104Y050-1.csv, opendata105Y050.csv)

用法
----
    python fetch_edu_mig.py                  # 全部跑
    python fetch_edu_mig.py --probe          # 只 probe 欄位

Output
------
    data/edu_ratio_panel.csv
"""

import argparse
import re
import ssl
import time
from pathlib import Path

import pandas as pd
import requests
from requests.adapters import HTTPAdapter

# ────────────────────────────────────────────────────────────
# 0. 全域設定
# ────────────────────────────────────────────────────────────
TAIPEI_DISTRICTS = [
    "松山區", "信義區", "大安區", "中山區", "中正區", "大同區",
    "萬華區", "文山區", "南港區", "內湖區", "士林區", "北投區",
]

PANEL_YEARS_ROC = list(range(104, 115))   # 104-114 → 2015-2025
PANEL_YEARS_AD  = [y + 1911 for y in PANEL_YEARS_ROC]

BASE_RIS = "https://www.ris.gov.tw/rs-opendata/api/v1/datastore"
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

RAW_EDU_DIR = Path("data_raw/education")
LOCAL_EDU_FILES = {
    104: RAW_EDU_DIR / "opendata104Y050-1.csv",
    105: RAW_EDU_DIR / "opendata105Y050.csv",
}

POLITE_DELAY = 0.6


# ────────────────────────────────────────────────────────────
# SSL adapter (台灣政府 legacy TLS)
# ────────────────────────────────────────────────────────────
class LegacySSLAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.create_default_context()
        ctx.options |= ssl.OP_LEGACY_SERVER_CONNECT
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        kwargs["ssl_context"] = ctx
        return super().init_poolmanager(*args, **kwargs)

SESSION = requests.Session()
SESSION.mount("https://", LegacySSLAdapter())
SESSION.headers.update({"User-Agent": "CatDogResearch/1.0"})


# ────────────────────────────────────────────────────────────
# 通用 helpers
# ────────────────────────────────────────────────────────────
def fetch_ris_all_pages(endpoint: str, params: dict = None,
                        max_pages: int = 200) -> list:
    """分頁拉取 ris.gov.tw API，回傳所有 responseData。"""
    if params is None:
        params = {}
    all_data = []
    for page in range(1, max_pages + 1):
        p = {**params, "PAGE": str(page)}
        url = f"{BASE_RIS}/{endpoint}"
        try:
            r = SESSION.get(url, params=p, timeout=30)
            r.raise_for_status()
        except Exception as e:
            print(f"  ⚠ page={page} 失敗: {e}")
            break
        body = r.json()
        resp_code = body.get("responseCode", "")
        # 成功碼: OD-0001 (舊), OD-0101-S (新) 等
        if not (resp_code == "OD-0001" or resp_code.endswith("-S")):
            print(f"  ⚠ {resp_code}: {body.get('responseMessage')}")
            break
        chunk = body.get("responseData", [])
        if not chunk:
            break
        all_data.extend(chunk)
        total_pages = int(body.get("totalPage", page))
        if page >= total_pages:
            break
        time.sleep(POLITE_DELAY)
    return all_data


def probe_fields(endpoint: str, params: dict = None, label: str = ""):
    """抓 1 頁印出欄位名稱。"""
    if params is None:
        params = {}
    params["PAGE"] = "1"
    url = f"{BASE_RIS}/{endpoint}"
    print(f"\n{'='*60}")
    print(f"PROBE: {label}  →  {url}")
    try:
        r = SESSION.get(url, params=params, timeout=30)
        r.raise_for_status()
        body = r.json()
        data = body.get("responseData", [])
        if data:
            print(f"  欄位 ({len(data[0])} 個):")
            for k, v in data[0].items():
                print(f"    {k:30s} = {v}")
        else:
            print(f"  ⚠ responseData 為空")
            print(f"  responseCode: {body.get('responseCode')}")
    except Exception as e:
        print(f"  ⚠ Error: {e}")
    print(f"{'='*60}\n")


def _find_col(df: pd.DataFrame, candidates: list) -> str | None:
    cols_lower = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand in df.columns:
            return cand
        if cand.lower() in cols_lower:
            return cols_lower[cand.lower()]
    return None


def _is_likely_numeric(df, col, n=20):
    sample = df[col].dropna().head(n)
    try:
        pd.to_numeric(sample)
        return True
    except (ValueError, TypeError):
        return False


# ────────────────────────────────────────────────────────────
# 本地 CSV 教育程度 (104-105)
# ────────────────────────────────────────────────────────────
def _load_local_edu(roc_y: int) -> list[dict]:
    """從本地 CSV 讀取 104/105 年臺北市教育程度，回傳同 fetch_edu_ratio 的 row 格式。"""
    csv_path = LOCAL_EDU_FILES.get(roc_y)
    if csv_path is None or not csv_path.exists():
        return []

    ad_y = roc_y + 1911
    df = pd.read_csv(csv_path, dtype=str)

    # 只留臺北市
    df = df[df["區域別"].str.startswith("臺北市")].copy()
    if df.empty:
        return []

    # 區名: "臺北市松山區" → "松山區"
    df["district"] = df["區域別"].str.replace("臺北市", "", n=1)

    # 轉數值
    num_cols = [c for c in df.columns if c not in ("統計年度", "區域別", "村里名稱", "年齡", "district")]
    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    # 大專以上欄位: 博畢 / 碩畢 / 大畢 / 專畢
    higher_cols = [c for c in num_cols if any(kw in c for kw in ("博畢", "碩畢", "大畢", "專畢"))]

    rows = []
    for dist, g in df.groupby("district"):
        if dist not in TAIPEI_DISTRICTS:
            continue
        h = g[higher_cols].sum().sum()
        t = g["總計"].sum()
        rows.append({
            "year": ad_y,
            "district": dist,
            "edu_higher_pop": int(h),
            "edu_total_pop": int(t),
            "edu_ratio": round(h / t, 6) if t > 0 else None,
        })
    return rows


# ════════════════════════════════════════════════════════════
# 1. 大專以上學歷比  (ODRP020)
# ════════════════════════════════════════════════════════════
def fetch_edu_ratio() -> pd.DataFrame:
    """
    ODRP020: 各村里教育程度資料 (年報, 民國年)
    村里層級 → 聚合到區級
    博士 + 碩士 + 大學 + 專科 = 大專以上
    """
    print("\n" + "─"*60)
    print("📚 大專以上學歷比  (ODRP020)")
    print("─"*60)

    rows = []
    for roc_y in PANEL_YEARS_ROC:
        ad_y = roc_y + 1911
        print(f"  民國 {roc_y} ({ad_y}) ...", end=" ", flush=True)

        # 104-105: 從本地 CSV 讀取
        if roc_y in LOCAL_EDU_FILES:
            local_rows = _load_local_edu(roc_y)
            if local_rows:
                rows.extend(local_rows)
                print(f"本地 CSV → {len(local_rows)} 筆區")
            else:
                print("⚠ 本地 CSV 讀取失敗")
            continue

        data = fetch_ris_all_pages(
            f"ODRP020/{roc_y}",
            params={"COUNTY": "臺北市"},
        )
        print(f"{len(data)} 筆村里")
        if not data:
            print(f"  ⚠ 無資料，跳過")
            continue

        df = pd.DataFrame(data)
        # 清除 BOM
        df.columns = [c.lstrip("\ufeff") for c in df.columns]

        # 首年印出欄位
        if roc_y == PANEL_YEARS_ROC[0] or (
                roc_y == 106 and PANEL_YEARS_ROC[0] < 106):
            print(f"  欄位: {list(df.columns)}")

        # ── 辨識 town 欄位 ──
        # API 新版用 site_id (值如 "臺北市松山區")
        town_col = _find_col(df, ["town", "TOWN", "site_id"])
        if town_col is None:
            for c in df.columns:
                if df[c].astype(str).str.contains("區").any():
                    town_col = c
                    break
        if town_col is None:
            print("  ⚠ 無法辨識 town 欄位，--probe 檢查")
            continue
        # 統一去除 "臺北市" 前綴
        df[town_col] = df[town_col].astype(str).str.replace("臺北市", "", n=1)

        # 轉數值
        num_cols = [c for c in df.columns
                    if c != town_col and _is_likely_numeric(df, c)]
        for c in num_cols:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

        # 大專以上欄位 (博士/碩士/大學/專科)
        # 排除 ungraduated (肄業), 只計算畢業，與本地 CSV 一致
        higher_kw = ["doctor", "master", "university",
                     "jrcollege", "juniorcollege",
                     "博士", "碩士", "大學", "專科"]
        higher_cols = [c for c in num_cols
                       if any(kw in c.lower() for kw in higher_kw)
                       and "ungraduated" not in c.lower()]

        # 總計欄位
        total_kw = ["total", "people_total", "合計", "總計"]
        total_cols = [c for c in num_cols
                      if any(kw in c.lower() for kw in total_kw)]

        if not higher_cols or not total_cols:
            print(f"  ⚠ 欄位匹配失敗 higher={higher_cols}, "
                  f"total={total_cols}")
            continue

        if roc_y in (PANEL_YEARS_ROC[0], 106):
            print(f"  → 大專以上: {higher_cols}")
            print(f"  → 總計:     {total_cols}")

        for dist, g in df.groupby(town_col):
            dist_clean = str(dist).strip()
            if dist_clean not in TAIPEI_DISTRICTS:
                continue
            h = g[higher_cols].sum().sum()
            t = g[total_cols].sum().sum()
            rows.append({
                "year": ad_y,
                "district": dist_clean,
                "edu_higher_pop": int(h),
                "edu_total_pop": int(t),
                "edu_ratio": round(h / t, 6) if t > 0 else None,
            })
        time.sleep(POLITE_DELAY)

    result = pd.DataFrame(rows)
    if len(result):
        out = DATA_DIR / "edu_ratio_panel.csv"
        result.to_csv(out, index=False, encoding="utf-8-sig")
        n_y = result["year"].nunique()
        print(f"\n  ✅ {out}  ({len(result)} 筆, {n_y} 年)")
    else:
        print("  ❌ 無資料產出")
    return result


# ────────────────────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="台北市 12 行政區: 教育程度 資料收集"
    )
    parser.add_argument("--probe", action="store_true",
                        help="只 probe API 欄位")
    args = parser.parse_args()

    if args.probe:
        print("🔍 PROBE MODE\n")
        probe_fields("ODRP020/106",
                      params={"COUNTY": "臺北市"},
                      label="ODRP020 教育程度 (106=API最早)")
        probe_fields("ODRP020/104",
                      params={"COUNTY": "臺北市"},
                      label="ODRP020 (104=預期無資料)")
        return

    try:
        fetch_edu_ratio()
    except Exception as e:
        print(f"\n  ❌ 失敗: {e}")
        import traceback
        traceback.print_exc()

    print("\n" + "="*60)
    print("🏁 完成!")
    print("="*60)


if __name__ == "__main__":
    main()
