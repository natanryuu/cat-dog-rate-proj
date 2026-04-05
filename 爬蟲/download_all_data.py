"""
======================================================================
台北市貓犬比 Panel Data — 一鍵資料下載腳本
======================================================================
使用方式：
  1. pip install requests pandas openpyxl
  2. python download_all_data.py
  3. 所有資料會下載到 ./data_raw/ 資料夾

注意：請在自己的電腦上執行，Claude 的環境無法連外網。
======================================================================
"""

import requests
import os
import zipfile
import time
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DIR = os.path.join(BASE_DIR, 'data_raw')
os.makedirs(RAW_DIR, exist_ok=True)

TAIPEI_DISTRICTS = [
    '松山區', '信義區', '大安區', '中山區', '中正區', '大同區',
    '萬華區', '文山區', '南港區', '內湖區', '士林區', '北投區'
]

# ═══════════════════════════════════════════════════════
# 1. 實價登錄 — 買賣（IV₁ 住宅坪數）+ 租賃（IV₄ 租賃活躍度）
# ═══════════════════════════════════════════════════════
def download_real_estate():
    """
    來源：內政部不動產成交案件實際資訊資料供應系統
    URL 規則：https://plvr.land.moi.gov.tw/DownloadSeason?season={民國年}S{季}&type=zip&fileName=lvr_landcsv.zip
    
    ZIP 內檔案說明：
    - a_lvr_land_a.csv = 買賣（台北市代碼 a）
    - a_lvr_land_c.csv = 租賃
    - a_lvr_land_b.csv = 預售屋（不需要）
    """
    buy_dir = os.path.join(RAW_DIR, 'buy')
    rent_dir = os.path.join(RAW_DIR, 'rent')
    zip_dir = os.path.join(RAW_DIR, 'zip_cache')
    os.makedirs(buy_dir, exist_ok=True)
    os.makedirs(rent_dir, exist_ok=True)
    os.makedirs(zip_dir, exist_ok=True)
    
    base_url = "https://plvr.land.moi.gov.tw/DownloadSeason"
    
    # 研究期間 2019-2024 = 民國 104-114，每年 4 季
    # 額外下載 104-107 備用（如果要擴展到 2015-2024）
    seasons = []
    for roc_year in range(104, 114):   # 104-114 = 2015-2025
        for s in range(1, 5):
            seasons.append(f"{roc_year}S{s}")
    
    # 2024 年可能只有到 S2，S3/S4 可能還沒出
    # 腳本會自動跳過下載失敗的
    
    print("=" * 60)
    print("  1. 下載實價登錄（買賣 + 租賃）")
    print(f"     共 {len(seasons)} 個季度：{seasons[0]} ~ {seasons[-1]}")
    print("=" * 60)
    
    success_buy = 0
    success_rent = 0
    
    for season in seasons:
        zip_path = os.path.join(zip_dir, f"{season}.zip")
        
        # 如果已下載過就跳過
        buy_file = os.path.join(buy_dir, f"{season}_a_lvr_land_a.csv")
        rent_file = os.path.join(rent_dir, f"{season}_a_lvr_land_c.csv")
        if os.path.exists(buy_file) and os.path.exists(rent_file):
            print(f"  [跳過] {season} 已存在")
            success_buy += 1
            success_rent += 1
            continue
        
        # 下載 ZIP
        url = f"{base_url}?season={season}&type=zip&fileName=lvr_landcsv.zip"
        print(f"  [下載] {season}...", end=" ", flush=True)
        
        try:
            resp = requests.get(url, timeout=60)
            if resp.status_code != 200 or len(resp.content) < 1000:
                print(f"❌ HTTP {resp.status_code} 或檔案太小")
                continue
            
            with open(zip_path, 'wb') as f:
                f.write(resp.content)
            
            # 解壓
            with zipfile.ZipFile(zip_path, 'r') as zf:
                names = zf.namelist()
                
                # 買賣 CSV（a_lvr_land_a.csv）
                buy_csv = [n for n in names if 'a_lvr_land_a' in n.lower() and n.endswith('.csv')]
                if buy_csv:
                    with zf.open(buy_csv[0]) as src, open(buy_file, 'wb') as dst:
                        dst.write(src.read())
                    success_buy += 1
                
                # 租賃 CSV（a_lvr_land_c.csv）
                rent_csv = [n for n in names if 'a_lvr_land_c' in n.lower() and n.endswith('.csv')]
                if rent_csv:
                    with zf.open(rent_csv[0]) as src, open(rent_file, 'wb') as dst:
                        dst.write(src.read())
                    success_rent += 1
            
            print(f"✅ 買賣{'✓' if buy_csv else '✗'} 租賃{'✓' if rent_csv else '✗'}")
            
        except Exception as e:
            print(f"❌ {e}")
        
        time.sleep(2)   # 避免打太快被擋
    
    print(f"\n  買賣 CSV: {success_buy}/{len(seasons)} 個季度")
    print(f"  租賃 CSV: {success_rent}/{len(seasons)} 個季度")


# ═══════════════════════════════════════════════════════
# 2. 台北市各行政區戶數（戶政司）— 用於 IV₃ 單人戶 + IV₄ 分母
# ═══════════════════════════════════════════════════════
def download_household_data():
    """
    來源：內政部戶政司 人口統計資料
    https://www.ris.gov.tw/app/portal/346
    
    這個網站是 JavaScript 動態載入，無法直接用 requests 爬。
    替代方案：用 data.gov.tw 的 Open Data API
    
    資料集 ID: 14299（戶數、人口數按戶別及性別 — 鄉鎮市區級）
    """
    print("\n" + "=" * 60)
    print("  2. 下載戶數資料（單人戶比例 + 住宅總戶數）")
    print("=" * 60)
    
    # data.gov.tw API
    # 資料集 14299 = 「戶數、人口數按戶別及性別」
    # 但這個 API 可能需要分頁下載
    
    # 方法 A：嘗試 data.gov.tw API
    api_url = "https://data.gov.tw/api/v2/rest/datastore/A01010302-14299-S"
    
    print("  嘗試從 data.gov.tw API 下載...")
    
    try:
        all_data = []
        offset = 0
        limit = 1000
        
        while True:
            params = {
                'offset': offset,
                'limit': limit,
                'filters': '{"statistic_yyy":">=108","site_id":"臺北市"}'
            }
            resp = requests.get(api_url, params=params, timeout=30)
            
            if resp.status_code == 200:
                data = resp.json()
                records = data.get('result', {}).get('records', [])
                if not records:
                    break
                all_data.extend(records)
                offset += limit
                if len(records) < limit:
                    break
                time.sleep(1)
            else:
                print(f"  ⚠ API 回傳 {resp.status_code}，嘗試備用方法")
                break
        
        if all_data:
            df = pd.DataFrame(all_data)
            out_path = os.path.join(RAW_DIR, 'household_by_type_govdata.csv')
            df.to_csv(out_path, index=False, encoding='utf-8-sig')
            print(f"  ✅ 下載 {len(df)} 筆，儲存至 {out_path}")
            return
    except Exception as e:
        print(f"  ⚠ API 失敗：{e}")
    
    # 方法 B：提供手動下載指引
    print("""
  ⚠ 自動下載失敗，請手動操作：
  
  步驟 1：開啟 https://www.ris.gov.tw/app/portal/346
  步驟 2：選「現住人口數、戶數」→「按區域別分」
  步驟 3：地區選「臺北市」→ 展開勾選 12 個行政區
  步驟 4：時間選 108-113 年底（12月）
  步驟 5：下載 Excel，存到 data_raw/household_by_type.csv
  
  或者，你之前上傳過的 CSV 已經有 108-114 年的資料，
  可以直接複製到 data_raw/household_by_type.csv 使用。
    """)


# ═══════════════════════════════════════════════════════
# 3. 台北市各行政區年底人口按年齡分（0-4歲幼兒比例）
# ═══════════════════════════════════════════════════════
def download_age_data():
    """
    來源：台北市民政局
    https://ca.gov.taipei/News_Content.aspx?n=8693DC9620A1AABF&sms=D19E9582624D83CB&s=78DC4B104D9D374E
    
    這裡有 86-114 年的 ODS 檔案，每年一份，按「行政區」分。
    ODS 檔是手動下載連結，無法直接用 URL pattern 爬取。
    
    備用方案：data.taipei API（臺北市各里人口數按年齡分）
    dataset ID: a6394e3f-3514-4542-87bd-de4310a40db3
    但這個只有 110 年起的資料。
    """
    print("\n" + "=" * 60)
    print("  3. 下載年齡結構資料（0-4 歲幼兒比例）")
    print("=" * 60)
    
    age_dir = os.path.join(RAW_DIR, 'age')
    os.makedirs(age_dir, exist_ok=True)
    
    # 方法 A：嘗試 data.taipei API（里級，110年起）
    dataset_id = "a6394e3f-3514-4542-87bd-de4310a40db3"
    api_url = f"https://data.taipei/api/v1/dataset/{dataset_id}"
    
    print("  嘗試從 data.taipei API 下載（110年起）...")
    
    try:
        all_data = []
        offset = 0
        limit = 1000
        
        while True:
            params = {'offset': offset, 'limit': limit}
            resp = requests.get(api_url, params=params, timeout=30)
            
            if resp.status_code == 200:
                data = resp.json()
                records = data.get('result', {}).get('results', [])
                if not records:
                    break
                all_data.extend(records)
                offset += limit
                print(f"    已下載 {len(all_data)} 筆...", end="\r", flush=True)
                if len(records) < limit:
                    break
                time.sleep(0.5)
            else:
                break
        
        if all_data:
            df = pd.DataFrame(all_data)
            out_path = os.path.join(age_dir, 'age_by_li_datataiwan.csv')
            df.to_csv(out_path, index=False, encoding='utf-8-sig')
            print(f"\n  ✅ 下載 {len(df)} 筆里級年齡資料，儲存至 {out_path}")
        else:
            print("  ⚠ API 回傳空資料")
    except Exception as e:
        print(f"  ⚠ API 失敗：{e}")
    
    # 方法 B：解析已存在的 ODS 檔案
    ods_files = sorted([f for f in os.listdir(age_dir) if f.endswith('.ods')])
    if not ods_files:
        print(f"""
  📋 找不到 ODS 檔案，請手動下載後放到 data_raw/age/：
  網址：https://ca.gov.taipei/News_Content.aspx?n=8693DC9620A1AABF&sms=D19E9582624D83CB&s=78DC4B104D9D374E
        """)
        return

    print(f"\n  偵測到 {len(ods_files)} 個 ODS 檔案，開始解析...")
    records = []

    for fname in ods_files:
        roc_year_str = fname.replace('年.ods', '')
        try:
            roc_year = int(roc_year_str)
        except ValueError:
            print(f"  [跳過] 無法解析年份：{fname}")
            continue

        ad_year = roc_year + 1911
        fpath = os.path.join(age_dir, fname)

        try:
            df = pd.read_excel(fpath, engine='odf', header=None)
            # row 1 = header, rows 2+ = data；只取「計」（性別欄 == '計'）
            data = df.iloc[2:].copy()
            data.columns = range(data.shape[1])
            data = data[data[1] == '計'].copy()

            for _, row in data.iterrows():
                district = str(row[0]).replace('\u3000', '').replace(' ', '')
                if district in ('總計', '總  計', '合計'):
                    continue
                # 清理區名（有時格式為 "松 山 區"）
                district = district.replace('\xa0', '').strip()
                try:
                    total_pop = int(row[2])
                    pop_0_4   = int(row[3])
                except (ValueError, TypeError):
                    continue
                records.append({
                    'ad_year':   ad_year,
                    'roc_year':  roc_year,
                    'district':  district,
                    'total_pop': total_pop,
                    'pop_0_4':   pop_0_4,
                    'ratio_0_4': round(pop_0_4 / total_pop, 6) if total_pop > 0 else None,
                })

            print(f"  ✅ {fname}")
        except Exception as e:
            print(f"  ❌ {fname}：{e}")

    if records:
        out_df = pd.DataFrame(records).sort_values(['ad_year', 'district'])
        out_path = os.path.join(age_dir, 'age_district_panel.csv')
        out_df.to_csv(out_path, index=False, encoding='utf-8-sig')
        print(f"\n  ✅ 已輸出 {len(out_df)} 筆 → {out_path}")
    else:
        print("  ⚠ 沒有成功解析任何資料")


# ═══════════════════════════════════════════════════════
# 4. 處理已有的 DV（petgov_raw.csv）
# ═══════════════════════════════════════════════════════
def setup_dv():
    """
    DV 資料（petgov_raw.csv）你已經有了，
    這一步只是確認格式並複製到 data_raw/
    """
    print("\n" + "=" * 60)
    print("  4. 設定 DV（貓犬登記數）")
    print("=" * 60)
    
    # 如果跟腳本同目錄有 petgov_raw.csv
    src = os.path.join(BASE_DIR, 'petgov_raw.csv')
    dst = os.path.join(RAW_DIR, 'petgov_raw.csv')
    
    if os.path.exists(src):
        import shutil
        shutil.copy2(src, dst)
        df = pd.read_csv(dst)
        print(f"  ✅ 已複製 petgov_raw.csv（{len(df)} 筆）")
    elif os.path.exists(dst):
        df = pd.read_csv(dst)
        print(f"  ✅ 已存在 data_raw/petgov_raw.csv（{len(df)} 筆）")
    else:
        print("  ⚠ 找不到 petgov_raw.csv，請手動複製到 data_raw/ 或腳本同目錄")


# ═══════════════════════════════════════════════════════
# 5. 資料處理：從原始 CSV 建構 Panel
# ═══════════════════════════════════════════════════════
def process_real_estate_to_panel():
    """
    將下載的實價登錄 CSV 處理成：
    - IV₁：各區年度平均住宅坪數
    - IV₄：各區年度租賃件數
    """
    print("\n" + "=" * 60)
    print("  5. 處理實價登錄 → Panel 格式")
    print("=" * 60)
    
    import glob
    
    # ─── IV₁：買賣 → 住宅坪數 ───
    buy_dir = os.path.join(RAW_DIR, 'buy')
    buy_files = glob.glob(os.path.join(buy_dir, '*.csv'))
    
    if buy_files:
        print(f"\n  處理買賣 CSV（{len(buy_files)} 個檔案）...")
        all_buy = []
        
        for f in buy_files:
            try:
                # 跳過第一行（有些檔案第一行是欄位中文說明）
                df = pd.read_csv(f, encoding='utf-8-sig', low_memory=False)
                
                # 如果第一行是說明文字，重新讀取
                if '鄉鎮市區' not in df.columns and len(df.columns) > 0:
                    df = pd.read_csv(f, encoding='utf-8-sig', low_memory=False, skiprows=1)
                
                if '鄉鎮市區' not in df.columns:
                    for enc in ['cp950', 'big5']:
                        try:
                            df = pd.read_csv(f, encoding=enc, low_memory=False)
                            if '鄉鎮市區' not in df.columns:
                                df = pd.read_csv(f, encoding=enc, low_memory=False, skiprows=1)
                            break
                        except: continue
                
                if '鄉鎮市區' not in df.columns:
                    continue
                
                # 篩選住宅
                if '主要用途' in df.columns:
                    df = df[df['主要用途'].astype(str).str.contains('住', na=False)]
                
                # 面積
                area_col = [c for c in df.columns if '建物移轉總面積' in str(c)]
                if not area_col:
                    area_col = [c for c in df.columns if '面積' in str(c)]
                if not area_col:
                    continue
                
                # 日期 → 年份
                date_col = [c for c in df.columns if '交易年月日' in str(c)][0]
                
                sub = df[['鄉鎮市區', area_col[0], date_col]].copy()
                sub.columns = ['district', 'area', 'date']
                sub['area'] = pd.to_numeric(sub['area'], errors='coerce')
                sub['date'] = pd.to_numeric(sub['date'], errors='coerce')
                sub['year'] = (sub['date'] // 10000 + 1911).astype('Int64')
                sub['area_ping'] = sub['area'] * 0.3025
                
                sub = sub[sub['district'].isin(TAIPEI_DISTRICTS)]
                sub = sub[(sub['area_ping'] > 5) & (sub['area_ping'] < 200)]
                
                all_buy.append(sub[['year', 'district', 'area_ping']].dropna())
            except Exception as e:
                print(f"    ⚠ {os.path.basename(f)}: {e}")
        
        if all_buy:
            combined = pd.concat(all_buy, ignore_index=True)
            panel_buy = combined.groupby(['year', 'district']).agg(
                avg_housing_size=('area_ping', 'mean'),
                median_housing_size=('area_ping', 'median'),
                n_buy=('area_ping', 'count')
            ).reset_index()
            panel_buy = panel_buy.round(2)
            
            out = os.path.join(RAW_DIR, 'iv1_housing_size_panel.csv')
            panel_buy.to_csv(out, index=False, encoding='utf-8-sig')
            print(f"  ✅ IV₁ 住宅坪數：{len(panel_buy)} 筆 → {out}")
    else:
        print("  ⚠ 沒有買賣 CSV，跳過 IV₁")
    
    # ─── IV₄：租賃 → 件數 ───
    rent_dir = os.path.join(RAW_DIR, 'rent')
    rent_files = glob.glob(os.path.join(rent_dir, '*.csv'))
    
    if rent_files:
        print(f"\n  處理租賃 CSV（{len(rent_files)} 個檔案）...")
        all_rent = []
        
        for f in rent_files:
            try:
                df = pd.read_csv(f, encoding='utf-8-sig', low_memory=False)
                if '鄉鎮市區' not in df.columns:
                    df = pd.read_csv(f, encoding='utf-8-sig', low_memory=False, skiprows=1)
                if '鄉鎮市區' not in df.columns:
                    for enc in ['cp950', 'big5']:
                        try:
                            df = pd.read_csv(f, encoding=enc, low_memory=False)
                            if '鄉鎮市區' not in df.columns:
                                df = pd.read_csv(f, encoding=enc, low_memory=False, skiprows=1)
                            break
                        except: continue
                
                if '鄉鎮市區' not in df.columns:
                    continue
                
                date_col = [c for c in df.columns if '租賃年月日' in str(c) or '交易年月日' in str(c)]
                if not date_col:
                    continue
                
                sub = df[['鄉鎮市區', date_col[0]]].copy()
                sub.columns = ['district', 'date']
                sub['date'] = pd.to_numeric(sub['date'], errors='coerce')
                sub['year'] = (sub['date'] // 10000 + 1911).astype('Int64')
                sub = sub[sub['district'].isin(TAIPEI_DISTRICTS)]
                
                all_rent.append(sub[['year', 'district']].dropna())
            except Exception as e:
                print(f"    ⚠ {os.path.basename(f)}: {e}")
        
        if all_rent:
            combined = pd.concat(all_rent, ignore_index=True)
            panel_rent = combined.groupby(['year', 'district']).size().reset_index(name='rental_count')
            
            out = os.path.join(RAW_DIR, 'iv4_rental_count_panel.csv')
            panel_rent.to_csv(out, index=False, encoding='utf-8-sig')
            print(f"  ✅ IV₄ 租賃件數：{len(panel_rent)} 筆 → {out}")
    else:
        print("  ⚠ 沒有租賃 CSV，跳過 IV₄")


# ═══════════════════════════════════════════════════════
# 主程式
# ═══════════════════════════════════════════════════════
def main():
    print("""
    ╔══════════════════════════════════════════════════╗
    ║  台北市貓犬比 Panel Data — 一鍵資料下載         ║
    ║  研究期間：2019-2024（民國 108-113）             ║
    ║  觀測單位：12 行政區 × 6 年 = 72 筆             ║
    ╚══════════════════════════════════════════════════╝
    """)
    
    # Step 1: 下載實價登錄（最耗時，約 5-10 分鐘）
    download_real_estate()
    
    # Step 2: 下載戶數資料
    download_household_data()
    
    # Step 3: 下載年齡資料
    download_age_data()
    
    # Step 4: 確認 DV
    setup_dv()
    
    # Step 5: 處理實價登錄
    process_real_estate_to_panel()
    
    # 最終摘要
    print("\n" + "=" * 60)
    print("  下載完成！資料夾結構：")
    print("=" * 60)
    for root, dirs, files in os.walk(RAW_DIR):
        level = root.replace(RAW_DIR, '').count(os.sep)
        indent = '  ' * (level + 1)
        print(f"{indent}{os.path.basename(root)}/")
        for f in sorted(files)[:5]:
            print(f"{indent}  {f}")
        if len(files) > 5:
            print(f"{indent}  ... 共 {len(files)} 個檔案")
    
    print(f"""
  ╔══════════════════════════════════════════════════╗
  ║  下一步：                                       ║
  ║  1. 確認缺漏的資料（見上方 ⚠ 提示）            ║
  ║  2. 執行 process_all_data.py 合併成 panel       ║
  ║  3. 執行建模 pipeline                           ║
  ╚══════════════════════════════════════════════════╝
    """)


if __name__ == '__main__':
    main()
