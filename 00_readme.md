# 台北市寵物登記 Panel Data 資料蒐集腳本

## 檔案結構

```
pet_panel_data/
├── 00_readme.md          ← 本說明
├── 01_fetch_pets.py      ← 貓狗登記資料（data.taipei API）
├── 02_fetch_household.py ← 單人戶比例 + 結婚率（戶政司 CSV）
├── 03_fetch_housing.py   ← 住宅坪數（實價登錄 CSV）
├── 04_merge_panel.py     ← 合併成 panel data + 診斷
└── data/
    ├── raw/              ← 原始下載檔
    └── clean/            ← 清理後的 CSV
```

## 執行順序

```bash
pip install pandas requests openpyxl
python 01_fetch_pets.py
python 02_fetch_household.py
python 03_fetch_housing.py
python 04_merge_panel.py
```

## 需要手動下載的檔案

| 檔案 | 來源網址 | 存放位置 |
|------|----------|----------|
| 台北市戶政統計（含單人戶、結婚） | https://ca.gov.taipei/News_Content.aspx?n=8693DC9620A1AABF | data/raw/household_raw.xlsx |
| 台北市統計年報（人口密度） | https://www-cl.gov.taipei/News_Content.aspx?n=133F09557E5F4DC6 | data/raw/stats_yearbook.xlsx |
