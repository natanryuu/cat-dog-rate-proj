# 實價登錄買賣原始資料 — 資料說明（`{季度}_a_lvr_land_a.csv`）

> 資料路徑：`data_raw/buy/`  
> 角色：**IV₁（住宅坪數）的原始來源**  
> 檔案數量：40 個（104S1 ~ 113S4，即 2015Q1 ~ 2024Q4）  
> 命名規則：`{民國年}S{季度}_a_lvr_land_a.csv`（例：`113S1_a_lvr_land_a.csv`）  
> 每檔筆數：約 2,000–6,000 筆（視季度而定）

---

## 欄位說明（關鍵欄位）

> 原始 CSV 第一列為中文欄位名，第二列為英文欄位名，**資料從第三列開始**（skiprows=1 讀取）。

| 中文欄位 | 英文欄位 | 型別 | 說明 |
|----------|----------|------|------|
| 鄉鎮市區 | The villages and towns urban district | str | 行政區名稱（格式：`XX區`，無「臺北市」前綴） |
| 交易標的 | transaction sign | str | 交易類型：`住家用`、`車位`、`土地`等；建模時篩選含「住家用」者 |
| 交易年月日 | transaction year month and day | str | 民國年格式，如 `1130301`（民113年3月1日） |
| 建物移轉總面積平方公尺 | building shifting total area | float | 平方公尺；換算為坪：`÷ 3.3058` |
| 主要用途 | main use | str | `住家用`、`停車空間`等 |
| 總價元 | total price NTD | int | 交易總金額（新台幣元） |
| 單價元平方公尺 | the unit price (NTD / square meter) | float | 每平方公尺單價 |
| 建物現況格局-房 | Building present situation pattern - room | int | 房間數 |
| 建物現況格局-廳 | building present situation pattern - hall | int | 廳數 |
| 建物現況格局-衛 | building present situation pattern - health | int | 衛浴數 |
| 建築完成年月 | construction to complete the years | str | 民國年月（如 `0941025`） |
| 建物型態 | building state | str | 公寓、大樓、透天厝等 |
| 主要建材 | main building materials | str | 鋼筋混凝土造等 |
| 電梯 | elevator | str | 有 / 無 |
| 編號 | serial number | str | 案件唯一識別碼 |

---

## 資料來源

- **來源：** 內政部不動產交易實價查詢服務網（lvr.land.moi.gov.tw）
- **資料集代號：** A 類（買賣成交）
- **下載腳本：** `download_all_data.py`（自動下載 ZIP 並解壓）
- **ZIP 快取：** `data_raw/zip_cache/`

---

## 讀取注意事項

```python
import pandas as pd

# 注意 skiprows=1：跳過英文欄位名那一列
df = pd.read_csv("data_raw/buy/113S1_a_lvr_land_a.csv",
                 skiprows=1,
                 low_memory=False)

# 篩選住宅買賣（排除車位、土地、工業廠房）
df_res = df[df["交易標的"].str.contains("住家用", na=False)]

# 面積換算為坪
df_res["建物坪數"] = df_res["建物移轉總面積平方公尺"].astype(float) / 3.3058

# 交易年份（民國年轉西元）
df_res["year"] = df_res["交易年月日"].astype(str).str[:3].astype(int) + 1911
```

---

## 與 IV₁ 的對應關係

此資料夾所有 CSV 已由 `download_all_data.py` 處理並彙總為：

```
data_raw/iv1_housing_size_panel.csv
```

通常無需直接存取原始買賣 CSV，除非需要重新計算或進行補充分析。詳見 [iv1_housing_size_panel_codebook.md](iv1_housing_size_panel_codebook.md)。

---

*最後更新：2026-04-01*
