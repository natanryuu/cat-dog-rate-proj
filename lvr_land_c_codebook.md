# 實價登錄租賃原始資料 — 資料說明（`{季度}_a_lvr_land_c.csv`）

> 資料路徑：`data_raw/rent/`  
> 角色：**IV₄（租賃件數）的原始來源**  
> 檔案數量：40 個（104S1 ~ 113S4，即 2015Q1 ~ 2024Q4）  
> 命名規則：`{民國年}S{季度}_a_lvr_land_c.csv`（例：`113S1_a_lvr_land_c.csv`）  
> 每檔筆數：約 1,000–4,000 筆（視季度而定）

---

## 欄位說明（關鍵欄位）

> 原始 CSV 第一列為中文欄位名，第二列為英文欄位名，**資料從第三列開始**（skiprows=1 讀取）。

| 中文欄位 | 英文欄位 | 型別 | 說明 |
|----------|----------|------|------|
| 鄉鎮市區 | The villages and towns urban district | str | 行政區名稱（格式：`XX區`） |
| 交易標的 | transaction sign | str | 租賃類型：`租賃房屋`、`車位`等；建模時篩選「租賃房屋」 |
| 租賃年月日 | transaction year month and day | str | 民國年格式，如 `1121018` |
| 建物總面積平方公尺 | building shifting total area | float | 租賃標的面積（平方公尺） |
| 主要用途 | main use | str | `住家用`、`停車空間`等 |
| 總額元 | total price NTD | int | 月租金（新台幣元） |
| 單價元平方公尺 | the unit price (NTD / square meter) | float | 每平方公尺月租金 |
| 租賃期間 | Rental period | str | 租期，格式如 `1121018~1141017` |
| 出租型態 | Rental type | str | `整棟(戶)出租`、`分租套房`等 |
| 有無附傢俱 | Whether there is attaches the furniture | str | 有 / 無 |
| 有無電梯 | elevator | str | 有 / 無 |
| 有無管理員 | Residential Manager | str | 有 / 無 |
| 附屬設備 | equipment | str | 冷氣、熱水器等設備清單 |
| 建物型態 | building state | str | 住宅大樓、華廈、公寓等 |
| 建築完成年月 | construction to complete the years | str | 民國年月 |
| 編號 | serial number | str | 案件唯一識別碼 |

---

## 資料來源

- **來源：** 內政部不動產交易實價查詢服務網（lvr.land.moi.gov.tw）
- **資料集代號：** C 類（租賃成交）
- **下載腳本：** `download_all_data.py`（自動下載 ZIP 並解壓）
- **ZIP 快取：** `data_raw/zip_cache/`

---

## 讀取注意事項

```python
import pandas as pd

# 注意 skiprows=1：跳過英文欄位名那一列
df = pd.read_csv("data_raw/rent/113S1_a_lvr_land_c.csv",
                 skiprows=1,
                 low_memory=False)

# 篩選住家用租賃（排除車位、商業用途）
df_res = df[df["主要用途"].str.contains("住家用", na=False)]

# 交易年份（民國年轉西元）
df_res["year"] = df_res["租賃年月日"].astype(str).str[:3].astype(int) + 1911
```

---

## 與 IV₄ 的對應關係

此資料夾所有 CSV 已由 `download_all_data.py` 處理並彙總為：

```
data_raw/iv4_rental_count_panel.csv
```

通常無需直接存取原始租賃 CSV，除非需要重新計算或進行補充分析。詳見 [iv4_rental_count_panel_codebook.md](iv4_rental_count_panel_codebook.md)。

### 申報率說明

台灣租賃市場實際申報率估計約 20–30%，此資料為**有申報的成交紀錄**，作為行政區間租賃活躍度的**相對指標**使用較為合理，不宜直接推估絕對租賃市場規模。

---

*最後更新：2026-04-01*
