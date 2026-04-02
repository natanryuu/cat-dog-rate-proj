# 年齡結構 Panel — 資料說明（`age_district_panel.csv`）

> 資料路徑：`data_raw/age/age_district_panel.csv`  
> 角色：**控制變數**（幼兒人口比例，控制育兒需求對寵物飼養的排擠效應）  
> 覆蓋範圍：台北市 12 行政區 × 2016–2025（民國105–114年），共 120 筆

---

## 欄位說明

| 欄位 | 型別 | 單位 | 說明 |
|------|------|------|------|
| `ad_year` | int | 西元年 | 統計年份 |
| `roc_year` | int | 民國年 | 民國年份（`ad_year - 1911`） |
| `district` | str | — | 行政區名稱（格式：`XX區`） |
| `total_pop` | int | 人 | 該行政區當年**總設籍人口** |
| `pop_0_4` | int | 人 | **0–4 歲**人口數（幼兒） |
| `ratio_0_4` | float | — | `pop_0_4 / total_pop`（幼兒人口比例，研究使用的控制變數） |

---

## 資料來源

- **原始資料：** 台北市政府民政局 — 各行政區年齡結構 ODS 檔案（分年度下載）
- **原始路徑：** `data_raw/age/` 目錄下的 ODS 檔案（105–114年共 10 個）
- **處理腳本：** `download_all_data.py`（ODS 讀取並合併為 Panel）

---

## 資料品質注意事項

- 人口數為**設籍人口**，非實際居住人口，對流動性高的行政區（如中正、大同）可能低估實際人口。
- `ratio_0_4` 為年底截面統計，與寵物登記的年底存量口徑一致，可直接對齊合併。
- 2025 年（民國114年）資料已包含，但研究期間截取 **2019–2024** 即可。

---

## 使用範例（Python）

```python
import pandas as pd

age = pd.read_csv("data_raw/age/age_district_panel.csv")

# 截取研究期間
age = age[age["ad_year"].between(2019, 2024)].rename(columns={"ad_year": "year"})

# 與主資料合併
df = df.merge(age[["year", "district", "total_pop", "ratio_0_4"]],
              on=["year", "district"], how="left")
```

---

*最後更新：2026-04-01*
