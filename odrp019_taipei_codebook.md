# IV₃ 原始資料：里別家庭組成 — 資料說明（`odrp019_taipei_106_113.csv`）

> 資料路徑：`data_raw/odrp019_taipei_106_113.csv`  
> 角色：**IV₃ 的原始里級資料**（需彙總至行政區後才能使用）  
> 覆蓋範圍：台北市 456 里 × 民國106–112年（西元2017–2023），共約 3,192 筆  
> 資料集代碼：ODRP019（台北市政府開放資料平台）

---

## 欄位說明

| 欄位 | 型別 | 說明 |
|------|------|------|
| `statistic_yyy` | int | 民國年（106–112） |
| `site_id` | str | 行政區名稱，格式為**「臺北市XX區」**（含前綴） |
| `village` | str | 里名稱（例：「莊敬里」） |
| `household_ordinary_total` | int | 普通戶總計（含所有家庭型態） |
| `household_business_total` | int | 集體戶（法人戶）總計 |
| `household_single_total` | int | **單人戶總計**（核心欄位） |
| `household_ordinary_m` | int | 普通戶男性人口 |
| `household_business_m` | int | 集體戶男性人口 |
| `household_single_m` | int | 單人戶男性人口 |
| `household_ordinary_f` | int | 普通戶女性人口 |
| `household_business_f` | int | 集體戶女性人口 |
| `household_single_f` | int | 單人戶女性人口 |
| `district_code` | str | 區代碼（部分為空值） |
| `year_ad` | int | 西元年（`statistic_yyy + 1911`） |

> 第一列為中文欄位名，第二列為英文欄位名（內政部標準格式），**資料從第三列開始**。

---

## 資料來源

- **來源：** 台北市政府民政局開放資料 — 戶政統計 ODRP019（每里家庭組成）
- **API 成功碼：** `OD-0101-S`
- **抓取腳本：** `fetch_odrp019_taipei.py`
- **API 注意：** 民國113年（2024年）資料尚未上架（截至 2026-04-01）

---

## 從里級彙總至行政區的處理方式

此原始資料為**里（村里）級**，使用前需彙總至**行政區**：

```python
import pandas as pd

df = pd.read_csv("data_raw/odrp019_taipei_106_113.csv",
                 skiprows=1,   # 跳過英文欄位名那一行（已含在第二列）
                 encoding="utf-8-sig")  # 處理 BOM

# 彙總至行政區
iv3 = df.groupby(["year_ad", "site_id"]).agg(
    single=("household_single_total", "sum"),
    ordinary=("household_ordinary_total", "sum"),
).reset_index()

# 統一行政區格式（去除「臺北市」前綴）
iv3["district"] = iv3["site_id"].str.replace("臺北市", "", regex=False)
iv3 = iv3.rename(columns={"year_ad": "year"})
iv3["ratio_single"] = iv3["single"] / iv3["ordinary"]
```

> 處理後的 Panel 已存放於 `data_raw/iv3_single_household_panel.csv`（見 `iv3_single_household_panel_codebook.md`）。

---

## 資料品質注意事項

- `household_business_total` 通常為 0（台北市集體戶極少）
- 部分里的 `district_code` 為空值，不影響以 `site_id` 彙總的操作
- 原始檔案含 UTF-8 BOM（`\ufeff`），讀取時需指定 `encoding="utf-8-sig"` 或使用 `encoding_errors="ignore"`

---

*最後更新：2026-04-01*
