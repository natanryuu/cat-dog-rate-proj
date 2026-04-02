# IV₃：單人戶比例 Panel — 資料說明（`iv3_single_household_panel.csv`）

> 資料路徑：`data_raw/iv3_single_household_panel.csv`  
> 工具變數角色：**IV₃**（單人戶比例，用於識別獨居生活型態對寵物飼養結構的影響）  
> 覆蓋範圍：台北市 12 行政區 × 2019–2023（民國108–112年），共 60 筆  
> **注意：2024 年（民國113年）資料尚未上架，目前缺口待補值**

---

## 欄位說明

| 欄位 | 型別 | 單位 | 說明 |
|------|------|------|------|
| `year` | int | 西元年 | 統計年份（由民國年 +1911 轉換） |
| `district` | str | — | 行政區名稱（格式：`XX區`） |
| `single_total` | int | 戶 | 當年行政區**單人戶（一人獨居）總數**（由里級加總） |
| `ordinary_total` | int | 戶 | 當年行政區**普通戶總數**（含所有家庭型態） |
| `ratio_single` | float | — | `single_total / ordinary_total`（單人戶占比，研究使用的 IV₃） |

---

## 資料來源與處理邏輯

- **原始資料：** 台北市政府民政局 — 戶政統計 ODRP019（里別家庭組成）  
  → 原始路徑：`data_raw/odrp019_taipei_106_113.csv`
- **覆蓋年度：** 民國106–112年（西元2017–2023）
- **處理腳本：** `fetch_odrp019_taipei.py`

### 彙總邏輯

1. 原始資料為**里（村里）級**，欄位含 `site_id`（`臺北市XX區`）、`village`（里名）
2. 以 `year_ad`、`site_id` 分組 groupby，對 `household_single_total` 與 `household_ordinary_total` 加總
3. `site_id` 去除「臺北市」前綴，統一為「XX區」格式
4. 研究期間截取 2019–2023（민국108–112）

---

## 2024 年缺口處理建議

| 方案 | 說明 | 風險 |
|------|------|------|
| 沿用 2023 值 | 假設單人戶比例一年內變化不大 | 低估近年獨居化趨勢 |
| 線性外插 | 用 2021–2023 趨勢推估 2024 | 若趨勢有拐點則偏誤 |
| 等待 API 上架 | 民政局通常 T+1 年中釋出 | 延誤分析進度 |

> 建議在 robustness check 中同時呈現含/不含 2024 年的模型結果。

---

## 工具變數合理性說明（IV 邏輯）

### 相關性（Relevance）
單人戶比例越高 → 獨居者傾向養貓（低維護成本）而非需要陪伴外出的狗 → **貓犬比上升**。  
預期符號：`ratio_single` ↑ → `cat_dog_ratio` ↑

### 排除限制（Exclusion Restriction）
家庭戶數結構主要由人口流動、城市化程度、婚育率等長期結構因素決定，與家庭**當期**對特定品種寵物的偏好無直接因果路徑，符合外生性假設。

---

## 使用範例（Python）

```python
import pandas as pd

iv3 = pd.read_csv("data_raw/iv3_single_household_panel.csv")

# 研究期間 2019–2023（2024 待補）
iv3_study = iv3[iv3["year"].between(2019, 2023)]

# 與主資料合併
df = df.merge(iv3[["year", "district", "ratio_single"]],
              on=["year", "district"], how="left")

# 缺口檢查
print(df[df["ratio_single"].isna()][["year", "district"]])
```

---

*最後更新：2026-04-01*
