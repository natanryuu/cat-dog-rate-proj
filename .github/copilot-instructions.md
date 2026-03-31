# Copilot 指令（專案導覽）

目的：讓 AI 開發助理能快速在本專案中開發、除錯與維護資料蒐集與面板整理腳本。

1) 大方向與架構
- 本專案為資料蒐集與面板建置流程（procedural scripts），主要用 Python 腳本依序下載、清理、合併資料後輸出 panel CSV。
- 主要模組/腳本：`01_fetch_pets.py`（寵物登記 API 抓取）、`02_fetch_household.py`（戶政資料清理）、`03_fetch_housing.py`（住宅坪數）、`04_merge_panel.py`（合併與診斷）、`build_rental_proxy.py`（實價登錄租賃 proxy）。
- 資料層次：`data/raw/`（原始下載）、`data/clean/`（清理後）、`data/raw_rental/`（實價登錄快取）。最終產物範例：`data/clean/panel_final.csv`、`rental_proxy_panel.csv`。

2) 執行流程與常用命令（最重要）
- 安裝：`pip install pandas requests openpyxl`
- 典型執行順序：
  - `python 01_fetch_pets.py`
  - `python 02_fetch_household.py`
  - `python 03_fetch_housing.py`
  - `python 04_merge_panel.py`
- 若只想產生租賃 proxy：`python build_rental_proxy.py`（會將快取存到 `data/raw_rental/`）。

3) 專案特有慣例與實務細節（請依此回答/修改程式）
- 檔案以數字開頭（`01_`、`02_`...）表示「執行順序/工作流程」。在修改腳本時保留此慣例。
- CSV 與 Excel 輸出皆使用 `encoding="utf-8-sig"`（避免 Excel 開啟亂碼）。
- 多數下載腳本使用小幅延遲（`time.sleep(0.5~0.8)`) 以避免被封鎖，維持此節奏或降低速率時需說明理由。
- 2024 年資料為上半年（S1+S2），專案採「年化」處理（在 `build_rental_proxy.py` 中可看到乘以 2 的註記），若要改處理方式需同步修改註解與輸出說明。

4) 常見 debug 點與可用函式
- `01_fetch_pets.py` 提供 `debug_api()`：用瀏覽器 DevTools 找不到 API 時請先執行此函式以檢視實際 JSON。
- 下載類腳本採用快取機制（`build_rental_proxy.py` 會把原始下載存至 `data/raw_rental/`），調試時可刪除快取以重跑。
- 若遇欄位名稱變動（不同年度 CSV 欄名不一致），腳本通常會嘗試「自動找欄位」（例如找包含 “鄉鎮市區” 的欄位），修改時沿用相同容錯策略。

5) 重要整合點與外部相依
- 主要外部資料來源：`www.pet.gov.tw`（寵物登記 API）、`plvr.land.moi.gov.tw`（實價登錄租賃下載）、戶政司/政府資料網站（戶數、人口密度）。
- 本專案 `.claude/settings.local.json` 明確允許的 WebFetch domain（例如 `www.pet.gov.tw`, `data.gov.tw`），AI 若需要實際網路存取請先確認此設定。

6) 回應與修改建議的風格
- 修改腳本時：
  - 保持原有程式風格（清晰函式分層、註解中文說明、避免一次性重構大量檔案）。
  - 在 PR/commit 訊息中註明「為何變更」（例如 API endpoint 變動、欄位名變更），並示範重現步驟（若需手動下載檔案請標明來源 URL 與放置路徑）。
- 若提出新參數或 CLI，請同時更新 `00_readme.md` 的執行範例。

7) 可舉例的修改任務（給 AI 的短任務提示）
- 「將 `01_fetch_pets.py` 的 `CANDIDATE_ENDPOINTS` 新增 X 路徑，並在 `debug_api()` 印出完整請求/回應 sample。」
- 「在 `build_rental_proxy.py` 加一個參數 `--household-csv`，若提供則使用官方資料，否則使用佔位值，並在 README 更新執行範例。」

若有遺漏或希望我把某段程式的檢查與範例加入指令檔，請指出要補強的檔案或主題。
