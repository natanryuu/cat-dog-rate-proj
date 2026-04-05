"""
residual_diagnostics.py — FE 模型殘差常態性診斷
================================================
1. 對 IV4_rental 做 log 轉換（skew=1.56 → 右偏修正）
2. 跑 FE OLS（district + year dummies）
3. 計算殘差並做完整常態性檢定
4. 如果殘差不符常態 → 自動嘗試 DV 轉換後重跑
5. 輸出診斷圖表

Input:  master_panel_final.csv
Output: residual_diagnostics.png + 終端報告
"""

import pandas as pd
import numpy as np
from scipy import stats
import statsmodels.api as sm
import matplotlib.pyplot as plt
import matplotlib
import warnings
warnings.filterwarnings("ignore")

for font in ["Arial Unicode MS", "Microsoft JhengHei", "Noto Sans CJK TC", "SimHei"]:
    try:
        matplotlib.rcParams["font.family"] = font
        break
    except:
        continue
matplotlib.rcParams["axes.unicode_minus"] = False


# ── Load ──
import os
# 自動偵測路徑：本地 or 雲端
for path in ["master_panel_final.csv", "data/master_panel_final.csv", "/mnt/user-data/outputs/master_panel_final.csv"]:
    if os.path.exists(path):
        break
df = pd.read_csv(path, encoding="utf-8-sig")
print("═" * 70)
print("🔬 FE 模型殘差常態性診斷")
print("═" * 70)

# ── Step 1: IV4_rental log transform ──
df["log_IV4_rental"] = np.log(df["IV4_rental"])
print(f"\n  Step 1: IV4_rental → log 轉換")
print(f"    skew: {df['IV4_rental'].skew():.3f} → {df['log_IV4_rental'].skew():.3f}")


# ── Step 2: Define models to compare ──
MODELS = {
    "Model A: DV=原始, IV4=log": {
        "dv": "cat_dog_ratio",
        "ivs": ["IV_highrise", "IV3_single_ratio", "IV_elder", "log_IV4_rental", "IV_edu"],
    },
    "Model B: DV=log, IV4=log": {
        "dv_transform": "log",
        "ivs": ["IV_highrise", "IV3_single_ratio", "IV_elder", "log_IV4_rental", "IV_edu"],
    },
}

fig, axes = plt.subplots(2, 4, figsize=(20, 10))
fig.suptitle("殘差常態性診斷", fontsize=16, fontweight="bold", y=0.98)

model_results = {}

for model_idx, (model_name, spec) in enumerate(MODELS.items()):
    print(f"\n{'─'*70}")
    print(f"  {model_name}")
    print(f"{'─'*70}")

    # Prepare DV
    if "dv_transform" in spec and spec["dv_transform"] == "log":
        y_col = "log_cat_dog_ratio"
        df[y_col] = np.log(df["cat_dog_ratio"])
        dv_label = "log(cat_dog_ratio)"
    else:
        y_col = "cat_dog_ratio"
        dv_label = "cat_dog_ratio"

    ivs = spec["ivs"]

    # FE: district + year dummies
    dummies_dist = pd.get_dummies(df["district"], prefix="d", drop_first=True, dtype=float)
    dummies_year = pd.get_dummies(df["year"], prefix="y", drop_first=True, dtype=float)

    X = pd.concat([df[ivs], dummies_dist, dummies_year], axis=1)
    X = sm.add_constant(X)
    y = df[y_col]

    # OLS
    model = sm.OLS(y, X).fit(cov_type="HC1")
    residuals = model.resid
    fitted = model.fittedvalues

    # ── Residual diagnostics ──
    sw_stat, sw_p = stats.shapiro(residuals)
    jb_stat, jb_p = stats.jarque_bera(residuals)
    ks_stat, ks_p = stats.kstest(residuals, 'norm', args=(residuals.mean(), residuals.std()))
    dw = sm.stats.durbin_watson(residuals)
    skew = residuals.skew()
    kurt = residuals.kurtosis()

    print(f"\n  OLS R² = {model.rsquared:.4f}, Adj R² = {model.rsquared_adj:.4f}")
    print(f"\n  殘差統計:")
    print(f"    Mean:     {residuals.mean():.6f} (should be ~0)")
    print(f"    Std:      {residuals.std():.6f}")
    print(f"    Skewness: {skew:.4f} {'✅' if abs(skew) < 0.5 else '⚠️' if abs(skew) < 1 else '❌'}")
    print(f"    Kurtosis: {kurt:.4f} {'✅' if abs(kurt) < 1 else '⚠️' if abs(kurt) < 3 else '❌'}")

    print(f"\n  常態性檢定:")
    print(f"    Shapiro-Wilk:  W={sw_stat:.4f}, p={sw_p:.4f} {'✅ p>0.05' if sw_p > 0.05 else '❌ p≤0.05'}")
    print(f"    Jarque-Bera:   JB={jb_stat:.4f}, p={jb_p:.4f} {'✅ p>0.05' if jb_p > 0.05 else '❌ p≤0.05'}")
    print(f"    Kolmogorov-S:  KS={ks_stat:.4f}, p={ks_p:.4f} {'✅ p>0.05' if ks_p > 0.05 else '❌ p≤0.05'}")
    print(f"\n  自相關:")
    print(f"    Durbin-Watson: {dw:.4f} {'✅ ~2' if 1.5 < dw < 2.5 else '⚠️ 可能自相關'}")

    # ── IV coefficients ──
    print(f"\n  IV 係數 (Robust SE):")
    print(f"    {'Variable':25s} {'Coef':>10s} {'SE':>10s} {'t':>8s} {'p':>8s}")
    print(f"    {'─'*65}")
    for iv in ivs:
        coef = model.params[iv]
        se = model.bse[iv]
        t = model.tvalues[iv]
        p = model.pvalues[iv]
        sig = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.1 else ""
        print(f"    {iv:25s} {coef:10.4f} {se:10.4f} {t:8.2f} {p:8.4f} {sig}")

    model_results[model_name] = {
        "r2": model.rsquared, "adj_r2": model.rsquared_adj,
        "sw_p": sw_p, "jb_p": jb_p, "ks_p": ks_p,
        "skew": skew, "kurt": kurt, "dw": dw,
        "residuals": residuals, "fitted": fitted,
    }

    # ── Plots ──
    row = model_idx

    # 1. Histogram + KDE
    ax = axes[row, 0]
    ax.hist(residuals, bins=20, density=True, alpha=0.7, color="#3b82f6", edgecolor="white", linewidth=0.5)
    xmin, xmax = residuals.min(), residuals.max()
    x_range = np.linspace(xmin - 0.1, xmax + 0.1, 200)
    ax.plot(x_range, stats.norm.pdf(x_range, residuals.mean(), residuals.std()),
            color="#ef4444", linewidth=2, label="Normal fit")
    ax.set_title(f"Histogram\n{model_name.split(':')[0]}", fontsize=10)
    ax.set_xlabel("Residual", fontsize=9)
    ax.legend(fontsize=8)

    # 2. Q-Q plot
    ax = axes[row, 1]
    (osm, osr), (slope, intercept, r_val) = stats.probplot(residuals, dist="norm")
    ax.scatter(osm, osr, s=15, alpha=0.7, color="#3b82f6", edgecolors="none")
    ax.plot(osm, slope * np.array(osm) + intercept, color="#ef4444", linewidth=2)
    ax.set_title(f"Q-Q Plot\nR²={r_val**2:.4f}", fontsize=10)
    ax.set_xlabel("Theoretical Quantiles", fontsize=9)
    ax.set_ylabel("Sample Quantiles", fontsize=9)

    # 3. Residuals vs Fitted
    ax = axes[row, 2]
    ax.scatter(fitted, residuals, s=15, alpha=0.6, color="#3b82f6", edgecolors="none")
    ax.axhline(y=0, color="#ef4444", linewidth=1, linestyle="--")
    # LOWESS smoothing
    try:
        lowess = sm.nonparametric.lowess(residuals, fitted, frac=0.5)
        ax.plot(lowess[:, 0], lowess[:, 1], color="#10b981", linewidth=2, label="LOWESS")
        ax.legend(fontsize=8)
    except:
        pass
    ax.set_title("Residuals vs Fitted\n(heteroscedasticity check)", fontsize=10)
    ax.set_xlabel("Fitted Values", fontsize=9)
    ax.set_ylabel("Residuals", fontsize=9)

    # 4. Scale-Location (√|residuals| vs fitted)
    ax = axes[row, 3]
    std_resid = np.sqrt(np.abs(residuals / residuals.std()))
    ax.scatter(fitted, std_resid, s=15, alpha=0.6, color="#f59e0b", edgecolors="none")
    try:
        lowess2 = sm.nonparametric.lowess(std_resid, fitted, frac=0.5)
        ax.plot(lowess2[:, 0], lowess2[:, 1], color="#ef4444", linewidth=2)
    except:
        pass
    ax.set_title("Scale-Location\n(variance stability)", fontsize=10)
    ax.set_xlabel("Fitted Values", fontsize=9)
    ax.set_ylabel("√|Standardized Residual|", fontsize=9)

plt.tight_layout(rect=[0, 0, 1, 0.95])
out_path = "residual_diagnostics.png"
plt.savefig(out_path, dpi=150, bbox_inches="tight")
plt.close()
print(f"\n💾 圖表: {out_path}")


# ── Final recommendation ──
print(f"\n{'═'*70}")
print(f"📝 FINAL RECOMMENDATION")
print(f"{'═'*70}")

a = model_results["Model A: DV=原始, IV4=log"]
b = model_results["Model B: DV=log, IV4=log"]

print(f"\n  {'':30s} {'Model A (DV原始)':>18s} {'Model B (DV=log)':>18s}")
print(f"  {'─'*68}")
print(f"  {'R²':30s} {a['r2']:18.4f} {b['r2']:18.4f}")
print(f"  {'Adj R²':30s} {a['adj_r2']:18.4f} {b['adj_r2']:18.4f}")
print(f"  {'Residual Skewness':30s} {a['skew']:18.4f} {b['skew']:18.4f}")
print(f"  {'Residual Kurtosis':30s} {a['kurt']:18.4f} {b['kurt']:18.4f}")
print(f"  {'Shapiro-Wilk p':30s} {a['sw_p']:18.4f} {b['sw_p']:18.4f}")
print(f"  {'Jarque-Bera p':30s} {a['jb_p']:18.4f} {b['jb_p']:18.4f}")
print(f"  {'Durbin-Watson':30s} {a['dw']:18.4f} {b['dw']:18.4f}")

better = "A" if a['sw_p'] > b['sw_p'] else "B"
print(f"\n  → 殘差常態性較佳: Model {better}")

if a['sw_p'] > 0.05:
    print(f"  → Model A 殘差已通過常態檢定 (SW-p={a['sw_p']:.4f} > 0.05)")
    print(f"    建議: 主模型用 Model A（DV 不轉換、IV4 用 log）")
elif b['sw_p'] > 0.05:
    print(f"  → Model B 殘差通過常態檢定 (SW-p={b['sw_p']:.4f} > 0.05)")
    print(f"    建議: 主模型用 Model B（DV 和 IV4 都取 log）")
else:
    print(f"  → 兩個模型的殘差都沒通過嚴格的常態檢定")
    print(f"    但 N=120 時 OLS 估計量仍具一致性（CLT）")
    better_model = "A" if abs(a['skew']) < abs(b['skew']) else "B"
    print(f"    建議: 用 Model {better_model}（skew 較小），")
    print(f"    報告裡寫:「殘差輕微偏態但 N=120 足以依賴 CLT，")
    print(f"    且使用 robust SE (HC1) 修正異質變異數」")

# ── Update master panel ──
df_out = df.copy()
df_out["log_IV4_rental"] = np.log(df_out["IV4_rental"])
df_out.to_csv("master_panel_final.csv", index=False, encoding="utf-8-sig")
print(f"\n💾 已更新 master_panel_final.csv（加入 log_IV4_rental 欄位）")

