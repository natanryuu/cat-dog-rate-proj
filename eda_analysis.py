"""
台北市貓犬比 Panel Data — 完整 EDA 分析腳本
用法：將 master_panel.csv 放在同目錄，執行此腳本即可輸出所有圖表
"""
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')           # 無 GUI 環境用這行；Jupyter 可改為 'inline'
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
warnings.filterwarnings('ignore')

# ── 字型設定（若需要中文，改為含中文的字型名稱） ────────────
plt.rcParams.update({
    'font.family': ['DejaVu Sans', 'sans-serif'],
    'axes.unicode_minus': False,
    'figure.dpi': 150,
    'savefig.dpi': 150,
    'savefig.bbox': 'tight',
})

# ── 載入資料 ─────────────────────────────────────────────
panel = pd.read_csv("master_panel.csv")

DISTRICTS_EN = {
    '中山區':'Zhongshan', '中正區':'Zhongzheng', '信義區':'Xinyi',
    '內湖區':'Neihu',     '北投區':'Beitou',     '南港區':'Nangang',
    '士林區':'Shilin',    '大同區':'Datong',      '大安區':'Daan',
    '文山區':'Wenshan',   '松山區':'Songshan',    '萬華區':'Wanhua'
}
panel['dist_en'] = panel['district'].map(DISTRICTS_EN)

ACCENT = '#c0392b'
BLUE   = '#2563eb'
GRAY   = '#6b7280'
DARK   = '#1a1a2e'
SOFT_BG = '#f8f7f4'

VAR_LABELS = {
    'cat_dog_ratio'   : 'DV: Cat/Dog Ratio',
    'avg_housing_size': 'IV1: Avg Housing Size (ping)',
    'ratio_0_4'       : 'IV2: Infant Ratio (0-4 yr)',
    'ratio_single'    : 'IV3: Single-Person HH Ratio',
    'rental_ratio'    : 'IV4: Rental Activity Ratio',
}
VARS = list(VAR_LABELS.keys())

# ════════════════════════════════════════════════════════════
# 4.1  單變量分佈 — Fig 1
# ════════════════════════════════════════════════════════════
fig, axes = plt.subplots(2, 3, figsize=(14, 8))
fig.patch.set_facecolor(SOFT_BG)
fig.suptitle('Fig 1 — Univariate Distributions (N=120)',
             fontsize=13, fontweight='bold', color=DARK, y=1.01)
colors = [ACCENT, BLUE, '#16a34a', '#7c3aed', '#d97706']

for i, (var, col) in enumerate(zip(VARS, colors)):
    ax = axes[i // 3][i % 3]
    ax.set_facecolor('white')
    vals = panel[var]
    ax.hist(vals, bins=18, color=col, alpha=0.75, edgecolor='white', lw=0.6)
    ax.axvline(vals.mean(),   color=DARK, lw=1.5, ls='--', label=f'Mean={vals.mean():.3f}')
    ax.axvline(vals.median(), color=col,  lw=1.5, ls=':',  label=f'Median={vals.median():.3f}')
    ax.set_title(VAR_LABELS[var], fontsize=9.5, fontweight='bold', color=DARK, pad=6)
    ax.set_xlabel('Value', fontsize=8, color=GRAY)
    ax.set_ylabel('Count',  fontsize=8, color=GRAY)
    ax.tick_params(labelsize=7.5, colors=GRAY)
    ax.spines[['top', 'right']].set_visible(False)
    ax.legend(fontsize=7, framealpha=0)
    ax.text(0.97, 0.95, f'skew={vals.skew():.2f}', transform=ax.transAxes,
            ha='right', va='top', fontsize=7, color=GRAY)

axes[1][2].set_visible(False)
plt.tight_layout()
plt.savefig('fig1_univariate.png')
plt.close()
print("Saved: fig1_univariate.png")

# ════════════════════════════════════════════════════════════
# 4.2  DV 時間趨勢 — Fig 2
# ════════════════════════════════════════════════════════════
fig, ax = plt.subplots(figsize=(13, 6))
fig.patch.set_facecolor(SOFT_BG)
ax.set_facecolor('white')

highlight = {'Wanhua': ACCENT, 'Songshan': BLUE, 'Daan': '#16a34a'}

for dist in sorted(panel['district'].unique()):
    sub = panel[panel['district'] == dist].sort_values('year')
    en  = DISTRICTS_EN[dist]
    c   = highlight.get(en, GRAY)
    lw  = 2.2 if en in highlight else 1.0
    alp = 1.0 if en in highlight else 0.45
    ax.plot(sub['year'], sub['cat_dog_ratio'],
            color=c, lw=lw, alpha=alp, marker='o', markersize=3.5 if en in highlight else 2)
    if en in highlight:
        last = sub.iloc[-1]
        ax.annotate(en, xy=(last['year'], last['cat_dog_ratio']),
                    xytext=(2024.2, last['cat_dog_ratio']),
                    fontsize=8, color=c, fontweight='bold')

ax.axhline(1.0, color=DARK, lw=1, ls='--', alpha=0.4)
ax.text(2015.1, 1.02, 'Cats = Dogs (ratio=1)', fontsize=7.5, color=DARK, alpha=0.6)
ax.set_xlim(2014.5, 2025.5)
ax.set_xticks(range(2015, 2025))
ax.set_xlabel('Year', fontsize=10, color=DARK)
ax.set_ylabel('Cat / Dog Registration Ratio', fontsize=10, color=DARK)
ax.set_title('Fig 2 — Cat/Dog Ratio Trends by District 2015–2024\n'
             '(All districts rose; Wanhua highest +1.44, Daan lowest +0.87)',
             fontsize=11, fontweight='bold', color=DARK)
ax.tick_params(labelsize=8.5, colors=GRAY)
ax.spines[['top', 'right']].set_visible(False)

from matplotlib.lines import Line2D
ax.legend(handles=[
    Line2D([0],[0], color=ACCENT,     lw=2,   label='Wanhua  (+1.44)'),
    Line2D([0],[0], color=BLUE,       lw=2,   label='Songshan (+1.43)'),
    Line2D([0],[0], color='#16a34a',  lw=2,   label='Daan     (+0.87)'),
    Line2D([0],[0], color=GRAY,       lw=1.0, alpha=0.5, label='Other districts'),
], loc='upper left', fontsize=8, framealpha=0.85)

plt.tight_layout()
plt.savefig('fig2_dv_trend.png')
plt.close()
print("Saved: fig2_dv_trend.png")

# ════════════════════════════════════════════════════════════
# 4.2  DV 各區箱型圖 — Fig 3
# ════════════════════════════════════════════════════════════
fig, ax = plt.subplots(figsize=(13, 5.5))
fig.patch.set_facecolor(SOFT_BG)
ax.set_facecolor('white')

order = (panel.groupby('district')['cat_dog_ratio'].mean()
              .sort_values(ascending=False).index.tolist())
data_box = [panel[panel['district'] == d]['cat_dog_ratio'].values for d in order]
bp = ax.boxplot(data_box, patch_artist=True,
                medianprops=dict(color=DARK, lw=2),
                whiskerprops=dict(color=GRAY), capprops=dict(color=GRAY),
                flierprops=dict(marker='o', color=ACCENT, ms=4, alpha=0.6))
for patch in bp['boxes']:
    patch.set_facecolor(BLUE); patch.set_alpha(0.25)

ax.set_xticklabels([DISTRICTS_EN[d] for d in order], rotation=35, ha='right', fontsize=9)
ax.set_ylabel('Cat / Dog Ratio', fontsize=10, color=DARK)
ax.set_title('Fig 3 — Cat/Dog Ratio Distribution by District (2015–2024)\n'
             'Ordered by mean; Wanhua and Nangang lead', fontsize=11, fontweight='bold', color=DARK)
ax.axhline(1.0, color=ACCENT, lw=1, ls='--', alpha=0.5)
ax.tick_params(colors=GRAY, labelsize=8.5)
ax.spines[['top', 'right']].set_visible(False)
plt.tight_layout()
plt.savefig('fig3_dv_boxplot.png')
plt.close()
print("Saved: fig3_dv_boxplot.png")

# ════════════════════════════════════════════════════════════
# 4.3  雙變量散佈圖 — Fig 4
# ════════════════════════════════════════════════════════════
fig, axes = plt.subplots(2, 2, figsize=(12, 9))
fig.patch.set_facecolor(SOFT_BG)
fig.suptitle('Fig 4 — Bivariate Scatter: IV vs DV (Pooled, N=120)',
             fontsize=12, fontweight='bold', color=DARK, y=1.01)

iv_cfg = [
    ('avg_housing_size', 'IV1: Avg Housing Size (ping)', BLUE),
    ('ratio_0_4',        'IV2: Infant Ratio',             '#16a34a'),
    ('ratio_single',     'IV3: Single-Person HH Ratio',   '#7c3aed'),
    ('rental_ratio',     'IV4: Rental Activity Ratio',    '#d97706'),
]
for idx, (iv, lbl, col) in enumerate(iv_cfg):
    ax = axes[idx // 2][idx % 2]
    ax.set_facecolor('white')
    x, y = panel[iv], panel['cat_dog_ratio']
    ax.scatter(x, y, color=col, alpha=0.45, s=35, edgecolors='none')
    m, b = np.polyfit(x, y, 1)
    xline = np.linspace(x.min(), x.max(), 100)
    ax.plot(xline, m * xline + b, color=DARK, lw=1.8)
    r = np.corrcoef(x, y)[0, 1]
    ax.text(0.05, 0.93, f'r = {r:.3f}', transform=ax.transAxes, fontsize=9.5,
            fontweight='bold', color=DARK,
            bbox=dict(boxstyle='round,pad=0.3', facecolor='white', alpha=0.85))
    ax.set_xlabel(lbl, fontsize=9, color=DARK)
    ax.set_ylabel('Cat/Dog Ratio', fontsize=9, color=DARK)
    ax.tick_params(labelsize=8, colors=GRAY)
    ax.spines[['top', 'right']].set_visible(False)

plt.tight_layout()
plt.savefig('fig4_scatter.png')
plt.close()
print("Saved: fig4_scatter.png")

# ════════════════════════════════════════════════════════════
# 4.4  相關矩陣熱力圖（Pooled vs Within）— Fig 5
# ════════════════════════════════════════════════════════════
fig, axes = plt.subplots(1, 2, figsize=(14, 5.5))
fig.patch.set_facecolor(SOFT_BG)
fig.suptitle('Fig 5 — Correlation Heatmaps: Pooled vs Within-District (FE Demeaned)',
             fontsize=12, fontweight='bold', color=DARK)

cols5  = VARS
labs5  = ['DV\ncat_dog', 'IV1\nhousing', 'IV2\ninfant', 'IV3\nsingle', 'IV4\nrental']
panel_dm = panel.copy()
for c in cols5:
    panel_dm[c] = panel[c] - panel.groupby('district')[c].transform('mean')

for ax, df, ttl in [
    (axes[0], panel[cols5].corr(),    'Pooled (N=120)'),
    (axes[1], panel_dm[cols5].corr(), 'Within-District (FE demeaned)'),
]:
    ax.set_facecolor('white')
    arr = df.values
    im  = ax.imshow(arr, cmap='RdBu_r', vmin=-1, vmax=1, aspect='auto')
    ax.set_xticks(range(5)); ax.set_xticklabels(labs5, fontsize=8.5)
    ax.set_yticks(range(5)); ax.set_yticklabels(labs5, fontsize=8.5)
    for i in range(5):
        for j in range(5):
            v  = arr[i, j]
            fc = 'white' if abs(v) > 0.6 else DARK
            fw = 'bold'  if abs(v) > 0.8 else 'normal'
            ax.text(j, i, f'{v:.2f}', ha='center', va='center',
                    fontsize=8.5, color=fc, fontweight=fw)
    ax.set_title(ttl, fontsize=10.5, fontweight='bold', color=DARK, pad=8)
    plt.colorbar(im, ax=ax, shrink=0.85)

plt.tight_layout()
plt.savefig('fig5_heatmap.png')
plt.close()
print("Saved: fig5_heatmap.png")

# ════════════════════════════════════════════════════════════
# 4.5  IV₄ 異常值說明 — Fig 6
# ════════════════════════════════════════════════════════════
fig, axes = plt.subplots(1, 2, figsize=(13, 5))
fig.patch.set_facecolor(SOFT_BG)
fig.suptitle('Fig 6 — IV4 Rental Activity: Structural Outlier Analysis',
             fontsize=12, fontweight='bold', color=DARK)

# 左：boxplot
ax = axes[0]; ax.set_facecolor('white')
ord_r  = (panel.groupby('district')['rental_ratio'].mean()
               .sort_values(ascending=False).index.tolist())
data_r = [panel[panel['district'] == d]['rental_ratio'].values for d in ord_r]
bp2    = ax.boxplot(data_r, patch_artist=True,
                   medianprops=dict(color=DARK, lw=1.8),
                   whiskerprops=dict(color=GRAY), capprops=dict(color=GRAY),
                   flierprops=dict(marker='o', color=ACCENT, ms=5))
for patch, dist in zip(bp2['boxes'], ord_r):
    patch.set_facecolor(ACCENT if dist == '中山區' else BLUE)
    patch.set_alpha(0.3)
ax.set_xticklabels([DISTRICTS_EN[d] for d in ord_r], rotation=40, ha='right', fontsize=8)
ax.set_ylabel('Rental Activity Ratio', fontsize=9, color=DARK)
ax.set_title('Distribution by District\n(Zhongshan = structural outlier, 3× average)',
             fontsize=9.5, color=DARK)
ax.tick_params(colors=GRAY, labelsize=8)
ax.spines[['top', 'right']].set_visible(False)

# 右：趨勢比較
ax2 = axes[1]; ax2.set_facecolor('white')
avg_r  = panel.groupby('year')['rental_ratio'].mean()
zhong  = panel[panel['district'] == '中山區'].sort_values('year')
ax2.plot(avg_r.index, avg_r.values, color=GRAY, lw=2, label='12-district average',
         marker='s', ms=5)
ax2.plot(zhong['year'], zhong['rental_ratio'], color=ACCENT, lw=2.5, label='Zhongshan',
         marker='o', ms=5)
ax2.fill_between(zhong['year'], avg_r.values, zhong['rental_ratio'],
                 alpha=0.12, color=ACCENT)
ax2.set_xlabel('Year', fontsize=9, color=DARK)
ax2.set_ylabel('Rental Activity Ratio', fontsize=9, color=DARK)
ax2.set_title('Zhongshan vs District Average\n(Retained: reflects genuine commercial density)',
              fontsize=9.5, color=DARK)
ax2.legend(fontsize=8.5, framealpha=0.85)
ax2.tick_params(colors=GRAY, labelsize=8.5)
ax2.spines[['top', 'right']].set_visible(False)

plt.tight_layout()
plt.savefig('fig6_rental_outlier.png')
plt.close()
print("Saved: fig6_rental_outlier.png")

print("\n=== 4.6 初步發現摘要 ===")
print(f"DV 全區均值: 2015={panel[panel.year==2015]['cat_dog_ratio'].mean():.3f} "
      f"→ 2024={panel[panel.year==2024]['cat_dog_ratio'].mean():.3f}")
for iv in ['avg_housing_size','ratio_0_4','ratio_single','rental_ratio']:
    r = panel['cat_dog_ratio'].corr(panel[iv])
    print(f"  Pooled r(DV, {iv:<20}) = {r:+.3f}")
print("Within VIF: ratio_0_4≈49, ratio_single≈47  → 兩者 near-collinear after FE")
print("IV4 outlier: Zhongshan district (all 10 yr) > 1.5*IQR — structural, not erroneous")
