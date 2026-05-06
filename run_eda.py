"""Regenerate EDA figures from merged_panel.csv (N=132, 2015-2025, 5 IVs)"""
import os
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib import font_manager

for f in ['PingFang TC', 'Heiti TC', 'Arial Unicode MS', 'Microsoft JhengHei']:
    if any(f in ff.name for ff in font_manager.fontManager.ttflist):
        plt.rcParams['font.family'] = f
        break
plt.rcParams['axes.unicode_minus'] = False
plt.rcParams['figure.dpi'] = 150
plt.rcParams['savefig.dpi'] = 150
plt.rcParams['savefig.bbox'] = 'tight'

OUT = 'EDA圖表/files'
os.makedirs(OUT, exist_ok=True)

df = pd.read_csv('merged_panel.csv')
N = len(df)
IVS = ['IV_highrise', 'IV3_single', 'IV_elder', 'log_IV4_rental', 'IV_edu']
LBL = {'cat_dog_ratio': 'DV: Cat/Dog Ratio',
       'IV_highrise': 'IV1: Highrise Ratio',
       'IV3_single': 'IV2: Single-HH Ratio',
       'IV_elder': 'IV3: Elder 65+ Ratio',
       'log_IV4_rental': 'IV4: log(Rental Count)',
       'IV_edu': 'IV5: Higher-Edu Ratio'}
DIST_EN = {'中山區':'Zhongshan','中正區':'Zhongzheng','信義區':'Xinyi',
           '內湖區':'Neihu','北投區':'Beitou','南港區':'Nangang',
           '士林區':'Shilin','大同區':'Datong','大安區':'Daan',
           '文山區':'Wenshan','松山區':'Songshan','萬華區':'Wanhua'}
df['dist_en'] = df['district'].map(DIST_EN)

ACCENT, BLUE, DARK, GRAY, BG = '#c0392b', '#2563eb', '#1a1a2e', '#6b7280', '#f8f7f4'
COLORS5 = [BLUE, '#16a34a', '#7c3aed', '#d97706', ACCENT]

# ============ Fig 1: DV time series ============
fig, ax = plt.subplots(figsize=(13, 6))
fig.patch.set_facecolor(BG); ax.set_facecolor('white')
highlight = {'Datong': ACCENT, 'Daan': BLUE, 'Beitou': '#16a34a'}
for d in sorted(df['district'].unique()):
    sub = df[df['district'] == d].sort_values('year')
    en = DIST_EN[d]
    c = highlight.get(en, GRAY)
    lw = 2.2 if en in highlight else 1.0
    a = 1.0 if en in highlight else 0.45
    ax.plot(sub['year'], sub['cat_dog_ratio'], color=c, lw=lw, alpha=a,
            marker='o', markersize=3.5 if en in highlight else 2)
    if en in highlight:
        last = sub.iloc[-1]
        ax.annotate(en, xy=(last['year'], last['cat_dog_ratio']),
                    xytext=(2025.2, last['cat_dog_ratio']),
                    fontsize=8.5, color=c, fontweight='bold')
ax.axhline(1.0, color=DARK, lw=1, ls='--', alpha=0.4)
ax.text(2015.1, 1.03, 'Cats = Dogs', fontsize=8, color=DARK, alpha=0.6)
ax.axvline(2021, color=ACCENT, lw=0.8, ls=':', alpha=0.5)
ax.text(2021.1, 0.55, 'All 12 districts >1.0', fontsize=8, color=ACCENT)
ax.set_xlim(2014.5, 2026)
ax.set_xticks(range(2015, 2026))
ax.set_xlabel('Year'); ax.set_ylabel('Cat/Dog Registration Ratio')
ax.set_title(f'Fig 1 — Cat/Dog Ratio Trend by District 2015–2025 (N={N})\n'
             'All 12 districts crossed 1.0 by 2021; 2025 min=1.90',
             fontsize=11.5, fontweight='bold', color=DARK)
ax.tick_params(labelsize=8.5, colors=GRAY)
ax.spines[['top','right']].set_visible(False)
ax.legend(handles=[
    Line2D([0],[0], color=ACCENT, lw=2, label='Datong (fastest rise)'),
    Line2D([0],[0], color=BLUE,   lw=2, label='Daan'),
    Line2D([0],[0], color='#16a34a', lw=2, label='Beitou (lowest)'),
    Line2D([0],[0], color=GRAY, lw=1.0, alpha=0.5, label='Other 9'),
], loc='upper left', fontsize=8.5, framealpha=0.9)
plt.tight_layout()
plt.savefig(f'{OUT}/eda_1_timeseries.png'); plt.close()
print('saved eda_1_timeseries.png')

# ============ Fig 2: distributions (univariate) ============
vars6 = ['cat_dog_ratio'] + IVS
fig, axes = plt.subplots(2, 3, figsize=(14, 8))
fig.patch.set_facecolor(BG)
fig.suptitle(f'Fig 2 — Univariate Distributions (N={N})',
             fontsize=13, fontweight='bold', color=DARK, y=1.01)
cols = [DARK] + COLORS5
for i, (v, c) in enumerate(zip(vars6, cols)):
    ax = axes[i // 3][i % 3]; ax.set_facecolor('white')
    vals = df[v]
    ax.hist(vals, bins=18, color=c, alpha=0.75, edgecolor='white', lw=0.6)
    ax.axvline(vals.mean(), color=DARK, lw=1.5, ls='--', label=f'Mean={vals.mean():.3f}')
    ax.axvline(vals.median(), color=c, lw=1.5, ls=':', label=f'Med={vals.median():.3f}')
    ax.set_title(LBL[v], fontsize=10, fontweight='bold', color=DARK)
    ax.tick_params(labelsize=7.5, colors=GRAY)
    ax.spines[['top','right']].set_visible(False)
    ax.legend(fontsize=7.5, framealpha=0)
    ax.text(0.97, 0.95, f'skew={vals.skew():.2f}', transform=ax.transAxes,
            ha='right', va='top', fontsize=7.5, color=GRAY)
plt.tight_layout()
plt.savefig(f'{OUT}/eda_2_distributions.png'); plt.close()
print('saved eda_2_distributions.png')

# ============ Fig 3: scatter IV vs DV ============
fig, axes = plt.subplots(2, 3, figsize=(14, 8))
fig.patch.set_facecolor(BG)
fig.suptitle(f'Fig 3 — Bivariate: IVs vs Cat/Dog Ratio (Pooled, N={N})',
             fontsize=12.5, fontweight='bold', color=DARK, y=1.01)
for idx, (iv, c) in enumerate(zip(IVS, COLORS5)):
    ax = axes[idx // 3][idx % 3]; ax.set_facecolor('white')
    x, y = df[iv], df['cat_dog_ratio']
    ax.scatter(x, y, color=c, alpha=0.5, s=30, edgecolors='none')
    m, b = np.polyfit(x, y, 1)
    xl = np.linspace(x.min(), x.max(), 100)
    ax.plot(xl, m * xl + b, color=DARK, lw=1.8)
    r = np.corrcoef(x, y)[0, 1]
    ax.text(0.05, 0.93, f'r = {r:+.3f}', transform=ax.transAxes,
            fontsize=10, fontweight='bold', color=DARK,
            bbox=dict(boxstyle='round,pad=0.3', facecolor='white', alpha=0.85))
    ax.set_xlabel(LBL[iv], fontsize=9.5, color=DARK)
    ax.set_ylabel('Cat/Dog Ratio', fontsize=9, color=DARK)
    ax.tick_params(labelsize=8, colors=GRAY)
    ax.spines[['top','right']].set_visible(False)
axes[1][2].set_visible(False)
plt.tight_layout()
plt.savefig(f'{OUT}/eda_3_scatter_iv_vs_dv.png'); plt.close()
print('saved eda_3_scatter_iv_vs_dv.png')

# ============ Fig 4: correlation heatmap (pooled vs within) ============
cols_heat = ['cat_dog_ratio'] + IVS
labs = ['DV', 'highrise', 'single', 'elder', 'rental', 'edu']
pooled = df[cols_heat].corr()
within = df[cols_heat + ['district']].copy()
for c in cols_heat:
    within[c] = within[c] - within.groupby('district')[c].transform('mean')
within_corr = within[cols_heat].corr()

fig, axes = plt.subplots(1, 2, figsize=(14, 6))
fig.patch.set_facecolor(BG)
fig.suptitle(f'Fig 4 — Correlation: Pooled vs Within-District FE (N={N})',
             fontsize=12.5, fontweight='bold', color=DARK)
for ax, mat, ttl in [(axes[0], pooled, 'Pooled'), (axes[1], within_corr, 'Within (FE demeaned)')]:
    ax.set_facecolor('white')
    im = ax.imshow(mat.values, cmap='RdBu_r', vmin=-1, vmax=1, aspect='auto')
    ax.set_xticks(range(len(labs))); ax.set_xticklabels(labs, fontsize=9)
    ax.set_yticks(range(len(labs))); ax.set_yticklabels(labs, fontsize=9)
    for i in range(len(labs)):
        for j in range(len(labs)):
            v = mat.values[i, j]
            fc = 'white' if abs(v) > 0.6 else DARK
            fw = 'bold' if abs(v) > 0.8 else 'normal'
            ax.text(j, i, f'{v:.2f}', ha='center', va='center',
                    fontsize=9, color=fc, fontweight=fw)
    ax.set_title(ttl, fontsize=11, fontweight='bold', color=DARK, pad=8)
    plt.colorbar(im, ax=ax, shrink=0.85)
plt.tight_layout()
plt.savefig(f'{OUT}/eda_4_correlation_heatmap.png'); plt.close()
print('saved eda_4_correlation_heatmap.png')

# ============ Fig 5: radar profiles (12 districts) ============
dist_means = df.groupby('district')[IVS + ['cat_dog_ratio']].mean()
dist_z = (dist_means - dist_means.mean()) / dist_means.std(ddof=0)
angles = np.linspace(0, 2 * np.pi, len(IVS), endpoint=False).tolist()
angles += angles[:1]

fig, axes = plt.subplots(3, 4, figsize=(15, 11), subplot_kw={'projection': 'polar'})
fig.patch.set_facecolor(BG)
fig.suptitle(f'Fig 5 — District Socio-Profile (z-score of 5 IVs), N={N}',
             fontsize=13, fontweight='bold', color=DARK, y=1.00)
for idx, d in enumerate(sorted(df['district'].unique())):
    ax = axes[idx // 4][idx % 4]
    vals = dist_z.loc[d, IVS].tolist() + [dist_z.loc[d, IVS].iloc[0]]
    ratio_z = dist_z.loc[d, 'cat_dog_ratio']
    c = ACCENT if ratio_z > 0 else BLUE
    ax.plot(angles, vals, color=c, lw=1.8)
    ax.fill(angles, vals, color=c, alpha=0.25)
    ax.set_xticks(angles[:-1])
    ax.set_xticklabels(['high', 'single', 'elder', 'rent', 'edu'], fontsize=7)
    ax.set_yticks([-2, 0, 2]); ax.set_yticklabels(['-2', '0', '+2'], fontsize=6.5, color=GRAY)
    ax.set_ylim(-2.5, 2.5)
    title = f'{DIST_EN[d]}\nratio z={ratio_z:+.2f}'
    ax.set_title(title, fontsize=9, fontweight='bold', color=c, pad=10)
plt.tight_layout()
plt.savefig(f'{OUT}/eda_5_radar_profiles.png'); plt.close()
print('saved eda_5_radar_profiles.png')

# ============ Fig 6: IV_elder paradox ============
fig, axes = plt.subplots(1, 2, figsize=(14, 5.5))
fig.patch.set_facecolor(BG)
raw_r = np.corrcoef(df['IV_elder'], df['cat_dog_ratio'])[0, 1]
dfw = df.copy()
dfw['elder_w'] = df['IV_elder'] - df.groupby('district')['IV_elder'].transform('mean')
dfw['ratio_w'] = df['cat_dog_ratio'] - df.groupby('district')['cat_dog_ratio'].transform('mean')
win_r = np.corrcoef(dfw['elder_w'], dfw['ratio_w'])[0, 1]

# 左：pooled scatter colored by district
ax = axes[0]; ax.set_facecolor('white')
for d in sorted(df['district'].unique()):
    sub = df[df['district'] == d]
    ax.scatter(sub['IV_elder'], sub['cat_dog_ratio'], alpha=0.7, s=35, label=DIST_EN[d])
m, b = np.polyfit(df['IV_elder'], df['cat_dog_ratio'], 1)
xl = np.linspace(df['IV_elder'].min(), df['IV_elder'].max(), 100)
ax.plot(xl, m * xl + b, color=DARK, lw=2)
ax.text(0.05, 0.93, f'Pooled r = {raw_r:+.3f}\n(hypothesis: negative)',
        transform=ax.transAxes, fontsize=10, fontweight='bold', color=ACCENT,
        bbox=dict(boxstyle='round,pad=0.4', facecolor='white'))
ax.set_xlabel('IV3: Elder 65+ Ratio'); ax.set_ylabel('Cat/Dog Ratio')
ax.set_title('(a) Pooled: direction contradicts H3', fontsize=11, fontweight='bold', color=DARK)
ax.legend(fontsize=6.5, ncol=2, loc='lower right', framealpha=0.85)
ax.spines[['top','right']].set_visible(False)
ax.tick_params(labelsize=8, colors=GRAY)

# 右：within-district scatter
ax2 = axes[1]; ax2.set_facecolor('white')
ax2.scatter(dfw['elder_w'], dfw['ratio_w'], color=BLUE, alpha=0.55, s=30)
m2, b2 = np.polyfit(dfw['elder_w'], dfw['ratio_w'], 1)
xl2 = np.linspace(dfw['elder_w'].min(), dfw['elder_w'].max(), 100)
ax2.plot(xl2, m2 * xl2 + b2, color=DARK, lw=2)
ax2.axhline(0, color=GRAY, lw=0.7, ls='--', alpha=0.5)
ax2.axvline(0, color=GRAY, lw=0.7, ls='--', alpha=0.5)
ax2.text(0.05, 0.93, f'Within r = {win_r:+.3f}\n(same direction, amplified)',
         transform=ax2.transAxes, fontsize=10, fontweight='bold', color=BLUE,
         bbox=dict(boxstyle='round,pad=0.4', facecolor='white'))
ax2.set_xlabel('IV_elder (demeaned)'); ax2.set_ylabel('Cat/Dog Ratio (demeaned)')
ax2.set_title('(b) Within-District (FE demeaned)', fontsize=11, fontweight='bold', color=DARK)
ax2.spines[['top','right']].set_visible(False)
ax2.tick_params(labelsize=8, colors=GRAY)

fig.suptitle(f'Fig 6 — IV_elder Paradox: pooled r={raw_r:+.2f}  →  within r={win_r:+.2f}   '
             f'H3 (elder→dog) rejected (N={N})',
             fontsize=12, fontweight='bold', color=DARK, y=1.01)
plt.tight_layout()
plt.savefig(f'{OUT}/eda_6_elder_paradox.png'); plt.close()
print('saved eda_6_elder_paradox.png')

print('\nall 6 figures regenerated with N=132')
