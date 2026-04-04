"""
各行政區單身男女差異分析
來源：data_raw/odrp019_taipei_104_114.csv（里級）
輸出：
  - data_raw/iv3_single_gender_panel.csv（區年級 Panel）
  - EDA圖表/fig7_single_gender.png（男女差異圖）
"""
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore')

plt.rcParams.update({
    'font.family': ['DejaVu Sans', 'sans-serif'],
    'axes.unicode_minus': False,
    'figure.dpi': 150,
    'savefig.dpi': 150,
    'savefig.bbox': 'tight',
})

DISTRICTS_EN = {
    '中山區':'Zhongshan', '中正區':'Zhongzheng', '信義區':'Xinyi',
    '內湖區':'Neihu',     '北投區':'Beitou',     '南港區':'Nangang',
    '士林區':'Shilin',    '大同區':'Datong',      '大安區':'Daan',
    '文山區':'Wenshan',   '松山區':'Songshan',    '萬華區':'Wanhua'
}

ACCENT = '#c0392b'
BLUE   = '#2563eb'
GRAY   = '#6b7280'
DARK   = '#1a1a2e'
SOFT_BG = '#f8f7f4'
PINK   = '#e11d48'
TEAL   = '#0891b2'

# ── 載入里級原始資料 ─────────────────────────────────────
raw = pd.read_csv("data_raw/odrp019_taipei_104_114.csv")

# ── 彙總至區-年級 ────────────────────────────────────────
agg = (raw.groupby(['year_ad', 'site_id'])
       .agg(single_m=('household_single_m', 'sum'),
            single_f=('household_single_f', 'sum'),
            single_total=('household_single_total', 'sum'))
       .reset_index())

# 行政區名稱統一為 "XX區"
agg['district'] = agg['site_id'].str.replace('臺北市', '', regex=False)
agg = agg.rename(columns={'year_ad': 'year'})

# 衍生指標
agg['diff_f_m'] = agg['single_f'] - agg['single_m']          # 女性多出人數
agg['share_f']  = agg['single_f'] / agg['single_total']      # 女性佔比

# 研究期間 2015–2024
panel = (agg[agg['year'].between(2015, 2024)]
         [['year', 'district', 'single_m', 'single_f', 'single_total',
           'diff_f_m', 'share_f']]
         .sort_values(['year', 'district'])
         .reset_index(drop=True))

panel.to_csv("data_raw/iv3_single_gender_panel.csv", index=False)
print(f"Saved: data_raw/iv3_single_gender_panel.csv  ({len(panel)} rows)")

# ── 基礎統計 ─────────────────────────────────────────────
print("\n=== 各區單身男女差異摘要 (2015-2024 平均) ===")
summary = (panel.groupby('district')
           .agg(avg_m=('single_m', 'mean'),
                avg_f=('single_f', 'mean'),
                avg_diff=('diff_f_m', 'mean'),
                avg_share_f=('share_f', 'mean'))
           .sort_values('avg_share_f', ascending=False))
summary['dist_en'] = summary.index.map(DISTRICTS_EN)
for _, row in summary.iterrows():
    print(f"  {row['dist_en']:<12} M={row['avg_m']:>7.0f}  F={row['avg_f']:>7.0f}  "
          f"F-M={row['avg_diff']:>+7.0f}  F%={row['avg_share_f']:.1%}")


# ════════════════════════════════════════════════════════════
# Fig 7 — 各行政區單身男女差異（三子圖）
# ════════════════════════════════════════════════════════════
fig, axes = plt.subplots(1, 3, figsize=(18, 6))
fig.patch.set_facecolor(SOFT_BG)
fig.suptitle('Fig 7 — Single-Person Household: Male vs Female by District',
             fontsize=13, fontweight='bold', color=DARK, y=1.02)

# ── (a) 2024 男女人數對比 (grouped bar) ──────────────────
ax = axes[0]
ax.set_facecolor('white')
latest = panel[panel['year'] == 2024].sort_values('single_total', ascending=True)
y_pos = np.arange(len(latest))
bar_h = 0.35

ax.barh(y_pos - bar_h/2, latest['single_m'], bar_h,
        color=TEAL, alpha=0.8, label='Male')
ax.barh(y_pos + bar_h/2, latest['single_f'], bar_h,
        color=PINK, alpha=0.8, label='Female')

ax.set_yticks(y_pos)
ax.set_yticklabels([DISTRICTS_EN[d] for d in latest['district']], fontsize=8.5)
ax.set_xlabel('Single-Person HH Population', fontsize=9, color=DARK)
ax.set_title('(a) Male vs Female Count (2024)', fontsize=10, fontweight='bold', color=DARK)
ax.legend(fontsize=8.5, loc='lower right', framealpha=0.85)
ax.tick_params(colors=GRAY, labelsize=8)
ax.spines[['top', 'right']].set_visible(False)

# ── (b) 女性佔比時間趨勢 ────────────────────────────────
ax = axes[1]
ax.set_facecolor('white')

highlight = {'Zhongshan': ACCENT, 'Wanhua': PINK, 'Daan': '#16a34a'}
for dist in sorted(panel['district'].unique()):
    sub = panel[panel['district'] == dist].sort_values('year')
    en = DISTRICTS_EN[dist]
    c  = highlight.get(en, GRAY)
    lw = 2.0 if en in highlight else 0.8
    alp = 1.0 if en in highlight else 0.35
    ax.plot(sub['year'], sub['share_f'], color=c, lw=lw, alpha=alp,
            marker='o', markersize=3 if en in highlight else 1.5)
    if en in highlight:
        last = sub.iloc[-1]
        ax.annotate(en, xy=(last['year'], last['share_f']),
                    xytext=(2024.2, last['share_f']),
                    fontsize=7.5, color=c, fontweight='bold')

ax.axhline(0.5, color=DARK, lw=1, ls='--', alpha=0.4)
ax.text(2015.1, 0.501, 'Parity (50%)', fontsize=7, color=DARK, alpha=0.6)
ax.set_xlim(2014.5, 2025.5)
ax.set_xticks(range(2015, 2025))
ax.set_xlabel('Year', fontsize=9, color=DARK)
ax.set_ylabel('Female Share of Single-Person HH', fontsize=9, color=DARK)
ax.set_title('(b) Female Share Trend by District', fontsize=10, fontweight='bold', color=DARK)
ax.tick_params(colors=GRAY, labelsize=8)
ax.spines[['top', 'right']].set_visible(False)

# ── (c) 女性多出人數 (F-M) 箱型圖 ──────────────────────
ax = axes[2]
ax.set_facecolor('white')

order = (panel.groupby('district')['diff_f_m'].mean()
              .sort_values(ascending=False).index.tolist())
data_box = [panel[panel['district'] == d]['diff_f_m'].values for d in order]
bp = ax.boxplot(data_box, patch_artist=True,
                medianprops=dict(color=DARK, lw=2),
                whiskerprops=dict(color=GRAY), capprops=dict(color=GRAY),
                flierprops=dict(marker='o', color=ACCENT, ms=4, alpha=0.6))
for patch in bp['boxes']:
    patch.set_facecolor(PINK)
    patch.set_alpha(0.25)

ax.axhline(0, color=DARK, lw=1, ls='--', alpha=0.4)
ax.set_xticklabels([DISTRICTS_EN[d] for d in order], rotation=35, ha='right', fontsize=8.5)
ax.set_ylabel('Female Excess (F - M)', fontsize=9, color=DARK)
ax.set_title('(c) Female Excess Distribution by District',
             fontsize=10, fontweight='bold', color=DARK)
ax.tick_params(colors=GRAY, labelsize=8)
ax.spines[['top', 'right']].set_visible(False)

plt.tight_layout()
plt.savefig('EDA圖表/fig7_single_gender.png')
plt.close()
print("\nSaved: EDA圖表/fig7_single_gender.png")
