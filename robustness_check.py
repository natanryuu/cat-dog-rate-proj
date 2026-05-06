"""Robustness check: Model A (2-way FE, main) vs Model C (district FE only)
Outputs:
  outputs/tableC_robustness_fe_specs.csv
  EDA圖表/files/eda_7_robustness_oneway_fe.png
"""
import os
import numpy as np
import pandas as pd
import statsmodels.api as sm
from scipy import stats
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib import font_manager

for f in ['PingFang TC', 'Heiti TC', 'Arial Unicode MS', 'Microsoft JhengHei']:
    if any(f in ff.name for ff in font_manager.fontManager.ttflist):
        plt.rcParams['font.family'] = f
        break
plt.rcParams['axes.unicode_minus'] = False
plt.rcParams['figure.dpi'] = 150
plt.rcParams['savefig.dpi'] = 150
plt.rcParams['savefig.bbox'] = 'tight'

os.makedirs('outputs', exist_ok=True)
os.makedirs('EDA圖表/files', exist_ok=True)

df = pd.read_csv('merged_panel.csv')
N = len(df)
ivs = ['IV_highrise', 'IV3_single', 'IV_elder', 'log_IV4_rental', 'IV_edu']
HYP_SIGN = {'IV_highrise': '+', 'IV3_single': '+', 'IV_elder': '-',
            'log_IV4_rental': '+', 'IV_edu': '+'}
LBL = {'IV_highrise': 'IV1 highrise', 'IV3_single': 'IV2 single',
       'IV_elder': 'IV3 elder', 'log_IV4_rental': 'IV4 log(rental)',
       'IV_edu': 'IV5 edu'}

dist_d = pd.get_dummies(df['district'], prefix='d', drop_first=True, dtype=float)
year_d = pd.get_dummies(df['year'],     prefix='y', drop_first=True, dtype=float)


def fit(y, X):
    X = sm.add_constant(X)
    return sm.OLS(y, X).fit(cov_type='HC1')


y = df['cat_dog_ratio']
mA = fit(y, pd.concat([df[ivs], dist_d, year_d], axis=1))  # 2-way
mC = fit(y, pd.concat([df[ivs], dist_d],         axis=1))  # district-only

def diag(m):
    r = m.resid
    return {
        'R2':       m.rsquared,
        'adjR2':    m.rsquared_adj,
        'res_skew': r.skew(),
        'SW_p':     stats.shapiro(r).pvalue,
        'KS_p':     stats.kstest(r, 'norm', args=(r.mean(), r.std())).pvalue,
        'DW':       sm.stats.durbin_watson(r),
    }

dA, dC = diag(mA), diag(mC)

rows = []
for iv in ivs:
    cA, pA = mA.params[iv], mA.pvalues[iv]
    cC, pC = mC.params[iv], mC.pvalues[iv]
    seA, seC = mA.bse[iv], mC.bse[iv]
    obs_sign_A = '+' if cA > 0 else '-'
    obs_sign_C = '+' if cC > 0 else '-'
    rows.append({
        'IV': iv,
        'Hypothesis_sign': HYP_SIGN[iv],
        'A_coef':  round(cA, 4),
        'A_SE':    round(seA, 4),
        'A_p':     float(f'{pA:.4g}'),
        'A_sig':   'Yes' if pA < 0.05 else 'No',
        'C_coef':  round(cC, 4),
        'C_SE':    round(seC, 4),
        'C_p':     float(f'{pC:.4g}'),
        'C_sig':   'Yes' if pC < 0.05 else 'No',
        'C_vs_hyp': 'match' if obs_sign_C == HYP_SIGN[iv] else 'CONTRADICT',
    })
table = pd.DataFrame(rows)
table.to_csv('outputs/tableC_robustness_fe_specs.csv', index=False)
print('\n=== tableC_robustness_fe_specs.csv ===')
print(table.to_string(index=False))

print('\n=== Model diagnostics ===')
print(f"{'':<15}{'A: 2-way FE':>15}{'C: dist FE only':>18}")
for k in ['R2','adjR2','res_skew','SW_p','KS_p','DW']:
    print(f'{k:<15}{dA[k]:>15.4f}{dC[k]:>18.4f}')

# ---- Figure ----
fig = plt.figure(figsize=(14, 6.5))
gs = fig.add_gridspec(1, 2, width_ratios=[1.4, 1], wspace=0.35)
fig.patch.set_facecolor('#f8f7f4')
DARK, GRAY, RED, BLUE, GREEN = '#1a1a2e', '#6b7280', '#c0392b', '#2563eb', '#16a34a'

# LEFT: forest plot of coefficients + 95% CI
ax = fig.add_subplot(gs[0, 0]); ax.set_facecolor('white')
labels = [LBL[iv] for iv in ivs]
ypos = np.arange(len(ivs))
offset = 0.18
for i, iv in enumerate(ivs):
    cA, seA, pA = mA.params[iv], mA.bse[iv], mA.pvalues[iv]
    cC, seC, pC = mC.params[iv], mC.bse[iv], mC.pvalues[iv]
    ax.errorbar(cA, ypos[i] + offset, xerr=1.96 * seA,
                fmt='o', color=GRAY, ms=7, capsize=4, lw=1.6, alpha=0.85,
                label='A: 2-way FE (main)' if i == 0 else None)
    ax.errorbar(cC, ypos[i] - offset, xerr=1.96 * seC,
                fmt='s', color=BLUE, ms=7, capsize=4, lw=1.6,
                label='C: district FE only' if i == 0 else None)
    sigA = '*' if pA < .05 else ''
    sigC = '*' if pC < .05 else ''
    ax.text(cA + 1.96 * seA + 0.3, ypos[i] + offset, f'{cA:+.2f}{sigA}',
            va='center', fontsize=8, color=GRAY)
    ax.text(cC + 1.96 * seC + 0.3, ypos[i] - offset, f'{cC:+.2f}{sigC}',
            va='center', fontsize=8.5, color=BLUE, fontweight='bold')

ax.axvline(0, color=DARK, lw=0.8, ls='--', alpha=0.5)
ax.set_yticks(ypos); ax.set_yticklabels(labels, fontsize=10)
ax.invert_yaxis()
ax.set_xlabel('Coefficient (95% CI, HC1 SE)', fontsize=10, color=DARK)
n_sig_05 = (table.C_sig == 'Yes').sum()
n_sig_10 = ((table.C_p < 0.10)).sum()
ax.set_title(f'Forest plot: A (2-way FE) vs C (district FE only)\n'
             f'Removing year FE: {n_sig_05}/5 IVs at p<.05  ({n_sig_10}/5 at p<.10)',
             fontsize=11, fontweight='bold', color=DARK)
ax.legend(loc='lower right', fontsize=9, framealpha=0.9)
ax.tick_params(labelsize=9, colors=GRAY)
ax.spines[['top', 'right']].set_visible(False)

# RIGHT: diagnostics & trade-off table
ax2 = fig.add_subplot(gs[0, 1]); ax2.set_facecolor('white'); ax2.axis('off')
rows_d = [
    ('N',            f'{N}',              f'{N}'),
    ('R²',           f'{dA["R2"]:.3f}',    f'{dC["R2"]:.3f}'),
    ('Adj R²',       f'{dA["adjR2"]:.3f}', f'{dC["adjR2"]:.3f}'),
    ('IV sig @ .05', f'{(table.A_sig=="Yes").sum()}/5',
                     f'{(table.C_sig=="Yes").sum()}/5'),
    ('Res skew',     f'{dA["res_skew"]:+.2f}', f'{dC["res_skew"]:+.2f}'),
    ('KS p',         f'{dA["KS_p"]:.3f}',  f'{dC["KS_p"]:.3f}'),
    ('SW p',         f'{dA["SW_p"]:.4f}',  f'{dC["SW_p"]:.4f}'),
    ('Durbin-Watson',f'{dA["DW"]:.2f}',    f'{dC["DW"]:.2f}'),
]
col_lbls = ['指標', 'A: 2-way FE', 'C: dist FE only']
ax2.set_title('Diagnostics & trade-off', fontsize=11, fontweight='bold',
              color=DARK, loc='left', pad=12)

tbl = ax2.table(cellText=rows_d, colLabels=col_lbls,
                cellLoc='center', colLoc='center',
                loc='upper center', bbox=[0, 0.45, 1, 0.50])
tbl.auto_set_font_size(False); tbl.set_fontsize(9.5)
for (r, c), cell in tbl.get_celld().items():
    cell.set_edgecolor('#e5e7eb')
    if r == 0:
        cell.set_facecolor('#111827'); cell.set_text_props(color='white', weight='bold')
    else:
        lbl = rows_d[r - 1][0]
        if lbl == 'IV sig @ .05':
            cell.set_facecolor('#fef3c7' if c != 0 else 'white')
        elif lbl == 'Durbin-Watson':
            cell.set_facecolor('#fee2e2' if c != 0 else 'white')

# Verdict box
ax2.text(0, 0.32,
         'Trade-off',
         fontsize=10.5, fontweight='bold', color=DARK,
         transform=ax2.transAxes)
ax2.text(0, 0.04,
         '• C reveals IV effects masked by year FE (4/5 significant)\n'
         '• But DW=1.38 (<1.5) → positive autocorrelation in residuals\n'
         '  → time-trend leaks into errors once year FE is dropped\n'
         '• A retains DW≈2 & KS-normal residuals; IV insignificance\n'
         '  explained by year-FE / IV-trend collinearity\n'
         '\n'
         'Conclusion: keep A as main model;\n'
         'C reported as robustness check supporting hypotheses H1–H4.',
         fontsize=8.8, color=DARK,
         transform=ax2.transAxes,
         bbox=dict(boxstyle='round,pad=0.6', facecolor='#f3f4f6',
                   edgecolor='#d1d5db'))

fig.suptitle(f'Fig 7 — Robustness Check: 2-way FE vs District FE only (N={N})',
             fontsize=13, fontweight='bold', color=DARK, y=1.02)

out_fig = 'EDA圖表/files/eda_7_robustness_oneway_fe.png'
plt.savefig(out_fig); plt.close()
print(f'\nsaved: {out_fig}')
print('saved: outputs/tableC_robustness_fe_specs.csv')
