"""Compare FE OLS: raw DV vs log(DV) on merged_panel.csv (N=132)"""
import numpy as np
import pandas as pd
import statsmodels.api as sm
from scipy import stats

df = pd.read_csv('merged_panel.csv')
ivs = ['IV_highrise', 'IV3_single', 'IV_elder', 'log_IV4_rental', 'IV_edu']

dist_d = pd.get_dummies(df['district'], prefix='d', drop_first=True, dtype=float)
year_d = pd.get_dummies(df['year'],     prefix='y', drop_first=True, dtype=float)
X = sm.add_constant(pd.concat([df[ivs], dist_d, year_d], axis=1))


def run(y, label):
    m = sm.OLS(y, X).fit(cov_type='HC1')
    r = m.resid
    return {
        'label': label,
        'R2': m.rsquared,
        'adjR2': m.rsquared_adj,
        'res_skew': r.skew(),
        'res_kurt': r.kurtosis(),
        'SW_p': stats.shapiro(r).pvalue,
        'JB_p': stats.jarque_bera(r).pvalue,
        'KS_p': stats.kstest(r, 'norm', args=(r.mean(), r.std())).pvalue,
        'DW': sm.stats.durbin_watson(r),
        'model': m,
        'resid': r,
    }


A = run(df['cat_dog_ratio'],         'A: raw DV')
B = run(np.log(df['cat_dog_ratio']), 'B: log(DV)')

print(f'\n{"指標":<22}{A["label"]:>18}{B["label"]:>18}')
print('-' * 58)
for k, fmt in [('R2','.4f'),('adjR2','.4f'),('res_skew','+.4f'),
               ('res_kurt','+.4f'),('SW_p','.4f'),('JB_p','.4f'),
               ('KS_p','.4f'),('DW','.4f')]:
    print(f'{k:<22}{format(A[k], fmt):>18}{format(B[k], fmt):>18}')

print('\n--- Model A 係數（IV） ---')
for iv in ivs:
    c = A['model'].params[iv]; p = A['model'].pvalues[iv]
    print(f'  {iv:<18} coef={c:+.4f}  p={p:.4f}')
print('\n--- Model B 係數（IV），解讀為半彈性 ---')
for iv in ivs:
    c = B['model'].params[iv]; p = B['model'].pvalues[iv]
    pct = (np.exp(c) - 1) * 100
    print(f'  {iv:<18} coef={c:+.4f}  → +1 單位 → ratio × {np.exp(c):.3f} (≈{pct:+.1f}%)  p={p:.4f}')
