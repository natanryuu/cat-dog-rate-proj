"""Compare FE specifications on merged_panel.csv (N=132)
  A: raw DV, two-way FE (district + year)
  B: log DV, two-way FE
  C: raw DV, district FE only
  D: log DV, district FE only
"""
import numpy as np
import pandas as pd
import statsmodels.api as sm
from scipy import stats

df = pd.read_csv('merged_panel.csv')
ivs = ['IV_highrise', 'IV3_single', 'IV_elder', 'log_IV4_rental', 'IV_edu']
dist_d = pd.get_dummies(df['district'], prefix='d', drop_first=True, dtype=float)
year_d = pd.get_dummies(df['year'],     prefix='y', drop_first=True, dtype=float)

specs = {
    'A: raw + 2way FE':  (df['cat_dog_ratio'],         pd.concat([df[ivs], dist_d, year_d], axis=1)),
    'B: log + 2way FE':  (np.log(df['cat_dog_ratio']), pd.concat([df[ivs], dist_d, year_d], axis=1)),
    'C: raw + dist FE':  (df['cat_dog_ratio'],         pd.concat([df[ivs], dist_d],         axis=1)),
    'D: log + dist FE':  (np.log(df['cat_dog_ratio']), pd.concat([df[ivs], dist_d],         axis=1)),
}

rows, coefs = [], {}
for name, (y, Xraw) in specs.items():
    X = sm.add_constant(Xraw)
    m = sm.OLS(y, X).fit(cov_type='HC1')
    r = m.resid
    rows.append({
        'spec':     name,
        'R2':       m.rsquared,
        'adjR2':    m.rsquared_adj,
        'res_skew': r.skew(),
        'res_kurt': r.kurtosis(),
        'SW_p':     stats.shapiro(r).pvalue,
        'KS_p':     stats.kstest(r, 'norm', args=(r.mean(), r.std())).pvalue,
        'DW':       sm.stats.durbin_watson(r),
    })
    coefs[name] = {iv: (m.params[iv], m.pvalues[iv]) for iv in ivs}

summary = pd.DataFrame(rows)
print(summary.to_string(index=False, float_format=lambda v: f'{v:+.4f}'))

print('\n' + '='*78)
print('IV 係數 (coef | p-value) 跨 4 個 spec')
print('='*78)
header = f'{"IV":<17}' + ''.join(f'{k:>16}' for k in specs.keys())
print(header)
print('-'*78)
for iv in ivs:
    line = f'{iv:<17}'
    for name in specs.keys():
        c, p = coefs[name][iv]
        sig = '***' if p<.01 else '**' if p<.05 else '*' if p<.1 else ''
        line += f'{c:+6.3f}|p={p:.3f}{sig:<3}'[:16].rjust(16)
    print(line)

print('\n*** p<.01, ** p<.05, * p<.1')
