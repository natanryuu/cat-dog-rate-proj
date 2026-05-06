import numpy as np
import pandas as pd
from scipy.stats import pearsonr, shapiro

df = pd.read_csv('merged_panel.csv')
print(f'rebuilding tables from merged_panel.csv  N={len(df)}')

name_map = {
    'IV3_single': 'IV3_single_ratio',
    'log_IV4_rental': 'IV4_rental_log',
}
dv = 'cat_dog_ratio'
ivs = ['IV_highrise', 'IV3_single', 'IV_elder', 'log_IV4_rental', 'IV_edu']

rows_a = []
for col in [dv] + ivs:
    s = df[col]
    sw_p = shapiro(s).pvalue
    rows_a.append({
        'Variable': name_map.get(col, col),
        'Mean': round(s.mean(), 4),
        'SD': round(s.std(ddof=1), 4),
        'Min': round(s.min(), 4),
        'Max': round(s.max(), 4),
        'Skew': round(s.skew(), 4),
        'Kurtosis': round(s.kurt(), 4),
        'Shapiro_p': float(f'{sw_p:.4g}'),
    })
tableA = pd.DataFrame(rows_a)
tableA.to_csv('outputs/tableA_descriptive_statistics.csv', index=False)
print('\n=== tableA_descriptive_statistics.csv ===')
print(tableA.to_string(index=False))

rows_b = []
for col in ivs:
    r, p = pearsonr(df[col], df[dv])
    rows_b.append({
        'IV': name_map.get(col, col),
        'r_vs_DV': round(r, 4),
        'p_value': float(f'{p:.4e}'),
        'Significant_alpha05': 'Yes' if p < 0.05 else 'No',
    })
tableB = pd.DataFrame(rows_b)
tableB.to_csv('outputs/tableB_pearson_correlation.csv', index=False)
print('\n=== tableB_pearson_correlation.csv ===')
print(tableB.to_string(index=False))
