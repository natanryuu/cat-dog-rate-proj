import pandas as pd
from statsmodels.stats.outliers_influence import variance_inflation_factor
from statsmodels.tools.tools import add_constant

df = pd.read_csv('merged_panel.csv')
ivs = ['IV_highrise', 'IV3_single', 'IV_elder', 'log_IV4_rental', 'IV_edu']
X = add_constant(df[ivs])

vif_table = pd.DataFrame({
    'Variable': ivs,
    'VIF': [variance_inflation_factor(X.values, i + 1) for i in range(len(ivs))]
})
print(vif_table.to_string(index=False))
