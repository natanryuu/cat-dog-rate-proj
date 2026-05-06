import numpy as np
import pandas as pd

DATA = 'data'

pet = pd.read_csv(f'{DATA}/petgov_panel_包含戶數.csv')[
    ['year', 'district', 'cat_count', 'dog_count', 'cat_dog_ratio', 'house_count']
]

house = pd.read_csv(f'{DATA}/taipei_housing_type_panel.csv')
house['IV_highrise'] = house['住宅大樓'] + house['華廈']
house = house[['year', 'district', 'IV_highrise']]

single = pd.read_csv(f'{DATA}/single_household_panel.csv')[
    ['year', 'district', 'ratio_single']
].rename(columns={'ratio_single': 'IV3_single'})

age = pd.read_csv(f'{DATA}/age_district_panel.csv').rename(
    columns={'ad_year': 'year', 'ratio_65_plus': 'IV_elder'}
)[['year', 'district', 'IV_elder', 'total_pop']]

rental = pd.read_csv(f'{DATA}/rental_count_panel.csv')[
    ['year', 'district', 'rental_count']
].rename(columns={'rental_count': 'IV4_rental'})

edu = pd.read_csv(f'{DATA}/edu_ratio_panel.csv')[
    ['year', 'district', 'edu_ratio']
].rename(columns={'edu_ratio': 'IV_edu'})

df = pet
for other, name in [(house, 'house'), (single, 'single'),
                    (age, 'age'), (rental, 'rental'), (edu, 'edu')]:
    before = len(df)
    df = df.merge(other, on=['year', 'district'], how='inner')
    print(f'after merge {name}: {before} -> {len(df)}')

df['log_IV4_rental'] = np.log(df['IV4_rental'])

dup = df.duplicated(subset=['year', 'district']).sum()
print(f'\nduplicated (year,district) keys: {dup}')
print(f'year × district cells expected: {df["year"].nunique() * df["district"].nunique()}')
print(f'actual rows: {len(df)}')
print(f'\nNA per column:')
print(df.isna().sum().to_string())

df = df.sort_values(['district', 'year']).reset_index(drop=True)
out = 'merged_panel.csv'
df.to_csv(out, index=False)
print(f'\nsaved: {out}  shape={df.shape}')
print(df.head(3).to_string())
