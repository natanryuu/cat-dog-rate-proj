import csv
from collections import defaultdict

hh = defaultdict(lambda: defaultdict(int))
with open('data_raw/odrp019_taipei_104_114.csv', encoding='utf-8') as f:
    for r in csv.DictReader(f):
        y = int(r['year_ad'])
        d = r['site_id'].replace('臺北市','')
        hh[y][d] += int(r['household_ordinary_total'] or 0) + int(r['household_single_total'] or 0)

pet = defaultdict(lambda: defaultdict(lambda: [0,0]))
with open('data_raw/pet/pet_registration_panel.csv', encoding='utf-8-sig') as f:
    for r in csv.DictReader(f):
        y = int(r['ad_year'])
        i = 0 if r['species'] == '狗' else 1
        pet[y][r['district']][i] = int(r['registered_count'])

ds = ['中山區','中正區','信義區','內湖區','北投區','南港區','士林區','大同區','大安區','文山區','松山區','萬華區']

print('=== yearly trend ===')
print('YEAR HH DOG CAT PET DOG/1KHH CAT/1KHH TOT/1KHH')
for y in range(2015, 2025):
    ht = sum(hh[y].get(d,0) for d in ds)
    dg = sum(pet[y][d][0] for d in ds)
    ct = sum(pet[y][d][1] for d in ds)
    pt = dg + ct
    print(f'{y} {ht} {dg} {ct} {pt} {dg/ht*1000:.2f} {ct/ht*1000:.2f} {pt/ht*1000:.2f}')

print()
print('=== district 2015 vs 2024 ===')
print('DIST HH15 HH24 HH_CHG PET15 PET24 PET_CHG')
for d in ds:
    h15 = hh[2015].get(d,0)
    h24 = hh[2024].get(d,0)
    p15 = pet[2015][d][0]+pet[2015][d][1]
    p24 = pet[2024][d][0]+pet[2024][d][1]
    hc = (h24-h15)/h15*100
    pc = (p24-p15)/p15*100
    print(f'{d} {h15} {h24} {hc:+.1f}% {p15} {p24} {pc:+.1f}%')
