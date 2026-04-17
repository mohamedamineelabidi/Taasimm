import json
nb = json.loads(open('notebooks/03_zone_mapping.ipynb', encoding='utf-8').read())
for i, c in enumerate(nb['cells'][48:51], start=49):
    src = ''.join(c['source'])
    print(f'=== Cell {i} [{c["cell_type"]}] ===')
    print(src[:400])
    print()
