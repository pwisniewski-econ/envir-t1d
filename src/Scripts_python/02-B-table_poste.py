import pandas as pd
import pyarrow.feather as feather
from pathlib import Path

def post_convert(data, level):
    grouped = data.groupby(['codepost', level], as_index=False).agg({'pop_tot': 'sum'})
    max_rows = grouped.loc[grouped.groupby('codepost')['pop_tot'].idxmax()]
    return max_rows[['codepost', level]].drop_duplicates()

# Lecture fichiers
table_passage_df = feather.read_feather("results_building/t_passage.feather")[
    ['code_insee24', 'arr24', 'bv2022']
].drop_duplicates()

com_pop = pd.read_csv("data/external/insee-population/base-cc-evol-struct-pop-2021.csv.gz", compression='gzip')[
    ['CODGEO', 'P21_POP']
].rename(columns={"CODGEO": "code_insee24", "P21_POP": "pop_tot"})

arr2com = feather.read_feather("results_building/arr2com.feather").merge(
    table_passage_df,
    left_on='code_com',
    right_on='code_insee24',
    how='left'
)[['code_arr', 'arr24', 'bv2022']].rename(columns={"code_arr": "code_insee24"})

postal_codes = pd.read_csv("data/external/poste-codes/base-officielle-codes-postaux_2024.csv")[
    ['code_postal', 'code_commune_insee']
].drop_duplicates().rename(columns={
    'code_postal': 'codepost',
    'code_commune_insee': 'code_insee24'
})

# Jointures
table_passage_df = pd.concat([table_passage_df, arr2com], ignore_index=True)
table_passage_df = table_passage_df.merge(com_pop, on='code_insee24', how='left')

postal_codes = postal_codes.merge(table_passage_df, on='code_insee24', how='left').dropna()

postal_converter = post_convert(postal_codes, 'arr24').merge(
    post_convert(postal_codes, 'bv2022'),
    on='codepost',
    how='left'
)

# Export .feather
Path("results_building").mkdir(parents=True, exist_ok=True)
feather.write_feather(postal_converter, "results_building/postal_converter.feather", compression='zstd')