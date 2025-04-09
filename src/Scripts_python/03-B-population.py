import pandas as pd
import pyarrow.feather as feather
from pathlib import Path

#  Tables de passage
table_passage_df = feather.read_feather("results_building/t_passage.feather")[
    ['code_insee24', 'arr24', 'bv2022']
].drop_duplicates()

def pop_sum(data, level):
    df = data.rename(columns={"CODGEO": "code_insee24"})
    df = df.merge(table_passage_df, on="code_insee24", how="left")
    df = df[df[level].notna()]
    
    grouped = df.groupby(level, as_index=False).agg({
        'P21_POP0014': 'sum',
        'P21_POP1529': 'sum',
        'P21_POP3044': 'sum',
        'P21_POP4559': 'sum',
        'P21_POP6074': 'sum',
        'P21_POP7589': 'sum',
        'P21_POP90P': 'sum'
    })

    grouped['pop_0044'] = grouped['P21_POP0014'] + grouped['P21_POP1529'] + grouped['P21_POP3044']
    grouped['pop_tot'] = (
        grouped['pop_0044'] + grouped['P21_POP4559'] + grouped['P21_POP6074'] +
        grouped['P21_POP7589'] + grouped['P21_POP90P']
    )

    return grouped.rename(columns={
        'P21_POP0014': 'pop_0014',
        'P21_POP1529': 'pop_1529',
        'P21_POP3044': 'pop_3044',
        'P21_POP4559': 'pop_4559',
        'P21_POP6074': 'pop_6074',
        'P21_POP7589': 'pop_7589',
        'P21_POP90P': 'pop_90p'
    })

# Population INSEE
pop_data = pd.read_csv("data/external/insee-population/base-cc-evol-struct-pop-2021.csv.gz", compression="gzip")

# Agrégations
pop_arr21 = pop_sum(pop_data, 'arr24')
pop_bv21 = pop_sum(pop_data, 'bv2022')


Path("data/interim/arrondissements").mkdir(parents=True, exist_ok=True)
Path("data/interim/bv2022").mkdir(parents=True, exist_ok=True)

feather.write_feather(pop_arr21, "data/interim/arrondissements/populations_2021.feather", compression="zstd")
feather.write_feather(pop_bv21, "data/interim/bv2022/populations_2021.feather", compression="zstd")
