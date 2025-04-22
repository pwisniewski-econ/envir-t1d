#!/usr/bin/env python3
"""
Python translation of R script 02-B-table_poste.R
Requires: pandas, pyarrow
"""
import pandas as pd

def post_convert(df: pd.DataFrame, level: str) -> pd.DataFrame:
    """
    For each postal code, find the level (e.g., arrondissement or BV) with the highest total population.
    Returns a DataFrame with columns ['codepost', level].
    """
    # 1. Sum populations by codepost and the chosen level
    aggregated = df.groupby(['codepost', level], as_index=False)['pop_tot'].sum()
    # 2. Within each codepost, keep only rows where pop_tot equals the maximum
    mask = aggregated.groupby('codepost')['pop_tot'].transform('max') == aggregated['pop_tot']
    result = (
        aggregated.loc[mask, ['codepost', level]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    return result


# 1. Load and dedupe the passage table
TABLE_PASSAGE_DF = (
    pd.read_feather("results_building/t_passage.feather")
    .loc[:, ['code_insee24', 'arr24', 'bv2022']]
    .drop_duplicates()
)

# 2. Load commune population data
COM_POP = (
    pd.read_csv(
        "data/external/insee-population/base-cc-evol-struct-pop-2021.csv.gz",
        compression="gzip",
        sep=";",
        dtype = {'CODGEO': str},
        low_memory=False,
        usecols=["CODGEO", "P21_POP"]
    )
    .rename(columns={
        'CODGEO': 'code_insee24',
        'P21_POP': 'pop_tot'
    })
)

# 3. Load arrondissement-to-commune mapping and enrich with passage info
ARR2COM = (
    pd.read_feather("results_building/arr2com.feather")
    .merge(
        TABLE_PASSAGE_DF,
        left_on="code_com",
        right_on="code_insee24",
        how="left"
    )
    .loc[:, ['code_arr', 'arr24', 'bv2022']]
    .rename(columns={'code_arr': 'code_insee24'})
    .drop_duplicates()
)

# 4. Load official postal codes
POSTAL_CODES = (
    pd.read_csv(
        "data/external/poste-codes/base-officielle-codes-postaux_2024.csv",
        dtype=str
    )
    .loc[:, ['code_postal', 'code_commune_insee']]
    .rename(columns={
        'code_postal': 'codepost',
        'code_commune_insee': 'code_insee24'
    })
    .drop_duplicates()
)

# 5. Combine passage data
TABLE_PASSAGE_DF = pd.concat([TABLE_PASSAGE_DF, ARR2COM], ignore_index=True)
TABLE_PASSAGE_DF = TABLE_PASSAGE_DF.merge(COM_POP, on='code_insee24', how='left')

# 6. Merge postal codes with geographic and population info
POSTAL_CODES = (
    POSTAL_CODES
    .merge(TABLE_PASSAGE_DF, on='code_insee24', how='left')
    .dropna()
)

# 7. Build the postal converter
postal_converter_arr = post_convert(POSTAL_CODES, 'arr24')
postal_converter_bv = post_convert(POSTAL_CODES, 'bv2022')
POSTAL_CONVERTER = postal_converter_arr.merge(
    postal_converter_bv,
    on='codepost',
    how='left'
)

# 8. Write out the result with zstd compression
POSTAL_CONVERTER.to_feather(
    "results_building/postal_converter.feather",
    compression="zstd"
)