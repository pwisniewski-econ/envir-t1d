# ---- Import Librarie ----
import pandas as pd

def post_convert(df: pd.DataFrame, level: str) -> pd.DataFrame:
    """
    For each postal code, find the level (arr or BV) with the highest total population.
    Returns a DataFrame with columns ['codepost', level].
    """
    aggregated = df.groupby(['codepost', level], as_index=False)['pop_tot'].sum()
    mask = aggregated.groupby('codepost')['pop_tot'].transform('max') == aggregated['pop_tot']
    result = (
        aggregated.loc[mask, ['codepost', level]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    return result


# Load passage table
TABLE_PASSAGE_DF = (
    pd.read_feather("results_building/t_passage.feather")
    .loc[:, ['code_insee24', 'arr24', 'bv2022']]
    .drop_duplicates()
)

# Load commune population data
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

# Load arrondissement-to-commune mapping and enrich with passage info
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

# Load official postal codes
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

TABLE_PASSAGE_DF = pd.concat([TABLE_PASSAGE_DF, ARR2COM], ignore_index=True)
TABLE_PASSAGE_DF = TABLE_PASSAGE_DF.merge(COM_POP, on='code_insee24', how='left')

# Merge postal codes with geographic and population info
POSTAL_CODES = (
    POSTAL_CODES
    .merge(TABLE_PASSAGE_DF, on='code_insee24', how='left')
    .dropna()
)

# Build the postal converter
postal_converter_arr = post_convert(POSTAL_CODES, 'arr24')
postal_converter_bv = post_convert(POSTAL_CODES, 'bv2022')
POSTAL_CONVERTER = postal_converter_arr.merge(
    postal_converter_bv,
    on='codepost',
    how='left'
)

# Export
POSTAL_CONVERTER.to_feather(
    "results_building/postal_converter.feather",
    compression="zstd"
)