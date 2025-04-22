import pandas as pd
import pyarrow as pa
import pyarrow.feather as feather
import numpy as np

def rename_join(df, join_df, new_col, join_by, rename_from, default_col=None):
    default_col = list(join_by.keys())[0]
    join_df = join_df.rename(columns={rename_from: new_col})
    
    merged_df = pd.merge(df, join_df, how='left', left_on=list(join_by.keys()), right_on=list(join_by.values()))
    merged_df[new_col] = merged_df[new_col].fillna(merged_df[default_col])
    
    return merged_df.drop('code_arr', axis=1)

# Read data
TABLE_PASSAGE_DF = feather.read_feather("results_building/t_passage.feather")
TABLE_PASSAGE_DF = TABLE_PASSAGE_DF[['code_insee24', 'arr24', 'bv2022']].drop_duplicates()

ARR2COM = feather.read_feather("results_building/arr2com.feather")

MOVEMENTS = pd.read_csv("data/external/insee-mouvements/base-flux-mobilite-residentielle-2021.csv", sep=";", low_memory=False)

# Apply join operations
MOVEMENTS = rename_join(MOVEMENTS, ARR2COM, 'com_to', {'CODGEO': 'code_arr'}, 'code_com')
MOVEMENTS = rename_join(MOVEMENTS, ARR2COM, 'com_from', {'DCRAN': 'code_arr'}, 'code_com')

def sum_movements(DATA: pd.DataFrame, level: str):
    """
    Replicates the R sum_movements function:
      - level: either "arr24" or "bv2022"
      - returns (incoming_df, flow_df)
    """
    # join to get arr_to
    join_to = (
        TABLE_PASSAGE_DF
        .rename(columns={level: "arr_to"})
        .loc[:, ["code_insee24", "arr_to"]]
    )
    df_lvl = DATA.merge(
        join_to,
        how="left",
        left_on="com_to",
        right_on="code_insee24"
    )

    # join to get arr_from
    join_from = (
        TABLE_PASSAGE_DF
        .rename(columns={level: "arr_from"})
        .loc[:, ["code_insee24", "arr_from"]]
    )
    df_lvl = df_lvl.merge(
        join_from,
        how="left",
        left_on="com_from",
        right_on="code_insee24"
    )

    # filter out within‑area moves
    df_lvl = df_lvl.dropna(subset=["arr_from", "arr_to"])
    df_lvl = df_lvl[df_lvl["arr_from"] != df_lvl["arr_to"]]

    incoming = (
        df_lvl
        .groupby("arr_to", as_index=False)["NBFLUX_C21_POP01P"]
        .sum()
        .round(2)
        .rename(columns={"NBFLUX_C21_POP01P": "incoming_pop21"})
    )

    flow = (
        df_lvl
        .groupby(["arr_to", "arr_from"], as_index=False)["NBFLUX_C21_POP01P"]
        .sum()
        .round(2)
        .rename(columns={"NBFLUX_C21_POP01P": "flow_pop21"})
    )

    flow["share_to"] = flow["flow_pop21"] / flow.groupby("arr_to")["flow_pop21"].transform("sum")

    return incoming, flow

# --- Arrondissement level ---
INCOMING_ARR, FLOW_ARR = sum_movements(MOVEMENTS, "arr24")

INCOMING_ARR.to_feather(
    "data/interim/arrondissements/incoming_pop.feather",
    compression="zstd"
)
FLOW_ARR.to_feather(
    "results_building/arrondissement-flow.feather",
    compression="zstd"
)

# Bassin de vie level ---
INCOMING_BV22, FLOW_BV22 = sum_movements(MOVEMENTS, "bv2022")

INCOMING_BV22 = INCOMING_BV22.rename(columns={"arr_to": "bv22_to"})
FLOW_BV22 = FLOW_BV22.rename(columns={
    "arr_to": "bv22_to",
    "arr_from": "bv22_from"
})

INCOMING_BV22.to_feather(
    "data/interim/bv2022/incoming_pop.feather",
    compression="zstd"
)
FLOW_BV22.to_feather(
    "results_building/bv2022-flow.feather",
    compression="zstd"
)