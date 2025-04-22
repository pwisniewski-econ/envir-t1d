#-----Import Libraries
import pandas as pd
import numpy as np

# Read and process TABLE_PASSAGE_DF
table_passage_df = (
    pd.read_feather("results_building/t_passage.feather")
      .loc[:, ["code_insee22", "arr24", "bv2022"]]
      .drop_duplicates(subset=["code_insee22"], keep="first")
)

# join ARR2COM to TABLE_PASSAGE_DF
arr2com = (
    pd.read_feather("results_building/arr2com.feather")
      .merge(
          table_passage_df,
          left_on="code_com",
          right_on="code_insee22",
          how="left"
      )
      .loc[:, ["code_arr", "arr24", "bv2022"]]
      .rename(columns={"code_arr": "code_insee22"})
)

# Append ARR2COM rows
table_passage_df = pd.concat([table_passage_df, arr2com], ignore_index=True)

# RPLS Excel
rpls = pd.read_excel(
    "data/external/medd-logements_sociaux/resultats_rpls_2022.xlsx",
    sheet_name="Commune",
    header=None,
    skiprows=4,
    engine="openpyxl"
)

# Columns selection
rpls = rpls.iloc[:, [2, 9, 14, 15, 65, 124]]
rpls.columns = [
    "code_insee22",
    "n_social",
    "social_houses",
    "social_appartments",
    "social_average_age",
    "mean_rent_m2",
]

# Join with TABLE_PASSAGE_DF
rpls = (
    table_passage_df
      .merge(rpls, on="code_insee22", how="left")
)
rpls[["n_social", "social_houses", "social_appartments"]] = (
    rpls[["n_social", "social_houses", "social_appartments"]]
      .fillna(0)
)

# Define aggregation function
def social_sum(df: pd.DataFrame, level: str) -> pd.DataFrame:
    # weighted sums
    df2 = df.copy()
    df2["age_weighted"] = df2["social_average_age"] * df2["n_social"]
    df2["rent_weighted"] = df2["mean_rent_m2"] * df2["n_social"]

    grouped = df2.groupby(level).agg(
        age_sum       = ("age_weighted",     "sum"),
        rent_sum      = ("rent_weighted",    "sum"),
        n_social      = ("n_social",         "sum"),
        social_houses = ("social_houses",    "sum"),
        social_appartments = ("social_appartments", "sum")
    ).reset_index()

    # compute averages
    grouped["social_average_age"] = grouped["age_sum"]  / grouped["n_social"]
    grouped["mean_rent_m2"]       = grouped["rent_sum"] / grouped["n_social"]

    return grouped[[level, "social_average_age", "mean_rent_m2", "n_social", "social_houses", "social_appartments"]]


rpls_arr = social_sum(rpls, "arr24")
rpls_arr = rpls_arr[rpls_arr["arr24"].astype(str).str.len() < 4]


rpls_bv22 = social_sum(rpls, "bv2022")

# Export
rpls_arr.to_feather(
    "data/interim/arrondissements/social_housing.feather",
    compression="zstd"
)
rpls_bv22.to_feather(
    "data/interim/bv2022/social_housing.feather",
    compression="zstd"
)
