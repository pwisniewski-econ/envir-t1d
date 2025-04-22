# Import ----------
import pandas as pd
import pyarrow.feather as feather

# Read the passage table
TABLE_PASSAGE_DF = (
    pd.read_feather("results_building/t_passage.feather")[[
        "code_insee24", "arr24", "bv2022"
    ]]
    .drop_duplicates()
)

# Diplomas data reading
DIPLOMES = pd.read_csv(
    "data/external/insee-diplomes/DS_RP_DIPLOMES_data.csv",
    sep=";", 
    low_memory=False
)

DIPLOMES.columns = DIPLOMES.columns.str.lower()

# Select columns
DIP1 = (
    DIPLOMES
    .loc[(DIPLOMES.geo_object == "COM") & (DIPLOMES.time_period == 2021),
         ["geo", "obs_value", "sex", "educ"]]
    .rename(columns={"geo": "code_insee24"})
)

# Map educ codes
def map_educ(code):
    if code.startswith("001T"): return "dip_001T"
    if code == "100_RP":      return "dip_100R"
    if code == "200_RP":      return "dip_200R"
    if code == "300_RP":      return "dip_300R"
    if code == "350T351_RP":  return "dip_350R"
    if code in ("500_RP", "500T702_RP"): return "dip_500R"
    if code in ("600_RP", "600T702_RP"): return "dip_600R"
    if code == "700_RP":      return "dip_700R"
    if code == "_T":          return "total"
    return code

DIP1["educ"] = DIP1["educ"].apply(map_educ)

# Function to aggregate and compute proportions/delta
def agreg_educ(df):
    # Pivot to wide format
    df_wide = (
        df.sort_values("educ")
          .pivot(index=["code_insee24", "sex"], columns="educ", values="obs_value")
          .reset_index()
    )
    # Identify diploma columns
    dip_cols = [c for c in df_wide.columns if c.startswith("dip_")]
    for c in dip_cols:
        df_wide[c] = df_wide[c].fillna(0) / df_wide["total"]

    # sums of higher diplomas M & F
    M = df_wide.query("sex == 'M'").copy()
    M["dip_sup_F"] = M["dip_500R"] + M["dip_600R"] + M["dip_700R"]
    M = M[["code_insee24", "dip_sup_F"]]

    F = df_wide.query("sex == 'F'").copy()
    F["dip_sup_M"] = F["dip_500R"] + F["dip_600R"] + F["dip_700R"]
    F = F[["code_insee24", "dip_sup_M"]]

    # Compute delta_educ
    C = pd.merge(M, F, on="code_insee24", how="left")
    C["delta_educ"] = C["dip_sup_M"] - C["dip_sup_F"]
    C = C[["code_insee24", "delta_educ"]]

    # Merge
    total = df_wide.query("sex == '_T'")
    result = pd.merge(total, C, on="code_insee24", how="left")
    cols = ["code_insee24"] + dip_cols + ["delta_educ"]
    return result[cols]

# Aggregate by arrondissement
DIP1_ARR = (
    pd.merge(DIP1, TABLE_PASSAGE_DF, on="code_insee24", how="left")
      .groupby(["arr24", "sex", "educ"], as_index=False)["obs_value"].sum()
      .rename(columns={"arr24": "code_insee24"})
)
DIP1_ARR = agreg_educ(DIP1_ARR)
DIP1_ARR = DIP1_ARR.rename(columns={"code_insee24": "arr24"})

DIP1_ARR = DIP1_ARR[DIP1_ARR["arr24"].str.len() < 4]

# Aggregate by BV2022
DIP1_BV22 = (
    pd.merge(DIP1, TABLE_PASSAGE_DF, on="code_insee24", how="left")
      .groupby(["bv2022", "sex", "educ"], as_index=False)["obs_value"].sum()
      .rename(columns={"bv2022": "code_insee24"})
)
DIP1_BV22 = agreg_educ(DIP1_BV22)
DIP1_BV22 = DIP1_BV22.rename(columns={"code_insee24": "bv2022"})

# Export
DIP1_ARR.to_feather(
    "data/interim/arrondissements/education.feather",
    compression="zstd"
)
DIP1_BV22.to_feather(
    "data/interim/bv2022/education.feather",
    compression="zstd"
)
