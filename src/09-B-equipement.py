# Import libraries
import pandas as pd

# Read equipment 
equip = pd.read_csv(
    "data/external/insee-equipement/DS_BPE_data.csv",
    sep=";",
    low_memory=False
)
equip.columns = equip.columns.str.lower()

# Read passage table,
table_passage_df = (
    pd.read_feather("results_building/t_passage.feather")
    [["code_insee24", "arr24", "bv2022"]]
    .drop_duplicates()
)


equip = (
    equip[equip["geo_object"] == "COM"]
    .merge(
        table_passage_df,
        how="left",
        left_on="geo",
        right_on="code_insee24"
    )
    [["geo", "arr24", "bv2022", "facility_sdom", "facility_dom", "obs_value"]]
    .rename(columns={"geo": "code_insee24"})
)

# Ensure obs_value is numeric
equip["obs_value"] = pd.to_numeric(equip["obs_value"], errors="coerce")

def equip_group(df: pd.DataFrame, var_group: str, level: str) -> pd.DataFrame:
    df2 = df.copy()
    # Create category column
    df2["cat"] = "n_equip_" + df2[level].str.lower()
    # Aggregate
    grouped = (
        df2
        .groupby([var_group, "cat"], dropna=False)["obs_value"]
        .sum()
        .reset_index(name="cnt")
    )
    # Pivot to wide
    wide = (
        grouped
        .pivot(index=var_group, columns="cat", values="cnt")
        .fillna(0)
        .reset_index()
    )
    return wide

# Apply arr
equip_arr = equip_group(equip, "arr24", "facility_sdom")
equip_arr = equip_arr[equip_arr["arr24"].str.len() < 4]

# Apply bv22
equip_bv22 = equip_group(equip, "bv2022", "facility_sdom")

# Export
equip_arr.to_feather(
    "data/interim/arrondissements/equipement.feather",
    compression="zstd"
)
equip_bv22.to_feather(
    "data/interim/bv2022/equipement.feather",
    compression="zstd"
)
