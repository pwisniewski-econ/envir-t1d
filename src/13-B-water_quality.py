import pandas as pd

# Import Data -------------
table_passage_df = (
    pd.read_feather("results_building/t_passage.feather")[[
        "code_insee24", "arr24", "bv2022"
    ]]
    .drop_duplicates()
)

coms = (
    pd.read_csv(
        "data/external/msp-eau/DIS_COM_UDI_2023.txt",
        sep=",",
        dtype=str,
        low_memory=False
    )
    [["cdreseau", "inseecommune"]]
    .drop_duplicates()
)

plv = (
    pd.read_csv(
        "data/external/msp-eau/DIS_PLV_2023.txt",
        sep=",",
        dtype=str,
        low_memory=False
    )
    [["referenceprel", "cdreseau"]]
    .drop_duplicates()
    .merge(coms, on="cdreseau", how="left")
    [["referenceprel", "inseecommune"]]
    .rename(columns={"inseecommune": "code_insee24"})
    .drop_duplicates()
)

# Prepare Water Quality DF -------------------
water_q = pd.read_csv(
    "data/external/msp-eau/DIS_RESULT_2023.txt",
    sep=",",
    dtype=str,
    low_memory=False
)

water_q = water_q[water_q["cdparametresiseeaux"].isin(["CL", "NO3", "PH", "SO4"])].copy()

water_q["rqana"] = (
    water_q["rqana"]
      .str.replace(r"[<>]", "", regex=True)
      .str.replace(",", ".", regex=False)
)
water_q["rqana"] = pd.to_numeric(water_q["rqana"], errors="coerce")

water_q = (
    water_q
    .groupby(["referenceprel", "cdparametresiseeaux"], as_index=False)["rqana"]
    .mean()
)
water_q = water_q.dropna(subset=["rqana"])
water_q = water_q.rename(columns={"cdparametresiseeaux": "water_param"})

water_qb = (
    water_q
    .merge(plv, on="referenceprel", how="left")
    .groupby(["code_insee24", "water_param"], as_index=False)["rqana"]
    .mean()
)

water_qb = water_qb.merge(table_passage_df, on="code_insee24", how="inner")

def sum_water(df: pd.DataFrame, level_col: str) -> pd.DataFrame:
    grp = (
        df.groupby([level_col, "water_param"], as_index=False)["rqana"]
        .mean()
    )
    piv = grp.pivot(index=level_col, columns="water_param", values="rqana").reset_index()
    piv.columns.name = None
    # rename the parameter columns to water_<param_in_lowercase>
    rename_map = {
        col: f"water_{col.lower()}" for col in piv.columns if col != level_col
    }
    return piv.rename(columns=rename_map)

# Main logic -------------------------
waterq_dep = sum_water(
    water_qb.assign(dep=water_qb["code_insee24"].str[:2]),
    "dep"
)

waterq_arr = sum_water(water_qb, "arr24")

waterq_bv22 = (
    sum_water(water_qb, "bv2022")
    .dropna(subset=["water_cl", "water_no3", "water_ph", "water_so4"])
)

# Export ------------------------
waterq_arr.to_feather(
    "data/interim/arrondissements/water_quality.feather",
    compression="zstd"
)
waterq_bv22.to_feather(
    "data/interim/bv2022/water_quality.feather",
    compression="zstd"
)
waterq_dep.to_feather(
    "data/interim/departement/water_quality.feather",
    compression="zstd"
)
