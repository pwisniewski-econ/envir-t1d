import pandas as pd
import pyarrow.feather as feather
from pathlib import Path

# Lecture données INSEE
table_passage_df = feather.read_feather("results_building/t_passage.feather")[
    ["code_insee24", "arr24", "bv2022"]
].drop_duplicates()

dip_raw = pd.read_csv("data/external/insee-diplomes/DS_RP_DIPLOMES_data.csv")
dip_raw.columns = dip_raw.columns.str.lower()

# Filtrage communes et année
dip1 = dip_raw.query("geo_object == 'COM' and time_period == 2021")[
    ["geo", "obs_value", "sex", "educ"]
].rename(columns={"geo": "code_insee24"})

# Transformation des codes éducation en noms lisibles
dip1["educ"] = dip1["educ"].map({
    "001T": "dip_001T",
    "100_RP": "dip_100R",
    "200_RP": "dip_200R",
    "300_RP": "dip_300R",
    "350T351_RP": "dip_350R",
    "500_RP": "dip_500R",
    "500T702_RP": "dip_500R",
    "600_RP": "dip_600R",
    "600T702_RP": "dip_600R",
    "700_RP": "dip_700R",
    "_T": "total"
})

# Fonction de traitement
def agreg_educ(dip1):
    pivot = dip1.pivot_table(index=["code_insee24", "sex"], columns="educ", values="obs_value", aggfunc="sum").fillna(0).reset_index()

    # Pourcentages
    for col in pivot.columns:
        if col.startswith("dip_"):
            pivot[col] = pivot[col] / pivot["total"]

    # Écart de diplôme supérieur entre sexes
    dip_m = pivot[pivot["sex"] == "M"][["code_insee24", "dip_500R", "dip_600R", "dip_700R"]].copy()
    dip_m["dip_sup_M"] = dip_m[["dip_500R", "dip_600R", "dip_700R"]].sum(axis=1)
    dip_m = dip_m[["code_insee24", "dip_sup_M"]]

    dip_f = pivot[pivot["sex"] == "F"][["code_insee24", "dip_500R", "dip_600R", "dip_700R"]].copy()
    dip_f["dip_sup_F"] = dip_f[["dip_500R", "dip_600R", "dip_700R"]].sum(axis=1)
    dip_f = dip_f[["code_insee24", "dip_sup_F"]]

    delta_df = dip_m.merge(dip_f, on="code_insee24", how="inner")
    delta_df["delta_educ"] = delta_df["dip_sup_M"] - delta_df["dip_sup_F"]

    # Assemblage final
    df_total = pivot[pivot["sex"] == "_T"].merge(delta_df[["code_insee24", "delta_educ"]], on="code_insee24", how="left")
    return df_total

# Agrégation par arrondissement
dip_arr = dip1.merge(table_passage_df, on="code_insee24", how="left")
dip_arr = dip_arr.groupby(["arr24", "sex", "educ"], as_index=False).agg({"obs_value": "sum"})
dip_arr["code_insee24"] = dip_arr["arr24"]
dip_arr = agreg_educ(dip_arr).rename(columns={"code_insee24": "arr24"})
dip_arr = dip_arr[dip_arr["arr24"].str.len() < 4]

# Agrégation par BV2022 
dip_bv = dip1.merge(table_passage_df, on="code_insee24", how="left")
dip_bv = dip_bv.groupby(["bv2022", "sex", "educ"], as_index=False).agg({"obs_value": "sum"})
dip_bv["code_insee24"] = dip_bv["bv2022"]
dip_bv = agreg_educ(dip_bv).rename(columns={"code_insee24": "bv2022"})

# Export
Path("data/interim/arrondissements").mkdir(parents=True, exist_ok=True)
Path("data/interim/bv2022").mkdir(parents=True, exist_ok=True)

feather.write_feather(dip_arr, "data/interim/arrondissements/education.feather", compression="zstd")
feather.write_feather(dip_bv, "data/interim/bv2022/education.feather", compression="zstd")