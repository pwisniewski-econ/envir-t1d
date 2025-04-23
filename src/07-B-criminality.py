import pandas as pd
import numpy as np

# Read  arrondissement‑to‑commune mapping
ARR2COM = pd.read_feather("results_building/arr2com.feather")

# Dictionary of crime codes
DICO_CRIMES = pd.DataFrame({
    "var_crimes": [
        "coups", "trafic", "vols_av", "vols_sv", "cambriolages"
    ],
    "crimes_desc": [
        "Coups et blessures volontaires",
        "Trafic de stupéfiants",
        "Vols violents sans arme",
        "Vols sans violence contre des personnes",
        "Cambriolages de logement"
    ]
})

# Build the TABLE_PASSAGE_DF analogue:
TABLE_PASSAGE_DF = (
    pd.read_feather("results_building/t_passage.feather")[["code_insee24", "arr24", "bv2022"]]
    .drop_duplicates()
    .groupby("code_insee24", as_index=False)
    .first()
)

# pre‑filter the raw crimes data
CRIMES_DF = pd.read_parquet("data/external/miom-crimes/crimes23_geo24.parquet")
CRIMES_DF = CRIMES_DF[CRIMES_DF["annee"] > 16]

# Select & clean up
CRIMES_DF = (
    CRIMES_DF[["CODGEO_2024", "annee", "faits", "classe", "unité.de.compte"]]
    .rename(columns={
        "CODGEO_2024": "code_insee24",
        "annee": "year",
        "faits": "crime_count",
        "classe": "crimes_desc",
        "unité.de.compte": "units"
    })
)
CRIMES_DF["year"] = CRIMES_DF["year"] + 2000
CRIMES_DF["crime_count"] = CRIMES_DF["crime_count"].fillna(0)
CRIMES_DF = CRIMES_DF[CRIMES_DF["crimes_desc"].isin(DICO_CRIMES["crimes_desc"])]

# Replace arr codes by commune codes
CRIMES_DF = CRIMES_DF.merge(
    ARR2COM,
    how="left",
    left_on="code_insee24",
    right_on="code_arr"
)
CRIMES_DF["code_insee24"] = np.where(
    CRIMES_DF["code_com"].notna(),
    CRIMES_DF["code_com"],
    CRIMES_DF["code_insee24"]
)

# Bring in the var_crimes code and the BV/bv2022 lookup
CRIMES_DF = CRIMES_DF.merge(DICO_CRIMES, on="crimes_desc", how="left")
CRIMES_DF = CRIMES_DF.merge(TABLE_PASSAGE_DF, on="code_insee24", how="left")

# === EXPORT DATA ===

# By arrondissement (sum of counts)
CRIMES_ARR = (
    CRIMES_DF
    .groupby(["arr24", "year", "var_crimes"], as_index=False)["crime_count"]
    .sum()
    .pivot(index=["arr24", "year"], columns="var_crimes", values="crime_count")
    .reset_index()
)

# By BV2022 (mean of counts)
CRIMES_BV22 = (
    CRIMES_DF
    .groupby(["bv2022", "year", "var_crimes"], as_index=False)["crime_count"]
    .mean()
    .pivot(index=["bv2022", "year"], columns="var_crimes", values="crime_count")
    .reset_index()
)

# Export
CRIMES_ARR.to_feather("data/interim/arrondissements/criminality.feather", compression="zstd")
CRIMES_BV22.to_feather("data/interim/bv2022/criminality.feather", compression="zstd")
