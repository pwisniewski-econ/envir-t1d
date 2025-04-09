import pandas as pd
import pyarrow.feather as feather
import pyarrow.parquet as pq
from pathlib import Path

# Table de passage
arr2com = feather.read_feather("results_building/arr2com.feather")
table_passage_df = feather.read_feather("results_building/t_passage.feather")[
    ["code_insee24", "arr24", "bv2022"]
].drop_duplicates().groupby("code_insee24").head(1)

# Dictionnaire de variables de crimes
dico_crimes = pd.DataFrame({
    "var_crimes": ["coups", "trafic", "vols_av", "vols_sv", "cambriolages"],
    "crimes_desc": [
        "Coups et blessures volontaires",
        "Trafic de stupéfiants",
        "Vols violents sans arme",
        "Vols sans violence contre des personnes",
        "Cambriolages de logement"
    ]
})

# Chargement du parquet
crimes_df = pq.read_table("data/external/miom-crimes/crimes23_geo24.parquet").to_pandas()
crimes_df = crimes_df[crimes_df["annee"] > 16]

# Sélection et renommage
crimes_df = crimes_df[[
    "CODGEO_2024", "annee", "faits", "classe", "unité.de.compte"
]].rename(columns={
    "CODGEO_2024": "code_insee24",
    "annee": "year",
    "faits": "crime_count",
    "classe": "crimes_desc",
    "unité.de.compte": "units"
})

crimes_df["year"] += 2000
crimes_df["crime_count"] = crimes_df["crime_count"].fillna(0)

# Filtrage sur les types de crimes d’intérêt
crimes_df = crimes_df[crimes_df["crimes_desc"].isin(dico_crimes["crimes_desc"])]

# Jointures géographiques
crimes_df = crimes_df.merge(arr2com, left_on="code_insee24", right_on="code_arr", how="left")
crimes_df["code_insee24"] = crimes_df["code_com"].combine_first(crimes_df["code_insee24"])
crimes_df = crimes_df.merge(dico_crimes, on="crimes_desc", how="left")
crimes_df = crimes_df.merge(table_passage_df, on="code_insee24", how="left")

# Agrégation par arrondissement
crimes_arr = crimes_df.groupby(["arr24", "year", "var_crimes"], as_index=False).agg({
    "crime_count": "sum"
})
crimes_arr = crimes_arr.pivot(index=["arr24", "year"], columns="var_crimes", values="crime_count").reset_index()

# Agrégation par BV2022 (moyenne)
crimes_bv22 = crimes_df.groupby(["bv2022", "year", "var_crimes"], as_index=False).agg({
    "crime_count": "mean"
})
crimes_bv22 = crimes_bv22.pivot(index=["bv2022", "year"], columns="var_crimes", values="crime_count").reset_index()

Path("data/interim/arrondissements").mkdir(parents=True, exist_ok=True)
Path("data/interim/bv2022").mkdir(parents=True, exist_ok=True)

feather.write_feather(crimes_arr, "data/interim/arrondissements/criminality.feather", compression="zstd")
feather.write_feather(crimes_bv22, "data/interim/bv2022/criminality.feather", compression="zstd")