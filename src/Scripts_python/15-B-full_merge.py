import pandas as pd
import pyarrow.feather as feather
from pathlib import Path

# Chargement base principale
base_table = feather.read_feather("results_building/t_passage.feather")
base_table = base_table[pd.to_numeric(base_table["code_insee24"], errors="coerce") < 97100]

# Répertoire données agrégées
arr_path = "data/interim/arrondissements/"
bv_path = "data/interim/bv2022/"
dep_path = "data/interim/departement/"

# Chargement données arrondissement
datasets_arr = {
    "ADEME": "ademe_dpe",
    "AIRQ": "air_quality",
    "CRIM": "criminality",
    "EDUC": "education",
    "EQUIP": "equipement",
    "FILO": "filosofi",
    "GHG": "greenhouse",
    "POP": "populations_2021",
    "SIRENE": "sirene_1624",
    "SOCIAL": "social_housing",
    "UNEMP": "unemployment",
    "WATER": "water_quality",
    "WEATHER": "weather",
    "INCOMING": "incoming_pop"
}

arr_dfs = {k: feather.read_feather(f"{arr_path}{v}.feather") for k, v in datasets_arr.items()}

# Prétraitement
arr_dfs["CRIM"] = arr_dfs["CRIM"].query("year == 2021").drop(columns="year")
arr_dfs["FILO"] = arr_dfs["FILO"].query("year == 2021").drop(columns="year")
arr_dfs["SIRENE"] = arr_dfs["SIRENE"].query("year == 2021").drop(columns="year")

# Fusion
arr_data = base_table[["arr24"]].drop_duplicates()
for df in ["EQUIP", "SOCIAL", "CRIM", "SIRENE", "GHG", "POP"]:
    arr_data = arr_data.merge(arr_dfs[df], on="arr24", how="left")

arr_data = arr_data.assign(**{
    col: arr_data[col] / arr_data["pop_tot"] * 1000
    for col in arr_data.columns if col not in ["arr24", "pop_tot"] and col.startswith("n_")
})

arr_data = arr_data.merge(arr_dfs["INCOMING"], left_on="arr24", right_on="arr_to", how="left")
arr_data["pop_renew"] = arr_data["incoming_pop21"] / arr_data["pop_tot"]
arr_data = arr_data.drop(columns="incoming_pop21")

for df in ["FILO", "UNEMP", "EDUC", "ADEME", "AIRQ", "WEATHER", "WATER"]:
    arr_data = arr_data.merge(arr_dfs[df], on="arr24", how="left")

# Export ARR
feather.write_feather(arr_data, "results_building/arrondissement-full.feather", compression="zstd")


# Merge BV2022

datasets_bv = {k: feather.read_feather(f"{bv_path}{v}.feather") for k, v in datasets_arr.items()}
for k in ["CRIM", "FILO", "SIRENE"]:
    datasets_bv[k] = datasets_bv[k].query("year == 2021").drop(columns="year")

bv_data = base_table[["bv2022"]].drop_duplicates()
for df in ["EQUIP", "SOCIAL", "CRIM", "SIRENE", "GHG", "POP"]:
    bv_data = bv_data.merge(datasets_bv[df], on="bv2022", how="inner")

bv_data = bv_data.assign(**{
    col: bv_data[col] / bv_data["pop_tot"] * 1000
    for col in bv_data.columns if col not in ["bv2022", "pop_tot"] and col.startswith("n_")
})

bv_data = bv_data.merge(datasets_bv["INCOMING"], left_on="bv2022", right_on="bv22_to", how="inner")
bv_data["pop_renew"] = bv_data["incoming_pop21"] / bv_data["pop_tot"]
bv_data = bv_data.drop(columns="incoming_pop21")

for df in ["FILO", "UNEMP", "EDUC", "ADEME", "AIRQ", "WATER"]:
    bv_data = bv_data.merge(datasets_bv[df], on="bv2022", how="inner")

bv_data = bv_data.dropna()  # Nettoyage final

# Export BV2022
feather.write_feather(bv_data, "results_building/bv2022-full.feather", compression="zstd")


# fusion DEPARTEMENT 

ademe_dep = feather.read_feather(f"{dep_path}ademe_dpe.feather")
weather_dep = feather.read_feather(f"{dep_path}weather_2023.feather")
filo_dep = feather.read_feather(f"{dep_path}filosofi.feather")

dep_data = weather_dep.query("dep.str.len() == 2 & dep.astype(int) < 97")
dep_data = dep_data.merge(ademe_dep, on="dep", how="inner")
dep_data = dep_data.merge(filo_dep, on="dep", how="inner")
dep_data = dep_data.dropna()

# Export DEPT
feather.write_feather(dep_data, "results_building/dep-full.feather", compression="zstd")