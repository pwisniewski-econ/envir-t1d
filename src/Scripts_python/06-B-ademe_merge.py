import pandas as pd
import pyarrow.dataset as ds
import pyarrow.feather as feather
from pathlib import Path
import re

# Tables de passage
table_passage_df = feather.read_feather("results_building/t_passage.feather")[
    ["code_insee24", "arr24", "bv2022", "code_insee21"]
]
arr2com = feather.read_feather("results_building/arr2com.feather")

# Chargement du dataset complet
dataset = ds.dataset("data/interim/_ademe/dpe_all.feather", format="feather")

# Extraction des identifiants à filtrer
old_replaced = dataset.to_table(columns=["ndperemplac"]).filter(ds.field("ndperemplac") != "").to_pandas()["ndperemplac"]
old_building = dataset.to_table(columns=["ndpeimmeubleassoci"]).filter(ds.field("ndpeimmeubleassoci") != "").to_pandas()["ndpeimmeubleassoci"]
old_filter = pd.DataFrame({"dpe_id": pd.concat([old_replaced, old_building]), "filt": True})

# Filtrage et renommage des colonnes
columns = {
    "ndpe": "dpe_id",
    "ndperemplac": "dpe_replaced",
    "ndpeimmeubleassoci": "dpe_building",
    "datevisitediagnostiqueur": "date_visited",
    "datetablissementdpe": "date_established",
    "modledpe": "type",
    "surfacehabitablelogement": "floor_area",
    "surfaceclimatise": "ac_area",
    "etiquetteges": "ghg_grade",
    "etiquettedpe": "dpe_grade",
    "anneconstruction": "year_built",
    "priodeconstruction": "period_built",
    "codeinseeban": "insee_code",
    "typenergieprincipalechauffage": "heating_energy",
    "typeinstallationchauffage": "heating_type"
}

dpe_table = dataset.to_table(columns=list(columns.keys()) + ["modledpe"]).filter(
    ds.field("modledpe") == "DPE 3CL 2021 méthode logement"
).to_pandas()

dpe_table = dpe_table.rename(columns=columns)
dpe_table["insee_code"] = dpe_table["insee_code"].astype(str).str.replace("old", "", regex=False).str.replace(" ", "", regex=False)

# Jointures géographiques
dpe_table = dpe_table.merge(arr2com, left_on="insee_code", right_on="code_arr", how="left")
dpe_table["insee_code"] = dpe_table["code_com"].combine_first(dpe_table["insee_code"])
dpe_table = dpe_table.merge(table_passage_df, left_on="insee_code", right_on="code_insee21", how="left")

# Suppression des doublons 
dpe_table = dpe_table.merge(old_filter, on="dpe_id", how="left")
dpe_table = dpe_table[dpe_table["filt"].isna()].drop(columns="filt")

# Conversion de période en année estimée
period_map = {
    "avant 1948": 1935,
    "1948-1974": 1961,
    "1975-1977": 1976,
    "1978-1982": 1980,
    "1983-1988": 1987,
    "1989-2000": 1995,
    "2001-2005": 2003,
    "2006-2012": 2009,
    "2013-2021": 2016,
    "après 2021": 2022
}
dpe_table["period"] = dpe_table["period_built"].map(period_map)
dpe_table["ac_area"] = dpe_table["ac_area"].fillna(0)
dpe_table["year_built"] = dpe_table["year_built"].fillna(dpe_table["period"])

# Encodage des lettres de DPE
dpe_encoder = pd.DataFrame({
    "dpe_grade": ["A", "B", "C", "D", "E", "F", "G"],
    "dpe_grade_num": list(range(1, 8))
})
dpe_table = dpe_table.merge(dpe_encoder, on="dpe_grade", how="left")

# Fonction d’agrégation
def group_dpe(df, groupvar):
    grouped = df.groupby(groupvar).agg(
        built_q2=("year_built", "median"),
        floor_area_q2=("floor_area", "median"),
        q1_dpe=("dpe_grade_num", lambda x: x.quantile(0.25)),
        q2_dpe=("dpe_grade_num", lambda x: x.quantile(0.5)),
        q3_dpe=("dpe_grade_num", lambda x: x.quantile(0.75)),
        n_dpe=("dpe_id", "count")
    ).reset_index()

    ac_df = df[df["ac_area"] > 0].groupby(groupvar).size().reset_index(name="ac_prop")
    merged = grouped.merge(ac_df, on=groupvar, how="left").fillna({"ac_prop": 0})

    merged["iqr_dpe_num"] = (merged["q3_dpe"].round() - merged["q1_dpe"].round()).astype(int)
    for q in ["q1_dpe", "q2_dpe", "q3_dpe"]:
        merged[q] = merged[q].round().astype(int)
    merged["ac_prop"] = merged["ac_prop"] / merged["n_dpe"]

    return merged

# Par niveau géographique
old_dpe_arr = group_dpe(dpe_table, "arr24").query("arr24.str.len() < 4")
old_dpe_bv = group_dpe(dpe_table, "bv2022").dropna(subset=["bv2022"])
dpe_table["dep"] = dpe_table["arr24"].str[:2]
old_dpe_dep = group_dpe(dpe_table.dropna(subset=["dep"]), "dep")

# Exports
Path("data/interim/arrondissements").mkdir(parents=True, exist_ok=True)
Path("data/interim/bv2022").mkdir(parents=True, exist_ok=True)
Path("data/interim/departement").mkdir(parents=True, exist_ok=True)

feather.write_feather(old_dpe_arr, "data/interim/arrondissements/ademe_dpe.feather", compression="zstd")
feather.write_feather(old_dpe_bv, "data/interim/bv2022/ademe_dpe.feather", compression="zstd")
feather.write_feather(old_dpe_dep, "data/interim/departement/ademe_dpe.feather", compression="zstd")