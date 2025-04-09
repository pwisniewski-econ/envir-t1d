import pandas as pd
import pyarrow.feather as feather
from pathlib import Path

# Lecture des données d'équipement
equip = pd.read_csv("data/external/insee-equipement/DS_BPE_data.csv")
equip.columns = equip.columns.str.lower()

# Lecture des données géographiques
table_passage_df = feather.read_feather("results_building/t_passage.feather")[
    ['code_insee24', 'arr24', 'bv2022']
].drop_duplicates()

# Filtrage sur les communes et jointure
equip = equip[equip["geo_object"] == "COM"]
equip = equip.merge(table_passage_df, left_on="geo", right_on="code_insee24", how="left")

equip = equip[[
    "geo", "arr24", "bv2022", "facility_sdom", "facility_dom", "obs_value"
]].rename(columns={"geo": "code_insee24"})

# Fonction d’agrégation
def equip_group(vargroup, level):
    grouped = equip.groupby([vargroup, level], as_index=False).agg(cnt=("obs_value", "sum"))
    grouped["cat"] = "n_equip_" + grouped[level].str.lower()
    pivoted = grouped.pivot(index=vargroup, columns="cat", values="cnt").fillna(0).reset_index()
    return pivoted

# Agrégation par arrondissement
equip_arr = equip_group("arr24", "facility_sdom")
equip_arr = equip_arr[equip_arr["arr24"].str.len() < 4]

# Agrégation par BV2022
equip_bv22 = equip_group("bv2022", "facility_sdom")

# Export
Path("data/interim/arrondissements").mkdir(parents=True, exist_ok=True)
Path("data/interim/bv2022").mkdir(parents=True, exist_ok=True)

feather.write_feather(equip_arr, "data/interim/arrondissements/equipement.feather", compression="zstd")
feather.write_feather(equip_bv22, "data/interim/bv2022/equipement.feather", compression="zstd")