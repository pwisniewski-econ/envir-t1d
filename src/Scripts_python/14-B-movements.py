import pandas as pd
import pyarrow.feather as feather
from pathlib import Path

# Table de passage
table_passage_df = feather.read_feather("results_building/t_passage.feather")[["code_insee24", "arr24", "bv2022"]].drop_duplicates()
arr2com = feather.read_feather("results_building/arr2com.feather")

# Fonction pour remplacer les arrondissements par les communes
def rename_join(df, join_df, colname, join_on):
    join_df = join_df.rename(columns={"code_com": colname})
    df = df.merge(join_df, left_on=list(join_on.keys())[0], right_on=join_on[list(join_on.keys())[0]], how="left")
    df[colname] = df["code_com"].combine_first(df[list(join_on.keys())[0]])
    return df.drop(columns=["code_com"])

# Chargement des flux
movements = pd.read_csv("data/external/insee-mouvements/base-flux-mobilite-residentielle-2021.csv", dtype=str)
movements["NBFLUX_C21_POP01P"] = pd.to_numeric(movements["NBFLUX_C21_POP01P"], errors="coerce")

# Remplacement des codes arr par communes (to/from)
movements = rename_join(movements, arr2com, "com_to", {"CODGEO": "code_arr"})
movements = rename_join(movements, arr2com, "com_from", {"DCRAN": "code_arr"})

# Fonction d’agrégation
def sum_movements(data, level):
    # Jointures pour attribuer les niveaux géographiques
    df = data.merge(table_passage_df.rename(columns={"code_insee24": "com_to", level: "arr_to"}), on="com_to", how="left")
    df = df.merge(table_passage_df.rename(columns={"code_insee24": "com_from", level: "arr_from"}), on="com_from", how="left")

    df = df[df["arr_from"] != df["arr_to"]]

    # Entrants par territoire
    incoming = df.groupby("arr_to", as_index=False).agg(incoming_pop21=("NBFLUX_C21_POP01P", "sum"))
    incoming["incoming_pop21"] = incoming["incoming_pop21"].round(2)

    # Matrice de flux
    flow = df.groupby(["arr_to", "arr_from"], as_index=False).agg(flow_pop21=("NBFLUX_C21_POP01P", "sum"))
    flow["flow_pop21"] = flow["flow_pop21"].round(2)
    flow["share_to"] = flow.groupby("arr_to")["flow_pop21"].transform(lambda x: x / x.sum())

    return incoming, flow

# Agrégation par arrondissement
incoming_arr, flow_arr = sum_movements(movements, "arr24")

# Agrégation par BV2022
incoming_bv22, flow_bv22 = sum_movements(movements, "bv2022")
incoming_bv22 = incoming_bv22.rename(columns={"arr_to": "bv22_to"})
flow_bv22 = flow_bv22.rename(columns={"arr_to": "bv22_to", "arr_from": "bv22_from"})

# Export
Path("data/interim/arrondissements").mkdir(parents=True, exist_ok=True)
Path("data/interim/bv2022").mkdir(parents=True, exist_ok=True)
Path("results_building").mkdir(parents=True, exist_ok=True)

feather.write_feather(incoming_arr, "data/interim/arrondissements/incoming_pop.feather", compression="zstd")
feather.write_feather(flow_arr, "results_building/arrondissement-flow.feather", compression="zstd")

feather.write_feather(incoming_bv22, "data/interim/bv2022/incoming_pop.feather", compression="zstd")
feather.write_feather(flow_bv22, "results_building/bv2022-flow.feather", compression="zstd")