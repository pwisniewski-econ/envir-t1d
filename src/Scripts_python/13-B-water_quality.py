import pandas as pd
import pyarrow.feather as feather
from pathlib import Path

# Tables de passage
table_passage_df = feather.read_feather("results_building/t_passage.feather")[["code_insee24", "arr24", "bv2022"]].drop_duplicates()

# Fichiers d’infrastructure eau
coms = pd.read_csv("data/external/msp-eau/DIS_COM_UDI_2023.txt", sep="\t", dtype=str)[["cdreseau", "inseecommune"]].drop_duplicates()

plv = pd.read_csv("data/external/msp-eau/DIS_PLV_2023.txt", sep="\t", dtype=str)[["referenceprel", "cdreseau"]].drop_duplicates()
plv = plv.merge(coms, on="cdreseau", how="left")[["referenceprel", "inseecommune"]].drop_duplicates()
plv = plv.rename(columns={"inseecommune": "code_insee24"})

# Données de qualité de l’eau
water_q = pd.read_csv("data/external/msp-eau/DIS_RESULT_2023.txt", sep="\t", dtype=str)

# Filtrage sur les paramètres d’intérêt
water_q = water_q[water_q["cdparametresiseeaux"].isin(["CL", "NO3", "PH", "SO4"])]

# Nettoyage 
water_q["rqana"] = (
    water_q["rqana"].str.replace("<", "", regex=False)
                     .str.replace(">", "", regex=False)
                     .str.replace(",", ".", regex=False)
                     .astype(float)
)

# Moyenne par prélèvement et paramètre
water_q = water_q.groupby(["referenceprel", "cdparametresiseeaux"], as_index=False).agg(rqana=("rqana", "mean"))

# Jointure avec les communes
water_q = water_q.merge(plv, on="referenceprel", how="left")

# Agrégation communale
water_qb = water_q.groupby(["code_insee24", "cdparametresiseeaux"], as_index=False).agg(rqana=("rqana", "mean"))
water_qb = water_qb.merge(table_passage_df, on="code_insee24", how="inner")

# Fonction d’agrégation par niveau géographique
def sum_water(data, level):
    df = data.groupby([level, "cdparametresiseeaux"], as_index=False).agg(rqana=("rqana", "mean"))
    df = df.pivot(index=level, columns="cdparametresiseeaux", values="rqana").reset_index()
    df.columns = [f"water_{c.lower()}" if c != level else c for c in df.columns]
    return df

# Agrégations
waterq_arr = sum_water(water_qb, "arr24")
waterq_bv22 = sum_water(water_qb, "bv2022")
waterq_bv22 = waterq_bv22.dropna(subset=["water_cl", "water_no3", "water_ph", "water_so4"])

# Export
Path("data/interim/arrondissements").mkdir(parents=True, exist_ok=True)
Path("data/interim/bv2022").mkdir(parents=True, exist_ok=True)

feather.write_feather(waterq_arr, "data/interim/arrondissements/water_quality.feather", compression="zstd")
feather.write_feather(waterq_bv22, "data/interim/bv2022/water_quality.feather", compression="zstd")