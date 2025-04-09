import pandas as pd
import pyarrow.feather as feather
from pathlib import Path

# Import FILOSOFI départementaux et BV2022 

def import_filosofi(filename, code_name):
    df = pd.read_csv(f"data/external/insee-filosofi/{filename}", dtype=str)
    df.columns = df.columns.str.lower()
    
    df = df.rename(columns={
        "codgeo": code_name,
        "nbpersmenfisc21": "nombre_pers_menage",
        "med21": "med21", "pimp21": "pimp21", "tp6021": "tp6021", 
        "ppsoc21": "ppsoc21", "ppfam21": "ppfam21", 
        "d121": "d121", "d921": "d921", "rd21": "rd21"
    })
    
    df["nombre_pers_menage"] = pd.to_numeric(df["nombre_pers_menage"].str.replace(",", "."), errors='coerce') / pd.to_numeric(df["nbmenfisc21"].str.replace(",", "."), errors='coerce')
    
    for col in df.columns.difference([code_name]):
        df[col] = pd.to_numeric(df[col].str.replace(",", "."), errors="coerce")
    
    return df[df["pimp21"].notna()]

filo_bv22 = import_filosofi("cc_filosofi_2021_BV2022.csv", "bv2022")
filo_dep = import_filosofi("cc_filosofi_2021_DEP.csv", "dep")

# Import FILOSOFI arrondissement (année par année) 

def import_filo_year(year):
    df = pd.read_csv(f"data/external/insee-filosofi/FILO{year}_DEC_ARR.csv", dtype=str)
    df.columns = df.columns.str.replace(r"\d{2}$", "", regex=True)

    df = df.rename(columns={"CODGEO": "arr24"})
    df["nombre_pers_menage"] = pd.to_numeric(df["NBPERS"], errors="coerce") / pd.to_numeric(df["NBMEN"], errors="coerce")

    to_keep = [
        "arr24", "nombre_pers_menage", "PMIMP", "D1", "Q1", "Q2", "Q3", "D9", "GI",
        "PACT", "PPEN", "TYM4Q1", "TYM4Q2", "TYM4Q3", "TYM5Q1", "TYM5Q2", "TYM5Q3"
    ]

    df = df[to_keep].copy()
    df["year"] = year

    for col in df.columns.difference(["arr24", "year"]):
        df[col] = pd.to_numeric(df[col].str.replace(",", "."), errors="coerce")
    
    return df[df["arr24"].str.len() < 4]

filo_arr = pd.concat([import_filo_year(y) for y in range(2018, 2022)], ignore_index=True)

# Lecture des données chômage (INSEE-RP)

def compute_unemp(df, level_code, level_name):
    df["OBS_VALUE"] = pd.to_numeric(df["OBS_VALUE"], errors="coerce")

    active = df.query(
        "AGE == 'Y15T64' & EMPSTA_ENQ == '1T2' & FREQ == 'A' & PCS == '_T' & TIME_PERIOD == 2021 & GEO_OBJECT == @level_code"
    )[['GEO', 'OBS_VALUE']].rename(columns={"GEO": level_name, "OBS_VALUE": "active_pop"})

    unemployed = df.query(
        "AGE == 'Y15T64' & EMPSTA_ENQ == '2' & FREQ == 'A' & PCS == '_T' & TIME_PERIOD == 2021 & GEO_OBJECT == @level_code"
    )[['GEO', 'OBS_VALUE']].rename(columns={"GEO": level_name, "OBS_VALUE": "unemployed_pop"})

    unemp_df = unemployed.merge(active, on=level_name, how="left")
    unemp_df["unemp_rate"] = unemp_df["unemployed_pop"] / unemp_df["active_pop"]
    return unemp_df[[level_name, "unemp_rate"]]

unemp_data = pd.read_csv("data/external/insee-chomage/DS_RP_EMPLOI_LR_COMP_data.csv", dtype=str)

unemp_arr = compute_unemp(unemp_data, "ARR", "arr24")
unemp_bv22 = compute_unemp(unemp_data, "BV2022", "bv2022")

# Export des résultats 
Path("data/interim/arrondissements").mkdir(parents=True, exist_ok=True)
Path("data/interim/bv2022").mkdir(parents=True, exist_ok=True)
Path("data/interim/departement").mkdir(parents=True, exist_ok=True)

feather.write_feather(filo_arr, "data/interim/arrondissements/filosofi.feather", compression="zstd")
feather.write_feather(filo_bv22, "data/interim/bv2022/filosofi.feather", compression="zstd")
feather.write_feather(filo_dep, "data/interim/departement/filosofi.feather", compression="zstd")
feather.write_feather(unemp_arr, "data/interim/arrondissements/unemployment.feather", compression="zstd")
feather.write_feather(unemp_bv22, "data/interim/bv2022/unemployment.feather", compression="zstd")