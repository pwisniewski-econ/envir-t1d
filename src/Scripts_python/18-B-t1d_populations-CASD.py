# ---- Import Libraries ----
import pandas as pd
import pyarrow.feather as feather
import pyarrow.dataset as ds

# ---- Fonctions auxiliaires ----
def id2people(id_df):
    dataset = ds.dataset("data/interim/rsa23_ano.feather", format="feather")
    df = dataset.to_table().to_pandas()
    people = df.merge(id_df, on="anonyme", how="inner")[["anonyme", "age", "sexe", "codepost"]]
    return people.drop_duplicates(subset="anonyme")

def seqmco2details(seqmco_df):
    dataset = ds.dataset("data/interim/rsa23_ano.feather", format="feather")
    df = dataset.to_table().to_pandas()
    merged = df.merge(seqmco_df, on=["seqmco", "finess"], how="inner")
    return merged[["anonyme", "seqmco", "finess", "sexe", "age", "codepost", "date_entree", "modeentree"]]

# ---- Codes T1D ----
T1D_CODES = ["E100", "E101", "E102", "E103", "E104", "E105", "E106", "E107", "E108", "E109"]

# ---- Chargement des diagnostics ----
diagnosis_df = feather.read_feather("data/interim/all_diagnosis.feather")
t1d_diag = diagnosis_df[diagnosis_df["diag"].isin(T1D_CODES)].copy()

# ---- Populations T1D ----
PREVALENT_INDIVIDUALS = id2people(t1d_diag[["anonyme"]].drop_duplicates())
INCIDENT_INDIVIDUALS = id2people(t1d_diag[t1d_diag["diag"] == "E101"][["anonyme"]].drop_duplicates())

# ---- Visites T1D ----
PREVALENT_VISITS = seqmco2details(t1d_diag[["seqmco", "finess"]].drop_duplicates())
INCIDENT_VISITS = seqmco2details(t1d_diag[t1d_diag["diag"] == "E101"][["seqmco", "finess"]].drop_duplicates())

# ---- Population complète ----
dataset = ds.dataset("data/interim/rsa23_ano.feather", format="feather")
all_individuals = dataset.to_table(columns=["anonyme", "age", "sexe", "codepost"]).to_pandas()
ALL_INDIVIDUALS = all_individuals.drop_duplicates(subset="anonyme")

# ---- Exports ----
feather.write_feather(PREVALENT_VISITS, "results_building/t1d-visits.feather", compression="zstd")
feather.write_feather(INCIDENT_VISITS, "results_building/t1d_e101-visits.feather", compression="zstd")
feather.write_feather(PREVALENT_INDIVIDUALS, "results_building/t1d-individuals.feather", compression="zstd")
feather.write_feather(INCIDENT_INDIVIDUALS, "results_building/t1d_e101-individuals.feather", compression="zstd")
feather.write_feather(ALL_INDIVIDUALS, "results_building/all-individuals.feather", compression="zstd")