# ---- Import Libraries ----
import pandas as pd
import pyarrow.feather as feather
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import geopandas as gpd

# ---- Fonctions auxiliaires ----
def seqmco2details(seqmco_df):
    dataset = ds.dataset("data/interim/rsa23_ano.feather", format="feather")
    df = dataset.to_table().to_pandas()
    return df.merge(seqmco_df, on=["seqmco", "finess"])[["anonyme", "seqmco", "finess", "sexe", "age", "codepost", "date_entree"]]

# ---- Données d’incidence ----
INCIDENT_VISITS = feather.read_feather("results_building/tld_e101-visits.feather")

# ---- Coordonnées de latitude pour 2A/2B ----
lat = gpd.read_file("data/external/arrondissements-version-simplifiee.geojson")
lat = lat[lat["code"].str.contains("2A|2B")]
lat = lat.geometry.centroid.y.to_numpy()

# ---- Dépendances météorologiques ----
DEP = pq.read_table("data/external/dep-full.parquet").to_pandas()
DEP["inst"] = DEP["inst"] / (60 * 24)

grouped = DEP.groupby("dep")
for col in ["tm", "txab", "tnab", "umm", "rr", "inst"]:
    DEP[f"{col}_lag"] = grouped[col].shift().where(grouped["dep"] == grouped["dep"].shift())

# ---- Population par département ----
DEP_POP = ds.dataset("data/external/arrondissement-full.parquet").to_table().to_pandas()
DEP_POP = DEP_POP.sort_values("arr24").copy()
DEP_POP["lat"] = lat
DEP_POP["dep"] = DEP_POP["arr24"].str[:2]

DEP_POP = DEP_POP.groupby("dep").agg({
    "pop_0029": "sum",
    "pop_tot": "sum",
    "pop_ftot": "sum",
    "pop_htot": "sum",
    "n_equip_a2": "sum",
    "lat": "mean"
}).reset_index()
DEP_POP = DEP_POP.rename(columns={"n_equip_a2": "n_equip_d2"})

# ---- Diagnostics Vitamine D ----
vitd_diag = ds.dataset("data/interim/all_diagnosis.feather", format="feather").to_table().to_pandas()
vitd_diag = vitd_diag[vitd_diag["diag"].str.startswith("E55")]
vitd_diag = vitd_diag[["seqmco", "finess"]].drop_duplicates()
VITD_VISITS = seqmco2details(vitd_diag)

# ---- Comptage de référence ----
dataset = ds.dataset("data/interim/rsa23_ano.feather", format="feather")
df = dataset.to_table().to_pandas()
df["dep"] = df["codepost"].astype(str).str[:2]
df["month"] = pd.to_datetime(df["date_entree"]).dt.month.astype(str).str.zfill(2)
ref_count = df.groupby(["dep", "month"]).agg(
    visits=("anonyme", "count"),
    visits_h=("sexe", lambda x: (x == 1).sum()),
)
ref_count["visits_f"] = ref_count["visits"] - ref_count["visits_h"]
ref_count = ref_count.reset_index()

# ---- Comptage Vit D ----
VITD_VISITS["dep"] = VITD_VISITS["codepost"].astype(str).str[:2]
VITD_VISITS["month"] = pd.to_datetime(VITD_VISITS["date_entree"]).dt.month.astype(str).str.zfill(2)

VITD_TES = VITD_VISITS.groupby(["dep", "month"]).agg(
    vitD_visits=("anonyme", "count"),
    vitD_h=("sexe", lambda x: (x == 1).sum())
).reset_index()
VITD_TES["vitD_f"] = VITD_TES["vitD_visits"] - VITD_TES["vitD_h"]

# ---- Jointure Vit D + Référence ----
VITD_TES = VITD_TES.merge(ref_count, on=["dep", "month"])
VITD_TES["vitD_def"] = VITD_TES["vitD_visits"] * 100 / VITD_TES["visits"]
VITD_TES["vitD_def_h"] = VITD_TES["vitD_h"] * 100 / VITD_TES["visits_h"]
VITD_TES["vitD_def_f"] = VITD_TES["vitD_f"] * 100 / VITD_TES["visits_f"]

# ---- Incidence Départementale ----
INCI_DEP = INCIDENT_VISITS.drop_duplicates(subset="anonyme")
INCI_DEP = INCI_DEP[INCI_DEP["age"] < 30].copy()
INCI_DEP["dep"] = INCI_DEP["codepost"].astype(str).str[:2]
INCI_DEP["month"] = pd.to_datetime(INCI_DEP["date_entree"]).dt.month.astype(str).str.zfill(2)

INCI_DEP = INCI_DEP.groupby(["dep", "month"]).agg(
    tld_all=("anonyme", "count"),
    tld_h=("sexe", lambda x: (x == 1).sum())
).reset_index()
INCI_DEP["tld_f"] = INCI_DEP["tld_all"] - INCI_DEP["tld_h"]

# ---- Intégration données météo + population + vitamine D ----
INCI_DEP = INCI_DEP.merge(DEP, on=["dep", "month"], how="right")
INCI_DEP = INCI_DEP.merge(DEP_POP, on="dep", how="inner")
INCI_DEP[["tld_all", "tld_h", "tld_f"]] = INCI_DEP[["tld_all", "tld_h", "tld_f"]].fillna(0)

INCI_DEP = INCI_DEP.merge(VITD_TES, on=["dep", "month"], how="left")

# ---- Normalisation par population ----
INCI_DEP["visits"] = INCI_DEP["visits"] * 1000 / INCI_DEP["pop_tot"]
INCI_DEP["visits_f"] = INCI_DEP["visits_f"] * 1000 / INCI_DEP["pop_ftot"]
INCI_DEP["visits_h"] = INCI_DEP["visits_h"] * 1000 / INCI_DEP["pop_htot"]

# ---- Export ----
feather.write_feather(INCI_DEP, "results_building/department_e101_0029.feather")