#---Import Libraries----------
import os
import pandas as pd
import pyarrow.feather as feather

# ─── READ BASE TABLE ────────────
BASE_TABLE = pd.read_feather("results_building/t_passage.feather")
# keep only code_insee24 < 97100
BASE_TABLE = BASE_TABLE.loc[
    pd.to_numeric(BASE_TABLE["code_insee24"], errors="coerce") < 97100
]

# ─── MERGE ARRONDISSEMENTS ───────────
arr_path = "data/interim/arrondissements/"

arr_files = [os.path.join(arr_path, fn) for fn in os.listdir(arr_path)]

ADEME_ARR = pd.read_feather(arr_path + "ademe_dpe.feather")
AIRQ_ARR = pd.read_feather(arr_path + "air_quality.feather")
CRIM_ARR = (
    pd.read_feather(arr_path + "criminality.feather")
    .query("year == 2021")
    .drop(columns="year")
)
EDUC_ARR = pd.read_feather(arr_path + "education.feather")
EQUIP_ARR = pd.read_feather(arr_path + "equipement.feather")
FILO_ARR = (
    pd.read_feather(arr_path + "filosofi.feather")
    .query("year == 2021")
    .drop(columns="year")
)
GHG_ARR = pd.read_feather(arr_path + "greenhouse.feather")
POP_ARR = pd.read_feather(arr_path + "populations_2021.feather")
SIRENE_ARR = (
    pd.read_feather(arr_path + "sirene_1624.feather")
    .query("year == 2021")
    .drop(columns="year")
)
SOCIAL_ARR = pd.read_feather(arr_path + "social_housing.feather")
UNEMP_ARR = pd.read_feather(arr_path + "unemployment.feather")
WATERQ_ARR = pd.read_feather(arr_path + "water_quality.feather")
WEATHER_ARR = pd.read_feather(arr_path + "weather.feather")
INCOMING_ARR = pd.read_feather(arr_path + "incoming_pop.feather")

# start from distinct arr24 values
ARR_DATA = (
    BASE_TABLE[["arr24"]]
    .drop_duplicates()
    .merge(EQUIP_ARR, on="arr24", how="left")
    .merge(SOCIAL_ARR, on="arr24", how="left")
    .merge(CRIM_ARR, on="arr24", how="left")
    .merge(SIRENE_ARR, on="arr24", how="left")
    .merge(GHG_ARR, on="arr24", how="left")
    .merge(POP_ARR, on="arr24", how="left")
)

# scale
for col in ARR_DATA.columns:
    if col not in ["arr24"] and not col.startswith("pop_"):
        ARR_DATA[col] = ARR_DATA[col] / ARR_DATA["pop_tot"] * 1e3

# incoming population & renewal rate
ARR_DATA = (
    ARR_DATA
    .merge(INCOMING_ARR, left_on="arr24", right_on="arr_to", how="left")
)
ARR_DATA["pop_renew"] = ARR_DATA["incoming_pop21"] / ARR_DATA["pop_tot"]
ARR_DATA = ARR_DATA.drop(columns="incoming_pop21")

# join the rest
for df in [FILO_ARR, UNEMP_ARR, EDUC_ARR, ADEME_ARR, AIRQ_ARR, WEATHER_ARR, WATERQ_ARR]:
    ARR_DATA = ARR_DATA.merge(df, on="arr24", how="left")

# Export
feather.write_feather(
    ARR_DATA.dropna().drop("arr_to", axis = 1),
    "results_building/arrondissement-full.feather",
    compression="zstd"
)


# ─── MERGE BV2022 ──────────────────────────────
bv_path = "data/interim/bv2022/"

ADEME_BV22 = pd.read_feather(bv_path + "ademe_dpe.feather")
AIRQ_BV22 = pd.read_feather(bv_path + "air_quality.feather")
CRIM_BV22 = (
    pd.read_feather(bv_path + "criminality.feather")
    .query("year == 2021")
    .drop(columns="year")
)
EDUC_BV22 = pd.read_feather(bv_path + "education.feather")
EQUIP_BV22 = pd.read_feather(bv_path + "equipement.feather")
FILO_BV22 = pd.read_feather(bv_path + "filosofi.feather")
GHG_BV22 = pd.read_feather(bv_path + "greenhouse.feather")
POP_BV22 = pd.read_feather(bv_path + "populations_2021.feather")
SIRENE_BV22 = (
    pd.read_feather(bv_path + "sirene_1624.feather")
    .query("year == 2021")
    .drop(columns="year")
)
SOCIAL_BV22 = pd.read_feather(bv_path + "social_housing.feather")
UNEMP_BV22 = pd.read_feather(bv_path + "unemployment.feather")
WATERQ_BV22 = pd.read_feather(bv_path + "water_quality.feather")
INCOMING_BV22 = pd.read_feather(bv_path + "incoming_pop.feather")

BV22_DATA = BASE_TABLE[["bv2022"]].drop_duplicates()
# inner joins
for df, key in [
    (EQUIP_BV22, "bv2022"),
    (SOCIAL_BV22, "bv2022"),
    (CRIM_BV22, "bv2022"),
    (SIRENE_BV22, "bv2022"),
    (GHG_BV22, "bv2022"),
    (POP_BV22, "bv2022")
]:
    BV22_DATA = BV22_DATA.merge(df, on=key, how="inner")

# scale
for col in BV22_DATA.columns:
    if col not in ["bv2022"] and not col.startswith("pop_"):
        BV22_DATA[col] = BV22_DATA[col] / BV22_DATA["pop_tot"] * 1e3

# incoming pop
BV22_DATA = BV22_DATA.merge(
    INCOMING_BV22,
    left_on="bv2022",
    right_on="bv22_to",
    how="inner"
)
BV22_DATA["pop_renew"] = BV22_DATA["incoming_pop21"] / BV22_DATA["pop_tot"]
BV22_DATA = BV22_DATA.drop(columns="incoming_pop21")

for df in [FILO_BV22, UNEMP_BV22, EDUC_BV22, ADEME_BV22, AIRQ_BV22, WATERQ_BV22]:
    BV22_DATA = BV22_DATA.merge(df, on="bv2022", how="inner")

BV22_DATA = BV22_DATA.dropna().drop("bv22_to", axis = 1)

# Export
feather.write_feather(
    BV22_DATA,
    "results_building/bv2022-full.feather",
    compression="zstd"
)


# ─── DEPARTMENTS ───────────────────────────
dep_path = "data/interim/departement/"

ADEME_DEP = pd.read_feather(dep_path + "ademe_dpe.feather")
WEATHER_DEP = pd.read_feather(dep_path + "weather_2023.feather")
FILO_DEP = pd.read_feather(dep_path + "filosofi.feather")
WATERQ_DEP = pd.read_feather(dep_path + "water_quality.feather")

DEP_DATA = WEATHER_DEP.loc[
    pd.to_numeric(WEATHER_DEP["dep"], errors="coerce") < 97
].merge(ADEME_DEP, on="dep", how="inner") \
 .merge(FILO_DEP, on="dep", how="inner") \
 .dropna()

feather.write_feather(
    DEP_DATA,
    "results_building/dep-full.feather",
    compression="zstd"
)
