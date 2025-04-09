import pandas as pd
import pyarrow.feather as feather
from pathlib import Path

path_insee_tables = "data/external/insee-table_de_passage/"

# Lecture des fichiers INSEE
arr2com = pd.read_excel(
    f"{path_insee_tables}table-appartenance-geo-communes-24.xlsx",
    sheet_name="ARM",
    skiprows=5
)[["COM", "CODGEO"]].rename(columns={"COM": "code_com", "CODGEO": "code_arr"})

table_passage_df = pd.read_excel(
    f"{path_insee_tables}table_passage_annuelle_2024.xlsx",
    sheet_name="COM",
    skiprows=5
)

# Renommage col
table_passage_df.columns = (
    table_passage_df.columns
    .str.replace("CODGEO_20", "code_insee", regex=False)
    .str.lower()
)

app_com24 = pd.read_excel(
    f"{path_insee_tables}table-appartenance-geo-communes-24.xlsx",
    sheet_name="COM",
    skiprows=5
)[["CODGEO", "ARR", "BV2022"]].rename(columns={
    "CODGEO": "code_insee24",
    "ARR": "arr24",
    "BV2022": "bv2022"
})

# Jointure
table_passage_df = table_passage_df.merge(app_com24, on="code_insee24", how="left")
table_passage_df = (
    table_passage_df[~table_passage_df["arr24"].isna() & ~table_passage_df["bv2022"].isna()]
    .drop_duplicates()
)[["code_insee24", "arr24", "bv2022"] + [col for col in table_passage_df.columns if col.startswith("code_insee")]]

Path("results_building").mkdir(parents=True, exist_ok=True)

feather.write_feather(table_passage_df, "results_building/t_passage.feather", compression="zstd")
feather.write_feather(arr2com, "results_building/arr2com.feather", compression="zstd")