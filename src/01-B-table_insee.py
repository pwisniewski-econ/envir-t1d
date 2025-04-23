# ---- Import Libraries ----
import pandas as pd
from pathlib import Path

# Title: PREMIUM Passage Table
# Script : France

# Setup ----
path_insee_tables = Path("data/external/insee-table_de_passage/")
output_dir = Path("results_building")
output_dir.mkdir(parents=True, exist_ok=True)

# Arrondissement municipal to commune level ----
arr2com = (
    pd.read_excel(
        path_insee_tables / "table-appartenance-geo-communes-24.xlsx",
        sheet_name="ARM",
        skiprows=5,
        dtype={"COM": str, "CODGEO": str}
    )
    .loc[:, ["COM", "CODGEO"]]
    .rename(columns={"COM": "code_com", "CODGEO": "code_arr"})
)

# Yearly table de passage ----
table_passage_df = (
    pd.read_excel(
        path_insee_tables / "table_passage_annuelle_2024.xlsx",
        sheet_name="COM",
        skiprows=5
    )
)

# Select columns starting with CODGEO_201 or CODGEO_202
cols_to_keep = [c for c in table_passage_df.columns if c.startswith("CODGEO_201") or c.startswith("CODGEO_202")]
table_passage_df = table_passage_df.loc[:, cols_to_keep]

# Clean column names
table_passage_df.columns = [
    col.replace("CODGEO_20", "code_insee").lower()
    for col in table_passage_df.columns
]

# Commune membership for 2024
app_com24 = (
    pd.read_excel(
        path_insee_tables / "table-appartenance-geo-communes-24.xlsx",
        sheet_name="COM",
        skiprows=5
    )
    .loc[:, ["CODGEO", "ARR", "BV2022"]]
    .rename(columns={
        "CODGEO": "code_insee24",
        "ARR": "arr24",
        "BV2022": "bv2022"
    })
)

# Yearly passage with Arrondissement ----
# prepare for export (Merge, filter, reorder, and deduplicate)
merged = (
    table_passage_df
    .merge(app_com24, left_on="code_insee24", right_on="code_insee24", how="left")
    .assign(
        code_insee24=lambda df: df["code_insee24"],
        arr24=lambda df: df["arr24"],
        bv2022=lambda df: df["bv2022"]
    )
    .loc[:, ["code_insee24", "arr24", "bv2022"] + 
         [c for c in table_passage_df.columns if c != "code_insee24"]]
    .dropna(subset=["arr24", "bv2022"])
    .drop_duplicates()
)

# Export ----
merged.reset_index(drop=True).to_feather(
    output_dir / "t_passage.feather", compression="zstd"
)
arr2com.to_feather(
    output_dir / "arr2com.feather", compression="zstd"
)
