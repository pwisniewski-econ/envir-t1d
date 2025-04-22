import pandas as pd
import pyarrow.feather as feather
from pathlib import Path
import re

# --- Chargement table de passage ---
table_passage_df = feather.read_feather("results_building/t_passage.feather")[["code_insee24", "arr24", "bv2022"]].drop_duplicates()

# --- Chargement données INSEE population ---
pop_data = pd.read_csv(
    "data/external/insee-population/base-cc-evol-struct-pop-2021.csv.gz",
    compression="gzip",
    sep=";",
    dtype={"CODGEO": str},
    low_memory=False
)

# --- Fonction principale d'agrégation ---
def pop_sum(df, level):
    df = df.rename(columns={"CODGEO": "code_insee24"})
    df = df.merge(table_passage_df, on="code_insee24", how="left")
    df = df[df[level].notna()]

    grouped = df.groupby(level, as_index=False).agg({
        "P21_POP0014": "sum", "P21_POP1529": "sum", "P21_POP3044": "sum",
        "P21_POP4559": "sum", "P21_POP6074": "sum", "P21_POP7589": "sum", "P21_POP90P": "sum"
    })

    grouped["pop_0029"] = grouped["P21_POP0014"] + grouped["P21_POP1529"]
    grouped["pop_0044"] = grouped["pop_0029"] + grouped["P21_POP3044"]
    grouped["pop_tot"] = grouped["pop_0044"] + grouped["P21_POP4559"] + grouped["P21_POP6074"] + grouped["P21_POP7589"] + grouped["P21_POP90P"]

    return grouped.rename(columns={
        "P21_POP0014": "pop_0014",
        "P21_POP1529": "pop_1529",
        "P21_POP3044": "pop_3044",
        "P21_POP4559": "pop_4559",
        "P21_POP6074": "pop_6074",
        "P21_POP7589": "pop_7589",
        "P21_POP90P": "pop_90p"
    })

# --- Fonction de filtrage homme/femme ---
def filter_pop_sum(df, sex_prefix, level):
    selected_cols = [col for col in df.columns if col.startswith(f"P21_{sex_prefix}")]
    renamed_cols = [re.sub(sex_prefix, "POP", col) for col in selected_cols]

    filtered_df = df[["CODGEO"] + selected_cols].copy()
    filtered_df.columns = ["CODGEO"] + renamed_cols
    return pop_sum(filtered_df, level).rename(columns={
        "pop_0029": f"pop_{sex_prefix.lower()}0029",
        "pop_0044": f"pop_{sex_prefix.lower()}0044",
        "pop_tot": f"pop_{sex_prefix.lower()}tot"
    })[[level, f"pop_{sex_prefix.lower()}0029", f"pop_{sex_prefix.lower()}0044", f"pop_{sex_prefix.lower()}tot"]]

# --- Traitement ARR24 ---
pop_arr = pop_sum(pop_data, "arr24")
pop_h_arr = filter_pop_sum(pop_data, "H", "arr24")
pop_f_arr = filter_pop_sum(pop_data, "F", "arr24")

pop_arr21 = pop_arr.merge(pop_h_arr, on="arr24").merge(pop_f_arr, on="arr24")

# --- Traitement BV2022 ---
pop_bv = pop_sum(pop_data, "bv2022")
pop_h_bv = filter_pop_sum(pop_data, "H", "bv2022")
pop_f_bv = filter_pop_sum(pop_data, "F", "bv2022")

pop_bv21 = pop_bv.merge(pop_h_bv, on="bv2022").merge(pop_f_bv, on="bv2022")

# --- Export des fichiers ---
feather.write_feather(pop_arr21, "data/interim/arrondissements/populations_2021.feather", compression="zstd")
feather.write_feather(pop_bv21, "data/interim/bv2022/populations_2021.feather", compression="zstd")