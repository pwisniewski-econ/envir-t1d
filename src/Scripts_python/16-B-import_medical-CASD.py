# ---- Import Libraries ----
import pandas as pd
import pyarrow.feather as feather
import gc

# ---- Fonction d'importation et d'écriture ----
def import_write(file):
    import_path = f"data/pmsi/{file}.csv"
    export_path = f"data/interim/{file}.feather"

    df = pd.read_csv(import_path)
    feather.write_feather(df, export_path, compression="zstd")
    gc.collect()

# ---- Définition des fichiers à importer ----
files = ["act23", "ano23", "das23", "pgv23", "rdth23", "rsa23", "rum23"]

# ---- Importation des fichiers ----
for f in files:
    import_write(f)

# ---- Export spécifique à certains fichiers ----
for f in ["ano23", "act23"]:
    df = pd.read_csv(f"data/pmsi/{f}.csv")
    feather.write_feather(df, f"data/interim/{f.split('23')[0]}.feather", compression="zstd")