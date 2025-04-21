# ---- Import Libraries ----
import pandas as pd
import pyarrow.feather as feather
import pyarrow.dataset as ds

# ---- Fonction de lecture des diagnostics ----
def read_diag(filename, var):
    path = f"data/interim/{filename}"
    dataset = ds.dataset(path, format="feather")
    
    if isinstance(var, dict):
        varname = list(var.values())[0]
    else:
        varname = var

    df = dataset.to_table(columns=["anonyme", "seqmco", "finess", varname]).to_pandas()
    df = df[df[varname] != ""].copy()
    df.rename(columns={varname: "diag"}, inplace=True)
    
    return df

# ---- Lecture des diagnostics ----
ALL_DIAG = pd.concat([
    read_diag("das23_ano.feather", "diag"),
    read_diag("rsa23_ano.feather", {"diag": "dp"}),
    read_diag("rsa23_ano.feather", {"diag": "dr"})
], ignore_index=True).drop_duplicates()

# ---- Export intermédiaire ----
feather.write_feather(ALL_DIAG, "data/interim/all_diagnosis.feather", compression="zstd")

# ---- Réduction aux colonnes utiles ----
ALL_DIAG = ALL_DIAG[["anonyme", "diag"]].drop_duplicates()

# ---- Export final ----
feather.write_feather(ALL_DIAG, "data/interim/people_diagnosis.feather", compression="zstd")