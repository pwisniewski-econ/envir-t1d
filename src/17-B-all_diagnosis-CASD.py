import polars as pl

#---- Diagnostics reading  ----
sources = [("das23_ano.feather", "diag"),
           ("rsa23_ano.feather", "dp"),
           ("rsa23_ano.feather", "dr")]

# ---- Selection and Export ----

all_diag = pl.concat([
    pl.read_ipc(f"data/interim/{fn}")
      .select(["anonyme","seqmco","finess", c])
      .rename({c: "diag"})
      .filter(pl.col("diag") != "")
    for fn, c in sources
]).unique()

all_diag.write_ipc("data/interim/all_diagnosis.feather", compression="zstd")

all_diag.select(["anonyme","diag"])\
        .unique()\
        .write_ipc("data/interim/people_diagnosis.feather", compression="zstd")
