import polars as pl

rsa = pl.read_ipc("data/interim/rsa23_ano.feather")
t1d = (
    pl.read_ipc("data/interim/all_diagnosis.feather")
      .filter(pl.col("diag").is_in([f"E{n}" for n in range(100,110)]))
)

# helpers
people = lambda df: (
    df.select("anonyme").unique()
      .join(rsa.select(["anonyme","age","sexe","codepost"]), on="anonyme")
      .unique(subset="anonyme")
)
visits = lambda df: (
    df.select(["seqmco","finess"]).unique()
      .join(rsa.select(["anonyme","seqmco","finess","sexe","age","codepost","date_entree","modeentree"]),
            on=["seqmco","finess"])
)

# cohorts
prevalent_inds  = people(t1d)
incident_inds   = people(t1d.filter(pl.col("diag")=="E101"))
prevalent_vis   = visits(t1d)
incident_vis    = visits(t1d.filter(pl.col("diag")=="E101"))
all_inds        = rsa.select(["anonyme","age","sexe","codepost"]).unique(subset="anonyme")

# write out
for name, df in [
    ("t1d-individuals", prevalent_inds),
    ("t1d_e101-individuals", incident_inds),
    ("t1d-visits",      prevalent_vis),
    ("t1d_e101-visits", incident_vis),
    ("all-individuals",  all_inds),
]:
    df.write_ipc(f"results_building/{name}.feather", compression="zstd")
