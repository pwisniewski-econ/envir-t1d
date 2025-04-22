import polars as pl

# load inputs
VITAMINS_ALL     = pl.read_ipc("results_building/all-vitamins.feather")
VITAMINS_PREVALENT = pl.read_ipc("results_building/t1d-vitamins.feather")
VITAMINS_INCIDENT  = pl.read_ipc("results_building/t1d_e101-vitamins.feather")
POSTAL_CONVERTER = pl.read_parquet("data/external/postal_converter.parquet")

# equivalent of dplyr::summarise dropping only the last grouping level
def das_stats(df, population, reference):
    ref_count = pl.read_ipc(f"results_building/{reference}").filter(pl.col("age") < 30).height()
    return (
        df.filter(pl.col("age") < 30)
          .groupby("specific_diag")
          .agg(pl.col("anonyme").n_unique().alias("cnt"))
          .with_columns([
              pl.lit(population).alias("population"),
              (pl.col("cnt") / ref_count * 100).alias("prop")
          ])
    )

# ASSOCIATIONS
assoc = pl.concat([
    das_stats(VITAMINS_ALL,     "All Visits",           "all-individuals.feather"),
    das_stats(VITAMINS_INCIDENT,"T1D - E101",           "t1d_e101-individuals.feather"),
    das_stats(VITAMINS_PREVALENT,"T1D - All",            "t1d-individuals.feather")
])
assoc.write_csv("results_analysis/associations.csv")

# CASD CONTROL
pc = (
    POSTAL_CONVERTER
    .join(
        pl.read_ipc("results_building/arrondissement-pmsi_full.feather")
          .select("arr24"),
        on="arr24", how="inner"
    )
)
(
    VITAMINS_ALL
    .filter(pl.col("specific_diag") == "tabacco")
    .with_columns(pl.col("codepost").cast(pl.Int32))
    .join(pc, on="codepost", how="inner")
    .groupby(["arr24", "specific_diag"])
    .agg(pl.count().alias("count"))
    .filter(pl.col("count") >= 11)
    .write_csv("controle_casd_residu_dml.csv")
)

# TIME SERIES OF VISITS
def get_week(df, population):
    df2 = df.with_columns([
        pl.when(pl.col("modeentree") == "8").then("Home")
          .when(pl.col("modeentree") == "7").then("Transfer")
          .otherwise("Other")
          .alias("modeentree"),
        pl.col("date_entree").dt.week().alias("week")
    ])
    gp = (
        df2.groupby("modeentree")
           .agg(pl.count().alias("count"))
           .with_columns([
               (pl.col("count") / pl.col("count").sum() * 100).alias("prop"),
               pl.lit("modeentree").alias("variable"),
               pl.lit(population).alias("population"),
               pl.col("modeentree").alias("value")
           ])
    )
    gw = (
        df2.groupby("week")
           .agg(pl.count().alias("count"))
           .with_columns([
               (pl.col("count") / pl.col("count").sum() * 100).alias("prop"),
               pl.lit("week").alias("variable"),
               pl.lit(population).alias("population"),
               pl.col("week").alias("value")
           ])
    )
    return pl.concat([gp, gw])

prev = pl.read_ipc("results_building/t1d-visits.feather")
incd = pl.read_ipc("results_building/t1d_e101-visits.feather")
visits = pl.concat([
    get_week(pl.read_ipc("data/interim/rsa23_ano.feather"), "All Visits"),
    get_week(incd, "T1D - E101"),
    get_week(prev, "T1D - All")
])
visits.write_csv("results_analysis/visits_ts.csv")

# PREVALENCE BY DEP
full = pl.read_ipc("results_building/arrondissement-pmsi_full.feather").select([
    "arr24","t1d_e101","t1d","pop_0029","pop_0044"
])
dep = (
    full
    .with_columns(pl.col("arr24").str.slice(0,2).alias("dep"))
    .groupby("dep")
    .agg([
        pl.sum("t1d_e101").alias("t1d_e101"),
        pl.sum("t1d").alias("t1d"),
    ])
    .melt(id_vars="dep", value_vars=["t1d_e101","t1d"],
          variable_name="type", value_name="count")
    .filter(pl.col("count") >= 11)
)
dep.write_csv("results_analysis/t1d_levels_dep.csv")

arr = (
    full
    .melt(id_vars="arr24", value_vars=["t1d_e101","t1d"],
          variable_name="type", value_name="level")
    .filter(pl.col("level") >= 11)
)
arr.write_csv("results_analysis/t1d_levels_arr.csv")

# TOTAL COUNTS BY AGE/SEXE
def total_by(path, out):
    df = pl.read_ipc(path)
    cnt = (
        df.groupby(["age","sexe"])
          .agg(pl.count().alias("count"))
          .with_columns(pl.col("count").sum().over("age").alias("total"))
          .sort("age")
          .drop_nulls()
    )
    keep = (
        cnt.filter(pl.col("count") >= 11)
           .groupby("age")
           .agg(pl.count().alias("n"))
           .filter(pl.col("n") == 2)
           .select("age")
    )
    cnt.filter(pl.col("age").is_in(keep["age"])).write_csv(out)

total_by("results_building/t1d-individuals.feather", "results_analysis/total_t1d.csv")
total_by("results_building/t1d_e101-individuals.feather", "results_analysis/total_t1d_e101.csv")
