import polars as pl
import geopandas as gpd

def seqmco2details(df: pl.DataFrame) -> pl.DataFrame:
    rsa = pl.read_ipc("data/interim/rsa23_ano.feather")
    return (
        rsa.join(df, on=["seqmco", "finess"])
           .select(["anonyme", "seqmco", "finess", "sexe", "age", "codepost", "date_entree"])
    )

# incident visits
INCIDENT_VISITS = pl.read_ipc("results_building/t1d_e101-visits.feather")

# arrondissement centroids
gdf = gpd.read_file("data/external/arrondissements-version-simplifiee.geojson")
gdf = gdf[~gdf["code"].str.contains("2A|2B")]
gdf["lat"] = gdf.geometry.centroid.y
lat = gdf["lat"].to_list()

# DEP data with lags
dep = pl.read_parquet("data/external/dep-full.parquet")
dep = dep.with_columns((pl.col("inst") / (60 * 24)).alias("inst"))
dep = dep.sort(["dep", "month"])
for c in ["tm", "txab", "tnab", "umm", "rr", "inst"]:
    dep = dep.with_columns(pl.col(c).shift(1).over("dep").alias(f"{c}_lag"))

# population by department
dep_pop = (
    pl.read_parquet("data/external/arrondissement-full.parquet")
      .with_columns(pl.Series("lat", lat))
      .with_columns(pl.col("arr24").str.slice(0, 2).alias("dep"))
      .groupby("dep")
      .agg([
          pl.sum("pop_0029").alias("pop_0029"),
          pl.sum("pop_tot").alias("pop_tot"),
          pl.sum("pop_ftot").alias("pop_ftot"),
          pl.sum("pop_htot").alias("pop_htot"),
          pl.sum("n_equip_a2").alias("n_equip_d2"),
          pl.mean("lat").alias("lat"),
      ])
)

# reference counts
ref_count = (
    pl.read_ipc("data/interim/rsa23_ano.feather")
      .with_columns([
          pl.col("codepost").str.slice(0, 2).alias("dep"),
          pl.col("date_entree").str.slice(5, 2).alias("month")
      ])
      .groupby(["dep", "month"])
      .agg([
          pl.count().alias("visits"),
          (pl.col("sexe") == 1).sum().alias("visits_h")
      ])
      .with_columns((pl.col("visits") - pl.col("visits_h")).alias("visits_f"))
)

# vitamin D visits
vitd_visits = (
    pl.read_ipc("data/interim/all_diagnosis.feather")
      .filter(pl.col("diag").str.slice(0, 3) == "E55")
      .select(["seqmco", "finess"])
      .unique()
)
vitd = seqmco2details(vitd_visits)
vitd_summary = (
    vitd.with_columns([
            pl.col("codepost").str.slice(0, 2).alias("dep"),
            pl.col("date_entree").str.slice(5, 2).alias("month")
        ])
        .groupby(["dep", "month"])
        .agg([
            pl.count().alias("vitD_visits"),
            (pl.col("sexe") == 1).sum().alias("vitD_h")
        ])
        .with_columns((pl.col("vitD_visits") - pl.col("vitD_h")).alias("vitD_f"))
        .join(ref_count, on=["dep", "month"], how="left")
        .with_columns([
            (pl.col("vitD_visits") * 100 / pl.col("visits")).alias("vitD_def"),
            (pl.col("vitD_h") * 100 / pl.col("visits_h")).alias("vitD_def_h"),
            (pl.col("vitD_f") * 100 / pl.col("visits_f")).alias("vitD_def_f"),
        ])
)

# alcohol-related visits
alc_visits = (
    pl.read_ipc("data/interim/all_diagnosis.feather")
      .filter(pl.col("diag").str.slice(0, 4) == "F102")
      .select(["seqmco", "finess"])
      .unique()
)
alc = seqmco2details(alc_visits)
alc_summary = (
    alc.with_columns([
            pl.col("codepost").str.slice(0, 2).alias("dep"),
            pl.col("date_entree").str.slice(5, 2).alias("month")
        ])
       .groupby(["dep", "month"])
       .agg([
           pl.count().alias("alcohol_visits"),
           (pl.col("sexe") == 1).sum().alias("alcohol_hvisits")
       ])
       .with_columns((pl.col("alcohol_visits") - pl.col("alcohol_hvisits")).alias("alcohol_fvisits"))
       .join(ref_count, on=["dep", "month"], how="left")
       .with_columns([
           (pl.col("alcohol_visits") * 100 / pl.col("visits")).alias("alcohol_prop"),
           (pl.col("alcohol_hvisits") * 100 / pl.col("visits_h")).alias("alcohol_hprop"),
           (pl.col("alcohol_fvisits") * 100 / pl.col("visits_f")).alias("alcohol_fprop"),
       ])
       .select(["dep", "month", "alcohol_prop", "alcohol_hprop", "alcohol_fprop"])
)

# incident T1D under 30
inci = INCIDENT_VISITS.groupby("anonyme").agg(pl.all().first())
inci = (
    inci.filter(pl.col("age") < 30)
        .with_columns([
            pl.col("codepost").str.slice(0, 2).alias("dep"),
            pl.col("date_entree").str.slice(5, 2).alias("month")
        ])
)
inci_summary = (
    inci.groupby(["dep", "month"])
        .agg([
            pl.count().alias("t1d_all"),
            (pl.col("sexe") == 1).sum().alias("t1d_h")
        ])
        .with_columns((pl.col("t1d_all") - pl.col("t1d_h")).alias("t1d_f"))
)

# assemble final table
df = (
    dep.join(inci_summary, on=["dep", "month"], how="left")
       .with_columns([
           pl.col("t1d_all").fill_null(0),
           pl.col("t1d_h").fill_null(0),
           pl.col("t1d_f").fill_null(0)
       ])
       .join(dep_pop, on="dep", how="inner")
       .join(vitd_summary, on=["dep", "month"], how="left")
       .join(alc_summary, on=["dep", "month"], how="left")
       .with_columns([
           (pl.col("visits") * 1000 / pl.col("pop_tot")).alias("visits"),
           (pl.col("visits_f") * 1000 / pl.col("pop_ftot")).alias("visits_f"),
           (pl.col("visits_h") * 1000 / pl.col("pop_htot")).alias("visits_h"),
       ])
)

df.write_ipc("results_building/department_e101_0029.feather")
