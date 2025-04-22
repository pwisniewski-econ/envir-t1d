import polars as pl

prevalent = pl.read_ipc("results_building/t1d-individuals.feather")
incident  = pl.read_ipc("results_building/t1d_e101-individuals.feather")
all_ind   = pl.read_ipc("results_building/all-individuals.feather")

malnutrition_codes = [f"E{n}" for n in range(40, 47)]

das_spec = (
    pl.read_ipc("data/interim/people_diagnosis.feather")
      .with_columns([
          # build a new column `specific_diag` by testing prefixes of `diag`
          pl.when( pl.col("diag").str.slice(0, 3) == "D52" ).then("Vitamin B9")
          .when( pl.col("diag").str.slice(0, 3) == "D51" ).then("Vitamin B12")
          .when( pl.col("diag").str.slice(0, 3) == "E55" ).then("Vitamin D")
          .when( pl.col("diag").str.slice(0, 3) == "E66" ).then("Overweight")
          .when( pl.col("diag").str.slice(0, 3).is_in(malnutrition_codes) ).then("Malnutrition")
          .when( pl.col("diag").str.slice(0, 4) == "F102" ).then("Alcohol")
          .when( pl.col("diag").str.slice(0, 4) == "F172" ).then("Tobacco")
          .when( pl.col("diag").str.slice(0, 4).is_in(["F112", "F132"]) ).then("Drugs")
          .when( pl.col("diag").str.slice(0, 5) == "E8351" ).then("Calcium")
          .when( pl.col("diag").str.slice(0, 4) == "E611" ).then("Iron")
          .when( pl.col("diag").str.slice(0, 3) == "E61" ).then("Other Minerals")
          .when( pl.col("diag").str.slice(0, 3) == "Z56" ).then("Employment")
          .when( pl.col("diag").str.slice(0, 3) == "Z59" ).then("Economics")
          .when( pl.col("diag").str.slice(0, 3) == "Z60" ).then("Social")
          .when( pl.col("diag").str.slice(0, 3) == "Z63" ).then("Family")
          .when( pl.col("diag").str.slice(0, 4) == "E780" ).then("Cholesterol")
          .otherwise(None)
          .alias("specific_diag")
      ])
      .filter(pl.col("specific_diag").is_not_null())
      .select(["anonyme", "specific_diag"])
      .unique()
)

def add_specific(data: pl.DataFrame, cohort: pl.DataFrame) -> pl.DataFrame:
    return (
        data
        .join(cohort.select("anonyme"), on="anonyme", how="inner")
        .unique(subset="anonyme")
    )

vitamins_all       = add_specific(das_spec, all_ind)
vitamins_prevalent = add_specific(das_spec, prevalent)
vitamins_incident  = add_specific(das_spec, incident)

vitamins_all.write_ipc("results_building/all-vitamins.feather",       compression="zstd")
vitamins_prevalent.write_ipc("results_building/t1d-vitamins.feather", compression="zstd")
vitamins_incident.write_ipc("results_building/t1d_e101-vitamins.feather", compression="zstd")
