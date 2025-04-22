# ---- Import Libraries ----
import polars as pl
import pyarrow.feather as feather

# ─── Load TABLE_PASSAGE_DF ─────
TABLE_PASSAGE_DF = (
    pl.read_ipc("results_building/t_passage.feather")
      .select(["code_insee24", "arr24", "bv2022"])
      .unique()
)

# ─── Load ARR2COM ───────
ARR2COM = pl.read_ipc("results_building/arr2com.feather")

# ─── Load & preprocess SIRENE_DT ─────
raw = pl.read_csv(
    "data/external/insee-sirene/StockEtablissement_utf8_122024.csv",
    separator=",",
    encoding="utf8",
    low_memory=False,
    infer_schema_length=None
)

SIRENE_DT = (
    raw
    .filter(pl.col("codeCommuneEtablissement") != "")
    .with_columns([
        pl.col("nic").cast(pl.Int64).alias("nic"),
        (pl.col("etatAdministratifEtablissement") == "A").alias("active"),

        # <-- corrected here:
        pl.col("dateCreationEtablissement")
          .str.strptime(pl.Date, "%Y-%m-%d")
          .alias("opened"),

        pl.when(pl.col("etatAdministratifEtablissement") != "A")
          .then(
            pl.col("dateDebut")
              .str.strptime(pl.Date, "%Y-%m-%d")
          )
          .otherwise(None)
          .alias("closed"),

        pl.col("libelleVoieEtablissement")
          .str.replace("-", " ")
          .alias("road_name"),
    ])
    .select([
        "siren",
        "nic",
        "siret",
        pl.col("activitePrincipaleEtablissement").alias("ape"),
        "active",
        pl.col("trancheEffectifsEtablissement").alias("eff_tranche"),
        "opened",
        "closed",
        pl.col("numeroVoieEtablissement").alias("road_number"),
        pl.col("typeVoieEtablissement").alias("road_type"),
        "road_name",
        pl.col("codePostalEtablissement").alias("postal_code"),
        pl.col("complementAdresseEtablissement").alias("adress_addition"),
        pl.col("libelleCommuneEtablissement").alias("city"),
        pl.col("codeCommuneEtablissement").alias("insee_code"),
        pl.col("coordonneeLambertAbscisseEtablissement").alias("lambert_x"),
        pl.col("coordonneeLambertOrdonneeEtablissement").alias("lambert_y"),
    ])
)

# ─── Join ARR2COM → override insee_code when mapping exists ────────
SIRENE_DT = (
    SIRENE_DT
      .join(ARR2COM, left_on="insee_code", right_on="code_arr", how="left")
      .with_columns([
          pl.when(pl.col("code_com").is_null())
            .then(pl.col("insee_code"))
            .otherwise(pl.col("code_com"))
            .alias("insee_code")
      ])
      .drop(["code_com"])
)

# ─── Define count_etabs (mimics data.table foverlaps) ────────
def count_etabs(df: pl.DataFrame) -> pl.DataFrame:
    # missing closed dates
    df2 = (
        df
        .with_columns([
            pl.when(pl.col("closed").is_null())
              .then(pl.lit("2100-01-01").str.strptime(pl.Date, "%Y-%m-%d"))
              .otherwise(pl.col("closed"))
              .alias("closed")
        ])
        .filter(pl.col("closed") > pl.col("opened"))
    )

    # years table 
    years = (
        pl.DataFrame({"year": list(range(2016, 2025))})
          .with_columns([
              (pl.col("year").cast(pl.Utf8) + "-01-01")
                .str.strptime(pl.Date, "%Y-%m-%d")
                .alias("start"),
              (pl.col("year").cast(pl.Utf8) + "-12-31")
                .str.strptime(pl.Date, "%Y-%m-%d")
                .alias("end"),
          ])
    )

    # overlaps via cross‐join + filter
    overlaps = (
        df2
        .join(years, how="cross")
        .filter(
            (pl.col("opened") <= pl.col("end")) &
            (pl.col("closed") >= pl.col("start"))
        )
    )

    # count per (year, insee_code)
    return (
        overlaps
        .group_by(["year", "insee_code"])
        .agg(pl.count().alias("N"))
        .sort(["insee_code", "year"])
    )


# ─── Equivalent of sirene_type (per APE code) ───────────
def sirene_type(ape_code: str) -> pl.DataFrame:
    df_ape = SIRENE_DT.filter(pl.col("ape") == ape_code)
    counted = count_etabs(df_ape)
    return counted.with_columns([
        pl.lit(ape_code.lower().replace(".", "")).alias("ape")
    ])

# ───  Combine all APE codes ─────────────
ape_ls = ["56.10C", "93.12Z", "56.30Z", "47.73Z",
          "47.23Z", "47.22Z", "47.11A", "10.71C"]
RESULTS = pl.concat([sirene_type(a) for a in ape_ls])

# ─── 8. Define siren_sum (group + pivot) ──────────
def siren_sum(data: pl.DataFrame, level: str) -> pl.DataFrame:
    joined = data.join(
        TABLE_PASSAGE_DF,
        left_on="insee_code", right_on="code_insee24",
        how="left"
    )
    agg = (
        joined
        .group_by([level, "year", "ape"])
        .agg(pl.sum("N").alias("N"))
    )
    wide = (
        agg
        .pivot(
            values="N",
            index=[level, "year"],
            columns="ape"
        )
        .fill_null(0)
        .sort([level, "year"])
    )
    rename_map = {
        col: f"n_ape{col}"
        for col in wide.columns
        if col not in {level, "year"}
    }
    return wide.rename(rename_map)

# ─── Build the two tables ───────
RESULTS_ARR = siren_sum(RESULTS, "arr24").filter(pl.col("arr24").is_not_null())
RESULTS_BV22 = siren_sum(RESULTS, "bv2022").filter(pl.col("bv2022").is_not_null())

# Export files
feather.write_feather(
    RESULTS_ARR.to_arrow(),
    "data/interim/arrondissements/sirene_1624.feather",
    compression="zstd"
)
feather.write_feather(
    RESULTS_BV22.to_arrow(),
    "data/interim/bv2022/sirene_1624.feather",
    compression="zstd"
)
