import polars as pl
import pyarrow.feather as feather
from pathlib import Path


# Read support_tables
table_passage = (
    pl.read_ipc("results_building/t_passage.feather")
    .select(["code_insee24", "arr24", "bv2022", "code_insee21"])
)
arr2com = pl.read_ipc("results_building/arr2com.feather")

# full DPE dataset
dpe_all = pl.read_ipc("data/interim/_ademe/dpe_all.feather")

# building-associated IDs
old_replaced = (
    dpe_all
    .select("ndperemplac")
    .unique()
    .filter(pl.col("ndperemplac") != "")
    .to_series()
    .to_list()
)
old_building = (
    dpe_all
    .select("ndpeimmeubleassoci")
    .unique()
    .filter(pl.col("ndpeimmeubleassoci") != "")
    .to_series()
    .to_list()
)
old_filter = pl.DataFrame({
    "dpe_id": old_replaced + old_building,
    "filt": True
})

# Preprocess DPE data
dpe = (
    dpe_all
    .filter(pl.col("modledpe") == "DPE 3CL 2021 méthode logement")
    .select([
        pl.col("ndpe").alias("dpe_id"),
        pl.col("ndperemplac").alias("dpe_replaced"),
        pl.col("ndpeimmeubleassoci").alias("dpe_building"),
        pl.col("datevisitediagnostiqueur").alias("date_visited"),
        pl.col("datetablissementdpe").alias("date_established"),
        pl.col("modledpe").alias("type"),
        pl.col("surfacehabitablelogement").alias("floor_area"),
        pl.col("surfaceclimatise").alias("ac_area"),
        pl.col("etiquetteges").alias("ghg_grade"),
        pl.col("etiquettedpe").alias("dpe_grade"),
        pl.col("anneconstruction").alias("year_built"),
        pl.col("priodeconstruction").alias("period_built"),
        pl.col("codeinseeban").alias("insee_code"),
        pl.col("typenergieprincipalechauffage").alias("heating_energy"),
        pl.col("typeinstallationchauffage").alias("heating_type"),
    ])
    .with_columns([
        pl.col("insee_code").str.replace_all("old", "").str.replace_all(" ", "").alias("insee_code")
    ])
    .join(arr2com, left_on="insee_code", right_on="code_arr", how="left")
    .with_columns([
        pl.when(pl.col("code_com").is_null())
        .then(pl.col("insee_code"))
        .otherwise(pl.col("code_com"))
        .alias("insee_code")
    ])
    .drop(["code_com"])
    .join(table_passage, left_on="insee_code", right_on="code_insee21", how="left")
)

# Exclude old entries
dpe = (
    dpe
    .join(old_filter, on="dpe_id", how="left")
    .filter(pl.col("filt").is_null())
    .drop("filt")
)

# Create mapping tables
period_map_df = pl.DataFrame({
    "period_built": [
        "1948-1974", "1975-1977", "1978-1982", "1983-1988",
        "1989-2000", "2001-2005", "2006-2012", "2013-2021",
        "après 2021", "avant 1948"
    ],
    "period": [1961, 1976, 1980, 1987, 1995, 2003, 2009, 2016, 2022, 1935]
})
grade_map_df = pl.DataFrame({
    "dpe_grade": list("ABCDEFG"),
    "dpe_grade_num": list(range(1, 8))
})

# Map construction periods and grades via joins
dpe = (
    dpe
    .join(period_map_df, on="period_built", how="left")
    .join(grade_map_df, on="dpe_grade", how="left")
)

# Period building
dpe = (
    dpe
    .with_columns([
        pl.col("ac_area").fill_null(0),
        pl.when(pl.col("year_built").is_null())
            .then(pl.col("period"))
            .otherwise(pl.col("year_built"))
            .alias("year_built")
    ])
)

# Define grouping function
def group_dpe(df: pl.DataFrame, groupvar: str) -> pl.DataFrame:
    agg = df.group_by(groupvar).agg([
        pl.col("year_built").median().alias("built_q2"),
        pl.col("floor_area").median().alias("floor_area_q2"),
        pl.col("dpe_grade_num").quantile(0.25).round().alias("q1_dpe"),
        pl.col("dpe_grade_num").quantile(0.5).round().alias("q2_dpe"),
        pl.col("dpe_grade_num").quantile(0.75).round().alias("q3_dpe"),
        pl.len().alias("n_dpe"),
    ])
    ac = df.filter(pl.col("ac_area") > 0).group_by(groupvar).agg([
        pl.len().alias("ac_count")
    ])
    res = (
        agg.join(ac, on=groupvar, how="left")
            .with_columns([
                (pl.col("ac_count") / pl.col("n_dpe")).alias("ac_prop"),
                (pl.col("q3_dpe") - pl.col("q1_dpe")).alias("iqr_dpe_num")
            ])
    ).drop("ac_count")
    return res

# Generate summaries
OLD_DPE_ARR = (
    group_dpe(dpe, "arr24")
    .filter(pl.col("arr24").str.len_chars() < 4)
    .sort("arr24")
)
OLD_DPE_BV = (
    group_dpe(dpe, "bv2022")
    .filter(pl.col("bv2022").is_not_null())
    .sort("bv2022")
)
dep_df = dpe.with_columns([pl.col("arr24").str.slice(0, 2).alias("dep")])
OLD_DPE_DEP = (
    group_dpe(dep_df, "dep")
    .filter(pl.col("dep").is_not_null())
    .sort("dep")
)

# Export files
for df_out, subdir in [
    (OLD_DPE_ARR, "arrondissements"),
    (OLD_DPE_BV, "bv2022"),
    (OLD_DPE_DEP, "departement"),
]:
    out_dir = Path(f"data/interim/{subdir}")
    out_dir.mkdir(parents=True, exist_ok=True)
    feather.write_feather(df_out.to_arrow(), out_dir / "ademe_dpe.feather", compression="zstd")


