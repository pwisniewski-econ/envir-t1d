# ---- Import Libraries ----
import pandas as pd
import pyarrow.feather as feather
import pyarrow.parquet as pq
import pyarrow.dataset as ds

# ---- Chargement des données ----
ALL_INDIVIDUALS = ds.dataset("results_building/all-individuals.feather", format="feather")

VITAMINS_ALL = feather.read_feather("results_building/all-vitamins.feather")
VITAMINS_ALL["specific_diag"] = (
    VITAMINS_ALL["specific_diag"].str.lower().str.replace(" ", "-", regex=False)
)   

POSTAL_CONVERTER = pq.read_table("data/external/postal_converter.parquet").to_pandas()

# ---- Fonctions auxiliaires ----
def get_tld_count(file, age_filt, gender_ls, level):
    dataset = ds.dataset(f"results_building/{file}", format="feather")
    df = dataset.to_table().to_pandas()
    df = df[(df["age"] < age_filt) & (df["sexe"].isin(gender_ls))]
    df["codepost"] = df["codepost"].astype(int)
    df = df.merge(POSTAL_CONVERTER, on="codepost", how="left")
    return df.groupby(level).size().reset_index(name="tld")

def get_vitamins(age_filt, gender_ls, level):
    ref_df = (
        ALL_INDIVIDUALS.to_table()
        .to_pandas()
        .query("age < @age_filt and sexe in @gender_ls")
        .groupby("codepost")
        .size()
        .reset_index(name="reference")
    )

    vit_df = (
        VITAMINS_ALL.query("age < @age_filt and sexe in @gender_ls")
        .groupby(["specific_diag", "codepost"])
        .size()
        .reset_index(name="cnt")
    )

    merged = pd.merge(vit_df, ref_df, on="codepost", how="outer")
    merged["codepost"] = merged["codepost"].astype(int)
    merged = merged.merge(POSTAL_CONVERTER, on="codepost", how="left")

    df = (
        merged.groupby([level, "specific_diag"])
        .agg(diag=("cnt", lambda x: (x.sum() / merged["reference"].sum()) * 100))
        .reset_index()
    )

    pivot = df.pivot(index=level, columns="specific_diag", values="diag").fillna(0)
    pivot.columns = [f"diag_{col}" for col in pivot.columns]
    pivot = pivot.reset_index()

    return pivot

def import_data(file, age_filt, gender_ls, level, join_key):
    df = pq.read_table(f"data/external/{file}").to_pandas()

    df = df.merge(get_vitamins(age_filt, gender_ls, level), on=join_key, how="left")

    tld_e101 = get_tld_count("tld_e101-individuals.feather", age_filt, gender_ls, level).rename(columns={"tld": "tld_e101"})
    tld_all = get_tld_count("tld-individuals.feather", age_filt, gender_ls, level)

    df = df.merge(tld_e101, on=join_key, how="left")
    df = df.merge(tld_all, on=join_key, how="left")

    for col in ["tld", "tld_e101"]:
        df[col] = df[col].fillna(0)

    return df

# ---- Génération des jeux de données ----
ARR_FULL   = import_data("arrondissement-full.parquet", 30, [1, 2], "arr24", "arr24")
ARR_MALE   = import_data("arrondissement-full.parquet", 30, [1], "arr24", "arr24")
ARR_FEMALE = import_data("arrondissement-full.parquet", 30, [2], "arr24", "arr24")

BV22_FULL   = import_data("bv2022-full.parquet", 30, [1, 2], "bv2022", "bv2022")
BV22_MALE   = import_data("bv2022-full.parquet", 30, [1], "bv2022", "bv2022")
BV22_FEMALE = import_data("bv2022-full.parquet", 30, [2], "bv2022", "bv2022")

# ---- Export ----
feather.write_feather(ARR_FULL, "results_building/arrondissement-pmsi-full.feather", compression="zstd")
feather.write_feather(ARR_MALE, "results_building/arrondissement-pmsi-male.feather", compression="zstd")
feather.write_feather(ARR_FEMALE, "results_building/arrondissement-pmsi-female.feather", compression="zstd")
feather.write_feather(BV22_FULL, "results_building/bv2022-pmsi-full.feather", compression="zstd")
feather.write_feather(BV22_MALE, "results_building/bv2022-pmsi-male.feather", compression="zstd")
feather.write_feather(BV22_FEMALE, "results_building/bv2022-pmsi-female.feather", compression="zstd")