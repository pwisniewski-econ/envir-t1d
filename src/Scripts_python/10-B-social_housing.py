import pandas as pd
import pyarrow.feather as feather
from pathlib import Path

# Tables de passage
table_passage_df = feather.read_feather("results_building/t_passage.feather")[
    ["code_insee22", "arr24", "bv2022"]
].drop_duplicates().groupby("code_insee22").head(1)

arr2com = feather.read_feather("results_building/arr2com.feather")

arr2com = arr2com.merge(table_passage_df, left_on="code_com", right_on="code_insee22", how="left")[
    ["code_arr", "arr24", "bv2022"]
].rename(columns={"code_arr": "code_insee22"})

# Assemblage final table passage
table_passage_df = pd.concat([table_passage_df, arr2com], ignore_index=True)

# Lecture données logement sociaux
rpls = pd.read_excel("data/external/medd-logements_sociaux/resultats_rpls_2022.xlsx",
                     sheet_name="Commune", skiprows=4, header=None)

rpls = rpls.iloc[:, [3, 9, 14, 15, 65, 124]]
rpls.columns = ["code_insee22", "n_social", "social_houses", "social_appartments", "social_average_age", "mean_rent_m2"]

# Jointure avec géographie
rpls = table_passage_df.merge(rpls, on="code_insee22", how="left")
rpls = rpls.fillna({"n_social": 0, "social_houses": 0, "social_appartments": 0})

# Fonction d’agrégation
def social_sum(df, level):
    grouped = df.groupby(level, as_index=False).agg(
        social_average_age=("social_average_age", lambda x: (x * df.loc[x.index, "n_social"]).sum() / max(df.loc[x.index, "n_social"].sum(), 1)),
        mean_rent_m2=("mean_rent_m2", lambda x: (x * df.loc[x.index, "n_social"]).sum() / max(df.loc[x.index, "n_social"].sum(), 1)),
        n_social=("n_social", "sum"),
        social_houses=("social_houses", "sum"),
        social_appartments=("social_appartments", "sum")
    )
    return grouped

# Agrégation arrondissement
rpls_arr = social_sum(rpls, "arr24")
rpls_arr = rpls_arr[rpls_arr["arr24"].str.len() < 4]

# Agrégation BV2022
rpls_bv22 = social_sum(rpls, "bv2022")

# Export
Path("data/interim/arrondissements").mkdir(parents=True, exist_ok=True)
Path("data/interim/bv2022").mkdir(parents=True, exist_ok=True)

feather.write_feather(rpls_arr, "data/interim/arrondissements/social_housing.feather", compression="zstd")
feather.write_feather(rpls_bv22, "data/interim/bv2022/social_housing.feather", compression="zstd")