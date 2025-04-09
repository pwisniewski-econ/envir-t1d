import pandas as pd
import pyarrow.feather as feather
from pathlib import Path

# Tables de passage
table_passage_df = feather.read_feather("results_building/t_passage.feather")[
    ['code_insee24', 'arr24', 'bv2022']
].drop_duplicates()

arr2com = feather.read_feather("results_building/arr2com.feather")

# Chargement données SIRENE
sirene_path = "data/external/insee-sirene/StockEtablissement_utf8_122024.csv"
sirene = pd.read_csv(sirene_path, dtype=str)
sirene = sirene[sirene["codeCommuneEtablissement"].notna()]

sirene_dt = pd.DataFrame({
    "siren": sirene["siren"],
    "nic": pd.to_numeric(sirene["nic"], errors='coerce'),
    "siret": sirene["siret"],
    "ape": sirene["activitePrincipaleEtablissement"],
    "active": sirene["etatAdministratifEtablissement"] == "A",
    "eff_tranche": sirene["trancheEffectifsEtablissement"],
    "opened": pd.to_datetime(sirene["dateCreationEtablissement"], errors='coerce'),
    "closed": pd.to_datetime(sirene["dateDebut"], errors='coerce'),
    "road_number": sirene["numeroVoieEtablissement"],
    "road_type": sirene["typeVoieEtablissement"],
    "road_name": sirene["libelleVoieEtablissement"].str.replace("-", " ", regex=False),
    "postal_code": sirene["codePostalEtablissement"],
    "adress_addition": sirene["complementAdresseEtablissement"],
    "city": sirene["libelleCommuneEtablissement"],
    "insee_code": sirene["codeCommuneEtablissement"],
    "lambert_x": sirene["coordonneeLambertAbscisseEtablissement"],
    "lambert_y": sirene["coordonneeLambertOrdonneeEtablissement"]
})

# Correction des codes INSEE pour arrondissements
sirene_dt = arr2com.rename(columns={"code_arr": "insee_code"}).merge(
    sirene_dt, on="insee_code", how="right"
)
sirene_dt["insee_code"] = sirene_dt["code_com"].fillna(sirene_dt["insee_code"])

# Fonction pour compter les établissements par an
def count_etabs(df):
    df["closed"] = df["closed"].fillna(pd.Timestamp("2100-01-01"))
    df = df[df["closed"] > df["opened"]]

    years = pd.date_range(start="2016-01-01", end="2024-12-31", freq="YS")
    all_years = []

    for year in years.year:
        mask = (df["opened"] <= f"{year}-12-31") & (df["closed"] >= f"{year}-01-01")
        counts = df[mask].groupby("insee_code").size().reset_index(name="N")
        counts["year"] = year
        all_years.append(counts)

    return pd.concat(all_years)

# Fonction pour extraire un code APE
def sirene_type(ape_code):
    filtered = sirene_dt[sirene_dt["ape"] == ape_code]
    df = count_etabs(filtered)
    df["ape"] = ape_code.lower().replace(".", "")
    return df

# Fonction d'agrégation par niveau géographique
def siren_sum(df, level):
    df = df.merge(table_passage_df, on="insee_code", how="left")
    grouped = df.groupby([level, "year", "ape"], as_index=False)["N"].sum()
    pivoted = grouped.pivot_table(index=[level, "year"], columns="ape", values="N", fill_value=0)
    pivoted.columns = [f"n_ape{col}" for col in pivoted.columns]
    return pivoted.reset_index()

# APE
ape_ls = ["56.10C", "93.12Z", "56.30Z", "47.73Z", "47.23Z", "47.22Z", "47.11A", "10.71C"]

# Traitement
results = pd.concat([sirene_type(ape) for ape in ape_ls])

results_arr = siren_sum(results, "arr24")
results_bv22 = siren_sum(results, "bv2022")

# Exports
Path("data/interim/arrondissements").mkdir(parents=True, exist_ok=True)
Path("data/interim/bv2022").mkdir(parents=True, exist_ok=True)

feather.write_feather(results_arr, "data/interim/arrondissements/sirene_1624.feather", compression="zstd")
feather.write_feather(results_bv22, "data/interim/bv2022/sirene_1624.feather", compression="zstd")