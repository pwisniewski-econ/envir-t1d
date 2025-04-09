import pandas as pd
import pyarrow.feather as feather
import geopandas as gpd
from pathlib import Path
import numpy as np
from shapely import wkt
from zoo import na_approx

# Air Quality GHG Weather 

# Lecture des fichiers de passage
table_passage_df = feather.read_feather("results_building/t_passage.feather")[["code_insee24", "arr24"]].drop_duplicates()

arr2com = feather.read_feather("results_building/arr2com.feather")
arr2com = arr2com.merge(table_passage_df, left_on="code_com", right_on="code_insee24", how="left")
arr2com = arr2com.rename(columns={"code_arr": "code_insee24"})[["code_insee24", "arr24"]]

table_passage_df = pd.concat([table_passage_df, arr2com], ignore_index=True)

# Lecture des géométries communales
geocom = gpd.read_file("data/external/communes-50m.geojson")[["code", "geometry"]].rename(columns={"code": "code_insee24"})

# Lecture et géocodage des points météo
weather = pd.read_csv("data/external/mfrance-meteo/meteo_2017_2024_plus.csv")
weather.columns = weather.columns.str.lower()
weather = gpd.GeoDataFrame(weather, geometry=gpd.points_from_xy(weather["lon"], weather["lat"]), crs="EPSG:4326")

# Association des stations météo aux communes
weather_geom = weather[["geometry"]].drop_duplicates()
weather_geom = gpd.sjoin(weather_geom, geocom, predicate="within", how="left")
weather_geom = weather_geom[~weather_geom["code_insee24"].isin(["75056", "13055", "69123"])]
weather_geom["wkt"] = weather_geom["geometry"].apply(lambda x: x.wkt)

# Attribution géographique aux lignes météo
weather["wkt"] = weather["geometry"].apply(lambda x: x.wkt)
weather = weather.drop(columns="geometry").merge(weather_geom[["wkt", "code_insee24"]], on="wkt", how="left")
weather = weather.merge(pd.read_csv("data/external/insee-population/base-cc-evol-struct-pop-2021.csv.gz")[["CODGEO", "P21_POP"]]
                        .rename(columns={"CODGEO": "code_insee24", "P21_POP": "pop_tot"}), on="code_insee24", how="left")

weather = weather.merge(table_passage_df, on="code_insee24", how="left")
weather["year"] = weather["aaaamm"].astype(str).str[:4]
weather["month"] = weather["aaaamm"].astype(str).str[4:6]

# Fonction d’agrégation météo
def sum_weather(data, geo, time):
    return data.groupby([geo, time]).apply(
        lambda g: pd.Series({
            "tm": np.average(g["tm"], weights=g["pop_tot"]),
            "txab": np.average(g["txab"], weights=g["pop_tot"]),
            "tnab": np.average(g["tnab"], weights=g["pop_tot"]),
            "umm": np.average(g["umm"], weights=g["pop_tot"]),
            "rr": np.average(g["rr"], weights=g["pop_tot"]),
            "inst": np.average(g["inst"], weights=g["pop_tot"]),
        })
    ).reset_index()

# Agrégation météo par saison
weather["season"] = weather["month"].map({
    "12": "winter", "01": "winter", "02": "winter",
    "03": "spring", "04": "spring", "05": "spring",
    "06": "summer", "07": "summer", "08": "summer",
    "09": "autumn", "10": "autumn", "11": "autumn"
})

weather_arr = sum_weather(weather, "arr24", "season").pivot(index="arr24", columns="season")
weather_arr.columns = [f"{a}_{b}" for a, b in weather_arr.columns]
weather_arr = weather_arr.reset_index()
weather_arr = weather_arr.interpolate(axis=1)

# Agrégation météo par mois (départements, 2023 uniquement)
weather["dep"] = weather["arr24"].astype(str).str[:2]
weather23 = weather[weather["year"] == "2023"]
weather_dep = sum_weather(weather23, "dep", "month").pivot(index="dep", columns="month")
weather_dep.columns = [f"{a}_{b}" for a, b in weather_dep.columns]
weather_dep = weather_dep.reset_index().interpolate(axis=1)

# Qualite de l'Air

table_passage_df2 = feather.read_feather("results_building/t_passage.feather")[["code_insee18", "arr24", "bv2022"]]
air_qual = pd.read_csv("data/external/medd-qualite_air/qualite_air_clean_final.csv")

air_qual["code_insee18"] = air_qual["CODE_INSEE"].astype(str).str.zfill(5)
air_qual = air_qual.merge(table_passage_df2, on="code_insee18", how="inner")
air_qual.columns = air_qual.columns.str.lower()

def sum_airq(data, level):
    return data.groupby(level).apply(
        lambda g: pd.Series({
            "no2_mean_concentration": np.average(g["no2_mean_concentration"], weights=g["population"]),
            "o3_mean_concentration": np.average(g["o3_mean_concentration"], weights=g["population"]),
            "somo35_mean": np.average(g["somo35_mean"], weights=g["population"]),
            "aot40_mean": np.average(g["aot40_mean"], weights=g["population"]),
            "pm10_mean_concentration": np.average(g["pm10_mean_concentration"], weights=g["population"]),
            "pm25_mean_concentration": np.average(g["pm25_mean_concentration"], weights=g["population"]),
        })
    ).reset_index()

airq_arr = sum_airq(air_qual, "arr24")
airq_bv22 = sum_airq(air_qual, "bv2022")

# GES emissions

table_passage_df3 = feather.read_feather("results_building/t_passage.feather")[["code_insee21", "arr24", "bv2022"]]
ges = pd.read_csv("data/external/medd-qualite_air/emission_gaz_2021_clean_final.csv")

ges["code_insee21"] = ges["CODE_COMMUNE_2021"].astype(str).str.zfill(5)
ges = ges.rename(columns={
    "EMISSIONS_GES_TOTAL": "ges_total",
    "EMISSIONS_GES_ROUTIER": "ges_routier",
    "EMISSIONS_GES_RESIDENTIEL": "ges_resid"
})

ges = ges[["code_insee21", "ges_total", "ges_routier", "ges_resid"]].merge(table_passage_df3, on="code_insee21", how="left")

def sum_ges(data, level):
    return data.groupby(level)[["ges_routier", "ges_resid"]].sum().reset_index()

ges_arr = sum_ges(ges, "arr24")
ges_bv22 = sum_ges(ges, "bv2022")

# Exports
Path("data/interim/arrondissements").mkdir(parents=True, exist_ok=True)
Path("data/interim/bv2022").mkdir(parents=True, exist_ok=True)
Path("data/interim/departement").mkdir(parents=True, exist_ok=True)

feather.write_feather(weather_arr, "data/interim/arrondissements/weather.feather", compression="zstd")
feather.write_feather(weather_dep.melt(id_vars="dep", var_name="month", value_name="value"), "data/interim/departement/weather_2023.feather", compression="zstd")
feather.write_feather(airq_arr, "data/interim/arrondissements/air_quality.feather", compression="zstd")
feather.write_feather(airq_bv22, "data/interim/bv2022/air_quality.feather", compression="zstd")
feather.write_feather(ges_arr, "data/interim/arrondissements/greenhouse.feather", compression="zstd")
feather.write_feather(ges_bv22, "data/interim/bv2022/greenhouse.feather", compression="zstd")
