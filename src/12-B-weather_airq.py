import pandas as pd
import geopandas as gpd
import numpy as np

# --- Air Quality: Population and Passage Data ---
pop_com = pd.read_csv(
    "data/external/insee-population/base-cc-evol-struct-pop-2021.csv.gz",
    delimiter=";",
    low_memory=False,
    compression="gzip",
    usecols=["CODGEO", "P21_POP"]
).rename(columns={"CODGEO": "code_insee24", "P21_POP": "pop_tot"})

# Passage table and arrondissement-to-commune mapping
table_passage_df = (
    pd.read_feather("results_building/t_passage.feather")[["code_insee24", "arr24"]]
    .drop_duplicates()
)

arr2com = (
    pd.read_feather("results_building/arr2com.feather")
    .merge(
        table_passage_df,
        left_on="code_com",
        right_on="code_insee24",
        how="left"
    )[["code_arr", "arr24"]]
    .rename(columns={"code_arr": "code_insee24"})
)

table_passage_df = pd.concat([table_passage_df, arr2com], ignore_index=True)

# Commune geometries
geocom = gpd.read_file("data/external/communes-50m.geojson")[["code", "geometry"]]
geocom = geocom.rename(columns={"code": "code_insee24"})

# Weather Data 
weather_df = pd.read_csv(
    "data/external/mfrance-meteo/meteo_2017_2024_plus.csv"
)

weather_gdf = gpd.GeoDataFrame(
    weather_df,
    geometry=gpd.points_from_xy(weather_df.LON, weather_df.LAT),
    crs="EPSG:4326"
)
weather_gdf.columns = weather_gdf.columns.str.lower()

### Spatial join: map stations to communes
weather_geoms = (
    weather_gdf[["geometry"]]
    .drop_duplicates()
    .pipe(lambda g: gpd.sjoin(g, geocom, how="left", predicate="within"))
    .loc[lambda df: ~df["code_insee24"].isin(["75056", "13055", "69123"])]
)
weather_geoms["wkt"] = weather_geoms.geometry.apply(lambda geom: geom.wkt)
weather_geoms = weather_geoms.drop(columns="geometry")

## Flatten weather and join attributes
weather = weather_gdf.copy()
weather["wkt"] = weather.geometry.apply(lambda geom: geom.wkt)
weather = weather.drop(columns="geometry")
weather = (
    weather
    .merge(weather_geoms[["wkt", "code_insee24"]], on="wkt", how="left")
    .merge(pop_com, on="code_insee24", how="left")
    .merge(table_passage_df, on="code_insee24", how="left")
)
weather["year"] = weather.aaaamm.astype(str).str[:4]
weather["month"] = weather.aaaamm.astype(str).str[4:6]



# Weighted summary function for weather metrics
def sum_weather(df: pd.DataFrame, geo_level: str, time_level: str) -> pd.DataFrame:
    metrics = ["tm", "txab", "tnab", "umm", "rr", "inst"]
    def agg(grp):
        out = {}
        for col in metrics:
            vals = grp[col]
            w    = grp["pop_tot"]
            mask = vals.notna() & w.notna()
            if mask.any():
                out[col] = np.average(vals[mask], weights=w[mask])
            else:
                out[col] = np.nan
        return pd.Series(out)
    return (
        df
        .groupby([geo_level, time_level])
        .apply(agg)
        .reset_index()
    )


## ------- Arrondissement-level seasonal summary ---------
weather["season"] = np.select(
    [weather.month.isin(["12","01","02"]),
     weather.month.isin(["03","04","05"]),
     weather.month.isin(["06","07","08"]),
     weather.month.isin(["09","10","11"])]
    , ["winter", "spring", "summer", "autumn"],
    default=None
)
weather_arr = sum_weather(weather, "arr24", "season")
weather_arr = (
    weather_arr
    .pivot(
        index="arr24", columns="season",
        values=["tm","txab","tnab","umm","rr","inst"]
    )
    .reset_index()
)

weather_arr.columns = ["_".join(filter(None, col)).strip("_") for col in weather_arr.columns.values]

drop_cols = [c for c in weather_arr if c.startswith("inst_")] + [
    "txab_autumn", "txab_spring", "txab_winter",
    "tnab_autumn", "tnab_spring", "tnab_summer"
]

weather_arr = weather_arr.drop(columns=drop_cols)
num_cols = weather_arr.select_dtypes(include=np.number).columns
weather_arr[num_cols] = weather_arr[num_cols].interpolate(
    method="linear", axis=0, limit_direction="both"
)

#------------ Department level monthly summary --------------
weather23_dep = (
    weather
      .assign(dep=weather.arr24.str[:2])
      .loc[lambda df: df.year == "2023"]
      .pipe(lambda df: sum_weather(df, "dep", "month"))
).dropna(subset=["tm"])

weather23_dep = weather23_dep.pivot(
    index="dep",
    columns="month",
    values=["tm","txab","tnab","umm","rr","inst"]
)

weather23_dep.columns = [
    f"{var}_{month}"
    for var, month in weather23_dep.columns
]

num_cols = weather23_dep.select_dtypes(include="number").columns
for col in num_cols:
    weather23_dep[col] = weather23_dep[col].interpolate(
        method="linear",
        axis=0,
        limit_direction="both"
    )

weather23_dep = (
    weather23_dep
      .reset_index()
      .melt(
         id_vars="dep",
         var_name="var_month",
         value_name="value"
      )
      .assign(
         var   = lambda df: df.var_month.str.split("_").str[0],
         month = lambda df: df.var_month.str.split("_").str[1]
      )
      .pivot(
         index=["dep","month"],
         columns="var",
         values="value"
      )
      .reset_index()
)

#-------- Export results to Feather ----------
weather_arr.to_feather(
    "data/interim/arrondissements/weather.feather",
    compression="zstd"
)
weather23_dep.to_feather(
    "data/interim/departement/weather_2023.feather",
    compression="zstd"
)

# --------- Air Quality Aggregation -------------

table_passage_df2 = (
    pd.read_feather("results_building/t_passage.feather")
    [["code_insee18", "arr24", "bv2022"]]
    .drop_duplicates()
)
air_qual = pd.read_csv("data/external/medd-qualite_air/qualite_air_clean_final.csv")
air_qual["code_insee18"] = (
    air_qual.CODE_INSEE.astype(str).str.zfill(5)
)
air_qual = air_qual.merge(table_passage_df2, on="code_insee18", how="inner")
air_qual.columns = air_qual.columns.str.lower()

def sum_airQ(df: pd.DataFrame, level: str) -> pd.DataFrame:
    metrics = [
        "no2_mean_concentration", "o3_mean_concentration", "somo35_mean",
        "aot40_mean", "pm10_mean_concentration", "pm25_mean_concentration"
    ]
    def agg(grp):
        out = {}
        for col in metrics:
            vals = grp[col]
            w    = grp["population"]
            mask = vals.notna() & w.notna()
            if mask.any():
                out[col] = np.average(vals[mask], weights=w[mask])
            else:
                out[col] = np.nan
        return pd.Series(out)
    return (
        df
        .groupby(level)
        .apply(agg)
        .reset_index()
    )

air_qual_arr = sum_airQ(air_qual, "arr24")
air_qual_bv22 = sum_airQ(air_qual, "bv2022")

air_qual_arr.to_feather(
    "data/interim/arrondissements/air_quality.feather",
    compression="zstd"
)
air_qual_bv22.to_feather(
    "data/interim/bv2022/air_quality.feather",
    compression="zstd"
)


# ---------- Greenhouse Gas Emissions Aggregation ----------------
table_passage_df3 = (
    pd.read_feather("results_building/t_passage.feather")
    [["code_insee21", "arr24", "bv2022"]]
    .drop_duplicates()
)

ges = pd.read_csv("data/external/medd-qualite_air/emission_gaz_2021_clean_final.csv")

ges["code_insee21"] = ges.CODE_COMMUNE_2021.astype(str).str.zfill(5)

ges = ges[[
    "code_insee21", "EMISSIONS_GES_TOTAL",
    "EMISSIONS_GES_ROUTIER", "EMISSIONS_GES_RESIDENTIEL"
]].rename(
    columns={
        "EMISSIONS_GES_ROUTIER": "ges_routier",
        "EMISSIONS_GES_RESIDENTIEL": "ges_resid"
    }
)
ges = ges.merge(table_passage_df3, on="code_insee21", how="left")

def sum_ges(df: pd.DataFrame, level: str) -> pd.DataFrame:
    metrics = ["ges_routier", "ges_resid"]
    return df.groupby(level)[metrics].sum().reset_index()

ges_arr = sum_ges(ges, "arr24")
ges_bv22 = sum_ges(ges, "bv2022")

ges_arr.to_feather(
    "data/interim/arrondissements/greenhouse.feather",
    compression="zstd"
)
ges_bv22.to_feather(
    "data/interim/bv2022/greenhouse.feather",
    compression="zstd"
)
