library(tidyverse)
library(data.table)
library(arrow)
library(sf)

# Air Quality ----
POP_COM <- fread("data/external/insee-population/base-cc-evol-struct-pop-2021.csv.gz") |>
  select(code_insee24 = CODGEO, pop_tot = P21_POP)

TABLE_PASSAGE_DF <- read_feather("results_building/t_passage.feather") |>
  select(code_insee24, arr24) |>
  unique()

ARR2COM <- read_feather("results_building/arr2com.feather") |>
  left_join(TABLE_PASSAGE_DF, by=c("code_com"="code_insee24")) |>
  select(code_insee24 = code_arr, arr24)

TABLE_PASSAGE_DF <- TABLE_PASSAGE_DF |> rbind(ARR2COM)

GEOCOM <- read_sf("data/external/communes-50m.geojson") |>
  select(code_insee24 = code, geometry)

WEATHER <- fread("data/external/mfrance-meteo/meteo_2017_2024_plus.csv") |>
  st_as_sf(coords = c("LON", "LAT"), crs = 4326) 

names(WEATHER) <- names(WEATHER) |> tolower()

WEATHER_GEOMS <- WEATHER |> 
  select(geometry) |> 
  unique() |>
  st_join(GEOCOM, join = st_within) |>
  filter(!code_insee24%in%c("75056", "13055", "69123")) |>
  mutate(wkt = as.character(geometry)) |>
  st_set_geometry(NULL)

WEATHER <- WEATHER |>
  mutate(wkt = as.character(geometry)) |>
  st_set_geometry(NULL) |>
  left_join(WEATHER_GEOMS, by = "wkt") |>
  left_join(POP_COM, by="code_insee24") 

WEATHER <- WEATHER |>
  left_join(TABLE_PASSAGE_DF, by="code_insee24") |>
  mutate(
    year = substr(aaaamm, 1,4),
    month = substr(aaaamm, 5, 6)
  ) 

sum_weather <- function(DATA, geo_level, time_level){
  DATA |>
    group_by({{geo_level}}, {{time_level}}) |>
    summarise(across(c(tm, txab, tnab, umm, rr, inst), ~ weighted.mean(.x, pop_tot, na.rm = TRUE))) |>
    ungroup() 
  #NB - This approximate values for humidity for small arr near Paris using data from near Paris arr
}


WEATHER_ARR <- WEATHER |>
  mutate(
    season = case_when(
      month%in%c("12", "01", "02") ~ "winter",   
      month%in%c("03", "04", "05") ~ "spring",
      month%in%c("06", "07", "08") ~ "summer",
      month%in%c("09", "10", "11") ~ "autumn"
    )
  ) |>
  sum_weather(arr24, season) |>
  pivot_wider(names_from = season, values_from = c(tm, txab, tnab, umm, rr, inst)) |>
  select(-c(starts_with("inst"), txab_autumn, txab_spring, txab_winter, tnab_autumn, tnab_spring, tnab_summer)) |>
  mutate(across(where(is.numeric), ~ zoo::na.approx(.x, rule = 2, na.rm = FALSE))) 
  

WEATHER23_DEP <- WEATHER |>
  mutate(dep = substr(arr24, 1, 2)) |>
  filter(year == "2023") |>
  sum_weather(dep, month) |>
  filter(!is.na(tm)) |>
  pivot_wider(names_from = month, values_from = c(tm, txab, tnab, umm, rr, inst)) |>
  mutate(across(where(is.numeric), ~ zoo::na.approx(.x, rule = 2, na.rm = FALSE))) |>
  pivot_longer(-dep, names_to = c(".value", "month"), names_sep = "_")

## Exports ----
write_feather(as.data.frame(WEATHER_ARR), "data/interim/arrondissements/weather.feather", compression = "zstd")
write_feather(as.data.frame(WEATHER23_DEP), "data/interim/departement/weather_2023.feather", compression = "zstd")


# Air quality ----
TABLE_PASSAGE_DF2 <- read_feather("results_building/t_passage.feather") |>
  select(code_insee18, arr24, bv2022) |>
  unique()

AIR_QUAL <- fread("data/external/medd-qualite_air/qualite_air_clean_final.csv") |>
  mutate(code_insee18 = str_pad(CODE_INSEE, 5, pad = "0")) |> 
  inner_join(TABLE_PASSAGE_DF2, by="code_insee18")

names(AIR_QUAL) <- names(AIR_QUAL) |> tolower()

sum_airQ <- function(DATA, level){
  DATA <- DATA  |>
    group_by({{level}}) |>
    summarise(across(c(no2_mean_concentration, o3_mean_concentration, somo35_mean, aot40_mean, pm10_mean_concentration, pm25_mean_concentration), ~ weighted.mean(.x, population, na.rm = TRUE)))
}

AIR_QUAL_ARR <- AIR_QUAL |> sum_airQ(arr24)
AIR_QUAL_BV22 <- AIR_QUAL |> sum_airQ(bv2022)

## Export ----
write_feather(as.data.frame(AIR_QUAL_ARR), "data/interim/arrondissements/air_quality.feather", compression = "zstd")
write_feather(as.data.frame(AIR_QUAL_BV22), "data/interim/bv2022/air_quality.feather", compression = "zstd")

# GHG emissions ----
TABLE_PASSAGE_DF3 <- read_feather("results_building/t_passage.feather") |>
  select(code_insee21, arr24, bv2022) |>
  unique()

GES <- fread("data/external/medd-qualite_air/emission_gaz_2021_clean_final.csv") |>
  mutate(code_insee21 = str_pad(CODE_COMMUNE_2021, 5, pad= "0")) |>
  select(
    code_insee21, ges_total = EMISSIONS_GES_TOTAL, 
    ges_routier = EMISSIONS_GES_ROUTIER, 
    ges_resid = EMISSIONS_GES_RESIDENTIEL
  ) |>
  left_join(TABLE_PASSAGE_DF3, by="code_insee21") 

sum_ges <- function(DATA, level){
  DATA |>
    group_by({{level}}) |>
    summarise(across(c(ges_routier, ges_resid), ~sum(.x, na.rm = T)))
}

GES_ARR <- GES |> sum_ges(arr24)
GES_BV22 <- GES |> sum_ges(bv2022)

## Export ----
write_feather(as.data.frame(GES_ARR), "data/interim/arrondissements/greenhouse.feather", compression = "zstd")
write_feather(as.data.frame(GES_BV22), "data/interim/bv2022/greenhouse.feather", compression = "zstd")


