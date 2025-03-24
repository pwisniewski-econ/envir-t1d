library(tidyverse)
library(data.table)
library(arrow)
library(sf)

# Air Quality ----
POP_COM <- fread("data/external/population/base-cc-evol-struct-pop-2021.csv.gz") |>
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

WEATHER <- fread("data/external/meteo/meteo_2017_2024_plus.csv") |>
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
  mutate(
    month = substr(aaaamm, 5, 6), 
    season = case_when(
      month%in%c("12", "01", "02") ~ "winter",   
      month%in%c("03", "04", "05") ~ "spring",
      month%in%c("06", "07", "08") ~ "summer",
      month%in%c("09", "10", "11") ~ "autumn"
    )
  ) |>
  left_join(TABLE_PASSAGE_DF, by="code_insee24")|>
  group_by(arr24, season) |>
  summarise(across(c(tm, txab, tnab, umm, rr), ~ weighted.mean(.x, pop_tot, na.rm = TRUE))) |>
  ungroup() |>
  pivot_wider(names_from = season, values_from = c(tm, txab, tnab, umm, rr)) |>
  select(-c(txab_autumn, txab_spring, txab_winter, tnab_autumn, tnab_spring, tnab_summer)) |>
  mutate(across(where(is.numeric), ~ zoo::na.approx(.x, rule = 2, na.rm = FALSE))) 
  #NB - This approximate values for humidity for small arr near Paris using data from near Paris arr

## Exports ----
write_feather(as.data.frame(WEATHER), "data/interim/arr_dynamique/arr_weather.feather", compression = "zstd")

# Air quality ----
TABLE_PASSAGE_DF2 <- read_feather("results_building/t_passage.feather") |>
  select(code_insee18, arr24) |>
  unique()

AIR_QUAL <- fread("data/external/qualite_air/qualite_air_clean_final.csv") |>
  mutate(code_insee18 = str_pad(CODE_INSEE, 5, pad = "0"))

names(AIR_QUAL) <- names(AIR_QUAL) |> tolower()

AIR_QUAL <- AIR_QUAL |> 
  inner_join(TABLE_PASSAGE_DF2, by="code_insee18") |>
  group_by(arr24) |>
  summarise(across(c(no2_mean_concentration, o3_mean_concentration, somo35_mean, aot40_mean, pm10_mean_concentration, pm25_mean_concentration), ~ weighted.mean(.x, population, na.rm = TRUE)))

## Export ----
write_feather(as.data.frame(AIR_QUAL), "data/interim/arr_dynamique/arr_airqual.feather", compression = "zstd")

# GHG emissions ----
TABLE_PASSAGE_DF3 <- read_feather("results_building/t_passage.feather") |>
  select(code_insee21, arr24) |>
  unique()

GES <- fread("data/external/qualite_air/emission_gaz_2021_clean_final.csv") |>
  mutate(code_insee21 = str_pad(CODE_COMMUNE_2021, 5, pad= "0")) |>
  select(
    code_insee21, ges_total = EMISSIONS_GES_TOTAL, 
    ges_routier = EMISSIONS_GES_ROUTIER, ges_resid = EMISSIONS_GES_RESIDENTIEL
  ) |>
  left_join(TABLE_PASSAGE_DF3, by="code_insee21") |>
  group_by(arr24) |>
  summarise(across(-code_insee21, ~sum(.x, na.rm = T)))

## Export ----
write_feather(as.data.frame(GES), "data/interim/arr_dynamique/arr_greenhouse.feather", compression = "zstd")


