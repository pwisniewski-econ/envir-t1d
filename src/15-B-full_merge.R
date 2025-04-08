library(here)
library(arrow)
library(tidyverse)
library(data.table)

BASE_TABLE <- read_feather("results_building/t_passage.feather") |>
  filter(as.numeric(code_insee24 )<97100) 

# MERGE ARRONDISSEMENTS -----

list.files("data/interim/arrondissements", full.names = )

arr_path <- "data/interim/arrondissements/"

ADEME_ARR <- read_feather(paste0(arr_path, "ademe_dpe.feather"))

AIRQ_ARR <- read_feather(paste0(arr_path, "air_quality.feather"))

CRIM_ARR <- read_feather(paste0(arr_path, "criminality.feather")) |>
  filter(year==2021) |>
  select(-year)

EDUC_ARR <- read_feather(paste0(arr_path, "education.feather"))

EQUIP_ARR <- read_feather(paste0(arr_path, "equipement.feather"))

FILO_ARR <- read_feather(paste0(arr_path, "filosofi.feather")) |>
  filter(year==2021) |>
  select(-year)

GHG_ARR <- read_feather(paste0(arr_path, "greenhouse.feather"))

POP_ARR <- read_feather(paste0(arr_path, "populations_2021.feather"))

SIRENE_ARR <- read_feather(paste0(arr_path, "sirene_1624.feather")) |>
  filter(year==2021) |>
  select(-year)

SOCIAL_ARR <- read_feather(paste0(arr_path, "social_housing.feather"))

UNEMP_ARR <- read_feather(paste0(arr_path, "unemployment.feather"))

WATERQ_ARR <- read_feather(paste0(arr_path, "water_quality.feather"))

WEATHER_ARR <- read_feather(paste0(arr_path, "weather.feather"))

INCOMING_ARR <- read_feather(paste0(arr_path, "incoming_pop.feather"))

ARR_DATA <- BASE_TABLE |>
  select(arr24) |>
  unique() |>
  #NO CORSICA AND NO DOMTOM |>
  left_join(EQUIP_ARR, by="arr24") |>
  left_join(SOCIAL_ARR, by="arr24") |>
  left_join(CRIM_ARR, by="arr24") |>
  left_join(SIRENE_ARR, by="arr24") |>
  left_join(GHG_ARR, by="arr24") |>
  left_join(POP_ARR, by="arr24") |>
  mutate(across(-c(arr24, starts_with("pop_")), ~ .x/pop_tot*1e3)) |>
  left_join(INCOMING_ARR, by=c("arr24"="arr_to")) |> 
  mutate(pop_renew = incoming_pop21/pop_tot) |>
  select(-incoming_pop21) |>
  left_join(FILO_ARR, by="arr24") |>
  left_join(UNEMP_ARR, by="arr24") |>
  left_join(EDUC_ARR, by="arr24") |>
  left_join(ADEME_ARR, by="arr24") |>
  left_join(AIRQ_ARR, by="arr24") |>
  left_join(WEATHER_ARR, by="arr24") |>
  left_join(WATERQ_ARR, by="arr24")
  

write_feather(ARR_DATA, "results_building/arrondissement-full.feather", compression = "zstd")
  
# MERGE BV2022

# MERGE ARRONDISSEMENTS -----

bv_path <- "data/interim/bv2022/"

ADEME_BV22 <- read_feather(paste0(bv_path, "ademe_dpe.feather"))

AIRQ_BV22 <- read_feather(paste0(bv_path, "air_quality.feather"))

CRIM_BV22 <- read_feather(paste0(bv_path, "criminality.feather")) |>
  filter(year==2021) |>
  select(-year)

EDUC_BV22 <- read_feather(paste0(bv_path, "education.feather"))

EQUIP_BV22 <- read_feather(paste0(bv_path, "equipement.feather"))

FILO_BV22 <- read_feather(paste0(bv_path, "filosofi.feather")) 

GHG_BV22 <- read_feather(paste0(bv_path, "greenhouse.feather"))

POP_BV22 <- read_feather(paste0(bv_path, "populations_2021.feather"))

SIRENE_BV22 <- read_feather(paste0(bv_path, "sirene_1624.feather")) |>
  filter(year==2021) |>
  select(-year)

SOCIAL_BV22 <- read_feather(paste0(bv_path, "social_housing.feather"))

UNEMP_BV22 <- read_feather(paste0(bv_path, "unemployment.feather"))

WATERQ_BV22 <- read_feather(paste0(bv_path, "water_quality.feather"))

INCOMING_BV22 <- read_feather(paste0(bv_path, "incoming_pop.feather"))


BV22_DATA <- BASE_TABLE |>
  select(bv2022) |>
  unique()  |>
  inner_join(EQUIP_BV22, by="bv2022") |>
  inner_join(SOCIAL_BV22, by="bv2022") |>
  inner_join(CRIM_BV22, by="bv2022") |>
  inner_join(SIRENE_BV22, by="bv2022") |>
  inner_join(GHG_BV22, by="bv2022") |>
  inner_join(POP_BV22, by="bv2022") |>
  mutate(across(-c(bv2022, starts_with("pop_")), ~ .x/pop_tot*1e3)) |>
  inner_join(INCOMING_BV22, by=c("bv2022"="bv22_to")) |> 
  mutate(pop_renew = incoming_pop21/pop_tot) |>
  select(-incoming_pop21) |>
  inner_join(FILO_BV22, by="bv2022") |>
  inner_join(UNEMP_BV22, by="bv2022") |>
  inner_join(EDUC_BV22, by="bv2022") |>
  inner_join(ADEME_BV22, by="bv2022") |>
  inner_join(AIRQ_BV22, by="bv2022") |>
  inner_join(WATERQ_BV22, by="bv2022") |>
  remove_missing()


write_feather(BV22_DATA, "results_building/bv2022-full.feather", compression = "zstd")

# DEPARTMENTS ----

dep_path <- "data/interim/departement/"

ADEME_DEP <- read_feather(paste0(dep_path, "ademe_dpe.feather"))
WEATHER_DEP <- read_feather(paste0(dep_path, "weather_2023.feather"))
FILO_DEP <- read_feather(paste0(dep_path, "filosofi.feather"))

DEP_DATA <- WEATHER_DEP |>
  filter(as.numeric(dep)<97) |>
  inner_join(ADEME_DEP, by="dep") |>
  inner_join(FILO_DEP, by="dep") |>
  remove_missing()
  
write_feather(DEP_DATA, "results_building/dep-full.feather", compression = "zstd")
