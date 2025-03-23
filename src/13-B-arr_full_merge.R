library(here)
library(arrow)
library(tidyverse)
library(data.table)
library(sf)
library(showtext)
library(ggrepel)  # For better text placement

TABLE_PASSAGE_DF <- read_feather("results_building/t_passage.feather") |>
  select(code_insee24, arr24) |>
  unique()

DIPFORM_ARR <- read_feather("data/interim/arr_dynamique/arr_diploma.feather") 

RPLS_ARR <- read_feather("data/interim/arr_dynamique/arr_social.feather") |>
  select(-c(social_average_age, mean_rent_m2 ))

SIRENE_ARR <- read_feather("data/interim/arr_dynamique/arr_sirene.feather") |> 
  filter(year==2021) |>
  select(-year)

REVENUS_ARR <- read_feather("data/interim/arr_dynamique/arr_revenus.feather") |> 
  filter(year==2021) |>
  select(-year)

POPULATION_ARR <- read_feather("data/interim/arr_dynamique/arr_populations21.feather") |> 
  filter(year==2021) |>
  select(-c(year))

EQUIP_ARR <- read_feather("data/interim/arr_dynamique/arr_equip.feather") 

DPE_ARR <- read_feather("data/interim/arr_dynamique/arr_dpe.feather") |>
  select(-n_dpe)

CRIMES_ARR <- read_feather("data/interim/arr_dynamique/arr_crimes.feather") |> 
  filter(year==2021) |>
  select(-year)

INCOMING_ARR <- read_feather("data/interim/arr_dynamique/arr_incoming.feather")
  
ARR_DATA <- EQUIP_ARR |>
  left_join(RPLS_ARR, by="arr24") |>
  left_join(CRIMES_ARR, by="arr24") |>
  left_join(SIRENE_ARR, by="arr24") |>
  left_join(POPULATION_ARR, by="arr24") |>
  mutate(across(-c(arr24, starts_with("pop_")), ~ .x/pop_tot)) |>
  left_join(INCOMING_ARR, by="arr24") |> 
  mutate(pop_renew = incoming_pop21/pop_tot) |>
  left_join(REVENUS_ARR, by="arr24") |>
  left_join(DIPFORM_ARR, by="arr24") |>
  left_join(DPE_ARR, by="arr24") |>
  remove_missing()

write_feather(as.data.frame(ARR_DATA), "results_building/arr.feather", compression = "zstd")