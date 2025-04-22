library(tidyverse)
library(data.table)
library(arrow)

TABLE_PASSAGE_DF <- read_feather("results_building/t_passage.feather") |>
  select(code_insee24, arr24, bv2022) |>
  unique()

COMS <- fread("data/external/msp-eau/DIS_COM_UDI_2023.txt") |>
  select(cdreseau, inseecommune) |>
  unique()

PLV <- fread("data/external/msp-eau/DIS_PLV_2023.txt") |>
  select(referenceprel, cdreseau) |>
  unique() |>
  left_join(COMS, by= "cdreseau") |>
  select(referenceprel, code_insee24 = inseecommune) |>
  unique()

WATER_Q <- fread("data/external/msp-eau/DIS_RESULT_2023.txt")

WATER_Q <- WATER_Q[cdparametresiseeaux%in%c("CL", "NO3", "PH", "SO4")] 

WATER_Q[, "rqana" := as.numeric(str_replace(str_remove(rqana, "<|>"), ",", "."))] 

WATER_Q <- WATER_Q[, .(rqana = mean(rqana, na.rm = TRUE)), by = .(referenceprel, cdparametresiseeaux)]

WATER_Q <- WATER_Q |>
  filter(!is.na(rqana)) |>
  select(referenceprel, water_param = cdparametresiseeaux, rqana) 

WATER_QB <- WATER_Q |> 
  left_join(PLV, by = "referenceprel") |>
  group_by(code_insee24, water_param) |>
  summarise(rqana = mean(rqana)) |>
  inner_join(TABLE_PASSAGE_DF, by= "code_insee24")

sum_water <- function(DATA, level){
  DATA |>
    group_by({{level}}, water_param) |>
    summarise(rqana = mean(rqana, na.rm=T)) |>
    pivot_wider(names_from = water_param, values_from = rqana) |>
    rename_with( ~ paste0("water_",tolower(.x)), -{{level}})
}

WATERQ_DEP <- sum_water(WATER_QB |> mutate(dep = substr(code_insee24, 1, 2)), dep)

WATERQ_ARR <- sum_water(WATER_QB, arr24)

WATERQ_BV22 <- sum_water(WATER_QB, bv2022) |>
  filter(!(is.na(water_cl)|is.na(water_no3)|is.na(water_ph)|is.na(water_so4)))

write_feather(as.data.frame(WATERQ_ARR), "data/interim/arrondissements/water_quality.feather", compression = "zstd")
write_feather(as.data.frame(WATERQ_BV22), "data/interim/bv2022/water_quality.feather", compression = "zstd")
write_feather(as.data.frame(WATERQ_DEP), "data/interim/departement/water_quality.feather", compression = "zstd")
