library(here)
library(tidyverse)
library(data.table)
library(arrow)

post_convert <- function(DATA, level){
  DATA |>
    group_by(codepost, {{level}}) |>
    summarise(pop_tot = sum(pop_tot)) |>
    filter(pop_tot == max(pop_tot)) |>
    ungroup() |>
    select(codepost, {{level}}) |>
    unique()
}

TABLE_PASSAGE_DF <- read_feather("results_building/t_passage.feather") |>
  select(code_insee24, arr24, bv2022) |>
  unique() 

COM_POP <- fread("data/external/insee-population/base-cc-evol-struct-pop-2021.csv.gz") |>
  select(code_insee24 = CODGEO, pop_tot = P21_POP)

ARR2COM <- read_feather("results_building/arr2com.feather") |>
  left_join(TABLE_PASSAGE_DF, by=c("code_com"="code_insee24")) |>
  select(code_insee24 = code_arr, arr24, bv2022)

POSTAL_CODES <- fread("data/external/poste-codes/base-officielle-codes-postaux_2024.csv") |>
  select(codepost = code_postal, code_insee24 = code_commune_insee) |>
  unique()

TABLE_PASSAGE_DF <- TABLE_PASSAGE_DF |>
  rbind(ARR2COM) |>
  left_join(COM_POP, by="code_insee24")

POSTAL_CODES <- POSTAL_CODES |>
  left_join(TABLE_PASSAGE_DF, by ="code_insee24") |>
  remove_missing()

POSTAL_CONVERTER <- post_convert(POSTAL_CODES, arr24) |> 
  left_join(post_convert(POSTAL_CODES, bv2022), by="codepost")

write_feather(as.data.frame(POSTAL_CONVERTER), "results_building/postal_converter.feather", compression = "zstd")
