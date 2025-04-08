library(here)
library(arrow)
library(tidyverse)
library(data.table)

pop_sum <- function(DATA, level){
   DATA |>
    rename(code_insee24 = CODGEO) |>
    left_join(TABLE_PASSAGE_DF, by = "code_insee24") |>
    filter(!is.na({{level}})) |>
    group_by({{level}}) |>
    summarise(
      pop_0014 = sum(P21_POP0014, na.rm = T), pop_1529 = sum(P21_POP1529, na.rm = T), 
      pop_3044 = sum(P21_POP3044, na.rm = T), pop_4559 = sum(P21_POP4559, na.rm = T), 
      pop_6074 = sum(P21_POP6074, na.rm = T), pop_7589 = sum(P21_POP7589, na.rm = T), 
      pop_90p = sum(P21_POP90P, na.rm = T)
    ) |> 
    mutate(
      pop_0044 = pop_0014 + pop_1529 + pop_3044, 
      pop_tot = pop_0044 + pop_4559 + pop_6074 + pop_7589 + pop_90p 
    )
}

TABLE_PASSAGE_DF <- read_feather("results_building/t_passage.feather") |>
  select(code_insee24, arr24, bv2022) |>
  unique() |>
  setDT()

POP_DATA <- fread("data/external/insee-population/base-cc-evol-struct-pop-2021.csv.gz")

POP_ARR21 <- pop_sum(POP_DATA, arr24)
write_feather(as.data.frame(POP_ARR21), "data/interim/arrondissements/populations_2021.feather", compression = "zstd")

POP_BV21 <- pop_sum(POP_DATA, bv2022)
write_feather(as.data.frame(POP_BV21), "data/interim/bv2022/populations_2021.feather", compression = "zstd")

