library(here)
library(arrow)
library(tidyverse)
library(data.table)

TABLE_PASSAGE_DF <- read_feather("results_building/t_passage.feather") |>
  select(code_insee24, arr24) |>
  unique() |>
  setDT()

POP_ARR21 <- fread("data/external/population/base-cc-evol-struct-pop-2021.csv.gz") |>
  rename(code_insee24 = CODGEO) |>
  left_join(TABLE_PASSAGE_DF, by = "code_insee24") |>
  filter(!is.na(arr24)) |>
  group_by(arr24) |>
  summarise(
    pop_0014 = sum(P21_POP0014), pop_1529 = sum(P21_POP1529), 
    pop_3044 = sum(P21_POP3044), pop_4559 = sum(P21_POP4559), 
    pop_6074 = sum(P21_POP6074), pop_7589 = sum(P21_POP7589), 
    pop_90p = sum(P21_POP90P)
  ) |> 
  mutate(
    pop_0044 = pop_0014 + pop_1529 + pop_3044, 
    pop_tot = pop_0044 + pop_4559 + pop_6074 + pop_7589 + pop_90p, 
    year = 2021
  )

write_feather(as.data.frame(POP_ARR21), "data/interim/arr_dynamique/arr_populations21.feather", compression = "zstd")
