library(tidyverse)
library(data.table)
library(arrow)

TABLE_PASSAGE_DF <- read_feather("results_building/t_passage.feather") |>
  select(code_insee24, arr24) |>
  unique()

#TBD