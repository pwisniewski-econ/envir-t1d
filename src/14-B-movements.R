library(tidyverse)
library(data.table)
library(arrow)

rename_join <- function(df, join_df, new_col, join_by, rename_from, default_col) {
  default_col <- names(join_by)[1]
  join_df <- join_df %>%
    rename("{{new_col}}" := {{rename_from}})
  
  df %>%
    left_join(join_df, by = join_by) %>%
    mutate("{{new_col}}" := if_else(is.na({{new_col}}), .data[[default_col]], {{new_col}}))
}

TABLE_PASSAGE_DF <- read_feather("results_building/t_passage.feather") |>
  select(code_insee24, arr24, bv2022) |>
  unique() |>
  setDT()

ARR2COM <- read_feather("results_building/arr2com.feather") |>
  setDT()

MOVEMENTS <- fread("data/external/insee-mouvements/base-flux-mobilite-residentielle-2021.csv") 

MOVEMENTS <- MOVEMENTS |>
  rename_join(ARR2COM, com_to, c("CODGEO" = "code_arr"), code_com) |>
  rename_join(ARR2COM, com_from, c("DCRAN" = "code_arr"), code_com) 


sum_movements <- function(DATA, level){
  DATA <- DATA |>
    left_join(rename(TABLE_PASSAGE_DF, arr_to = {{level}}), by=c("com_to"="code_insee24")) |>
    left_join(rename(TABLE_PASSAGE_DF, arr_from = {{level}}), by=c("com_from"="code_insee24")) |>
    filter(arr_from != arr_to) 
  
  INCOMING_DATA <- DATA  |>
    group_by(arr_to) |>
    summarise(incoming_pop21 = round(sum(NBFLUX_C21_POP01P), 2), .groups = "drop")
  
  FLOW_DATA <- DATA  |>
    group_by(arr_to, arr_from) |>
    summarise(flow_pop21 = round(sum(NBFLUX_C21_POP01P),2)) |>
    mutate(share_to = flow_pop21 / sum(flow_pop21))
  
  return(list(INCOMING_DATA, FLOW_DATA))
}

movements_arr <- sum_movements(MOVEMENTS, arr24)
INCOMING_ARR <- movements_arr[[1]]
FLOW_ARR <- movements_arr[[2]]

write_feather(as.data.frame(INCOMING_ARR), "data/interim/arrondissements/incoming_pop.feather", compression = "zstd")
write_feather(as.data.frame(FLOW_ARR), "results_building/arrondissement-flow.feather", compression = "zstd")


movements_bv22 <- sum_movements(MOVEMENTS, bv2022)
INCOMING_BV22 <- movements_bv22[[1]] |> rename(bv22_to = arr_to)
FLOW_BV22 <- movements_bv22[[2]] |> rename(bv22_to = arr_to, bv22_from = arr_from)

write_feather(as.data.frame(INCOMING_BV22), "data/interim/bv2022/incoming_pop.feather", compression = "zstd")
write_feather(as.data.frame(FLOW_BV22), "results_building/bv2022-flow.feather", compression = "zstd")
