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
  select(code_insee24, arr24) |>
  unique() |>
  setDT()

ARR2COM <- read_feather("results_building/arr2com.feather") |>
  setDT()

MOVEMENTS <- fread("data/external/migrations/base-flux-mobilite-residentielle-2021.csv") 

MOVEMENTS_ARR <- MOVEMENTS |>
  rename_join(ARR2COM, com_to, c("CODGEO" = "code_arr"), code_com) |>
  rename_join(ARR2COM, com_from, c("DCRAN" = "code_arr"), code_com) |>
  left_join(rename(TABLE_PASSAGE_DF, arr_to = arr24), by=c("com_to"="code_insee24")) |>
  left_join(rename(TABLE_PASSAGE_DF, arr_from = arr24), by=c("com_from"="code_insee24")) |>
  filter(arr_from != arr_to) 

INCOMING_ARR <- MOVEMENTS_ARR  |>
  group_by(arr24 = arr_to) |>
  summarise(incoming_pop21 = round(sum(NBFLUX_C21_POP01P), 2), .groups = "drop")

FLOW_ARR <- MOVEMENTS_ARR  |>
  group_by(arr_to, arr_from) |>
  summarise(flow_pop21 = round(sum(NBFLUX_C21_POP01P),2)) |>
  mutate(share_to = flow_pop21 / sum(flow_pop21))

write_feather(as.data.frame(FLOW_ARR), "results_building/arr_flows.feather", compression = "zstd")
write_feather(as.data.frame(INCOMING_ARR), "data/interim/arr_dynamique/arr_incoming.feather", compression = "zstd")
