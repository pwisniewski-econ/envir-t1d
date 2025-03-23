library(data.table)

TABLE_PASSAGE_DF <- read_feather("results_building/t_passage.feather") |>
  select(code_insee24, arr24) |>
  unique()

DIPLOMES <- fread("data/external/diplome_forma/DS_RP_DIPLOMES_data.csv") 
names(DIPLOMES) <- names(DIPLOMES) |> tolower()

DIP1 <- DIPLOMES |> 
  filter(geo_object=="COM"&time_period==2021) |>
  select(code_insee24 = geo, obs_value, sex, educ) |>
  mutate(educ = case_when(
    str_starts(educ, "001T") ~ "dip_001T", 
    educ=="100_RP" ~ "dip_100R",
    educ=="200_RP" ~ "dip_200R",
    educ=="300_RP" ~ "dip_300R",
    educ=="350T351_RP" ~ "dip_350R",
    educ=='500_RP'|educ=="500T702_RP" ~ "dip_500R", 
    educ=="600_RP"|educ=="600T702_RP" ~ "dip_600R",
    educ=="700_RP" ~ "dip_700R", 
    educ=="_T"~"total"
  ))

agreg_educ <- function(DIP1){
  DIP1B <- DIP1 |> 
    arrange(educ) |>
    pivot_wider(names_from = educ, values_from = obs_value) |>
    mutate(across(starts_with("dip"), ~if_else(is.na(.x), 0, .x/total)))
  
  DIP1M <- DIP1B|>
    filter(sex=="M") |>
    mutate(dip_sup_F = dip_500R+dip_600R+dip_700R) |>
    select(code_insee24, dip_sup_F)
  
  DIP1F <-  DIP1B|>
    filter(sex=="F") |>
    mutate(dip_sup_M = dip_500R+dip_600R+dip_700R)|>
    select(code_insee24, dip_sup_M)
  
  DIP1C <- DIP1M |>
    left_join(DIP1F, by = "code_insee24") |>
    mutate(delta_educ = dip_sup_M - dip_sup_F) |>
    select(code_insee24, delta_educ)
  
  DF <- DIP1B |>
    filter(sex=="_T") |>
    left_join(DIP1C, by="code_insee24") |>
    select(code_insee24, starts_with("dip"), delta_educ)
  
  return(DF)
}

DIPFORM_COM <- agreg_educ(DIP1)

DIP1_ARR <- DIP1 |>
  left_join(TABLE_PASSAGE_DF, by="code_insee24") |>
  group_by(code_insee24 = arr24, sex, educ) |>
  summarise(obs_value = sum(obs_value), .groups = "drop")

DIPFORM_ARR <- agreg_educ(DIP1_ARR) |>
  rename(arr24 = code_insee24) |>
  filter(nchar(arr24)<4)

write_feather(as.data.frame(DIPFORM_ARR), "data/interim/arr_dynamique/arr_diploma.feather", compression = "zstd")
write_feather(as.data.frame(DIPFORM_COM), "data/interim/com_statique/com_diploma.feather", compression = "zstd")
