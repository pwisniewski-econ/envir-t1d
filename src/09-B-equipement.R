library(data.table)
library(tidyverse)
library(arrow)

EQUIP <- fread("data/external/insee-equipement/DS_BPE_data.csv") 
names(EQUIP) <- names(EQUIP) |> tolower()
  
TABLE_PASSAGE_DF <- read_feather("results_building/t_passage.feather") |>
  select(code_insee24, arr24, bv2022) |>
  unique()

EQUIP <- EQUIP |> 
  filter(geo_object=="COM") |>
  left_join(TABLE_PASSAGE_DF, by = c("geo" = "code_insee24")) |>
  select(code_insee24 = geo, arr24, bv2022, facility_sdom, facility_dom, obs_value)

equip_group <- function(vargroup, level){
  EQUIP |>
    group_by({{vargroup}}, cat = {{level}}) |>
    summarise(cnt = sum(obs_value, na.rm = T), .groups = "drop") |>
    mutate(cat = paste0("n_equip_",tolower(cat))) |>
    pivot_wider(names_from = cat, values_from = cnt) |>
    mutate(across(where(is.numeric), ~ replace_na(.x, 0)))
}

EQUIP_ARR <- equip_group(arr24, facility_sdom) |>
  filter(nchar(arr24)<4)

EQUIP_BV22 <- equip_group(bv2022, facility_sdom) 

write_feather(as.data.frame(EQUIP_ARR), "data/interim/arrondissements/equipement.feather", compression = "zstd")
write_feather(as.data.frame(EQUIP_BV22), "data/interim/bv2022/equipement.feather", compression = "zstd")
