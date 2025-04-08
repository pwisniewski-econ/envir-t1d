library(readxl)
library(data.table)
library(tidyverse)
library(arrow)

import_filosofi <- function(filename){
  DATA <- fread(paste0("data/external/insee-filosofi/", filename))
  names(DATA) <- names(DATA) |> tolower()
  
  DATA |> 
    mutate(nbpersmenfisc21 = nbpersmenfisc21 / nbmenfisc21) |>
    select(codgeo, nombre_pers_menage = nbpersmenfisc21, med21, pimp21, tp6021, ppsoc21, ppfam21, d121, d921, rd21) |>
    mutate(across(-codgeo, ~str_replace(.x, ",", ".") |> as.numeric())) |>
    filter(!is.na(pimp21))
}

FILO_BV22 <- import_filosofi("cc_filosofi_2021_BV2022.csv") |> 
  rename(bv2022 = codgeo)

FILO_DEP <- import_filosofi("cc_filosofi_2021_DEP.csv") |> 
  rename(dep = codgeo)

#Works for 2018-2021
import_filo <- function(year){
  
  FILO_DF <- fread(paste0("data/external/insee-filosofi/FILO",year,"_DEC_ARR.csv"))
  
  names(FILO_DF) <- names(FILO_DF) |> str_remove("\\d{2}$")
  
  FILO_DF <- FILO_DF |> 
    mutate(nombre_pers_menage = NBPERS / NBMEN) |>
    select(
      arr24 = CODGEO,
      nombre_pers_menage,
      proportion_imposable_ens_arr = PMIMP,
      d1_ens_arr = D1, 
      q1_ens_arr = Q1,
      q2_ens_arr = Q2, 
      q3_ens_arr = Q3, 
      d9_ens_arr = D9, 
      gini_ens_arr = GI, 
      proportion_activite_ens_arr = PACT, 
      proportion_retraite_ens_arr = PPEN, 
      q1_couple_enfants_arr = TYM4Q1, 
      q2_couple_enfants_arr = TYM4Q2, 
      q3_couple_enfants_arr = TYM4Q3, 
      q1_parent_seul_arr = TYM5Q1, 
      q2_parent_seul_arr = TYM5Q2, 
      q3_parent_seul_arr = TYM5Q3, 
    ) |> 
    mutate(across(!arr24, ~gsub(",", ".",.x) |> as.numeric()), year= year) |>
    filter(nchar(arr24)<4)

  return(FILO_DF)
}

years_ls <- 2018:2021

FILO_ARR <- lapply(years_ls, import_filo) |>
  rbindlist() |>
  select(arr24, year, everything())

write_feather(as.data.frame(FILO_ARR), "data/interim/arrondissements/filosofi.feather", compression = "zstd")
write_feather(as.data.frame(FILO_BV22), "data/interim/bv2022/filosofi.feather", compression = "zstd")
write_feather(as.data.frame(FILO_DEP), "data/interim/departement/filosofi.feather", compression = "zstd")

# Chomage ----
UNEMP <- fread("data/external/insee-chomage/DS_RP_EMPLOI_LR_COMP_data.csv")

compute_unemp <- function(DATA, level, level_name){
  ACTIVE <- DATA |>
    filter(AGE=="Y15T64"&EMPSTA_ENQ=="1T2"&FREQ=="A"&GEO_OBJECT==level&PCS=="_T"&TIME_PERIOD==2021) |>
    select(geo = GEO, active_pop = OBS_VALUE)
  
  DATA |>
    filter(AGE=="Y15T64"&EMPSTA_ENQ=="2"&FREQ=="A"&GEO_OBJECT==level&PCS=="_T"&TIME_PERIOD==2021) |>
    select(geo = GEO, unemployed_pop = OBS_VALUE) |>
    left_join(ACTIVE, by="geo") |>
    mutate(unemp_rate = unemployed_pop / active_pop) |>
    select(geo, unemp_rate)|>
    rename({{level_name}} := geo)
}

UNEMP_ARR <- compute_unemp(UNEMP, "ARR", "arr24")
UNEMP_BV22 <- compute_unemp(UNEMP, "BV2022", "bv2022")

write_feather(as.data.frame(UNEMP_ARR), "data/interim/arrondissements/unemployment.feather", compression = "zstd")
write_feather(as.data.frame(UNEMP_BV22), "data/interim/bv2022/unemployment.feather", compression = "zstd")