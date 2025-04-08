#Title: Table de Passage PREMIUM
#Script: France

# Setup ----
library(here)
library(arrow)
library(tidyverse)
library(data.table)
library(readxl)

path_insee_tables <- "data/external/insee-table_de_passage/"

# Arrondissement municipal to commune level ----
ARR2COM <- read_excel(paste0(path_insee_tables, "table-appartenance-geo-communes-24.xlsx"), 
                                   sheet = "ARM", skip = 5) |> 
  select(code_com = COM, code_arr = CODGEO)


# Yearly table de passage ----
TABLE_PASSAGE_DF <- read_excel(paste0(path_insee_tables, "table_passage_annuelle_2024.xlsx"), 
                               sheet = "COM", skip = 5) |>
  select(starts_with("CODGEO_201"), starts_with("CODGEO_202"))

names(TABLE_PASSAGE_DF) <- names(TABLE_PASSAGE_DF) |> 
  str_replace("CODGEO_20", "code_insee") |>
  tolower() 

APP_COM24 <- read_excel(paste0(path_insee_tables, "table-appartenance-geo-communes-24.xlsx"), 
                        sheet = "COM", skip = 5) |> 
  select(code_insee24 = CODGEO, arr24 = ARR, bv2022 = BV2022)

# Yearly passage with Arrondissement ---- 
TABLE_PASSAGE_DF <- TABLE_PASSAGE_DF |>
  left_join(APP_COM24, by="code_insee24") |>
  select(code_insee24, arr24, bv2022, everything()) |> 
  filter(!is.na(arr24)&!is.na(bv2022)) |>
  unique()

# Export ----
write_feather(as.data.frame(TABLE_PASSAGE_DF), "results_building/t_passage.feather", compression = "zstd")
write_feather(as.data.frame(ARR2COM), "results_building/arr2com.feather", compression = "zstd")
