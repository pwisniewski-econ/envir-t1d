library(arrow)
library(tidyverse)
library(readxl)

ARR2COM <- read_feather("results_building/arr2com.feather") 

DICO_CRIMES <- data.frame(
  var_crimes = c("coups", "trafic", "vols_av", "vols_sv", "cambriolages"), 
  crimes_desc = c("Coups et blessures volontaires", "Trafic de stupéfiants", "Vols violents sans arme", "Vols sans violence contre des personnes", "Cambriolages de logement")
)

TABLE_PASSAGE_DF <- read_feather("results_building/t_passage.feather") |>
  select(code_insee24, arr24, bv2022) |>
  unique() |>
  group_by(code_insee24) |>
  filter(row_number()==1) |>
  ungroup()

CRIMES_DF <- read_parquet("data/external/miom-crimes/crimes23_geo24.parquet") |>
  filter(annee>16)

CRIMES_DF <- CRIMES_DF |>
  select(code_insee24 = CODGEO_2024, 
         year = annee, 
         crime_count = faits,
         crimes_desc = classe,
         units = unité.de.compte) |>
  mutate(
    year = year+2000, 
    crime_count = if_else(is.na(crime_count), 0, crime_count)
  ) |>
  filter(crimes_desc%in%DICO_CRIMES$crimes_desc) 

CRIMES_DF <- CRIMES_DF |>
  left_join(ARR2COM, by = c("code_insee24" = "code_arr")) |>
  mutate(code_insee24 = if_else(is.na(code_com), code_insee24, code_com)) |>
  left_join(DICO_CRIMES, by = "crimes_desc") |>
  left_join(TABLE_PASSAGE_DF, by = c("code_insee24")) 

#EXPORT READY DATA -----
CRIMES_ARR <- CRIMES_DF |> 
  group_by(arr24, year, crimes = var_crimes) |>
  summarise(crime_count = sum(crime_count), .groups = "drop") |> 
  pivot_wider(names_from = crimes, values_from = crime_count)

CRIMES_BV22 <- CRIMES_DF |> 
  group_by(bv2022, year, crimes = var_crimes) |>
  summarise(crime_count = mean(crime_count), .groups = "drop") |> 
  pivot_wider(names_from = crimes, values_from = crime_count)

#EXPORTS ----
write_feather(as.data.frame(CRIMES_ARR), "data/interim/arrondissements/criminality.feather", compression = "zstd")
write_feather(as.data.frame(CRIMES_BV22), "data/interim/bv2022/criminality.feather", compression = "zstd")

