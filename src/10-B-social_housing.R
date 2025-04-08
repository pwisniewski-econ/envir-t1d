library(readxl)
library(data.table)
library(tidyverse)
library(arrow)

TABLE_PASSAGE_DF <- read_feather("results_building/t_passage.feather") |>
  select(code_insee22, arr24, bv2022) |>
  unique() |>
  group_by(code_insee22) |>
  filter(row_number()==1) |>
  ungroup()

ARR2COM <- read_feather("results_building/arr2com.feather") |>
  left_join(TABLE_PASSAGE_DF, by=c("code_com"="code_insee22")) |>
  select(code_insee22 = code_arr, arr24, bv2022)

TABLE_PASSAGE_DF <- TABLE_PASSAGE_DF |> rbind(ARR2COM)

RPLS <- read_excel("data/external/medd-logements_sociaux/resultats_rpls_2022.xlsx", 
                                           sheet = "Commune", col_names = FALSE, 
                                           skip = 4)
RPLS <- RPLS[c(3, 10, 15,16, 66, 125)]
names(RPLS) <- c("code_insee22", "n_social", "social_houses", "social_appartments", "social_average_age", "mean_rent_m2")

RPLS <- TABLE_PASSAGE_DF |>
  left_join(RPLS, by = "code_insee22") |>
  replace_na(list(n_social = 0, social_houses= 0, social_appartments=0))
  

social_sum <- function(DATA, level){
  DATA |>
    group_by({{level}}) |>
    summarise(
      social_average_age = sum(social_average_age*n_social, na.rm=T)/sum(n_social, na.rm=T),
      mean_rent_m2 = sum(mean_rent_m2*n_social, na.rm=T)/sum(n_social, na.rm=T),
      n_social = sum(n_social), social_houses = sum(social_houses), social_appartments = sum(social_appartments),
    ) 
}

RPLS_ARR <- social_sum(RPLS, arr24) |>
  filter(nchar(arr24)<4)

RPLS_BV22 <- social_sum(RPLS, bv2022)

write_feather(as.data.frame(RPLS_ARR), "data/interim/arrondissements/social_housing.feather", compression = "zstd")
write_feather(as.data.frame(RPLS_BV22), "data/interim/bv2022/social_housing.feather", compression = "zstd")
