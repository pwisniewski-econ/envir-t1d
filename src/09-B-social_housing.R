library(readxl)
TABLE_PASSAGE_DF <- read_feather("results_building/t_passage.feather") |>
  select(code_insee24, arr24, code_insee22) |>
  unique()

RPLS <- read_excel("data/external/logements_soc/resultats_rpls_2022.xlsx", 
                                           sheet = "Commune", col_names = FALSE, 
                                           skip = 4)
RPLS <- RPLS[c(3, 10, 15,16, 66, 125)]
names(RPLS) <- c("code_insee22", "n_social", "social_houses", "social_appartments", "social_average_age", "mean_rent_m2")

RPLS <- TABLE_PASSAGE_DF |>
  left_join(RPLS, by = "code_insee22") |>
  replace_na(list(n_social = 0, social_houses= 0, social_appartments=0))
  
RPLS_ARR <- RPLS |>
  group_by(arr24) |>
  summarise(
    social_average_age = sum(social_average_age*n_social, na.rm=T)/sum(n_social, na.rm=T),
    mean_rent_m2 = sum(mean_rent_m2*n_social, na.rm=T)/sum(n_social, na.rm=T),
    n_social = sum(n_social), social_houses = sum(social_houses), social_appartments = sum(social_appartments),
  ) |>
  filter(nchar(arr24)<4)

RPLS_COM <- RPLS |>
  select(-c(arr24, code_insee22))

write_feather(as.data.frame(RPLS_ARR), "data/interim/arr_dynamique/arr_social.feather", compression = "zstd")
write_feather(as.data.frame(RPLS_COM), "data/interim/com_statique/com_social.feather", compression = "zstd")
