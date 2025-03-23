#Title: Table de Passage PREMIUM
#Script: France

ARR2COM <- read_excel("data/table-appartenance-geo-communes-24.xlsx", 
                                   sheet = "ARM", skip = 5) |> 
  select(code_com = COM, code_arr = CODGEO)

TABLE_PASSAGE_DF <- read_excel("data/table_passage_annuelle_2024.xlsx", 
                               sheet = "COM", skip = 5) |>
  select(starts_with("CODGEO_201"), starts_with("CODGEO_202"))

names(TABLE_PASSAGE_DF) <- names(TABLE_PASSAGE_DF) |> 
  str_replace("CODGEO_20", "code_insee") |>
  tolower() 

APP_COM24 <- read_excel("data/table-appartenance-geo-communes-24.xlsx", 
                        sheet = "COM", skip = 5) |> 
  select(code_insee24 = CODGEO, arr24 = ARR)

TABLE_PASSAGE_DF <- TABLE_PASSAGE_DF |>
  left_join(APP_COM24, by="code_insee24") |>
  select(code_insee24, arr24, everything()) |> 
  filter(!is.na(arr24)) |>
  unique()

write_feather(as.data.frame(TABLE_PASSAGE_DF), "results_building/t_passage.feather", compression = "zstd")
write_feather(as.data.frame(ARR2COM), "results_building/arr2com.feather", compression = "zstd")
