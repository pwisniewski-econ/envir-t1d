library(here)
library(tidyverse)
library(data.table)
library(arrow)

TABLE_PASSAGE_DF <- read_feather("results_building/t_passage.feather") |>
  select(code_insee24, arr24, bv2022) |>
  unique() 

ARR2COM <- read_feather("results_building/arr2com.feather") |>
  setDT()

SIRENE_DT <- fread(here("data", "external", "insee-sirene", "StockEtablissement_utf8_122024.csv"))[codeCommuneEtablissement!="",
   .(
    siren, 
    nic = as.numeric(nic), 
    siret, 
    ape = activitePrincipaleEtablissement, 
    active = etatAdministratifEtablissement == "A",
    eff_tranche = trancheEffectifsEtablissement,
    opened = dateCreationEtablissement, 
    closed = ifelse(etatAdministratifEtablissement != "A", dateDebut, NA),
    road_number = numeroVoieEtablissement, 
    road_type = typeVoieEtablissement, 
    road_name = str_replace(libelleVoieEtablissement, "-", " "),
    postal_code = codePostalEtablissement, 
    adress_addition = complementAdresseEtablissement,
    city = libelleCommuneEtablissement, 
    insee_code = codeCommuneEtablissement, 
    lambert_x = coordonneeLambertAbscisseEtablissement, 
    lambert_y = coordonneeLambertOrdonneeEtablissement
  )
]

SIRENE_DT <- ARR2COM[SIRENE_DT, on = .(code_arr = insee_code)][,
  insee_code := if_else(is.na(code_com), code_arr, code_com)
]

count_etabs <- function(data){
  dt <- (data)
  dt[is.na(closed), closed := as.Date("2100-01-01")]
  dt <- dt[closed>opened]
  
  years_dt <- data.table(year = seq(2016, 2024))
  years_dt[, start := as.Date(paste0(year, "-01-01"))]
  years_dt[, end := as.Date(paste0(year, "-12-31"))]
  
  setkey(years_dt, start, end)  
  dt[, `:=`(opened = opened, closed = closed)] 
  
  dt[, `:=`(opened = as.Date(opened), 
            closed = as.Date(closed))]
  establishments <- dt[, .(insee_code, opened, closed)]
  setkey(establishments, opened, closed)
  
  result <- foverlaps(establishments, years_dt, by.x = c("opened", "closed"), 
                      by.y = c("start", "end"), nomatch = 0L)
  
  final_result <- result[, .N, by = .(year, insee_code)]
  final_result <- final_result |> arrange(insee_code, year)
}

sirene_type <- function(ape_code){
  SIRENE_DF <- SIRENE_DT[ape==ape_code] 
  SIRENE_DF <- count_etabs(SIRENE_DF) |> setDT()
  SIRENE_DF[, "ape" := tolower(str_remove(ape_code, "\\."))]
  return(SIRENE_DF)
}

siren_sum <- function(DATA, level){
  DATA |>
    left_join(TABLE_PASSAGE_DF, by = c("insee_code"="code_insee24")) |>
    group_by({{level}}, year, ape) |>
    summarise(N = sum(N, na.rm = T)) |>
    pivot_wider(names_from = ape, values_from = N, names_prefix = "n_ape") |>
    mutate(across(where(is.numeric), ~if_else(is.na(.x), 0, .x))) 
}

ape_ls <- c("56.10C", "93.12Z", "56.30Z", "47.73Z", "47.23Z", "47.22Z", "47.11A", "10.71C")

RESULTS <- lapply(ape_ls, sirene_type) |>
  rbindlist() 

RESULTS_ARR <- siren_sum(RESULTS, arr24)
RESULTS_BV22 <- siren_sum(RESULTS, bv2022)

write_feather(as.data.frame(RESULTS_ARR), "data/interim/arrondissements/sirene_1624.feather", compression = "zstd")
write_feather(as.data.frame(RESULTS_BV22), "data/interim/bv2022/sirene_1624.feather", compression = "zstd")
