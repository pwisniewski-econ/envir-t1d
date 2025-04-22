library(DoubleML)
library(ranger)
library(mlr3)
library(paradox)
library(future)
plan(multisession, workers = 2)
set.seed(5415341)
library(data.table)
library(tidyverse)
library(arrow)
library(pscl)

INCI_DEP <- read_feather("results_building/department_e101_0029.feather")

NUTRIMENTS <- read_feather("results_building/nutriments.feather")

DEP <- fread("data/external/INCA3/departements-region.csv") |>
  mutate(region = case_when(
    str_starts(region_name, "Auvergne") ~ 9, 
    str_starts(region_name, "Hauts-de-France") ~ 6, 
    str_starts(region_name, "Provence-Alpes") ~ 10, 
    str_starts(region_name, "Grand Est") ~ 7, 
    str_starts(region_name, "Occitanie") ~ 11, 
    str_starts(region_name, "Normandie") ~ 2, 
    str_starts(region_name, "Nouvelle-Aquitaine") ~ 12, 
    str_starts(region_name, "Centre-Val") ~ 3, 
    str_starts(region_name, "Bourgogne-") ~ 8, 
    str_starts(region_name, "Bretagne") ~ 5, 
    str_starts(region_name, "Pays de la Loire") ~ 4, 
    str_ends(region_name, "de-France") ~ 1
  )) |> 
  remove_missing() |>
  select(num_dep, region) |>
  unique()

NUTRIMENTS_DEP <- DEP |>
  left_join(NUTRIMENTS, by = "region") |>
  left_join(INCI_DEP, by = c("num_dep" = "dep")) |>
  setDT() 


## DATA ------
PPC1 <- fread("pca_arr_ses.csv") |>
  select(arr24, PC1, PC2, PC3) |>
  mutate(arr24 = str_pad(arr24, 3, pad = "0"))

ARR24 <- read_feather("results_building/arrondissement-pmsi_full.feather") |>
  left_join(PPC1, by= "arr24") |>
  remove_missing() 

PPC1_BV <- fread("pca_bv_ses.csv") |>
  select(bv2022, PC1, PC2, PC3) |>
  mutate(bv2022 = str_pad(bv2022, 5, pad = "0"))

BV2022 <- read_feather("results_building/bv2022-pmsi_full.feather") |>
  left_join(PPC1_BV, by= "bv2022") |>
  remove_missing()

## DYNAMIC DEPARTMENT -- TESTING VITAMIN D HYPOTHESIS
INST_DEP <- NUTRIMENTS_DEP |> filter(youth == 2 & month == "01") |> 
  select(dep=num_dep,inst,prop_robinet, nutriment52, nutriment10,nutriment11, nutriment1, nutriment5, nutriment3, nutriment31)

ARR24B <- ARR24 |>
  mutate(dep = substr(arr24, 1, 2)) |>
  left_join(INST_DEP, by = c("dep"))


get_gender <- function(DATA, gender, t1d_name, tabacco_name){
  read_feather(paste0("results_building/arrondissement-pmsi_",gender,".feather")) |>
    select(arr24, {{t1d_name}} := t1d_e101, {{tabacco_name}} := diag_tabacco) |>
    left_join(DATA, by = "arr24") |>
    remove_missing()
}

ARRZ <- ARR24B |>
  get_gender("female", t1df_e101, diag_tabacco_F) |>
  get_gender("male", t1dh_e101, diag_tabacco_H) |>
  mutate(
    t1d_e101_rate = t1d_e101 / pop_0029 * 1000, 
    t1df_e101_rate = t1df_e101 / pop_f0029 * 1000, 
    t1dh_e101_rate = t1dh_e101 / pop_h0029* 1000
  )

list_var <- c(
  "diag_tabacco", "diag_calcium", "diag_vitamin_b12",
  "n_ape4722z", "n_ape5610c", "n_ape9312z",  
  "pm10_mean_concentration", "no2_mean_concentration", 
  "coups"
)


run_dml <- function(DATA, outcome_var, treatment_var, desc){
  
  cat(paste0(Sys.time(), " - starting: ", treatment_var, "\n"), file = "log.txt", append = TRUE)
  
  setDT(DATA)
  
  cofounders <- names(DATA)[!str_starts(names(DATA), paste0("dep|pop|n_dpe|PC|arr24|t1d|diag_vitamin_d|diag_tabacco_|", treatment_var))]
  cofounders <- c(cofounders, "pop_tot")
  
  dml_data <- DoubleMLData$new(
    DATA, 
    y_col = outcome_var, 
    d_cols = treatment_var, 
    x_cols = cofounders
  )  
  
  dml_plr <- DoubleMLPLR$new(
    data = dml_data,
    ml_l = lrn("regr.ranger"), 
    ml_m = lrn("regr.ranger"), 
    n_folds = 3,
    n_rep = 1, 
    score = "partialling out"
  )
  
  tune_params <- paradox::ps(
    mtry = paradox::p_int(lower = 2, upper = floor(.7*length(cofounders))),
    num.trees = paradox::p_int(lower = 300, upper = 1500)
  )
  
  tune_settings <- list(
    terminator = mlr3tuning::trm("evals", n_evals = 12), 
    algorithm = mlr3tuning::tnr("random_search"),
    rsmp_tune = rsmp("cv", folds = 3)
  )
  
  dml_plr$tune(
    param_set = list(ml_l = tune_params, ml_m = tune_params), 
    tune_settings = tune_settings, 
    tune_on_folds = FALSE
  )
  
  ml_l <- lrn("regr.ranger")
  ml_m <- lrn("regr.ranger")
  
  ml_l$param_set$values <- dml_plr$tuning_res$diag_tabacco$ml_l$params[[1]]
  ml_m$param_set$values <- dml_plr$tuning_res$diag_tabacco$ml_m$params[[1]]
  
  dml_plr <- DoubleMLPLR$new(
    data = dml_data,
    ml_l = ml_l, 
    ml_m = ml_m, 
    n_folds = 4,
    n_rep = 6, 
    score = "partialling out"
  )$fit(store_predictions = TRUE)
  
  
  dml_plr <- dml_plr$fit(store_predictions = TRUE)
  
  FIT <- data.frame(
    beta = dml_plr$coef, 
    std = dml_plr$se, 
    tstat = dml_plr$t_stat, 
    pval = dml_plr$pval, 
    desc = desc
  )
  
  RESIDUALS <- data.frame(
    arr24 = dml_data$data$arr24,
    real_value = dml_data$data[, .SD, .SDcols = treatment_var] |> unlist(use.names = FALSE),
    mean_prediction = dml_plr$predictions$ml_m |> rowMeans(),
    median_prediction = apply(dml_plr$predictions$ml_m, 1, median),
    desc = desc
  )
  
  
  return(list(FIT, RESIDUALS))
  
}


dml_helper <- function(treatment_var){
  run_dml(ARRZ, "t1d_e101_rate", treatment_var, paste0("dml-", treatment_var))
}

dml_results <- lapply(list_var, dml_helper)

RESIDUALS <- lapply(dml_results, function(x) x[[2]]) |> 
  rbindlist()

RESIDUALS <- RESIDUALS |>
  mutate(real_value = 
    if_else(
      !str_starts(desc, "dml-diag")|desc=="dml-diag_tabacco",
      real_value, NA
    )
  )

fwrite(RESIDUALS, "results_analysis/dml-residuals.csv")

DML_RESULTS <- lapply(dml_results, function(x) x[[1]]) |> 
  rbindlist()

dml_tabacco_female <- run_dml(ARRZ, "t1df_e101_rate", "diag_tabacco_F", "dml-tabacco-female")[[1]]
dml_tabacco_male <- run_dml(ARRZ, "t1dh_e101_rate", "diag_tabacco_H", "dml-tabacco-male")[[1]]

DML_RESULTS <- DML_RESULTS |>
  rbind(dml_tabacco_female) |>
  rbind(dml_tabacco_male)

fwrite(DML_RESULTS, "results_analysis/dml-results.csv")

