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


main_regression <- function(DATA, formula, desc){
  
  environment(formula) <- environment()
  
  dispersion_poisson <- function(model){
    model$deviance / model$df.residual
  }
  
  model2df <- function(model, desc){
    RESULTS <- as.data.frame(summary(model)$coefficients)
    
    names(RESULTS) <- c("beta", "std", "zval", "pval")
    
    RESULTS |>
      mutate(description = desc,
             dispersion = dispersion_poisson(model), 
             pseudo_r2 = pR2(model)[4], 
             aic = model$aic,
             nobs = model$df.null+1, 
             df = model$df.residual) |>
      rownames_to_column(var = "variable")
  }
  
  mod_poisson <- glm(
      formula,
      data = DATA, 
      family = "poisson", 
      offset = log(DATA$pop_0029)
    ) 
  
  mod_negbinom <- MASS::glm.nb(
    update.formula(formula, ~ . + offset(log(DATA$pop_0029))),
    data = DATA, 
    maxit = 100,
  ) 
  
  model2df(mod_poisson, paste0("poisson-", desc)) |>
  rbind(
    model2df(mod_negbinom, paste0("negbinom-", desc))
  )
  
}

gendered_regressions <- function(gender, pop){
  ARR24 <- read_feather(paste0("results_building/arrondissement-pmsi_",gender,".feather")) |>
    select(arr24, t1d_e101) |>
    left_join(ARR24B |> select(-t1d_e101), by = "arr24") |>
    mutate(pop_0029 = {{pop}}) |>
    remove_missing()
  
  BV2022 <- read_feather(paste0("results_building/bv2022-pmsi_",gender,".feather")) |>
    select(bv2022, t1d_e101) |>
    left_join(BV2022 |> select(-t1d_e101), by = "bv2022") |>
    mutate(pop_0029 = {{pop}}) |>
    remove_missing()
  
  main_regression(ARR24, arr24_formula, paste0("arr24-",gender)) |>
    rbind(main_regression(BV2022, bv2022_formula, paste0("bv2022-",gender)))
}

arr24_formula <- formula(t1d_e101 ~  
  PC1 + PC2 + PC3 + # SES VARIABLES
  tm_summer + tm_winter + # TEMPERATURES
  diag_vitamin_d + diag_calcium + diag_family + diag_other_minerals + # PMSI DATA (1/2)
  diag_tabacco + diag_vitamin_b9 + diag_vitamin_b12 + # PMSI DATA (2/2)
  somo35_mean + pm10_mean_concentration + o3_mean_concentration + # AIRQ(1/2)
  no2_mean_concentration + ges_resid + # AIRQ (2/2)
  water_no3*prop_robinet + water_ph*prop_robinet +  # WATERQ
  n_equip_d2 + n_equip_a1 + n_equip_c1 + n_equip_c2 + # EQUIPMENT CONTROLS
  n_ape5610c + n_ape9312z + n_ape4722z + n_ape4723z + # BUSINESSES
  coups + ac_prop # LIFEQ 
)

bv2022_formula <- update.formula(arr24_formula, 
  ~ . -tm_summer - tm_winter - water_no3*prop_robinet - water_ph*prop_robinet
)


REG_RESULTS <- main_regression(ARR24B, arr24_formula, "arr24") |>
  rbind(main_regression(BV2022, bv2022_formula, "bv2022"))

REG_RESULTS_F <- gendered_regressions("female", pop_f0029)
REG_RESULTS_H <- gendered_regressions("male", pop_h0029)

RESULTS <- rbind(REG_RESULTS, REG_RESULTS_F, REG_RESULTS_H)

fwrite(RESULTS, "results_analysis/main_results.csv")