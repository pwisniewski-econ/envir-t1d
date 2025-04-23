library(lfe)

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

get_dispersion_femlm <- function(mod){
  resid <- residuals(mod, type = "pearson")  
  df <- mod$nobs - length(coef(mod))  
  sum(resid^2) / df   
}

fixest_dep <- function(DATA, desc){
  setDT(DATA)
  mod <- fixest::femlm(t1d_all ~  tm + txab + tnab + umm + inst + rr, 
                       data = DATA, 
                       family = "poisson", 
                       offset = log(DATA$pop_0029),
                       fixef = "num_dep")
  as.data.frame(mod$coeftable) |>
    mutate(description = desc,
           dispersion = get_dispersion_femlm(mod), 
           pseudo_r2 = mod$pseudo_r2, 
           nobs = mod$nobs)
}


check_corr <- felm(vitD_def ~  inst + visits | dep | 0 | dep, data = INCI_DEP) |>
  summary()

FIXED_ALL <- fixest_dep(NUTRIMENTS_DEP[youth == 2],"fixed-poisson-all")
FIXED_HIGH <- fixest_dep(NUTRIMENTS_DEP[youth == 2 & nutriment52 > 2.553], "fixed-poisson-highD")
FIXED_LOW <- fixest_dep(NUTRIMENTS_DEP[youth == 2 & nutriment52 < 2.553], "fixed-poisson-lowD")
CORR_VITD <- as.data.frame(check_corr$coefficients) |>
  mutate(desc = "corr-vitD-inst", dispersion = NA, pseudo_r2 = check_corr$r2, nobs = check_corr$N)
names(CORR_VITD) <- names(FIXED_LOW)

models <- rbind(FIXED_ALL, FIXED_LOW, FIXED_HIGH, CORR_VITD)
fwrite(models, "results_analysis/poisson-fixed_effects.csv")

