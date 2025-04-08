library(tidyverse)
library(data.table)
library(arrow)

TABLE_PASSAGE_DF <- read_feather("results_building/t_passage.feather") |>
  select(code_insee24, arr24, bv2022, code_insee21)

ARR2COM <- read_feather("results_building/arr2com.feather") 

old_replaced <- open_dataset("data/interim/_ademe/dpe_all.feather", format = "arrow") |> 
  select(ndperemplac) |> 
  unique() |>
  collect() |>
  filter(ndperemplac!="")

old_building <- open_dataset("data/interim/_ademe/dpe_all.feather", format = "arrow") |> 
  select(ndpeimmeubleassoci) |>
  unique() |>
  collect() |>
  filter(ndpeimmeubleassoci!="")

old_filter <- data.table(dpe_id = c(old_replaced[[1]], old_building[[1]]), filt = T)

OLD_DPE <- open_dataset("data/interim/_ademe/dpe_all.feather", format = "arrow") |> 
  filter(modledpe=="DPE 3CL 2021 méthode logement")|>
  select(
    dpe_id = ndpe,
    dpe_replaced = ndperemplac,
    dpe_building = ndpeimmeubleassoci,
    date_visited = datevisitediagnostiqueur, 
    date_established = datetablissementdpe,
    type = modledpe,
    floor_area = surfacehabitablelogement, 
    ac_area = surfaceclimatise,
    ghg_grade = etiquetteges,
    dpe_grade = etiquettedpe, 
    year_built = anneconstruction, 
    period_built = priodeconstruction,
    insee_code = codeinseeban,
    heating_energy = typenergieprincipalechauffage,
    heating_type = typeinstallationchauffage
  ) |>
  mutate("insee_code" = str_remove_all(str_remove(insee_code, "old"), " ")) |>
  left_join(ARR2COM, by = c("insee_code"="code_arr")) |>
  mutate(insee_code = if_else(is.na(code_com), insee_code, code_com)) |>
  left_join(TABLE_PASSAGE_DF, by=c("insee_code"="code_insee21")) |>
  collect() |>
  setDT()

OLD_DPE <- old_filter[OLD_DPE, on = .(dpe_id)]
OLD_DPE <- OLD_DPE[is.na(filt)][, "filt" := NULL]

OLD_DPE[, "period" := fcase(
  period_built=="1948-1974", 1961,
  period_built=="1975-1977", 1976,
  period_built=="1978-1982", 1980, 
  period_built=="1983-1988", 1987,
  period_built=="1989-2000", 1995, 
  period_built=="2001-2005", 2003, 
  period_built=="2006-2012", 2009, 
  period_built=="2013-2021", 2016, 
  period_built=="après 2021", 2022, 
  period_built=="avant 1948", 1935
)]

OLD_DPE[, "ac_area" := if_else(is.na(ac_area), 0, ac_area)]
OLD_DPE[, "year_built" := if_else(is.na(year_built), period, year_built)]

DPE_ENCODER <- data.table(
  dpe_grade = c("A", "B", "C", "D", "E", "F", "G"),
  dpe_grade_num = 1:7
)

OLD_DPE <- OLD_DPE[DPE_ENCODER, on = .(dpe_grade)]

group_dpe <- function(OLD_DPE, groupvar){
  
  DPE_DF <- OLD_DPE[, 
    .(built_q2 = median(year_built, na.rm=T), floor_area_q2 = median(floor_area, na.rm=T),
      q1_dpe = quantile(dpe_grade_num, .25), q2_dpe = quantile(dpe_grade_num, .5), 
      q3_dpe = quantile(dpe_grade_num, .75), n_dpe = .N), 
    by = groupvar
  ]

  AC_DF <- OLD_DPE[ac_area >0, 
    .(ac_prop = .N), 
    by= groupvar
  ]

  DPE_DF <- DPE_DF |>
  mutate(iqr_dpe_num = round(q3_dpe) - round(q1_dpe)) |>
  pivot_longer(c(q1_dpe, q2_dpe, q3_dpe)) |>
  mutate(value = round(value, digits = 0)) |>
  pivot_wider(names_from = name, values_from = value) |>
  left_join(AC_DF, {{groupvar}}) |>
  mutate(ac_prop = ac_prop/n_dpe)

  return(DPE_DF)

}

OLD_DPE_ARR <- group_dpe(OLD_DPE, "arr24") |>
  select(arr24, everything()) |>
  filter(nchar(arr24)<4) |>
  arrange(arr24)

OLD_DPE_BV <- group_dpe(OLD_DPE, "bv2022") |>
  select(bv2022, everything()) |>
  filter(!is.na(bv2022)) |>
  arrange(bv2022)

OLD_DPE_DEP <- OLD_DPE |>
  mutate(dep = substr(arr24, 1, 2)) |>
  group_dpe("dep") |>
  select(dep, everything()) |>
  filter(!is.na(dep)) |>
  arrange(dep)

write_feather(as.data.frame(OLD_DPE_ARR), "data/interim/arrondissements/ademe_dpe.feather", compression = "zstd")
write_feather(as.data.frame(OLD_DPE_BV), "data/interim/bv2022/ademe_dpe.feather", compression = "zstd")
write_feather(as.data.frame(OLD_DPE_DEP), "data/interim/departement/ademe_dpe.feather", compression = "zstd")
