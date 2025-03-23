library(httr)
library(tidyverse)
library(data.table)

#Custom function
clean_names <- function(names) { 
  names <- gsub("[^a-zA-Z0-9 ]", "", names)  |>            
    tolower()                       
  return(names)
}

get_first_url <- function(old=T, size = 10e3, format_param = "csv"){
  
  select_params <- c(
    "N°DPE", "Date_réception_DPE", "Date_établissement_DPE", "Date_visite_diagnostiqueur", 
    "Modèle_DPE", "N°_DPE_remplacé", "Date_fin_validité_DPE", "Version_DPE", 
    "N°_DPE_immeuble_associé", "Etiquette_GES", "Etiquette_DPE", 
    "Type_bâtiment", "Type_installation_chauffage", 
    "Surface_habitable_logement", "Code_INSEE_(BAN)", "Code_postal_(BAN)", 
    "N°_étage_appartement", "Type_énergie_principale_chauffage", "Type_énergie_principale_ECS", "Type_installation_ECS",
    "Type_installation_chauffage_n°1", "Type_ventilation", "Surface_ventilée", 
    "Conso_refroidissement_annuel", "Type_générateur_froid", "Surface_climatisée"
  )
  
  if(old==T){
    select_params <- c(select_params, "Année_construction", "Période_construction")
  }
  
  data_type <- if(old==T){"logements-existants"}else{"logements-neufs"}
  
  base_url <- paste0("https://data.ademe.fr/data-fair/api/v1/datasets/dpe-v2-", data_type, "/lines")
  
  query_string <- paste0(
    "?size=", size,
    "&select=", URLencode(paste(select_params, collapse = ","), reserved = TRUE),
    "&format=", format_param
  )
  
  ademe_url <- paste0(base_url, query_string)
  
  return(ademe_url)

}

get_dpe <- function(old, lim, export_path){
  ademe_url <- get_first_url(old=old)
  counter <- 1
  while(str_starts(ademe_url, "https://")&counter<lim){
    print(counter)
    response <- GET(ademe_url)
    if(status_code(response)!=200){
      write(paste0(Sys.time(), "\nError at count: ", counter, "\nurl:", ademe_url, "\n"),file="ademe_log.txt",append=TRUE)
      break
    }
    csv_content <- content(response, "text")
    data <- fread(csv_content)
    names(data) <- names(data) |> clean_names()
    fwrite(as.data.frame(data), paste0(export_path, counter, ".csv"))
    ademe_url <- sub("^<([^>]+)>; rel=next$", "\\1", response$headers$link)
    counter <- counter + 1
  }
}

#GET API -----
get_dpe(old=T, 1050, "data/external/ademe_dpe/dpe_old/ademe_dpe_old_")
get_dpe(old=F, 101, "data/external/ademe_dpe/dpe_new/ademe_dpe_new_")

#JOIN AND EXPORT DATA -----
new_ls <- list.files("data/external/ademe_dpe/dpe_new/", full.names = T)
old_ls <- list.files("data/external/ademe_dpe/dpe_old/", full.names = T)

new <- lapply(new_ls, fread)
old <- lapply(old_ls, fread)

NEW_DF <- new |> 
  rbindlist() |>
  mutate(anneconstruction = NA, priodeconstruction=NA)

OLD_DF <- old |>
  rbindlist()

DPE_DF <- rbindlist(list(NEW_DF, OLD_DF), use.names = T)

write_feather(DPE_DF, "data/interim/temp/dpe_all.feather", compression = "zstd")
