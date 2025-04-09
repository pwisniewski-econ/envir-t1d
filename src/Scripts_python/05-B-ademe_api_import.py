import os
import re
import requests
import urllib.parse
import pandas as pd
from pathlib import Path
from datetime import datetime
from time import sleep

def clean_names(names):
    return [re.sub(r'[^a-zA-Z0-9 ]', '', col).lower() for col in names]

def get_first_url(old=True, size=10_000, format_param="csv"):
    select_params = [
        "N°DPE", "Date_réception_DPE", "Date_établissement_DPE", "Date_visite_diagnostiqueur",
        "Modèle_DPE", "N°_DPE_remplacé", "Date_fin_validité_DPE", "Version_DPE",
        "N°_DPE_immeuble_associé", "Etiquette_GES", "Etiquette_DPE",
        "Type_bâtiment", "Type_installation_chauffage",
        "Surface_habitable_logement", "Code_INSEE_(BAN)", "Code_postal_(BAN)",
        "N°_étage_appartement", "Type_énergie_principale_chauffage", "Type_énergie_principale_ECS", "Type_installation_ECS",
        "Type_installation_chauffage_n°1", "Type_ventilation", "Surface_ventilée",
        "Conso_refroidissement_annuel", "Type_générateur_froid", "Surface_climatisée"
    ]

    if old:
        select_params.extend(["Année_construction", "Période_construction"])

    data_type = "logements-existants" if old else "logements-neufs"
    base_url = f"https://data.ademe.fr/data-fair/api/v1/datasets/dpe-v2-{data_type}/lines"

    query_string = (
        f"?size={size}"
        f"&select={urllib.parse.quote(','.join(select_params))}"
        f"&format={format_param}"
    )

    return base_url + query_string

def get_dpe(old, lim, export_path):
    ademe_url = get_first_url(old=old)
    counter = 1

    while ademe_url.startswith("https://") and counter < lim:
        print(f"Request #{counter} - URL: {ademe_url}")
        response = requests.get(ademe_url)

        if response.status_code != 200:
            with open("ademe_log.txt", "a") as log_file:
                log_file.write(f"{datetime.now()} - Error at count: {counter}\nurl: {ademe_url}\n")
            break

        csv_content = response.content.decode('utf-8')
        df = pd.read_csv(pd.compat.StringIO(csv_content))
        df.columns = clean_names(df.columns)
        Path(export_path).parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(f"{export_path}{counter}.csv", index=False)

        link_header = response.headers.get("link", "")
        match = re.match(r'^<([^>]+)>; rel=next$', link_header)
        ademe_url = match.group(1) if match else ""
        counter += 1
        sleep(0.5)

# Récupération depuis l'API
get_dpe(old=True, lim=1050, export_path="data/external/ademe-dpe/dpe_old/ademe_dpe_old_")
get_dpe(old=False, lim=101, export_path="data/external/ademe-dpe/dpe_new/ademe_dpe_new_")

# Regroupement
from glob import glob
import pyarrow.feather as feather

def aggregate_csvs(path_list):
    dataframes = [pd.read_csv(f) for f in path_list]
    return pd.concat(dataframes, ignore_index=True)

new_files = sorted(glob("data/external/ademe-dpe/dpe_new/*.csv"))
old_files = sorted(glob("data/external/ademe-dpe/dpe_old/*.csv"))

df_new = aggregate_csvs(new_files)
df_new["anneconstruction"] = pd.NA
df_new["priodeconstruction"] = pd.NA

df_old = aggregate_csvs(old_files)

dpe_df = pd.concat([df_new, df_old], ignore_index=True)

# Export .feather compressé
feather.write_feather(dpe_df, "data/interim/_ademe/dpe_all.feather", compression="zstd")