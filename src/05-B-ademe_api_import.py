import os
import re
import io
import glob
from urllib.parse import quote

import requests
import pandas as pd
import pyarrow.feather as feather

def clean_names(cols):
    return [
        re.sub(r"[^a-zA-Z0-9 ]", "", col).lower()
        for col in cols
    ]

def get_first_url(old=True, size=10_000, fmt="csv"):
    select_params = [
        "N°DPE", "Date_réception_DPE", "Date_établissement_DPE", "Date_visite_diagnostiqueur",
        "Modèle_DPE", "N°_DPE_remplacé", "Date_fin_validité_DPE", "Version_DPE",
        "N°_DPE_immeuble_associé", "Etiquette_GES", "Etiquette_DPE",
        "Type_bâtiment", "Type_installation_chauffage",
        "Surface_habitable_logement", "Code_INSEE_(BAN)", "Code_postal_(BAN)",
        "N°_étage_appartement", "Type_énergie_principale_chauffage",
        "Type_énergie_principale_ECS", "Type_installation_ECS",
        "Type_installation_chauffage_n°1", "Type_ventilation", "Surface_ventilée",
        "Conso_refroidissement_annuel", "Type_générateur_froid", "Surface_climatisée"
    ]
    if old:
        select_params += ["Année_construction", "Période_construction"]

    data_type = "logements-existants" if old else "logements-neufs"
    base = f"https://data.ademe.fr/data-fair/api/v1/datasets/dpe-v2-{data_type}/lines"

    qs = (
        f"?size={size}"
        f"&select={quote(','.join(select_params), safe='')}"
        f"&format={fmt}"
    )
    return base + qs

def get_dpe(old: bool, lim: int, export_prefix: str):

    url = get_first_url(old=old)
    os.makedirs(os.path.dirname(export_prefix), exist_ok=True)
    counter = 1

    while url.startswith("https://") and counter < lim:
        print(f"Fetching page {counter}…")
        resp = requests.get(url)
        if resp.status_code != 200:
            with open("ademe_log.txt", "a") as log:
                log.write(f"{pd.Timestamp.now()}\n"
                          f"Error at count: {counter}\n"
                          f"URL: {url}\n\n")
            break

        df = pd.read_csv(io.StringIO(resp.text))

        df.columns = clean_names(df.columns)

        out_path = f"{export_prefix}{counter}.csv"
        df.to_csv(out_path, index=False)

        link = resp.headers.get("link", "")

        next_url = resp.links.get("next", {}).get("url")

        if not next_url:
            break

        url = next_url
        counter += 1



get_dpe(old=True,  lim=1050, export_prefix="data/external/ademe-dpe/dpe_old/ademe_dpe_old_")
get_dpe(old=False, lim=101,  export_prefix="data/external/ademe-dpe/dpe_new/ademe_dpe_new_")

# Join & export ----------------
new_files = sorted(glob.glob("data/external/ademe-dpe/dpe_new/*.csv"))
old_files = sorted(glob.glob("data/external/ademe-dpe/dpe_old/*.csv"))

# Read In ----------------
old_dfs = [pd.read_csv(f) for f in old_files]
old_df = pd.concat(old_dfs, ignore_index=True, sort=False)

# Fill New ----------------
new_dfs = [pd.read_csv(f) for f in new_files]
new_df = pd.concat(new_dfs, ignore_index=True, sort=False)
new_df["anneconstruction"] = pd.NA
new_df["priodeconstruction"] = pd.NA

# Row Bind filling missing cols with NaN ----------------
dpe_df = pd.concat([new_df, old_df], ignore_index=True, sort=False)

# Export ----------------
os.makedirs("data/interim/_ademe", exist_ok=True)
feather.write_feather(dpe_df, "data/interim/_ademe/dpe_all.feather", compression="zstd")
