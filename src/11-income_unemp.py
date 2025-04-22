import pandas as pd
import pyarrow.feather as feather

def import_filosofi(filename):
    path = f"data/external/insee-filosofi/{filename}"
    df = pd.read_csv(path, dtype=str, delimiter=";")
    df.columns = df.columns.str.lower()

    # compute per‑person metric from the raw strings
    df['nbpersmenfisc21'] = (
        pd.to_numeric(df['nbpersmenfisc21']
                      .astype(str)
                      .str.replace(',', '.', regex=False),
                      errors='coerce')
        /
        pd.to_numeric(df['nbmenfisc21']
                      .astype(str)
                      .str.replace(',', '.', regex=False),
                      errors='coerce')
    )

    df = df[['codgeo', 'nbpersmenfisc21', 'med21', 'pimp21',
             'tp6021', 'ppsoc21', 'ppfam21', 'd121', 'd921', 'rd21']]
    df = df.rename(columns={'nbpersmenfisc21': 'nombre_pers_menage'})

    # convert every non‑geo column to numeric, safely via string
    for col in df.columns.drop('codgeo'):
        df[col] = pd.to_numeric(
            df[col].astype(str).str.replace(',', '.', regex=False),
            errors='coerce'
        )

    df = df[df['pimp21'].notna()]
    return df

FILO_BV22 = import_filosofi("cc_filosofi_2021_BV2022.csv")\
               .rename(columns={'codgeo': 'bv2022'})
FILO_DEP  = import_filosofi("cc_filosofi_2021_DEP.csv")\
               .rename(columns={'codgeo': 'dep'})


def import_filo(year):
    path = f"data/external/insee-filosofi/FILO{year}_DEC_ARR.csv"
    df = pd.read_csv(path, dtype=str, delimiter=";")

    # drop trailing two‑digit suffixes from column names
    df.columns = df.columns.str.replace(r'\d{2}$', '', regex=True)

    df['nombre_pers_menage'] = (
        pd.to_numeric(df['NBPERS']
                      .astype(str)
                      .str.replace(',', '.', regex=False),
                      errors='coerce')
        /
        pd.to_numeric(df['NBMEN']
                      .astype(str)
                      .str.replace(',', '.', regex=False),
                      errors='coerce')
    )

    df = df.rename(columns={
        'CODGEO': 'arr24',
        'PMIMP': 'proportion_imposable_ens_arr',
        'D1':    'd1_ens_arr',
        'Q1':    'q1_ens_arr',
        'Q2':    'q2_ens_arr',
        'Q3':    'q3_ens_arr',
        'D9':    'd9_ens_arr',
        'GI':    'gini_ens_arr',
        'PACT':  'proportion_activite_ens_arr',
        'PPEN':  'proportion_retraite_ens_arr',
        'TYM4Q1':'q1_couple_enfants_arr',
        'TYM4Q2':'q2_couple_enfants_arr',
        'TYM4Q3':'q3_couple_enfants_arr',
        'TYM5Q1':'q1_parent_seul_arr',
        'TYM5Q2':'q2_parent_seul_arr',
        'TYM5Q3':'q3_parent_seul_arr',
    })

    cols = [
        'arr24', 'nombre_pers_menage',
        'proportion_imposable_ens_arr', 'd1_ens_arr',
        'q1_ens_arr', 'q2_ens_arr', 'q3_ens_arr',
        'd9_ens_arr', 'gini_ens_arr',
        'proportion_activite_ens_arr', 'proportion_retraite_ens_arr',
        'q1_couple_enfants_arr', 'q2_couple_enfants_arr',
        'q3_couple_enfants_arr', 'q1_parent_seul_arr',
        'q2_parent_seul_arr', 'q3_parent_seul_arr'
    ]
    df = df[cols]

    for col in df.columns.drop('arr24'):
        df[col] = pd.to_numeric(
            df[col].astype(str).str.replace(',', '.', regex=False),
            errors='coerce'
        )

    df['year'] = year
    df = df[df['arr24'].str.len() < 4]
    return df

years = range(2018, 2022)
FILO_ARR = pd.concat((import_filo(y) for y in years), ignore_index=True)


# Write out all three levels as Feather with zstd compression
feather.write_feather(FILO_ARR, "data/interim/arrondissements/filosofi.feather", compression="zstd")
feather.write_feather(FILO_BV22, "data/interim/bv2022/filosofi.feather",         compression="zstd")
feather.write_feather(FILO_DEP,  "data/interim/departement/filosofi.feather",     compression="zstd")


# === Unemployment ===
UNEMP = pd.read_csv("data/external/insee-chomage/DS_RP_EMPLOI_LR_COMP_data.csv", dtype=str, delimiter=";")

def compute_unemp(df, level, level_name):
    # Active population (EMPSTA 1T2)
    act = df[
        (df['AGE']=="Y15T64") &
        (df['EMPSTA_ENQ']=="1T2") &
        (df['FREQ']=="A") &
        (df['GEO_OBJECT']==level) &
        (df['PCS']=="_T") &
        (df['TIME_PERIOD']=="2021")
    ][['GEO','OBS_VALUE']].rename(columns={'GEO':'geo','OBS_VALUE':'active_pop'})

    # Unemployed population (EMPSTA 2)
    un = df[
        (df['AGE']=="Y15T64") &
        (df['EMPSTA_ENQ']=="2") &
        (df['FREQ']=="A") &
        (df['GEO_OBJECT']==level) &
        (df['PCS']=="_T") &
        (df['TIME_PERIOD']=="2021")
    ][['GEO','OBS_VALUE']].rename(columns={'GEO':'geo','OBS_VALUE':'unemployed_pop'})

    # Merge and compute rate
    merged = pd.merge(un, act, on='geo', how='left')
    merged['unemp_rate'] = (
        merged['unemployed_pop'].astype(float)
        / merged['active_pop'].astype(float)
    )
    return merged[['geo','unemp_rate']].rename(columns={'geo': level_name})

UNEMP_ARR  = compute_unemp(UNEMP, "ARR",    "arr24")
UNEMP_BV22 = compute_unemp(UNEMP, "BV2022","bv2022")

# Write unemployment rates to Feather
feather.write_feather(UNEMP_ARR,  "data/interim/arrondissements/unemployment.feather", compression="zstd")
feather.write_feather(UNEMP_BV22, "data/interim/bv2022/unemployment.feather",         compression="zstd")
