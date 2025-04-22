# ---- Import Libraries ----
import pandas as pd
import numpy as np
import warnings
import random
from datetime import datetime
from scipy.stats import randint
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import RandomizedSearchCV, KFold
from doubleml import DoubleMLData, DoubleMLPLR

# Silence warnings
warnings.filterwarnings("ignore")
# Reproducibility
SEED = 5415341
np.random.seed(SEED)
random.seed(SEED)

# ---- Load Data ----
INCI_DEP = pd.read_feather("results_building/department_e101_0029.feather")
NUTRIMENTS = pd.read_feather("results_building/nutriments.feather")

# ---- Map DEP to Region ----
DEP = pd.read_csv(
    "data/external/INCA3/departements-region.csv",
    dtype={"region_name": str}
)
conds = [
    DEP.region_name.str.startswith("Auvergne"),
    DEP.region_name.str.startswith("Hauts-de-France"),
    DEP.region_name.str.startswith("Provence-Alpes"),
    DEP.region_name.str.startswith("Grand Est"),
    DEP.region_name.str.startswith("Occitanie"),
    DEP.region_name.str.startswith("Normandie"),
    DEP.region_name.str.startswith("Nouvelle-Aquitaine"),
    DEP.region_name.str.startswith("Centre-Val"),
    DEP.region_name.str.startswith("Bourgogne-"),
    DEP.region_name.str.startswith("Bretagne"),
    DEP.region_name.str.startswith("Pays de la Loire"),
    DEP.region_name.str.endswith("de-France")
]
choices = [9, 6, 10, 7, 11, 2, 12, 3, 8, 5, 4, 1]
DEP["region"] = np.select(conds, choices, default=np.nan)
DEP = DEP.dropna(subset=["region"]).loc[:, ["num_dep", "region"]].drop_duplicates()

# ---- Merge Nutriments and Incidence Data ----
NUTRIMENTS_DEP = (
    DEP
    .merge(NUTRIMENTS, on="region", how="left")
    .merge(INCI_DEP, left_on="num_dep", right_on="dep", how="left")
)

# ---- PCA Data ----
PPC1 = (
    pd.read_csv("pca_arr_ses.csv", dtype={"arr24": str})
      .loc[:, ["arr24", "PC1", "PC2", "PC3"]]
      .assign(arr24=lambda df: df.arr24.str.zfill(3))
)
ARR24 = (
    pd.read_feather("results_building/arrondissement-pmsi_full.feather")
      .merge(PPC1, on="arr24", how="left")
      .dropna()
)

PPC1_BV = (
    pd.read_csv("pca_bv_ses.csv", dtype={"bv2022": str})
      .loc[:, ["bv2022", "PC1", "PC2", "PC3"]]
      .assign(bv2022=lambda df: df.bv2022.str.zfill(5))
)
BV2022 = (
    pd.read_feather("results_building/bv2022-pmsi_full.feather")
      .merge(PPC1_BV, on="bv2022", how="left")
      .dropna()
)

# ---- 5. Vitamin D hypothesis: filter youth & month, join at arrondissement level ----
INST_DEP = (
    NUTRIMENTS_DEP
    .query("youth == 2 and month == '01'")
    .loc[:, ["num_dep", "inst", "prop_robinet",
             "nutriment52","nutriment10","nutriment11",
             "nutriment1","nutriment5","nutriment3","nutriment31"]]
    .rename(columns={"num_dep": "dep"})
)
ARR24B = (
    ARR24
    .assign(dep=lambda df: df.arr24.str[:2])
    .merge(INST_DEP, on="dep", how="left")
)

# ---- 6. Add gender-specific diagnosis & populations ----
# Merge total and gender pops for arrondissements
pop_arr21 = pd.read_feather("data/interim/arrondissements/populations_2021.feather")
ARR24B = ARR24B.merge(pop_arr21, on="arr24", how="left")
# pop_arr21 expected cols: arr24,pop_0029,pop_f0029,pop_h0029
# Rename pop_0029 to pop_tot and drop the others
ARR24B = ARR24B.assign(pop_tot=ARR24B.pop_0029).drop(columns=["pop_0029"])

# Merge female data
df_f = (
    pd.read_feather("results_building/arrondissement-pmsi_female.feather")
      .loc[:, ["arr24", "t1d_e101", "diag_tabacco"]]
      .rename(columns={
          "t1d_e101": "t1df_e101",
          "diag_tabacco": "diag_tabacco_F"
      })
)
ARR24B = ARR24B.merge(df_f, on="arr24", how="left")
# Merge male data
df_m = (
    pd.read_feather("results_building/arrondissement-pmsi_male.feather")
      .loc[:, ["arr24", "t1d_e101", "diag_tabacco"]]
      .rename(columns={
          "t1d_e101": "t1dh_e101",
          "diag_tabacco": "diag_tabacco_H"
      })
)
ARR24B = ARR24B.merge(df_m, on="arr24", how="left")

# Drop any rows still missing key info
ARRZ = ARR24B.dropna(subset=["pop_tot", "t1d_e101", "t1df_e101", "t1dh_e101"])

# Compute incidence rates per 1000
ARRZ = ARRZ.assign(
    t1d_e101_rate = ARRZ.t1d_e101 / ARRZ.pop_tot * 1000,
    t1df_e101_rate = ARRZ.t1df_e101 / ARRZ.pop_f0029 * 1000,
    t1dh_e101_rate = ARRZ.t1dh_e101 / ARRZ.pop_h0029 * 1000
)

# ---- 7. DML helper ----
def run_dml(data, outcome_var, treatment_var, desc):
    # Log start
    with open("log.txt", "a") as fl:
        fl.write(f"{datetime.now()} - starting: {treatment_var}\n")

    # Drop NA in key columns
    df = data.dropna(subset=[outcome_var, treatment_var])

    # Identify confounders: exclude fixed prefixes & the vars
    exclude_pref = ("dep", "n_dpe", "PC", "arr24", "t1d", "diag_vitamin_d", "diag_tabacco_")
    exclude_exact = {outcome_var, treatment_var}
    cofounders = [
        col for col in df.columns
        if not col.startswith(exclude_pref)
        and col not in exclude_exact
    ]
    # Ensure total population included
    if "pop_tot" not in cofounders:
        cofounders.append("pop_tot")

    # Prepare matrices
    X = df[cofounders]
    y = df[outcome_var].values
    D = df[treatment_var].values

    # Hyperparameter tuning distributions
    max_feat = max(2, int(np.floor(0.7 * len(cofounders))))
    param_dist = {
        'n_estimators': randint(300, 1501),
        'max_features': randint(2, max_feat + 1)
    }
    # Tune l (Y ~ X)
    search_l = RandomizedSearchCV(
        RandomForestRegressor(random_state=SEED, n_jobs=-1),
        param_distributions=param_dist,
        n_iter=12, cv=3, random_state=SEED, n_jobs=-1
    )
    search_l.fit(X, y)
    best_l = search_l.best_estimator_

    # Tune m (D ~ X)
    search_m = RandomizedSearchCV(
        RandomForestRegressor(random_state=SEED, n_jobs=-1),
        param_distributions=param_dist,
        n_iter=12, cv=3, random_state=SEED, n_jobs=-1
    )
    search_m.fit(X, D)
    best_m = search_m.best_estimator_

    # DoubleML setup
    dml_data = DoubleMLData.from_arrays(X=X, y=y, d=D)
    dml_plr = DoubleMLPLR(
        dml_data,
        ml_l=best_l,
        ml_m=best_m,
        n_folds=4,
        n_rep=6,
        score="partialling out"
    )
    dml_plr.fit(store_predictions=True)

    # Extract fit results
    fit_df = pd.DataFrame({
        'beta': dml_plr.coef,
        'std': dml_plr.se,
        'tstat': dml_plr.t_stat,
        'pval': dml_plr.pval,
        'desc': desc
    })

    # Extract residuals / predictions
    preds = dml_plr.predictions['ml_m']  # shape (n_obs, folds*reps)
    resid_df = pd.DataFrame({
        'arr24': df['arr24'].values,
        'real_value': D,
        'mean_prediction': preds.mean(axis=1),
        'median_prediction': np.median(preds, axis=1),
        'desc': desc
    })

    return fit_df, resid_df

# ---- 8. Run DML for chosen variables ----
list_var = [
    "diag_tabacco", "diag_calcium", "diag_vitamin_b12",
    "n_ape4722z", "n_ape5610c", "n_ape9312z",
    "pm10_mean_concentration", "no2_mean_concentration",
    "coups"
]

all_results = []
all_residuals = []
for var in list_var:
    res, resid = run_dml(ARRZ, "t1d_e101_rate", var, f"dml-{var}")
    all_results.append(res)
    all_residuals.append(resid)

# Gender-specific tobacco DML
res_f, _ = run_dml(ARRZ, "t1df_e101_rate", "diag_tabacco_F", "dml-tabacco-female")
res_m, _ = run_dml(ARRZ, "t1dh_e101_rate", "diag_tabacco_H", "dml-tabacco-male")
all_results += [res_f, res_m]

# ---- 9. Export outputs ----
pd.concat(all_residuals, ignore_index=True).to_csv(
    "results_analysis/dml-residuals.csv", index=False
)
pd.concat(all_results, ignore_index=True).to_csv(
    "results_analysis/dml-results.csv", index=False
)
