# ---- Import Libraries ----
import pandas as pd
import numpy as np
import statsmodels.api as sm
from statsmodels.discrete.discrete_model import Poisson

# ---- Load Data ----
INCI_DEP = pd.read_feather("results_building/department_e101_0029.feather")
NUTRIMENTS = pd.read_feather("results_building/nutriments.feather")

# ---- Département to Région Mapping ----
DEP = pd.read_csv("data/external/INCA3/departements-region.csv", dtype={"region_name": str})
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
DEP['region'] = np.select(conds, choices, default=np.nan)
DEP = DEP.dropna(subset=['region'])[['num_dep','region']].drop_duplicates()

# ---- Join Nutrients and Incidence ----
NUTRIMENTS_DEP = (
    DEP
    .merge(NUTRIMENTS, on='region', how='left')
    .merge(INCI_DEP, left_on='num_dep', right_on='dep', how='left')
)

# ---- Function: Get Dispersion ----
def get_dispersion_pearson(res):
    resid = res.resid_pearson
    df = res.df_resid
    return np.sum(resid**2) / df

# ---- Function: Poisson Fixed Effects via GLM with Dummy Fixed Effects ----
def fixest_dep(data, desc):
    df = data.copy()
    df = df.dropna(subset=['pop_0029', 't1d_all'])
    # Offset
    offset = np.log(df['pop_0029'])
    # Variables endogène et exogènes (sans intercept)
    y = df['t1d_all'].values
    exog_vars = ['tm', 'txab', 'tnab', 'umm', 'inst', 'rr']
    X_base = df[exog_vars]
    # Dummies département
    dummies = pd.get_dummies(df['num_dep'], prefix='dep', drop_first=True)
    # Concat exogènes + dummies + constante
    X = pd.concat([X_base, dummies], axis=1)
    X = sm.add_constant(X)
    # Fit Poisson
    model = sm.GLM(y, X, family=sm.families.Poisson(), offset=offset).fit()

    # Résumé
    tab = model.summary2().tables[1].copy().rename(columns={
        'Coef.':'beta','Std.Err.':'std','z':'zval','P>|z|':'pval'
    })
    tab['description'] = desc
    tab['dispersion'] = get_dispersion_pearson(model)
    tab['pseudo_r2'] = 1 - model.deviance/model.null_deviance
    tab['nobs'] = model.nobs
    tab = tab.reset_index().rename(columns={'index':'variable'})
    return tab

# OLS FE + cluster (corrélation vitamin D vs inst)
def corr_vitD(data):
    df = data.copy()
    df = df.dropna(subset=['vitD_def','inst','visits','dep'])
    # Endog & exog
    y = df['vitD_def'].values
    X = df[['inst','visits']]
    # Dummies DEP by fixed effects
    dummies = pd.get_dummies(df['dep'], prefix='dep', drop_first=True)
    X = pd.concat([X, dummies], axis=1)
    X = sm.add_constant(X)
    # OLS with clustered SE
    ols = sm.OLS(y, X).fit(cov_type='cluster', cov_kwds={'groups':df['dep']})

    # Resume
    results = pd.DataFrame({
        'variable':ols.params.index,
        'beta':ols.params.values,
        'std':ols.bse.values,
        'zval':ols.tvalues.values,
        'pval':ols.pvalues.values
    })
    results['description'] = 'corr-vitD-inst'
    results['dispersion'] = np.nan
    results['pseudo_r2'] = ols.rsquared
    results['nobs'] = int(ols.nobs)
    return results

# ---- Run Models ----
if __name__ == '__main__':
    # FE Poisson
    FIXED_ALL  = fixest_dep(NUTRIMENTS_DEP.query('youth == 2'), 'fixed-poisson-all')
    FIXED_HIGH = fixest_dep(NUTRIMENTS_DEP.query('youth == 2 and nutriment52 > 2.553'),
                             'fixed-poisson-highD')
    FIXED_LOW  = fixest_dep(NUTRIMENTS_DEP.query('youth == 2 and nutriment52 < 2.553'),
                             'fixed-poisson-lowD')
    # Correlation
    CORR_VITD  = corr_vitD(INCI_DEP)

    # Harmonisation of columns
    # identify targets from FIXED_LOW
    cols = FIXED_LOW.columns.tolist()
    CORR_VITD = CORR_VITD[cols]

    # concat and export
    models = pd.concat([FIXED_ALL, FIXED_LOW, FIXED_HIGH, CORR_VITD], ignore_index=True)
    models.to_csv('results_analysis/poisson-fixed_effects.csv', index=False)