# ---- Import Libraries ----
import pandas as pd
import numpy as np
import statsmodels.api as sm
from statsmodels.discrete.discrete_model import NegativeBinomial
from patsy import dmatrices

# ---- Load Data ----
INCI_DEP = pd.read_feather("results_building/department_e101_0029.feather")
NUTRIMENTS = pd.read_feather("results_building/nutriments.feather")

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
    DEP.region_name.str.endswith("de-France"),
]
choices = [9, 6, 10, 7, 11, 2, 12, 3, 8, 5, 4, 1]
DEP["region"] = np.select(conds, choices, default=np.nan)
DEP = (
    DEP.dropna(subset=["region"])
       .loc[:, ["num_dep", "region"]]
       .drop_duplicates()
)

NUTRIMENTS_DEP = (
    DEP
    .merge(NUTRIMENTS, on="region", how="left")
    .merge(INCI_DEP, left_on="num_dep", right_on="dep", how="left")
)

# ---- PCA and Population Data ----
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

# ---- Vitamin D Hypothesis - Department Level ----
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
    .assign(
        dep=ARR24.arr24
            .str[:2]
            .astype(int)
            .astype(str)
            .str.zfill(2)
    )
    .merge(INST_DEP, on="dep", how="left")
    .dropna(subset=["inst"])
)

# ---- Patsy formulas ----
arr24_formula = """
    t1d_e101 ~ 
      PC1 + PC2 + PC3 +
      tm_summer + tm_winter +
      diag_vitamin_d + diag_calcium + diag_family + diag_other_minerals +
      diag_tabacco + diag_vitamin_b9 + diag_vitamin_b12 +
      somo35_mean + pm10_mean_concentration + o3_mean_concentration +
      no2_mean_concentration + ges_resid +
      water_no3:prop_robinet + water_ph:prop_robinet +
      n_equip_d2 + n_equip_a1 + n_equip_c1 + n_equip_c2 +
      n_ape5610c + n_ape9312z + n_ape4722z + n_ape4723z +
      coups + ac_prop
"""
bv2022_formula = arr24_formula \
    .replace("tm_summer + tm_winter + ", "") \
    .replace("water_no3:prop_robinet + water_ph:prop_robinet + ", "")

# ---- Regression Functions ----
def main_regression(df, formula, desc):
    offset = np.log(df["pop_0029"])
    y, X = dmatrices(formula, df, return_type="dataframe")
    y = np.asarray(y).ravel()

    pois = sm.GLM(y, X, family=sm.families.Poisson(), offset=offset).fit()
    nb = NegativeBinomial(endog=y, exog=X, offset=offset).fit(
        maxiter=100, method="newton", disp=False
    )

    def summarize(res, label):
        tab = res.summary2().tables[1].reset_index().rename(columns={
            "index": "variable",
            "Coef.": "beta",
            "Std.Err.": "std",
            "z": "zval",
            "P>|z|": "pval"
        })
        tab["description"] = label
        tab["dispersion"] = res.deviance / res.df_resid
        tab["pseudo_r2"] = (1 - res.deviance / res.null_deviance) \
            if hasattr(res, "null_deviance") else np.nan
        tab["aic"] = res.aic
        tab["nobs"] = res.nobs
        tab["df"] = res.df_resid
        return tab

    df_p = summarize(pois, f"poisson-{desc}")
    df_nb = summarize(nb,   f"negbinom-{desc}")
    return pd.concat([df_p, df_nb], ignore_index=True)


def gendered_regressions(gender, pop_arr24, pop_bv):
    df_arr = (
        pd.read_feather(f"results_building/arrondissement-pmsi_{gender}.feather")
          .loc[:, ["arr24", "t1d_e101"]]
          .merge(ARR24B, on="arr24")
          .merge(pop_arr24.rename("pop_0029"), left_on="arr24", right_index=True)
          .dropna()
    )
    df_bv = (
        pd.read_feather(f"results_building/bv2022-pmsi_{gender}.feather")
          .loc[:, ["bv2022", "t1d_e101"]]
          .merge(BV2022, on="bv2022")
          .merge(pop_bv.rename("pop_0029"), left_on="bv2022", right_index=True)
          .dropna()
    )
    res_arr = main_regression(df_arr, arr24_formula, f"arr24-{gender}")
    res_bv  = main_regression(df_bv,  bv2022_formula, f"bv2022-{gender}")
    return pd.concat([res_arr, res_bv], ignore_index=True)

# ---- populations Data 0-29 YO (arrondissements & bv) ----
pop_arr21 = pd.read_feather("data/interim/arrondissements/populations_2021.feather")
pop_f_arr24 = pop_arr21.set_index("arr24")["pop_f0029"]
pop_h_arr24 = pop_arr21.set_index("arr24")["pop_h0029"]

pop_bv21 = pd.read_feather("data/interim/bv2022/populations_2021.feather")
pop_f_bv = pop_bv21.set_index("bv2022")["pop_f0029"]
pop_h_bv = pop_bv21.set_index("bv2022")["pop_h0029"]

# ---- the main run for results ----
if __name__ == "__main__":
    res_dep_arr = main_regression(ARR24B, arr24_formula, "arr24")
    res_dep_bv  = main_regression(BV2022,  bv2022_formula, "bv2022")
    res_f = gendered_regressions("female", pop_f_arr24, pop_f_bv)
    res_h = gendered_regressions("male",   pop_h_arr24, pop_h_bv)
    RESULTS = pd.concat([res_dep_arr, res_dep_bv, res_f, res_h], ignore_index=True)
    RESULTS.to_csv("results_analysis/main_results.csv", index=False)