
import numpy as np
import pandas as pd

from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer


def make_pca(
    df: pd.DataFrame,
    col_for_PCA: list = None,
    col_to_remove: list = None,
    n_components: int = 10
):

    """
    Effectue une PCA sur un dataframe
    Si col_for_PCA est renseigné, la PCA est effectué sur ces colonnes,
    Si col_to_remove est renseigné, ces colonnes sont supprimées du dataframe et la
    PCA est faite sur le DataFrame

    Renvoie:
    - df_pca: DataFrame contenant les composantes principales
    - variance_expliquee: Variance expliquée par chaque composante
    - loadings_df: DataFrame contenant les loadings
    """

    scaler = StandardScaler()
    imputer = SimpleImputer(missing_values=np.nan, strategy='mean')
    pca = PCA(n_components=n_components)

    df2 = df.copy()

    if col_to_remove is not None:
        df2 = df.drop(col_to_remove, axis=1)

    elif col_for_PCA is not None:
        df2 = df[col_for_PCA]

    # On scale les variables de notre dataframe
    df_scaled = scaler.fit_transform(df2)

    # On applique la stratégie pour les valeurs manquantes
    df_imputed = pd.DataFrame(imputer.fit_transform(df_scaled))

    # On réalise la PCA
    pca_result = pca.fit_transform(df_imputed)

    # On stocke la PCA dans un dataframe
    df_pca = pd.DataFrame(
        pca_result,
        columns=[f'PC{i+1}' for i in range(pca_result.shape[1])],
        index=df['arr24']
        )

    variance_expliquee = pca.explained_variance_ratio_
    loadings = pca.components_.T * np.sqrt(pca.explained_variance_)
    loadings_df = pd.DataFrame(
        loadings,
        columns=[f'PC{i+1}' for i in range(loadings.shape[1])],
        index=df2.columns
        )

    return df_pca, variance_expliquee, loadings_df

###################################################################


chemin = "./results_building/"

# IMPORT DES VARIABLES D'ARRONDISSEMENT
df_arr_full = pd.read_feather(path=chemin + "arrondissement-full.feather")


# PCA POUR TOUTES LES VARIABLES
col_to_remove = [
    'arr24'
]

pca_arr, var_exp_arr, load_arr = make_pca(
    df=df_arr_full,
    col_to_remove=col_to_remove,
    n_components=10
    )

pca_arr.to_csv("./results_analysis/pca_arr.csv")
print("PCA exportée")
load_arr.to_csv("./results_analysis/load_arr.csv")
print("Loads exportés")
df = pd.DataFrame(var_exp_arr)
df.to_csv("./results_analysis/var_arr.csv")
print("variance exportée")

d = []
for i in range(len(var_exp_arr)):
    d.append({"pc" : i + 1, "var": var_exp_arr[i] })
print(var_exp_arr)
print(d)

df = pd.DataFrame(d, index=None, columns=["pc","var"])
df.to_csv("./results_analysis/var_arr.csv")
print("variance exportée")

# PCA POUR LES VARIABLES SOCIO ECONOMIQUES
col_for_PCA = [
  'dip_001T',
  'dip_200R',
  'dip_300R',
  'dip_350R',
  'dip_500R',
  'dip_600R',
  'dip_700R',
  'proportion_imposable_ens_arr',
  'd1_ens_arr',
  'q1_ens_arr',
  'q2_ens_arr',
  'q3_ens_arr',
  'd9_ens_arr',
  'gini_ens_arr',
  ]

pca_arr_ses, var_exp_arr_ses, load_arr_ses = make_pca(
    df=df_arr_full,
    col_for_PCA=col_for_PCA,
    n_components=10
    )

pca_arr_ses.to_csv("./results_analysis/pca_arr_ses.csv")
print("PCA exportée")
load_arr_ses.to_csv("./results_analysis/load_arr_ses.csv")
print("Loads exportés")

d = []
for i in range(len(var_exp_arr_ses)):
    d.append({"pc" : i + 1, "var": var_exp_arr_ses[i] })

df = pd.DataFrame(d, index=None, columns=["pc","var"])

df.to_csv("./results_analysis/var_arr_ses.csv")
print("variance exportée")

