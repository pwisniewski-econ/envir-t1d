# 🌍 Modélisation Écologique du Diabète de Type 1 en France Métropolitaine

## 🎯 Objectif du Projet

Ce projet, réalisé dans le cadre du **Business Data Challenge 2025** à l’ENSAE Paris, vise à identifier les **facteurs environnementaux, sociaux et économiques** associés à l’incidence du diabète de type 1 (DT1) chez les jeunes en France métropolitaine.  
L’analyse repose sur des données hospitalières issues du **PMSI** et de jeux de données enrichis (pollution, climat, conditions de vie, équipements, etc.).

---

## 🗂️ Structure du dépôt

```
.
├── data/
│   ├── external/                   # Données INSEE, INCA3, SIRENE...
│   └── interim/                    # Données intermédiaires (arrondissements, BV)
│
├── results_building/               # Jeux de données nettoyés & enrichis
│   ├── arr2com.feather
│   ├── arrondissement-full.feather
│   ├── arrondissement-flow.feather
│   ├── bv2022-full.feather
│   ├── bv2022-flow.feather
│   ├── dep-full.feather
│   ├── t_passage.feather
│   ├── postal_converter.feather
│   └── dictionary.md              # Dictionnaire des variables
│
├── results_analysis/               # Résultats finaux (modèles)
│   ├── main_results.csv
│   ├── poisson-fixed_effects.csv
│   ├── dml-results.csv
│   ├── dml-residuals.csv
│   ├── t1d_levels_*.csv           # Incidence DT1 par niveau géographique
│   ├── total_t1d.csv              # Comptages globaux
│   ├── pca_arr.csv / pca_arr_ses.csv
│   ├── var_arr.csv / var_arr_ses.csv
│   └── readme.md
│
├── scripts/
│   ├── 01-B-table_insee.py         # Traitement des tables INSEE
│   ├── 23-A-poisson-CASD.py        # Régressions Poisson & NB
│   ├── 24-A-fixed_poisson-CASD.py  # Effets fixes (ensoleillement, climat)
│   └── 25-double_ml-CASD.py        # Double Machine Learning
│
├── report.pdf                      # Rapport (version provisoire)
└── README.md
```

---

## 🔬 Méthodologie

Ce projet vise l’**inférence causale** (et non prédictive). Trois approches :

### 1. Régressions Poisson & Binomiale Négative
- Niveaux géographiques : **arrondissements** & **bassins de vie (BV2022)**
- Variables explicatives : climat, pollution, infrastructures, nutrition, précarité...
- Réduction de dimension par **ACP** (PC1 à PC3)

### 2. Modèles à Effets Fixes
- Modèles mensuels au niveau départemental
- Inclusion de **températures et ensoleillement** pour tester le rôle de la **vitamine D**
- Tests sur sous-groupes : carence élevée vs faible

### 3. Double Machine Learning (DML)
- Méthode robuste pour estimer des effets causaux partiels
- Régressions avec **forêts aléatoires** + cross-validation
- Analyses séparées par sexe pour capturer des effets hétérogènes

---

## 📊 Résultats clés

| Hypothèse | Observation |
|-----------|-------------|
| **Soleil & Vitamine D** | Chaque jour ensoleillé est associé à une **baisse de 3.8%** de l’incidence mensuelle de DT1 |
| **Tabac** | Fortement associé à une hausse de l’incidence, surtout chez les hommes |
| **Nutrition & Fast-Food** | Corrélation significative avec des nutriments spécifiques et la densité de fast-food |
| **Contexte socio-économique** | Les **PCs issus de l’ACP** capturent des effets significatifs (éducation, emploi, etc.) |

---

## 📁 Résultats disponibles

| Fichier CSV                       | Description |
|----------------------------------|-------------|
| `main_results.csv`               | Régressions Poisson et NB |
| `poisson-fixed_effects.csv`      | Modèles à effets fixes (ensoleillement, climat) |
| `dml-results.csv`                | Résultats Double Machine Learning |
| `dml-residuals.csv`              | Résidus DML pour évaluation |
| `t1d_levels_*.csv`               | Incidence par département / arrondissement |
| `pca_arr_ses.csv`                | Scores de précarité (ACP SES) |
| `var_arr.csv`, `var_arr_ses.csv`| Statistiques descriptives |

---

## ⚙️ Reproduire l’analyse

1. Installer l’environnement :
```bash
conda create -n dt1-env python=3.10
conda activate dt1-env
pip install -r requirements.txt
```

2. Lancer les scripts dans l’ordre :
```bash
python scripts/01-B-table_insee.py
python scripts/23-A-poisson-CASD.py
python scripts/24-A-fixed_poisson-CASD.py
python scripts/25-double_ml-CASD.py
```

> ⚠️ **Attention** : les données PMSI sont confidentielles et ne peuvent pas être partagées publiquement.

---

## 👥 Équipe projet

- Éléa Bordais – ENSAE Paris
- Ismaël Dembele – ENSAE Paris
- Paul Toudret – ENSAE Paris
- Patryk Wiśniewski – ENSAE Paris

**Encadrement :**
- Pr. Azadeh Khaleghi (ENSAE Paris)
- Partenaire : **Sanofi** (Noémie Allali, Olivia Carnapete, Thomas Séjourné)

---

## 📚 Remerciements

Merci à **Sanofi** pour leur accompagnement, ainsi qu’à nos encadrants pour leur soutien scientifique et méthodologique tout au long du challenge.

---