# 🌍 Modeling of Type 1 Diabetes in Metropolitan France

## 🎯 Project Overview

This repository contains the full analytical pipeline of a project developed for the **Business Data Challenge 2025** at ENSAE Paris.  

The goal is to identify and quantify **environmental, social, and economic factors** associated with the incidence of **Type 1 Diabetes (T1D)** among individuals in metropolitan France.

We combine **individual-level hospital data** from the French PMSI system with rich external datasets (pollution, weather, nutrition, socio-demographics, infrastructure), and apply causal inference techniques to analyze risk and protective factors.

---

## 🗂️ Repository Structure

```
.
├── data/
│   ├── external/                   # INSEE, SIRENE, INCA3, Météo France
│   └── interim/                    # Intermediate datasets (arrondissements, BV2022)
│
├── results_building/               # Cleaned and enriched datasets
├── results_analysis/               # Final model results (CSV format)
│
├── scripts/                        # Feature engineering, data preparation
│   ├── 01-B-table_insee.py
│   ├── ...
│   └── 22-Z-clean_final.py
│
├── src/
│   ├── 01-B-table_insee.py        # Construction des tables INSEE
|   .
|   .      #processing tables and variables
|   .
│   ├── 23-A-poisson-CASD.py        # Python version of count models
│   ├── 23-A-poisson-CASD.R         # ✅ R version used for CASD execution (Poisson/NB)
│   ├── 24-A-fixed_poisson-CASD.py  # Python equivalent of FE Poisson
│   ├── 24-A-fixed_poisson-CASD.R   # ✅ R version used in CASD (climate/vit. D models)
│   └── 25-double_ml-CASD.R         # ✅ R-only script for Double ML (not feasible in Python)
│
├── report.pdf                      # Final report (in progress)
└── README.md                       # You are here
```

> ✅ **Note on modeling scripts**:  
> - Scripts `23-A-poisson-CASD.R` and `24-A-fixed_poisson-CASD.R` were executed in **R** within the **CASD environment** to produce the official results used in the report.  
> - Their corresponding `.py` versions are faithful **Python transcriptions**, included for documentation, readability, and reproducibility.  
> - The Double Machine Learning model (`25-double_ml-CASD.R`) was developed **exclusively in R** as the DML package was not functionning with the CASD

---

## 🧠 Methodology

### 1. Poisson & Negative Binomial Regressions (R/Python)
- Aggregated data at **arrondissement** and **BV2022** levels
- Predictors: air quality, nutrition, water quality, public infrastructure, socio-demographics (via PCA)
- Models estimated using **Poisson GLM** and **Negative Binomial**

### 2. Fixed Effects Poisson Model (R/Python)
- Monthly incidence data at **department** level
- Investigates the **sunlight–vitamin D hypothesis**
- Controls for unobserved heterogeneity with department fixed effects

### 3. Double Machine Learning (R only)
- Partial effect estimation of selected "treatments" (e.g. tobacco use, pollution)
- Implements **DoubleMLPLR** with **Random Forests**

---

## 📈 Key Results

| Variable / Factor               | Effect on T1D | Notes |
|--------------------------------|---------------|-------|
| 🌞 Sunlight exposure            | ⬇ -3.8%/day   | Strong evidence from fixed effects models |
| 🚬 Tobacco addiction (Males)   | ⬆            | Significant and robust across models |
| 🍔 Fast-food access             | ⬆            | Moderately significant in DML |

---

## 🔁 How to Reproduce (Python version)

> ⚠️ Due to confidentiality rules, **PMSI hospital data is not shared publicly**. Scripts below assume access.
A copy of the public data can be found in a [Google Drive](https://drive.google.com/drive/folders/1MWx1YN4SSBCv7_a05P1RpGsR9kHiRRD0?usp=drive_link)


1. Set up Python environment:
```bash
conda create -n dt1-env python=3.10
conda activate dt1-env
pip install -r requirements.txt
```

2. Run scripts:
```bash
# Data cleaning & preparation
python scripts/01-B-table_insee.py
...

# Optional model replication in Python
python src/23-A-poisson-CASD.py
python src/24-A-fixed_poisson-CASD.py
```

3. Run modeling in R (preferred / official results):
```r
# Main causal models
source("src/23-A-poisson-CASD.R")
source("src/24-A-fixed_poisson-CASD.R")
source("src/25-double_ml-CASD.R")
```

---

## 👨‍🔬 Project Team

- **Éléa Bordais** – ENSAE Paris  
- **Ismaël Dembele** – ENSAE Paris  
- **Paul Toudret** – ENSAE Paris  
- **Patryk Wiśniewski** – ENSAE Paris  

Supervised by **Professor Azadeh Khaleghi**  
With the support of **Sanofi France**: Noémie Allali, Olivia Carnapete, Thomas Séjourné

---

## 🙏 Acknowledgements

We thank **Sanofi** for providing data and expert guidance, and the **CASD & DataStorm** teams for enabling secure data access. Special thanks to our academic coach for methodological direction throughout the project.


---