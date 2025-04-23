# DATA README

For replication purpose the entire `data` folder is made available in a compressed archive on Google Drive. Simply download the data and decompress the `.7z` archive.

## Data Sources

All external data sources are prefixed by the name of the data producer. We additionnaly report in the table below the url used to access the data. We can't guarantee that the url may are still functionnal or that the data available remotely was not modified since the time we accessed it. 

| ID | Variable              | Description                                                                                      |
|:--:|:----------------------|:-------------------------------------------------------------------------------------------------|
| 1  | ademe-dpe             | https://observatoire-dpe-audit.ademe.fr/donnees-dpe-publiques                                     |
| 2  | insee-chomage         |                                                                                                  |
| 3  | insee-diplomes        | https://www.insee.fr/fr/statistiques/8268840                                                     |
| 4  | insee-equipement      | https://www.insee.fr/fr/metadonnees/source/serie/s1161                                           |
| 5  | insee-famille         | https://www.insee.fr/fr/statistiques/8205182?sommaire=8205238                                     |
| 6  | insee-filosofi        | https://www.insee.fr/fr/metadonnees/source/operation/s2146/presentation                          |
| 7  | insee-logements       | https://www.insee.fr/fr/statistiques/6543302                                                     |
| 8  | insee-mouvements      | https://www.insee.fr/fr/statistiques/8201894                                                     |
| 9  | insee-population      | https://www.insee.fr/fr/information/7735186                                                      |
| 10 | insee-sirene          | https://www.data.gouv.fr/fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/ |
| 11 | insee-table_de_passage| https://www.insee.fr/fr/information/7671867                                                      |
| 12 | medd-logements_sociaux| https://www.data.gouv.fr/fr/datasets/repertoire-des-logements-locatifs-des-bailleurs-sociaux/     |
| 13 | medd-qualite_air      | given by Sanofi                                                                                  |
| 14 | mfrance-meteo         | https://www.data.gouv.fr/fr/datasets/donnees-climatologiques-de-base-mensuelles/                  |
| 15 | miom-crimes           | https://www.interieur.gouv.fr/fr/Interstats/Infractions-et-sentiment-d-insecurite/Vols/Vols-sans-violence/Insecurite-et-delinquance-en-2021-bilan-statistique |
| 16 | msp-eau               | https://www.data.gouv.fr/fr/datasets/resultats-du-controle-sanitaire-de-leau-distribuee-commune-par-commune/ |
| 17 | poste-codes           | https://www.data.gouv.fr/fr/datasets/base-officielle-des-codes-postaux/                          |


## Data Dictionary 

### Table de Passage

| ID | Variable       | Description                         |
|:--:|:---------------|:------------------------------------|
| 11 | code_insee24   | Code commune INSEE 2024             |
| 11 | arr24          | Code arrondissement INSEE 2024      |
| 11 | bv2022         | Code bassin de vie INSEE 2022       |
| 11 | code_inseeXX   | Older commune codes                 |

### ARR2COM

| ID | Variable        | Description                             |
|:--:|:----------------|:----------------------------------------|
| 3  | code_com        | Code commune INSEE 2024                 |
| 4  | code_arr        | Code arrondissement municipal INSEE 2024|

### Postal Converter

| ID | Variable        | Description                          |
|:--:|:----------------|:-------------------------------------|
| 1  | ademe-dpe       |                                      |
| 2  | insee-chomage   |                                      |
| 3  | codepost        | Postal code                          |
| 4  | arr24           | Code arrondissement INSEE 2024       |
| 5  | bv2022          | Code bassin de vie INSEE 2022        |

### Flow Data

| ID | Variable   | Description                           |
|:--:|:-----------|:--------------------------------------|
| 1  | arr_to     | 'arr24' code for commune of departure |
| 2  | arr_from   | 'arr24' code for commune of arrival   |
| 3  | flow_pop21 | Number of persons                     |
| 4  | share_to   | Share of population                   |

### Department

| ID | Variable               | Description                                               |
|:--:|:-----------------------|:----------------------------------------------------------|
| 1  | dep                    | Code of département                                       |
| 2  | month                  | Month of year                                             |
| 3  | tm                     | Average temperature                                       |
| 4  | txab                   | Maximal temperature                                       |
| 5  | tnab                   | Minimal temperature                                       |
| 6  | umm                    | Mean precipitation                                        |
| 7  | rr                     | Monthly cumulative precipitation                          |
| 8  | inst                   | Average monthly sunshine                                  |
| 9  | built_q2               | Average built-up surface per 1000 inhabitants             |
| 10 | floor_area_q2          | Average floor area of dwellings                           |
| 11 | n_dpe                  | Number of DPE files                                       |
| 12 | iqr_dpe_num            | Interquartile range of DPE scores                         |
| 13 | q1_dpe                 | First quantile of DPE (1–5)                               |
| 14 | q2_dpe                 | Second quantile of DPE (1–5)                              |
| 15 | q3_dpe                 | Third quantile of DPE (1–5)                               |
| 16 | ac_prop                | Share of households with air conditioning                 |
| 17 | nombre_pers_menage     | Average number of persons per household                   |
| 18 | med21                  | Median value of income                                    |
| 19 | pimp21                 | Share of persons paying taxes                             |
| 20 | tp6021                 |                                                            |
| 21 | ppsoc21                |                                                            |
| 22 | ppfam21                |                                                            |
| 23 | d121                   |                                                            |
| 24 | d921                   |                                                            |
| 25 | rd21                   |                                                            |

### Arrondissements / Bassins de Vie 2022

| ID | Variable                    | Description                                                      |
|:--:|:----------------------------|:-----------------------------------------------------------------|
|    | arr24                       | Code arrondissement INSEE 2024 / bv2022 for bassins de vie       |
| 4  | n_equip_a1                  | Public services per 1000 inhabitants                             |
| 4  | n_equip_a2                  | General services per 1000 inhabitants                            |
| 4  | n_equip_a3                  | Car services per 1000 inhabitants                                |
| 4  | n_equip_a4                  | Building trades per 1000 inhabitants                             |
| 4  | n_equip_a5                  | Other services per 1000 inhabitants                              |
| 4  | n_equip_b1                  | Large retail stores per 1000 inhabitants                         |
| 4  | n_equip_b2                  | Food retail stores per 1000 inhabitants                          |
| 4  | n_equip_b3                  | Specialized non food retail per 1000 inhabitants                 |
| 4  | n_equip_c1                  | Primary education institutions per 1000 inhabitants              |
| 4  | n_equip_c2                  | Lower education institutions per 1000 inhabitants                |
| 4  | n_equip_c3                  | High schools – “lycée” per 1000 inhabitants                      |
| 4  | n_equip_c4                  | Higher Education – Non-university per 1000 inhabitants           |
| 4  | n_equip_c5                  | University Higher Education per 1000 inhabitants                 |
| 4  | n_equip_c6                  | Continuing education per 1000 inhabitants                        |
| 4  | n_equip_d1                  | Healthcare Facilities and services per 1000 inhabitants          |
| 4  | n_equip_d2                  | Liberal healthcare facilities per 1000 inhabitants               |
| 4  | n_equip_d3                  | Other health-related facilities and services per 1000 inhabitants|
| 4  | n_equip_d4                  | Social services for the elderly per 1000 inhabitants             |
| 4  | n_equip_d5                  | Social services for young children per 1000 inhabitants          |
| 4  | n_equip_d6                  | Social services for the disabled per 1000 inhabitants            |
| 4  | n_equip_e1                  | Infrastructures and transports per 1000 inhabitants              |
| 4  | n_equip_f1                  | Sport facilities per 1000 inhabitants                            |
| 4  | n_equip_f2                  | Recreational facilities per 1000 inhabitants                     |
| 4  | n_equip_f3                  | Cultural and social facilities per 1000 inhabitants              |
| 4  | n_equip_g1                  | Accommodation for tourists per 1000 inhabitants                  |
| 4  | n_equip_c7                  | Other educational services per 1000 inhabitants                  |
| 12 | social_average_age          | Average age of the population                                    |
| 12 | mean_rent_m2                | Mean rent per square meter                                       |
| 12 | n_social                    | Number of social housing per 1000 inhabitants                    |
| 12 | social_houses               | Number of social houses per 1000 inhabitants                     |
| 12 | social_appartments          | Number of social apartments per 1000 inhabitants                 |
| 15 | cambriolages                | Number of burglaries per 1000 inhabitants                        |
| 15 | coups                       | Assault complaints per 1000 inhabitants                          |
| 15 | trafic                      | Drug trafficking incidents per 1000 inhabitants                  |
| 15 | vols_av                     | Violent robberies per 1000 inhabitants                           |
| 15 | vols_sv                     | Non-violent robberies per 1000 inhabitants                       |
| 10 | n_ape1071c                  | Bakeries per 1000 inhabitants                                    |
| 10 | n_ape4711a                  | Retail sale of frozen foods per 1000 inhabitants                 |
| 10 | n_ape4722z                  | Butchers per 1000 inhabitants                                    |
| 10 | n_ape4723z                  | Fishmongers per 1000 inhabitants                                 |
| 10 | n_ape4773z                  | Pharmaceutical retail in specialized stores per 1000 inhabitants |
| 10 | n_ape5610c                  | Fast food outlets per 1000 inhabitants                           |
| 10 | n_ape5630z                  | Bars and similar establishments per 1000 inhabitants             |
| 10 | n_ape9312z                  | Gyms per 1000 inhabitants                                        |
| 13 | ges_routier                 | Total greenhouse-gas emissions from road transport               |
| 13 | ges_resid                   | Total greenhouse-gas emissions from residential housing          |
| 9  | pop_0014                    | Population aged 0–14 years                                       |
| 9  | pop_1529                    | Population aged 15–29 years                                      |
| 9  | pop_3044                    | Population aged 30–44 years                                      |
| 9  | pop_4559                    | Population aged 45–59 years                                      |
| 9  | pop_6074                    | Population aged 60–74 years                                      |
| 9  | pop_7589                    | Population aged 75–89 years                                      |
| 9  | pop_90p                     | Population aged 90+ years                                        |
| 9  | pop_0029                    | Population aged 0–29 years                                       |
| 9  | pop_0044                    | Population aged 0–44 years                                       |
| 9  | pop_tot                     | Total population in 2023                                         |
| 9  | pop_h0029                   | Male population aged 0–29 years                                  |
| 9  | pop_h0044                   | Male population aged 0–44 years                                  |
| 9  | pop_htot                    | Total male population                                            |
| 9  | pop_f0029                   | Female population aged 0–29 years                                |
| 9  | pop_f0044                   | Female population aged 0–44 years                                |
| 9  | pop_ftot                    | Total female population                                          |
| 6  | nombre_pers_menage          | Number of persons per household                                  |
| 6  | proportion_imposable_ens_arr| Share of persons paying taxes                                    |
| 6  | d1_ens_arr                  | First decile of income                                           |
| 6  | q1_ens_arr                  | First quartile of income                                         |
| 6  | q2_ens_arr                  | Second quartile of income                                        |
| 6  | q3_ens_arr                  | Third quartile of income                                         |
| 6  | d9_ens_arr                  | Ninth decile of income                                           |
| 6  | gini_ens_arr                | Gini coefficient of local income inequality                      |
| 6  | proportion_activite_ens_arr | Share of working-age persons                                     |
| 6  | proportion_retraite_ens_arr | Share of retirees                                                |
| 6  | q1_couple_enfants_arr       | First quartile of couples with children                          |
| 6  | q2_couple_enfants_arr       | Second quartile of couples with children                         |
| 6  | q3_couple_enfants_arr       | Third quartile of couples with children                          |
| 6  | q1_parent_seul_arr          | First quartile of single-parent households                       |
| 6  | q2_parent_seul_arr          | Second quartile of single-parent households                      |
| 6  | q3_parent_seul_arr          | Third quartile of single-parent households                       |
| 2  | unemp_rate                  | Unemployment rate                                                |
| 3  | dip_001T                    | Share of working-age persons with no diploma                     |
| 3  | dip_200R                    | Share with “Brevet”                                              |
| 3  | dip_300R                    | Share with “Bac technique”                                       |
| 3  | dip_350R                    | Share with “Bac général”                                         |
| 3  | dip_500R                    | Share with “Bac + 2”                                             |
| 3  | dip_600R                    | Share with “Bac + 4”                                             |
| 3  | dip_700R                    | Share with “Bac + 5 and more”                                    |
| 3  | delta_educ                  | Educational gap metric                                           |
| 1  | built_q2                    | Built-up surface per 1000 inhabitants                            |
| 1  | floor_area_q2               | Average floor area of dwellings                                  |
| 1  | n_dpe                       | Number of DPE (energy performance) files                         |
| 1  | iqr_dpe_num                 | Interquartile range of DPE scores                                |
| 1  | q1_dpe                      | First quantile of DPE (1–5)                                      |
| 1  | q2_dpe                      | Second quantile of DPE                                           |
| 1  | q3_dpe                      | Third quantile of DPE                                            |
| 1  | ac_prop                     | Share of households with air conditioning                        |
| 13 | no2_mean_concentration      | Mean NO₂ concentration (μg/m³)                                   |
| 13 | o3_mean_concentration       | Mean O₃ concentration (μg/m³)                                    |
| 13 | somo35_mean                 | Annual mean of SOMO₃₅ (ozone exposure)                           |
| 13 | aot40_mean                  | Annual mean of AOT₄₀                                              |
| 13 | pm10_mean_concentration     | Mean PM₁₀ concentration (μg/m³)                                  |
| 13 | pm25_mean_concentration     | Mean PM₂.₅ concentration (μg/m³)                                 |
| 14 | tm_autumn                   | Average temperature in autumn 2023                               |
| 14 | tm_spring                   | Average temperature in spring 2023                               |
| 14 | tm_summer                   | Average temperature in summer 2023                               |
| 14 | tm_winter                   | Average temperature in winter 2023                               |
| 14 | txab_summer                 | Maximum temperature in summer 2023                               |
| 14 | tnab_winter                 | Minimum temperature in winter 2023                               |
| 14 | umm_autumn                  | Mean precipitation in autumn 2023                                |
| 14 | umm_spring                  | Mean precipitation in spring 2023                                |
| 14 | umm_summer                  | Mean precipitation in summer 2023                                |
| 14 | umm_winter                  | Mean precipitation in winter 2023                                |
| 14 | rr_spring                   | Cumulative precipitation in spring (“rr_spring”)                 |
| 14 | rr_autumn                   | Cumulative precipitation in autumn (“rr_autumn”)                 |
| 14 | rr_summer                   | Cumulative precipitation in summer (“rr_summer”)                 |
| 14 | rr_winter                   | Cumulative precipitation in winter (“rr_winter”)                 |
| 16 | water_cl                    | Chloride concentration in drinking water                         |
| 16 | water_no3                   | Nitrate concentration in drinking water                          |
| 16 | water_ph                    | pH of drinking water                                             |
| 16 | water_so4                   | Sulfate concentration in drinking water                          |


