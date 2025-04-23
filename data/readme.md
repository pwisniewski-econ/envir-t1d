# DATA README

For replication purpose the entire `data` folder is made available in a compressed archive on Google Drive. Simply download the data and decompress the `.7z` archive.

## Data Sources

All external data sources are prefixed by the name of the data producer. We additionnaly report in the table below the url used to access the data. We can't guarantee that the url may are still functionnal or that the data available remotely was not modified since the time we accessed it. 

| ID | Data Name | Source |
:--------:|:---------|:---------|
|1|ademe-dpe|https://observatoire-dpe-audit.ademe.fr/donnees-dpe-publiques|
|2|insee-chomage||
|3|insee-diplomes|https://www.insee.fr/fr/statistiques/8268840|
|4|insee-equipement|https://www.insee.fr/fr/metadonnees/source/serie/s1161|
|5|insee-famille|https://www.insee.fr/fr/statistiques/8205182?sommaire=8205238|
|6|insee-filosofi|https://www.insee.fr/fr/metadonnees/source/operation/s2146/presentation|
|7|insee-logements|https://www.insee.fr/fr/statistiques/6543302|  
|8|insee-mouvements|https://www.insee.fr/fr/statistiques/8201894|
|9|insee-population|https://www.insee.fr/fr/information/7735186|
|10|insee-sirene|https://www.data.gouv.fr/fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/|
|11|insee-table_de_passage|https://www.insee.fr/fr/information/7671867|
|12|medd-logements_sociaux|https://www.data.gouv.fr/fr/datasets/repertoire-des-logements-locatifs-des-bailleurs-sociaux/|
|13|medd-qualite_air|given by sanofi|
|14|mfrance-meteo|https://www.data.gouv.fr/fr/datasets/donnees-climatologiques-de-base-mensuelles/|
|15|miom-crimes|https://www.interieur.gouv.fr/fr/Interstats/Infractions-et-sentiment-d-insecurite/Vols/Vols-sans-violence/Insecurite-et-delinquance-en-2021-bilan-statistique|
|16|msp-eau|https://www.data.gouv.fr/fr/datasets/resultats-du-controle-sanitaire-de-leau-distribuee-commune-par-commune/|
|17|poste-codes|https://www.data.gouv.fr/fr/datasets/base-officielle-des-codes-postaux/|


## Data Dictionary 

### Table de Passage

| ID | Variable | Description |
:--------:|:---------|:---------|
|1|code_insee24|Code commune INSEE 2024|
|2|arr24|Code arrondissement INSEE 2024|
|3|bv2022|Code bassin de vie INSEE 2022|

### ARR2COM

| ID | Variable | Description |
:--------:|:---------|:---------|
|1|code_com|Code commune INSEE 2024|
|2|code_arr|Code arrondissement INSEE 2024|

### Postal Converter

| ID | Variable | Description |
:--------:|:---------|:---------|
|1|codepost|Postal code|
|2|arr24|Code arrondissement INSEE 2024|
|3|bv2022|Code bassin de vie INSEE 2022|

### Flow Data
| ID | Variable | Description |
:--------:|:---------|:---------|
|1|arr_to|'arr24' code for commune of departure| 
|2|arr_from|'arr24' code for commune of arrival|
|3|flow_pop21|Number of persons|
|4|share_to|Share of population|

### Department

|1|dep|Code of departement|
|2| month|Month of year|
|3| tm|Average temperature|
|4| txab|Maximal temperature|
|5| tnab|Minimal temperature|
|6| umm|Mean precipitaion|
|7| rr|Monthly cumulative precipitation|
|8| inst| Average monthly sunshine|
|9| built_q2|Average of Built-up surfaces per 1000 inhabitant|
|10| floor_area_q2|Average floor area of dwellings|
|11| n_dpe|Number of DPE files|
|12| iqr_dpe_num|Interquartile of DPE score|
|13| q1_dpe|First quantile of DPE from 1 to 5|
|14|q2_dpe|Second quantile of DPE from 1 to 5|
|15|q3_dpe|Third quantile of DPE from 1 to 5|
|16| ac_prop|Share of households with AC|
|17| nombre_pers_menage|Average number of persons per housholds
|18| med21|Median value of income|
|19| pimp21|Share of persons paying taxes|
|20| tp6021||
|21| ppsoc21||
|22| ppfam21||
|23| d121||
|24| d921||
|25| rd21||

### Arrondissements / Bassins de Vie 2022

| ID | Variable | Description |
:--------:|:---------|:---------|
|1|arr24|Code commune INSEE 2024|
|2|n_equip_a1|Public services per 1000 inhabitants|
|3|n_equip_a2|General services per 1000 inhabitants|
|4|n_equip_a3|Car services per 1000 inhabitants|
|5|n_equip_a4|Building trades per 1000 inhabitants|
|6|n_equip_a5|Other services per 1000 inhabitants|  
|7|n_equip_b1|Large retail stores per 1000 inhabitants|
|8|n_equip_b2|Food retail stores per 1000 inhabitants|
|9|n_equip_b3|Specialized non food retail per 1000 inhabitants|
|10|n_equip_c1|Primary education institutions per 1000 inhabitants|
|11|n_equip_c2|Lower education institutions per 1000 inhabitants|
|12|n_equip_c3|High schools - "lycée" per 1000 inhabitants|
|13|n_equip_c4|Higher Education - Non university per 1000 inhabitants|
|14|n_equip_c5|University Higher Education per 1000 inhabitants|
|15|n_equip_c6|Continuing education per 1000 inhabitants|
|16|n_equip_d1|Healthcare Facilities and services per 1000 inhabitantss|
|17|n_equip_d2|Liberal healthcare facilities per 1000 inhabitants|
|18|n_equip_d3|Others health related facilities and services per 1000 inhabitants|
|19|n_equip_d4|Social services for the eldery per 1000 inhabitants|
|20|n_equip_d5|Social services for young children per 1000 inhabitants|
|21|n_equip_d6|Social services for the disabled per 1000 inhabitants|
|22|n_equip_e1|infrastructures and transports per 1000 inhabitants|
|23|n_equip_f1|Sport facilites per 1000 inhabitants|
|24|n_equip_f2|Recreational facilites per 1000 inhabitants|
|25|n_equip_f3|Cultural and social facilities per 1000 inhabitants|
|26|n_equip_g1|Accomodation for tourists per 1000 inhabitants|
|27|n_equip_c7|Other educational services per 1000 inhabitants|
|28|social_average_age|Average age of the population|
|29|mean_rent_m2|Mean of the rent per sqare meters|
|30|n_social|Number of social housing per 1000 inhabitants|
|31|social_houses|number of social houses per 1000 inhabitants|
|32|social_appartments|number of social appartements per 1000 inhabitants|
|33|cambriolages|Number of burglaries per inhabitants per 1000 inhabitants|
|34|coups|Assault complaints per inhabitants per 1000 inhabitants|
|35|trafic|Drugs trafic per inhabitants per 1000 inhabitants|
|36|vols_av|violent robberies per inhabitants per 1000 inhabitants|
|37|vols_sv|non violent robberies per inhabitants per 1000 inhabitants|
|38|n_ape1071c|Bakeries per 1000 inhabitants|
|39|n_ape4711a|Retail sale of frozen foods per 1000 inhabitants|
|40|n_ape4722z|Butchers per 1000 inhabitants|
|41|n_ape4723z|Fishmongers per 1000 inhabitants|
|42|n_ape4773z|Pharmaceutical retail in specialized stores per 1000 inhabitants|
|43|n_ape5610c|Fast Food per 1000 inhabitants|
|44|n_ape5630z|Bars and similar establishments per 1000 inhabitants|
|45|n_ape9312z|Gyms per 1000 inhabitants|
|46|ges_routier|Total Emission of Greenhous Gas for road transport|
|47|ges_resid|Total Emission of Greenhous Gas for residential housing|
|48|pop_0014|Population aged between 0 and 14 years|
|49|pop_1529|Population aged between 15 and 29 years|
|50|pop_3044|Population aged between 30 and 44 years|
|51|pop_4559|Population aged between 45 and 59 years|
|52|pop_6074|Population aged between 60 and 74 years|
|53|pop_7589|Population aged between 75 and 89 years|
|54|pop_90p|Population aged over 90 years|
|55|pop_0029|Population aged between 0 and 29 years|
|56|pop_0044|Population aged between 0 and 44 years|
|57|pop_tot|Population in total in 2023|
|58|pop_h0029|Population of men aged between 0 and 29 years|
|59|pop_h0044|Population of men aged between 0 and 44 years|
|60|pop_htot|Population of men in total|
|61|pop_f0029|Population of women aged between 0 and 29 years|
|62|pop_f0044|Population of women aged between 0 and 44 years|
|63|pop_ftot|opulation of women in total|
|64|nombre_pers_menage|Number of person per households|
|65|proportion_imposable_ens_arr|Share of persons paying taxes|
|66|d1_ens_arr|First decile of income|
|67|q1_ens_arr|First quantile of income|
|68|q2_ens_arr|Second quantile of income|
|69|q3_ens_arr|Third quantile of income|
|70|d9_ens_arr|Ninth decile of income|
|71|gini_ens_arr|Gini indicator relativ to inegality in localities|
|72|proportion_activite_ens_arr|Share of working persones|
|73|proportion_retraite_ens_arr|Share of retreated persons|
|74|q1_couple_enfants_arr|First quantile of couples with children|
|75|q2_couple_enfants_arr|Second quantile of couples with children||
|76|q3_couple_enfants_arr|Third quantile of couples with children||
|77|q1_parent_seul_arr|First quantile of Single-parent household|
|78|q2_parent_seul_arr|Second quantile of Single-parent household|
|79|q3_parent_seul_arr|Third quantile of Single-parent household|
|80|unemp_rate|Unemployement rate|
|81|dip_001T|Share of working-age persons with no diploma|
|82|dip_200R|Share of working-age persons with "Brevet"|
|83|dip_300R|Share of working-age persons with "Bac technique"|
|84|dip_350R|Share of working-age persons with "Bac Général"|
|85|dip_500R|Share of working-age persons with "Bac + 2"|
|86|dip_600R|Share of working-age persons with "Bac + 4"|
|87|dip_700R|Share of working-age persons with "Bac + 5 and more"|
|88|delta_educ|Measurement of educational gap|
|89|built_q2|Average of Built-up surfaces per 1000 inhabitant|
|90|floor_area_q2|Average floor area of dwellings|
|91|n_dpe|Number of DPE files|
|92|iqr_dpe_num|Interquartile of DPE score|
|93|q1_dpe|First quantile of DPE from 1 to 5|
|94|q2_dpe|Second quantile of DPE from 1 to 5|
|95|q3_dpe|Third quantile of DPE from 1 to 5|
|96|ac_prop|Share of households with AC|
|97|no2_mean_concentration|Mean NO2 concentration(μg/m3)|
|98|o3_mean_concentration|Mean O3 concentration(μg/m3)|
|99|somo35_mean|Annual mean of SOMO35(ozone exposure)|
|100|aot40_mean|ANnual mean of AOT40|
|101|pm10_mean_concentration|Mean PM10 concentration(μg/m3)|
|102|pm25_mean_concentration|Mean PM25 concentration(μg/m3)|
|103|tm_autumn|Average temperature during autumn 2023|
|104|tm_spring|Average temperature during spring 2023|
|105|tm_summer|Average temperature during summer 2023|
|106|tm_winter|Average temperature during winter 2023|
|107|txab_summer|Maximum tempertaure during summer 2023|
|108|tnab_winter|Minimum temperature during winter 2023|
|109|umm_autumn|Mean precipitaion during autumn 2023|
|110|umm_spring|Mean precipitaion during spring 2023||
|111|umm_summer|Mean precipitaion during summer 2023||
|112|umm_winter|Mean precipitaion during winter 2023||
|113|rr_spring|Monthly cumulative precipitation (in mm and 1/10 mm) for spring|
|114|rr_autumn|Monthly cumulative precipitation (in mm and 1/10 mm) for autumn|
|115|rr_summer|Monthly cumulative precipitation (in mm and 1/10 mm) for summer|
|116|rr_winter|Monthly cumulative precipitation (in mm and 1/10 mm) for winter|
|117|water_cl|Cl concetration in drinking water|
|118|water_no3|NO3 concetration in drinking water|
|119|water_ph|pH of drinking water|
|120|water_so4|SO4 concetration in drinking water|

