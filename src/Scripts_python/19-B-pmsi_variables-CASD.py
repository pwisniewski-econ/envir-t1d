# ---- Import Libraries ----
import pandas as pd
import pyarrow.feather as feather
import pyarrow.dataset as ds

# ---- Load Feather files ----
PREVALENT_INDIVIDUALS = feather.read_feather("results_building/t1d-individuals.feather")
INCIDENT_INDIVIDUALS = feather.read_feather("results_building/t1d_e101-individuals.feather")
ALL_INDIVIDUALS = feather.read_feather("results_building/all-individuals.feather")

# ---- VITAMINS ----
malnutrition_codes = [f"E{i}" for i in range(40, 47)]

dataset = ds.dataset("data/interim/people_diagnosis.feather", format="feather")
table = dataset.to_table().to_pandas()

def classify_diag(diag):
    if diag.startswith("E50"):
        return "vita_def"
    elif diag[:4] in ["E511", "E512", "E530", "E531", "E532"]:
        return "vitB1-B7_def"
    elif diag.startswith("D52"):
        return "vitamin B9"
    elif diag.startswith("D51"):
        return "vitamin B12"
    elif diag.startswith("E53"):
        return "vitC_def"
    elif diag.startswith("E55"):
        return "vitamin D"
    elif diag.startswith("E56"):
        return "vitK_def"
    elif diag.startswith("E66"):
        return "Overweight"
    elif diag[:3] in malnutrition_codes:
        return "Malnutrition"
    elif diag.startswith("F10"):
        return "Alcohol"
    elif diag.startswith("F17"):
        return "Tabacco"
    elif diag[:4] in ["F112", "F132"]:
        return "Drugs"
    elif diag.startswith("E58"):
        return "Calcium"
    elif diag.startswith("D50"):
        return "Iron"
    elif diag.startswith("E61"):
        return "Other Minerals"
    elif diag.startswith("Z56"):
        return "Employment"
    elif diag.startswith("Z59"):
        return "Economics"
    elif diag.startswith("Z60"):
        return "Social"
    elif diag.startswith("Z63"):
        return "Family"
    elif diag.startswith("E780"):
        return "Cholesterol"
    else:
        return None

table["specific_diag"] = table["diag"].apply(classify_diag)
DAS_SPEC = table.dropna(subset=["specific_diag"])[["anonyme", "specific_diag"]].drop_duplicates()

def add_specific(data, filt_data):
    merged = pd.merge(filt_data, data, on="anonyme", how="inner")
    return merged.drop_duplicates(subset=["anonyme"])

VITAMINS_ALL = add_specific(DAS_SPEC, pd.DataFrame(ALL_INDIVIDUALS))
VITAMINS_PREVALENT = add_specific(DAS_SPEC, pd.DataFrame(PREVALENT_INDIVIDUALS))
VITAMINS_INCIDENT = add_specific(DAS_SPEC, pd.DataFrame(INCIDENT_INDIVIDUALS))

feather.write_feather(VITAMINS_ALL, "results_building/all-vitamins.feather", compression="zstd")
feather.write_feather(VITAMINS_PREVALENT, "results_building/t1d-vitamins.feather", compression="zstd")
feather.write_feather(VITAMINS_INCIDENT, "results_building/t1d_e101-vitamins.feather", compression="zstd")