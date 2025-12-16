# ============================================================
# Pipeline & Well Data Cleaning and Standardization Script
# ============================================================
#
# Objective:
# Clean and standardize Alberta pipeline registry and well casing
# failure datasets for downstream analysis and dashboarding.
#
# This script demonstrates a complete analyst-style workflow:
# raw data ingestion → cleaning → standardization → aggregation → export.
#
# ------------------------------------------------------------
# Workflow Steps:
#
# 1. Load raw CSV files for pipelines and well failures
# 2. Inspect data structure, column names, and missing values
# 3. Normalize column names (lowercase, underscores)
# 4. Select analysis-relevant columns
# 5. Rename columns for clarity and consistency
# 6. Remove duplicate records using stable identifiers
# 7. Standardize numeric and datetime fields
# 8. Aggregate well failures by company
# 9. Export clean, analysis-ready CSV files
# ------------------------------------------------------------
# Tools Used:
# - Python
# - pandas

# ------------------------------------------------------------

# 1. Load Data

import pandas as pd

pipelines = pd.read_csv(r"C:\Users\rohan\OneDrive\Documents\WORK Documents\### Data Analytics\Personal Projects\Oil and Gas Project\MySQL_Excel\Pipeline Registry - Midstream Infrastructure Composition & Integrity Insights\Modified Data\Pipelines.csv")
wells = pd.read_csv(r"C:\Users\rohan\OneDrive\Documents\WORK Documents\### Data Analytics\Personal Projects\Oil and Gas Project\MySQL_Excel\Well-Casing Failure - Operational Reliability & Risk Analysis\Modified Data\Well Casing Failures.csv")

# 2. Verify Tables

(pipelines.head())
(pipelines.tail())
(pipelines.shape)
(pipelines.columns)
(pipelines.info)
(pipelines.isna().sum().sort_values(ascending = True))

(wells.head())
(wells.tail())
(wells.shape)
(wells.columns)
(wells.info)
(wells.isna().sum().sort_values(ascending = True))


# 3. Normalizing Columns

pipelines.columns = (
    pipelines.columns
    .str.strip()
    .str.lower()
    .str.replace(" ", "_")
)

wells.columns = (
    wells.columns
    .str.strip()
    .str.lower()
    .str.replace(" ","_")
)
# 4. Remove Unutilized Columns

pipelines_mod = pipelines[[
    "pipeline_licence_segment_id",
    "segment_length",
    "pipe_outside_diameter",
    "pipe_wall_thickness",
    "pipe_max_operating_pressure",
    "pipe_stress_level",
    "h2s_content",
    "approx_lat",
    "approx_lon",
    "permit_approval_date",
]].copy()


wells_mod = wells[[
    "01.licence_number",
    "03.licensee_name",
    "10.failure_top_depth_(mkb)",
    "11.failure_depth_bottom_(mkb)",
    "approx_lat",
    "approx_lon",
    "06.detection_date",
    "07.report_date",
    "16.final_drill_date",
]].copy()

#5 Renaming Columns

pipelines_mod = pipelines_mod.rename(columns={
    "pipeline_licence_segment_id": "pipeline_id",
    "segment_length": "segment_length_km",
    "pipe_outside_diameter": "pipe_outside_diameter_mm",
    "pipe_wall_thickness": "pipe_wall_thickness_mm",
    "pipe_max_operating_pressure": "pipe_max_operating_pressure_kpa",
    "pipe_stress_level": "pipe_stress_level_yield_strength",
    "h2s_content": "h2s_content_mol_percentage",
    "approx_lat": "lat",
    "approx_lon": "lon",
})

wells_mod = wells_mod.rename(columns={
    "01.licence_number": "licence_number",
    "03.licensee_name": "company_name",
    "10.failure_top_depth_(mkb)": "failure_top_depth_mkb",
    "11.failure_depth_bottom_(mkb)": "failure_bottom_depth_mkb",
    "06.detection_date": "detection_date",
    "07.report_date": "report_date",
    "16.final_drill_date": "final_drill_date",
    "approx_lat": "lat",
    "approx_lon": "lon",
})


#6 Remove Duplicate Rows

pipelines_mod = pipelines_mod.drop_duplicates(
    subset=["pipeline_id"], keep="first"
)

wells_mod = wells_mod.drop_duplicates(
    subset=["licence_number"], keep="first"
)

#7 Standardize Data

pipe_num = [
    "segment_length_km",
    "pipe_outside_diameter_mm",
    "pipe_wall_thickness_mm",
    "pipe_max_operating_pressure_kpa",
    "pipe_stress_level_yield_strength",
    "h2s_content_mol_percentage",
    "lat",
    "lon",
]

pipelines_mod[pipe_num] = pipelines_mod[pipe_num].apply(pd.to_numeric, errors = "coerce")

pipelines_mod["permit_approval_date"] = pd.to_datetime(pipelines_mod["permit_approval_date"].astype(str).str.split(" ").str[0],
            errors = "coerce")

wells_num = [
    "failure_top_depth_mkb",
    "failure_bottom_depth_mkb",
    "lat",
    "lon",
    ]

wells_mod[wells_num] = wells_mod[wells_num].apply(pd.to_numeric, errors ="coerce")

wells_mod["detection_date"] = pd.to_datetime(wells_mod["detection_date"], format="%d-%b-%y", errors = "coerce")
wells_mod["report_date"] = pd.to_datetime(wells_mod["report_date"], format= "%d-%b-%y", errors = "coerce")
wells_mod["final_drill_date"] = pd.to_datetime(wells_mod["final_drill_date"], format = "%d-%b-%y", errors = "coerce")


failures_by_company = (
    wells_mod
    .groupby("company_name")
    .agg(
        failure_count=("company_name", "count"),
        avg_top_depth=("failure_top_depth_mkb", "mean"),
    )
    .reset_index()
)

PIPELINES_PATH = r"C:\Users\rohan\OneDrive\Documents\WORK Documents\### Data Analytics\Personal Projects\Oil and Gas Project\MySQL_Excel\Pipeline Registry - Midstream Infrastructure Composition & Integrity Insights\Modified Data\Pipelines.csv"
WELLS_PATH = r"C:\Users\rohan\OneDrive\Documents\WORK Documents\### Data Analytics\Personal Projects\Oil and Gas Project\MySQL_Excel\Well-Casing Failure - Operational Reliability & Risk Analysis\Modified Data\Well Casing Failures.csv"


from pathlib import Path

PIPELINES_PATH = r"C:\Users\rohan\OneDrive\Documents\WORK Documents\### Data Analytics\Personal Projects\Oil and Gas Project\MySQL_Excel\Pipeline Registry - Midstream Infrastructure Composition & Integrity Insights\Modified Data\Pipelines.csv"
WELLS_PATH = r"C:\Users\rohan\OneDrive\Documents\WORK Documents\### Data Analytics\Personal Projects\Oil and Gas Project\MySQL_Excel\Well-Casing Failure - Operational Reliability & Risk Analysis\Modified Data\Well Casing Failures.csv"

# Output folder (pipelines folder)
OUTPUT_DIR = Path(PIPELINES_PATH).parent

pipelines_mod.to_csv(OUTPUT_DIR / "pipelines_clean.csv", index=False)
wells_mod.to_csv(OUTPUT_DIR / "wells_clean.csv", index=False)

print("Clean tables exported")

