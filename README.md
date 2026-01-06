**Problem:** Upstream well failures and midstream pipeline risks are published separately. These datasets need to be analyzed together to assess pipeline integrity risk

**Method:** Built an end-to-end ETL pipeline (MySQL, Python, Power Query, Power BI) to clean, link, and analyze 97k pipeline segments with 5.9k well failures.

**Insight:** Failure risk clusters geographically and correlates with material type and corrosion-consistent operating environments—steel pipelines show elevated risk under certain conditions. Well failures show a major increase in the past couple decades, however this may be a sampling bias due to technological advancements in data collection.

**Action:** Recommend prioritizing inspections for pipelines operating in high-risk environments.


Code:

# Oil & Gas Pipeline Failure Risk Analysis

End-to-end data analysis project using SQL, Python, and Power BI.

## Stack
- SQL (MySQL) – data cleaning & normalization
- Python (pandas) – validation & preprocessing
- Power BI – risk & failure visualization

## Outputs
- Clean pipeline and well failure tables
- Failure trends by material, substance, geography
- Interactive dashboard
