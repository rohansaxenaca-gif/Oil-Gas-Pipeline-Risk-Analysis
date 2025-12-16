/* ============================================================
   Oil & Gas Pipeline + Well Failure Data Cleaning (SQL)
   ============================================================

   Objective
   - Create cleaned, analysis-ready tables from raw pipeline registry
     and well casing failure datasets for reporting / Power BI.

   Inputs
   - pipelines_raw
   - well_failures_raw

   Outputs (tables)
   - pipelines_mod      (cleaned copy of pipelines_raw)
   - well_failures_mod  (cleaned copy of well_failures_raw)

   What this script does:
   1) Clone raw tables into *_mod tables (keeps raw immutable)
   2) Remove duplicates using ROW_NUMBER() window logic
   3) Remove invalid / null key fields (e.g., Licence_Number)
   4) Drop unused columns to reduce noise
   5) Rename columns for consistency (units in names)
   6) Standardize values:
      - Convert blanks -> 0 / NULL where appropriate
      - Validate numeric fields via REGEXP
      - Standardize dates (Permit_Approval_Date, Detection/Report/Final_Drill)
   7) Convert column datatypes to numeric/date types for analytics
   
   ============================================================ */


-- 1. Create Modified Tables

CREATE TABLE pipelines_mod
LIKE pipelines_raw;
INSERT INTO pipelines_mod
SELECT * FROM pipelines_raw;

CREATE TABLE well_failures_mod
LIKE well_failures_raw;
INSERT INTO well_failures_mod
SELECT * FROM well_failures_raw;


SELECT * FROM pipelines_mod;
SELECT * FROM well_failures_mod;

-- 2. Data Cleaning

-- 2. a) Remove Duplicates

TRUNCATE TABLE pipelines_mod; 
INSERT INTO pipelines_mod
SELECT * FROM pipelines_raw;

TRUNCATE TABLE well_failures_mod; 
INSERT INTO well_failures_mod
SELECT * FROM well_failures_raw;

SELECT Pipeline_Licence_Segment_Id, COUNT(*) AS count_dup
FROM pipelines_mod
GROUP BY Pipeline_Licence_Segment_Id
HAVING count_dup > 1;

ALTER TABLE pipelines_mod
ADD COLUMN Row_ID INT NOT NULL AUTO_INCREMENT PRIMARY KEY;

WITH delete_cte1 AS (
	SELECT Row_ID
    FROM (
		SELECT
			Row_ID, Pipeline_Licence_Segment_Id, ROW_NUMBER() OVER (PARTITION BY Pipeline_Licence_Segment_Id ORDER BY Row_ID) AS row_num
		FROM pipelines_mod
	) dcte1
    WHERE row_num > 1
   ) 
   
    DELETE pm1
    FROM pipelines_mod pm1
    JOIN delete_cte1 dup1
	ON pm1.Row_ID = dup1.Row_ID;

SELECT Licence_Number, COUNT(*) AS count_dup
FROM well_failures_mod
GROUP BY Licence_Number
HAVING count_dup > 1;

ALTER TABLE well_failures_mod
ADD COLUMN Row_ID INT NOT NULL AUTO_INCREMENT PRIMARY KEY;

WITH delete_cte2 AS (
	SELECT Row_ID
    FROM (
		SELECT
			Row_ID, Licence_Number, ROW_NUMBER() OVER (PARTITION BY Licence_Number ORDER BY Row_ID) AS row_num
		FROM well_failures_mod
        ) dcte2
	WHERE row_num > 1
	) 
        
	DELETE pm2
	FROM well_failures_mod pm2
	JOIN delete_cte2 dup2
	ON pm2.Row_ID = dup2.Row_ID;
        
-- 2.b) Remove Null Values

DELETE FROM pipelines_mod
WHERE Licence_Number IS NULL OR Licence_Number = '';
    
DELETE FROM well_failures_mod
WHERE Licence_Number IS NULL OR Licence_Number = '';

-- 2.c) Remove Non-Utilized Columns

 ALTER TABLE pipelines_mod
    DROP COLUMN Mapped_Unmapped, DROP COLUMN H2S_Release_Level, DROP COLUMN NEB_Pipeline_Indicator, DROP COLUMN Segment_Line_Number,
    DROP COLUMN Licence_Line_Number, DROP COLUMN BA_Code, DROP COLUMN Pipeline_Specification_Id, DROP COLUMN Segment_From_Facility,
    DROP COLUMN H2S_Release_Volume, DROP COLUMN Pipe_Technical_Standard, DROP COLUMN Pipe_Internal_Protection, DROP COLUMN Bidirectional_Pipeline_Ind,
    DROP COLUMN HDD_Bored_Ind, DROP COLUMN Liner_Grade, DROP COLUMN Liner_Type, DROP COLUMN Pipeline_External_Protection, DROP COLUMN Pipeline_Class_Location,
    DROP COLUMN Substance_2, DROP COLUMN Substance_3, DROP COLUMN Original_Licence_Number, DROP COLUMN Original_Pipe_Specification_Id,
    DROP COLUMN Original_Segment_Line_Number, DROP COLUMN Licence_Approval_Date, DROP COLUMN Original_Licence_Issue_Date, DROP COLUMN Permit_Expiry_Date,
    DROP COLUMN Geometry_Source;
    
ALTER TABLE pipelines_mod
    DROP COLUMN Licence_Number;

 ALTER TABLE well_failures_mod
	DROP COLUMN Orig_BA_Code, DROP COLUMN Failure_Top_Depth_mKB, DROP COLUMN Failure_Bottom_Depth_mKB, DROP COLUMN Steam_Scheme_Type;
    
ALTER TABLE well_failures_mod
ADD COLUMN Failure_Top_Depth_mKB TEXT;

ALTER TABLE well_failures_mod
ADD COLUMN Failure_Bottom_Depth_mKB TEXT;

UPDATE well_failures_mod mod_wf
JOIN well_failures_raw raw_wf
	ON mod_wf.Licence_Number = raw_wf.Licence_Number
SET mod_wf.Failure_Top_Depth_mKB = raw_wf.Failure_Top_Depth_mKB;

UPDATE well_failures_mod mod_wf
JOIN well_failures_raw raw_wf
	ON mod_wf.Licence_Number = raw_wf.Licence_Number
SET mod_wf.Failure_Bottom_Depth_mKB = raw_wf.Failure_Bottom_Depth_mKB;

-- 2.d) Update Column Names
    
ALTER TABLE pipelines_mod
CHANGE COLUMN Segment_Length Segment_Length_km TEXT,
CHANGE COLUMN Approx_Lat Approx_Lat_degrees TEXT,
CHANGE COLUMN Approx_Lon Approx_Lon_degrees TEXT,
CHANGE COLUMN H2S_Content H2S_Content_mol_percentage TEXT,
CHANGE COLUMN Pipe_Outside_Diameter Pipe_Outside_Diameter_mm TEXT,
CHANGE COLUMN Pipe_Wall_Thickness Pipe_Wall_Thickness_mm TEXT,
CHANGE COLUMN Pipe_Max_Operating_Pressure Pipe_Max_Operating_Pressure_kPa TEXT,
CHANGE COLUMN Pipe_Stress_Level Pipe_Stress_Level_Yield_Strength TEXT,
CHANGE COLUMN Shape_Length Shape_Length_meters TEXT;

ALTER TABLE well_failures_mod
CHANGE COLUMN Licensee_Name Company_Name TEXT;

ALTER TABLE well_failures_mod
CHANGE COLUMN Approx_Lat Approx_Lat_degrees TEXT,
CHANGE COLUMN Approx_Lon Approx_Lon_degrees TEXT;

-- 3. Standardize and Verify Values
	-- 3. a) Update Column Datatypes

-- Pipelines Table

UPDATE pipelines_mod
SET H2S_Content_mol_percentage = '0'
WHERE H2S_Content_mol_percentage = '';

UPDATE pipelines_mod
SET Pipe_Wall_Thickness_mm = '0'
WHERE Pipe_Wall_Thickness_mm = '';

SELECT DISTINCT Pipe_Wall_Thickness_mm
FROM pipelines_mod
WHERE Pipe_Wall_Thickness_mm IS NOT NULL
  AND TRIM(Pipe_Wall_Thickness_mm) NOT REGEXP '^-?[0-9]+(\\.[0-9]+)?$';

UPDATE pipelines_mod
SET Pipe_Stress_Level_Yield_Strength = '0'
WHERE Pipe_Stress_Level_Yield_Strength = '';

UPDATE pipelines_mod
SET Shape_Length_meters = NULL
WHERE TRIM(Shape_Length_meters) = ''
   OR Shape_Length_meters NOT REGEXP '^[0-9]+(\\.[0-9]+)?$';

UPDATE pipelines_mod
SET Last_Occurrence_Year = NULL
WHERE TRIM(Last_Occurrence_Year) = ''
   OR Last_Occurence_Year NOT REGEXP '^[0-9]+(\\.[0-9]+)?$';

UPDATE pipelines_mod
SET Permit_Approval_Date = NULL
WHERE Permit_Approval_Date = '';

DELETE FROM pipelines_mod
WHERE Permit_Approval_Date IS NULL;

UPDATE pipelines_mod
SET Permit_Approval_Date = DATE_ADD('1900-01-01', INTERVAL (Permit_Approval_Date - 1) DAY)
WHERE Permit_Approval_Date REGEXP '^[0-9]+$';

SELECT *
FROM pipelines_mod
WHERE Permit_Approval_Date LIKE '%-%';

DELETE FROM pipelines_mod
WHERE Permit_Approval_Date LIKE '%-%';

SELECT Permit_Approval_Date
FROM pipelines_mod
WHERE Permit_Approval_Date NOT REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$';

DELETE FROM pipelines_mod
WHERE Permit_Approval_Date NOT REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$';

UPDATE pipelines_mod
SET Permit_Approval_Date = STR_TO_DATE(Permit_Approval_Date, '%c/%e/%Y')
WHERE Permit_Approval_Date IS NOT NULL;

UPDATE pipelines_mod
SET Permit_Approval_Date = SUBSTRING_INDEX(Permit_Approval_Date, ' ', 1)
WHERE Permit_Approval_Date LIKE '% %';

  -- Modify Columns that contain numerical values or dates

ALTER TABLE pipelines_mod

    MODIFY COLUMN Segment_Length_km			  			DECIMAL(9,2),
    MODIFY COLUMN Approx_Lat_degrees          			DECIMAL(9,2),
	MODIFY COLUMN Approx_Lon_degrees          			DECIMAL(9,2),
    MODIFY COLUMN H2S_Content_mol_percentage  			DECIMAL(8,2),
	MODIFY COLUMN Pipe_Outside_Diameter_mm    			DECIMAL(8,2),
	MODIFY COLUMN Pipe_Wall_Thickness_mm       			DECIMAL(8,2),
	MODIFY COLUMN Pipe_Max_Operating_Pressure_kPa       DECIMAL(10,2),
	MODIFY COLUMN Pipe_Stress_Level_Yield_Strength      DECIMAL(6,2),
	MODIFY COLUMN Shape_Length_meters                   DECIMAL(14,2),
	MODIFY COLUMN Last_Occurrence_Year            	    INT,
	MODIFY COLUMN Permit_Approval_Date 					DATE;

-- Well Failure Table

DELETE FROM well_failures_mod
WHERE (Failure_Top_Depth_mKB IS NULL OR Failure_Top_Depth_mKB = ''); 

DELETE FROM well_failures_mod
WHERE (Failure_Bottom_Depth_mKB IS NULL OR Failure_Bottom_Depth_mKB = '');

DELETE FROM well_failures_mod
WHERE Detection_Date IS NULL OR Detection_Date = '';

DELETE FROM well_failures_mod
WHERE Detection_Date NOT REGEXP '^[0-9]{1,2}-[A-Za-z]{3}-[0-9]{2}$';

UPDATE well_failures_mod
SET Detection_Date = STR_TO_DATE(Detection_Date, '%e-%b-%')
WHERE Detection_Date IS NOT NULL;

DELETE FROM well_failures_mod
WHERE Report_Date IS NULL OR Report_Date = '';

DELETE FROM well_failures_mod
WHERE Report_Date NOT REGEXP '^[0-9]{1,2}-[A-Za-z]{3}-[0-9]{2}$';

UPDATE well_failures_mod
SET Report_Date = STR_TO_DATE(Report_Date, '%e-%b-%y')
WHERE Report_Date IS NOT NULL;

DELETE FROM well_failures_mod
WHERE Final_Drill_Date IS NULL OR Final_Drill_Date = '';

DELETE FROM well_failures_mod
WHERE Final_Drill_Date NOT REGEXP '^[0-9]{1,2}-[A-Za-z]{3}-[0-9]{2}$';

UPDATE well_failures_mod
SET Final_Drill_Date = STR_TO_DATE(Final_Drill_Date, '%e-%b-%y')
WHERE Final_Drill_Date IS NOT NULL;

ALTER TABLE well_failures_mod
    MODIFY COLUMN Approx_Lat_degrees 			DECIMAL(9,2),
    MODIFY COLUMN Approx_Lon_degrees 			DECIMAL(9,2),
    MODIFY COLUMN Failure_Top_Depth_mKB 		DECIMAL(10,2),
    MODIFY COLUMN Failure_Bottom_Depth_mKB		DECIMAL(10,2),
    MODIFY COLUMN Detection_Date 				DATE,
    MODIFY COLUMN Report_Date 					DATE,
    MODIFY COLUMN Final_Drill_Date 				DATE;
    
DESCRIBE well_failures_mod;

















		
