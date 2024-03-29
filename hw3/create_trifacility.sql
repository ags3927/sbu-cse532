DROP TABLE CSE532.TRIFacility;

CREATE TABLE CSE532.TRIFacility(
    FACILITY_NAME,
    ST,
    LATITUDE,
    LONGITUDE,
    CHEMICAL,
    CARCINOGEN
);

IMPORT FROM "2021_us.csv" OF DEL
MODIFIED BY COLDEL, DECPT.
METHOD p (4, 8, 12, 13, 34, 43)
MESSAGES "import.msg"
INSERT INTO CSE532.TRIFacility (FACILITY_NAME, ST, LATITUDE, LONGITUDE, CHEMICAL, CARCINOGEN);

ALTER TABLE CSE532.TRIFacility ADD COLUMN LOCATION_DATA db2gse.st_point;


!db2se register_spatial_column sample
-tableSchema      CSE532
-tableName        TRIFacility
-columnName       LOCATION_DATA
-srsName          nad83_srs_1
;

UPDATE CSE532.TRIFacility
SET LOCATION_DATA = db2gse.st_point(LONGITUDE, LATITUDE, 1)
;

REORG TABLE CSE532.TRIFacility;

SELECT COUNT(*) FROM CSE532.TRIFacility;