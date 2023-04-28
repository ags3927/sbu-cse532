! erase zipcode_table.msg;

CONNECT TO SAMPLE;

DROP TABLE CSE532.ZIPCODE_TABLE;

CREATE TABLE CSE532.ZIPCODE_TABLE(
    ZIPCODE CHAR(5),
    SHAPE db2gse.st_multipolygon
);

!db2se import_shape sample
-fileName         tl_2022_us_zcta520.shp
-inputAttrColumns N(ZCTA5CE20)
-srsName          nad83_srs_1
-tableSchema      CSE532
-tableName        ZIPCODE_TABLE
-tableAttrColumns ZIPCODE
-createTableFlag  0
-spatialColumn    SHAPE
-typeSchema       db2gse
-typeName         st_multipolygon
-messagesFile     zipcode_table.msg
-client 1
;

!db2se register_spatial_column sample
-tableSchema      CSE532
-tableName        ZIPCODE_TABLE
-columnName       SHAPE
-srsName          nad83_srs_1
;

SELECT COUNT(*) FROM CSE532.ZIPCODE_TABLE;

DESCRIBE TABLE CSE532.ZIPCODE_TABLE;