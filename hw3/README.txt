Extra Task
----------
Ran both query1 and query2 with different parameters and obtained the following results:

~

1. For the following grid cell size parameters:

CREATE INDEX CSE532.TRIFacility_IDX ON CSE532.TRIFacility(LOCATION_DATA) EXTEND USING db2gse.spatial_index(0.75, 2, 5);

CREATE INDEX CSE532.ZIPCODE_TABLE_IDX ON CSE532.ZIPCODE_TABLE(SHAPE) EXTEND USING db2gse.spatial_index(.25, 1.0, 2);

query1 time: 27s 832ms

query2 time: 663ms

~

2. For the following grid cell size parameters (based on recommendations from Index Advisor):

CREATE INDEX CSE532.TRIFacility_IDX ON CSE532.TRIFacility(LOCATION_DATA) EXTEND USING db2gse.spatial_index(0.05, 2, 5);

CREATE INDEX CSE532.ZIPCODE_TABLE_IDX ON CSE532.ZIPCODE_TABLE(SHAPE) EXTEND USING db2gse.spatial_index(0.8, 1.6, 4);

query1 time: 28s 176ms

query2 time: 820ms


3. After deleting both indices and running the queries in an unindexed table.

query1 time: 31s 285ms
query2 time: 1s 52ms

