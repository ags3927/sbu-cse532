Assuming we are in the root directory containing all .sql files and the .java file

Task 1:
db2 -tf "createtable.sql" 
db2 -tf "load.sql"

Task 2:
db2 -tf "pearsonCCsp.sql"


Task 3:
javac PearsonCC
java PearsonCoefficient dbname username password stock1Name stock2Name 
// Use "stock1" = "AAL", "stock2" = "XOM"


