CREATE OR REPLACE PROCEDURE cse532.PearsonCC(IN StockName1 CHAR(4), IN StockName2 CHAR(4), OUT CC DECIMAL(4,3))
LANGUAGE SQL
BEGIN
DECLARE SQLSTATE CHAR(5) DEFAULT '00000';

DECLARE total_s1_cp DOUBLE DEFAULT 0;
DECLARE total_s1_cp_sq DOUBLE DEFAULT 0;
DECLARE total_s2_cp DOUBLE DEFAULT 0;
DECLARE total_s2_cp_sq DOUBLE DEFAULT 0;
DECLARE total_s1_s2_cp_mul DOUBLE DEFAULT 0;
DECLARE s1_cp DOUBLE;
DECLARE s2_cp DOUBLE;
DECLARE s1_cp_sq DOUBLE;
DECLARE s2_cp_sq DOUBLE;
DECLARE s1_s2_cp_mul DOUBLE;
DECLARE n INTEGER DEFAULT 0;
DECLARE r DOUBLE;
DECLARE n_sum_s1_s2 DOUBLE;
DECLARE prod_sum_s1_sum_s2 DOUBLE;
DECLARE n_sum_s1_sq DOUBLE;
DECLARE sq_sum_s1 DOUBLE;
DECLARE n_sum_s2_sq DOUBLE;
DECLARE sq_sum_s2 DOUBLE;
DECLARE numerator;
DECLARE denominator;

DECLARE s1_cursor CURSOR FOR SELECT ClosingPrice FROM cse532.stock WHERE StockName = StockName1;
DECLARE s2_cursor CURSOR FOR SELECT ClosingPrice FROM cse532.stock WHERE StockName = StockName2;

SET total_s1_cp = 0.0;
SET total_s1_cp_sq = 0.0;
SET total_s2_cp = 0.0;
SET total_s2_cp_sq = 0.0;
SET total_s1_s2_cp_mul = 0.0;
SET n = 0;

/* Open cursors */
OPEN s1_cursor;
OPEN s2_cursor;

/* Fetch first x and y */
FETCH FROM s1_cursor INTO s1_cp;
FETCH FROM s2_cursor INTO s2_cp;

WHILE(SQLSTATE = '00000') DO
    SET n = n + 1;
    
    /* Calculate xy*/
    SET s1_s2_cp_mul = s1_cp * s2_cp;
    
    /* Add xy to sum */
    SET total_s1_s2_cp_mul = total_s1_s2_cp_mul + s1_s2_cp_mul;
    
    /* Add y to sum */
    SET total_s2_cp = total_s2_cp + s2_cp;
    
    /* Calculate y^2 */
    SET s2_cp_sq = s2_cp * s2_cp;

    /* Add y^2 to sum */
    SET total_s2_cp_sq = total_s2_cp_sq + s2_cp_sq;

    /* Add x to sum */
    SET total_s1_cp = total_s1_cp + s1_cp;

    /* Calculate x^2 */
    SET s1_cp_sq = s1_cp * s1_cp;

    /* Add x^2 to sum */
    SET total_s1_cp_sq = total_s1_cp_sq + s1_cp_sq;
    
    /* Fetch next x and y */
    FETCH FROM s1_cursor INTO s1_cp;
    FETCH FROM s2_cursor INTO s2_cp;

END WHILE;

/* Close cursors */
CLOSE s1_cursor;
CLOSE s2_cursor;

/* The term for y in denominator */
SET n_sum_s2_sq = n * total_s2_cp_sq;
SET sq_sum_s2 = total_s2_cp * total_s2_cp;

/* The term for x in denominator */
SET n_sum_s1_sq = n * total_s1_cp_sq;
SET sq_sum_s1 = total_s1_cp * total_s1_cp;

/* The denominator */
SET denominator = SQRT((n_sum_s1_sq - sq_sum_s1) * (n_sum_s2_sq - sq_sum_s2));

/* The numerator */
SET n_sum_s1_s2 = n * total_s1_s2_cp_mul;
SET prod_sum_s1_sum_s2 = total_s1_cp * total_s2_cp;
SET numerator = n_sum_s1_s2 - prod_sum_s1_sum_s2;

/* The correlation coefficient */
SET r = numerator / denominator;

SET CC = CAST(r AS DECIMAL(4,3));

END@

CALL cse532.PearsonCC('XOM','AAL',?)@