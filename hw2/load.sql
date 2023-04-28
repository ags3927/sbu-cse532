ALTER TABLE cse532.stock ALTER COLUMN StockName SET DEFAULT 'AAL';
LOAD FROM "/Users/ags/Documents/Study/Spring 2023/Database/sbu-cse532/data/AAL.csv" OF DEL INSERT INTO cse532.stock (DateVar, OpeningPrice, HighestPrice, LowestPrice, ClosingPrice, AdjClosingPrice, Volume);
ALTER TABLE cse532.stock ALTER COLUMN StockName SET DEFAULT 'XOM';
LOAD FROM "/Users/ags/Documents/Study/Spring 2023/Database/sbu-cse532/data/XOM.csv" OF DEL INSERT INTO cse532.stock (DateVar, OpeningPrice, HighestPrice, LowestPrice, ClosingPrice, AdjClosingPrice, Volume);