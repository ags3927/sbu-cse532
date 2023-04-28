CREATE TABLE cse532.stock
    (
        StockName varchar(4),
        DateVar date,
        OpeningPrice float,
        HighestPrice float,
        LowestPrice float,
        ClosingPrice float,
        AdjClosingPrice float,
        Volume int
    );

CREATE INDEX date_index ON cse532.stock (DateVar);