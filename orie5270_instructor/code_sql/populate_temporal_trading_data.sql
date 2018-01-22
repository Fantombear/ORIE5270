
INSERT INTO trading
(A, B, date_transaction)
SELECT
    A,
    B,
    :d
FROM
    temporal_trading;

TRUNCATE TABLE temporal_trading;
