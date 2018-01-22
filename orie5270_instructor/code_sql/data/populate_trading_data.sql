INSERT INTO trading
(A, B)
SELECT
    A,
    B
FROM
    temporal_trading;

TRUNCATE TABLE temporal_trading;
