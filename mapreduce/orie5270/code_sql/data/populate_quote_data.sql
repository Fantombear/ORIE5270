INSERT INTO quote
(A, C)
SELECT
    A,
    C
FROM
    temporal_quote;

TRUNCATE TABLE temporal_quote;
