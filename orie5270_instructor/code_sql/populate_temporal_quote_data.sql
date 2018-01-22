
INSERT INTO quote
(A, C, date_quote)
SELECT
    A,
    C,
    :d
FROM
    temporal_quote;

TRUNCATE TABLE temporal_quote;
