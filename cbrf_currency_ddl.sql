-- DROP TABLE IF EXISTS cbrf.currency;

CREATE TABLE cbrf.currency(
    valute_id TEXT NOT NULL,
    valute_numcode CHAR(3) NOT NULL,
    valute_charcode CHAR(3) NOT NULL,
    valute_nominal INT NOT NULL,
    valute_name TEXT NOT NULL,
    valute_value NUMERIC,
    valute_load DATE DEFAULT now()::DATE,
    PRIMARY KEY(valute_id, valute_load)
);

SELECT
    valute_charcode, valute_value, valute_load
FROM cbrf.currency
WHERE valute_charcode = 'USD'
    AND valute_load
    BETWEEN '2021-11-01' AND '2021-11-05';