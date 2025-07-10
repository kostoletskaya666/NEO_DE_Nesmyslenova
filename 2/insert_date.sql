-- Заполнение начальных остатков на 31.12.2017
INSERT INTO DM.DM_ACCOUNT_BALANCE_F
(on_date, account_rk, balance_out, balance_out_rub)
SELECT 
    '2017-12-31'::DATE AS on_date,
    b.account_rk,
    b.balance_out,
    b.balance_out * COALESCE(er.reduced_cource, 1) AS balance_out_rub
FROM DS.FT_BALANCE_F b
LEFT JOIN DS.MD_ACCOUNT_D a ON b.account_rk = a.account_rk 
    AND '2017-12-31'::DATE BETWEEN a.data_actual_date AND a.data_actual_end_date
LEFT JOIN DS.MD_EXCHANGE_RATE_D er ON a.currency_rk = er.currency_rk 
    AND '2017-12-31'::DATE BETWEEN er.data_actual_date AND er.data_actual_end_date
WHERE b.on_date = '2017-12-31'::DATE;