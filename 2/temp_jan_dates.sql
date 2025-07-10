-- Создаем временную таблицу с датами января 2018
CREATE TEMP TABLE jan_dates AS
SELECT DATE '2018-01-01' + (n || ' day')::INTERVAL AS calc_date
FROM generate_series(0, 30) n;

-- Расчет витрин для каждой даты января
DO $$
DECLARE
    d DATE;
BEGIN
    FOR d IN SELECT calc_date::DATE FROM jan_dates ORDER BY calc_date
    LOOP
        -- Расчет витрины оборотов
        CALL DS.FILL_ACCOUNT_TURNOVER_F(d);
        
        -- Расчет витрины остатков
        CALL DS.FILL_ACCOUNT_BALANCE_F(d);
        
        RAISE NOTICE 'Рассчитаны витрины за %', d;
    END LOOP;
END $$;