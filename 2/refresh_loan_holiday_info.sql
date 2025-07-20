CREATE OR REPLACE PROCEDURE dm.refresh_loan_holiday_info()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Создаем временную таблицу с актуальными данными
    CREATE TEMP TABLE temp_refresh AS 
    WITH deal_active AS (
        SELECT * FROM rd.deal_info 
        WHERE CURRENT_DATE BETWEEN effective_from_date AND COALESCE(effective_to_date, '9999-12-31')
    ),
    holiday_active AS (
        SELECT * FROM rd.loan_holiday 
        WHERE CURRENT_DATE BETWEEN effective_from_date AND COALESCE(effective_to_date, '9999-12-31')
    ),
    product_active AS (
        SELECT * FROM rd.product 
        WHERE CURRENT_DATE BETWEEN effective_from_date AND COALESCE(effective_to_date, '9999-12-31')
    )
    SELECT 
        d.deal_rk,
        GREATEST(d.effective_from_date, lh.effective_from_date, p.effective_from_date) as effective_from_date,
        LEAST(
            COALESCE(d.effective_to_date, '9999-12-31'),
            COALESCE(lh.effective_to_date, '9999-12-31'),
            COALESCE(p.effective_to_date, '9999-12-31')
        ) as effective_to_date,
        d.agreement_rk,
        d.client_rk,
        d.department_rk,
        d.product_rk,
        p.product_name,
        d.deal_type_cd,
        d.deal_start_date,
        d.deal_name,
        d.deal_num as deal_number,
        d.deal_sum,
        lh.loan_holiday_type_cd,
        lh.loan_holiday_start_date,
        lh.loan_holiday_finish_date,
        lh.loan_holiday_fact_finish_date,
        lh.loan_holiday_finish_flg,
        lh.loan_holiday_last_possible_date
    FROM deal_active d
    LEFT JOIN holiday_active lh ON d.deal_rk = lh.deal_rk
    LEFT JOIN product_active p ON d.product_rk = p.product_rk;
    
    -- Очищаем и перезаполняем витрину
    TRUNCATE TABLE dm.loan_holiday_info;
    INSERT INTO dm.loan_holiday_info SELECT * FROM temp_refresh;
    
    -- Логируем обновление
    INSERT INTO dm.load_log(table_name, load_date, records_loaded)
    VALUES ('loan_holiday_info', NOW(), (SELECT COUNT(*) FROM temp_refresh));
    
    DROP TABLE temp_refresh;
    COMMIT;
END;
$$;