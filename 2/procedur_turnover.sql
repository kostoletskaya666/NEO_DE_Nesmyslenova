CREATE OR REPLACE PROCEDURE DS.FILL_ACCOUNT_TURNOVER_F(i_OnDate DATE)
AS $$
DECLARE
    v_process_name VARCHAR(100) := 'FILL_ACCOUNT_TURNOVER_F';
    v_start_time TIMESTAMP := CURRENT_TIMESTAMP;
    v_rows_processed INTEGER := 0;
    v_error_message TEXT;
BEGIN
    -- Логирование начала процедуры
    INSERT INTO LOGS.ETL_LOGS 
    (process_name, start_time, status, rows_processed)
    VALUES (v_process_name, v_start_time, 'STARTED', 0);
    
    -- Удаление данных за указанную дату
    DELETE FROM DM.DM_ACCOUNT_TURNOVER_F WHERE on_date = i_OnDate;
    
    -- Заполнение витрины оборотами
    INSERT INTO DM.DM_ACCOUNT_TURNOVER_F
    (on_date, account_rk, credit_amount, credit_amount_rub, debet_amount, debet_amount_rub)
    WITH credit_turnover AS (
        SELECT 
            credit_account_rk AS account_rk,
            SUM(credit_amount) AS credit_amount,
            SUM(credit_amount * COALESCE(er.reduced_cource, 1)) AS credit_amount_rub
        FROM DS.FT_POSTING_F p
        LEFT JOIN DS.MD_ACCOUNT_D a ON p.credit_account_rk = a.account_rk 
            AND i_OnDate BETWEEN a.data_actual_date AND a.data_actual_end_date
        LEFT JOIN DS.MD_EXCHANGE_RATE_D er ON a.currency_rk = er.currency_rk 
            AND i_OnDate BETWEEN er.data_actual_date AND er.data_actual_end_date
        WHERE p.oper_date = i_OnDate
        GROUP BY credit_account_rk
    ),
    debit_turnover AS (
        SELECT 
            debit_account_rk AS account_rk,
            SUM(debit_amount) AS debit_amount,
            SUM(debit_amount * COALESCE(er.reduced_cource, 1)) AS debit_amount_rub
        FROM DS.FT_POSTING_F p
        LEFT JOIN DS.MD_ACCOUNT_D a ON p.debit_account_rk = a.account_rk
            AND i_OnDate BETWEEN a.data_actual_date AND a.data_actual_end_date
        LEFT JOIN DS.MD_EXCHANGE_RATE_D er ON a.currency_rk = er.currency_rk 
            AND i_OnDate BETWEEN er.data_actual_date AND er.data_actual_end_date
        WHERE p.oper_date = i_OnDate
        GROUP BY debit_account_rk
    ),
    combined AS (
        SELECT 
            COALESCE(c.account_rk, d.account_rk) AS account_rk,
            COALESCE(c.credit_amount, 0) AS credit_amount,
            COALESCE(c.credit_amount_rub, 0) AS credit_amount_rub,
            COALESCE(d.debit_amount, 0) AS debit_amount,
            COALESCE(d.debit_amount_rub, 0) AS debit_amount_rub
        FROM credit_turnover c
        FULL OUTER JOIN debit_turnover d ON c.account_rk = d.account_rk
    )
    SELECT 
        i_OnDate AS on_date,
        account_rk,
        credit_amount,
        credit_amount_rub,
        debit_amount,
        debit_amount_rub
    FROM combined
    WHERE credit_amount != 0 OR debit_amount != 0;
    
    -- Получаем количество обработанных строк
    GET DIAGNOSTICS v_rows_processed = ROW_COUNT;
    
    -- Логирование успешного завершения
    UPDATE LOGS.ETL_LOGS 
    SET end_time = CURRENT_TIMESTAMP, 
        status = 'SUCCESS',
        rows_processed = v_rows_processed
    WHERE process_name = v_process_name 
      AND start_time = v_start_time;

EXCEPTION
    WHEN OTHERS THEN
        -- Получаем сообщение об ошибке
        GET STACKED DIAGNOSTICS v_error_message = MESSAGE_TEXT;
        
        -- Логирование ошибки
        UPDATE LOGS.ETL_LOGS 
        SET end_time = CURRENT_TIMESTAMP, 
            status = 'FAILED',
            error_message = v_error_message
        WHERE process_name = v_process_name 
          AND start_time = v_start_time;
        
        -- Возвращаем ошибку вызывающему коду
        RAISE EXCEPTION '%', v_error_message;
END;
$$ LANGUAGE plpgsql;