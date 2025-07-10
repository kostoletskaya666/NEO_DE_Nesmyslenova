CREATE OR REPLACE PROCEDURE DS.FILL_ACCOUNT_BALANCE_F(i_OnDate DATE)
AS $$
DECLARE
    v_process_name VARCHAR(100) := 'FILL_ACCOUNT_BALANCE_F';
    v_start_time TIMESTAMP := CURRENT_TIMESTAMP;
    v_rows_processed INTEGER := 0;
    v_error_message TEXT;
BEGIN
    -- Логирование начала процедуры
    INSERT INTO LOGS.ETL_LOGS 
    (process_name, start_time, status, rows_processed)
    VALUES (v_process_name, v_start_time, 'STARTED', 0);
    
    -- Удаление данных за указанную дату
    DELETE FROM DM.DM_ACCOUNT_BALANCE_F WHERE on_date = i_OnDate;
    
    -- Заполнение витрины остатков
    INSERT INTO DM.DM_ACCOUNT_BALANCE_F
    (on_date, account_rk, balance_out, balance_out_rub)
    WITH previous_balance AS (
        SELECT 
            account_rk,
            balance_out,
            balance_out_rub
        FROM DM.DM_ACCOUNT_BALANCE_F
        WHERE on_date = i_OnDate - INTERVAL '1 day'
    ),
    current_turnover AS (
        SELECT 
            account_rk,
            credit_amount,
            credit_amount_rub,
            debet_amount,
            debet_amount_rub
        FROM DM.DM_ACCOUNT_TURNOVER_F
        WHERE on_date = i_OnDate
    ),
    active_accounts AS (
        SELECT 
            a.account_rk,
            a.char_type,
            a.currency_rk
        FROM DS.MD_ACCOUNT_D a
        WHERE i_OnDate BETWEEN a.data_actual_date AND a.data_actual_end_date
    )
    SELECT 
        i_OnDate AS on_date,
        a.account_rk,
        CASE 
            WHEN a.char_type = 'А' THEN 
                COALESCE(pb.balance_out, 0) + COALESCE(ct.debet_amount, 0) - COALESCE(ct.credit_amount, 0)
            WHEN a.char_type = 'П' THEN 
                COALESCE(pb.balance_out, 0) - COALESCE(ct.debet_amount, 0) + COALESCE(ct.credit_amount, 0)
            ELSE 0
        END AS balance_out,
        CASE 
            WHEN a.char_type = 'А' THEN 
                COALESCE(pb.balance_out_rub, 0) + COALESCE(ct.debet_amount_rub, 0) - COALESCE(ct.credit_amount_rub, 0)
            WHEN a.char_type = 'П' THEN 
                COALESCE(pb.balance_out_rub, 0) - COALESCE(ct.debet_amount_rub, 0) + COALESCE(ct.credit_amount_rub, 0)
            ELSE 0
        END AS balance_out_rub
    FROM active_accounts a
    LEFT JOIN previous_balance pb ON a.account_rk = pb.account_rk
    LEFT JOIN current_turnover ct ON a.account_rk = ct.account_rk;
    
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