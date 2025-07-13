CREATE OR REPLACE PROCEDURE dm.fill_f101_round_f(i_OnDate DATE)
AS $$
DECLARE
    v_process_name TEXT := 'FILL_f101_ROUND_F';
    v_start_time TIMESTAMP := CURRENT_TIMESTAMP;
    v_log_id BIGINT;
    v_rows_processed INTEGER := 0;
    v_error_info TEXT;
BEGIN
    -- 1. Начальная запись лога
    BEGIN
        INSERT INTO logs.etl_logs 
        (process_name, start_time, status, rows_processed)
        VALUES (v_process_name, v_start_time, 'STARTED', 0)
        RETURNING log_id INTO v_log_id;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Не удалось записать начальный лог: %', SQLERRM;
        v_log_id := NULL;
    END;

    -- 2. Основная логика с гарантированным округлением
    BEGIN
        -- Удаление старых данных
        DELETE FROM DM.DM_F101_ROUND_F 
        WHERE FROM_DATE = DATE_TRUNC('month', i_OnDate - INTERVAL '1 month')::DATE;
        
        -- Вставка новых данных с явным округлением
        INSERT INTO DM.DM_F101_ROUND_F (
            FROM_DATE, TO_DATE, CHAPTER, LEDGER_ACCOUNT, CHARACTERISTIC,
            BALANCE_IN_RUB, R_BALANCE_IN_RUB,
            BALANCE_IN_VAL, R_BALANCE_IN_VAL,
            BALANCE_IN_TOTAL, R_BALANCE_IN_TOTAL,
            TURN_DEB_RUB, R_TURN_DEB_RUB,
            TURN_DEB_VAL, R_TURN_DEB_VAL,
            TURN_DEB_TOTAL, R_TURN_DEB_TOTAL,
            TURN_CRE_RUB, R_TURN_CRE_RUB,
            TURN_CRE_VAL, R_TURN_CRE_VAL,
            TURN_CRE_TOTAL, R_TURN_CRE_TOTAL,
            BALANCE_OUT_RUB, R_BALANCE_OUT_RUB,
            BALANCE_OUT_VAL, R_BALANCE_OUT_VAL,
            BALANCE_OUT_TOTAL, R_BALANCE_OUT_TOTAL
        )
        SELECT 
            DATE_TRUNC('month', i_OnDate - INTERVAL '1 month')::DATE,
            (DATE_TRUNC('month', i_OnDate) - INTERVAL '1 day')::DATE,
            la.chapter,
            SUBSTRING(a.account_number, 1, 5),
            a.char_type,
            
            -- Остатки на начало (явное округление)
            SUM(CASE WHEN a.currency_code IN ('810', '643') THEN b_prev.balance_out_rub ELSE 0 END),
            CAST(ROUND(CAST(SUM(CASE WHEN a.currency_code IN ('810', '643') THEN b_prev.balance_out_rub ELSE 0 END) AS NUMERIC), 2) AS NUMERIC(23,8)),
            
            SUM(CASE WHEN a.currency_code NOT IN ('810', '643') THEN b_prev.balance_out_rub ELSE 0 END),
            CAST(ROUND(CAST(SUM(CASE WHEN a.currency_code NOT IN ('810', '643') THEN b_prev.balance_out_rub ELSE 0 END) AS NUMERIC), 2) AS NUMERIC(23,8)),
            
            SUM(b_prev.balance_out_rub),
            CAST(ROUND(CAST(SUM(b_prev.balance_out_rub) AS NUMERIC), 2) AS NUMERIC(23,8)),
            
            -- Обороты по дебету (явное округление)
            SUM(CASE WHEN a.currency_code IN ('810', '643') THEN t.debet_amount_rub ELSE 0 END),
            CAST(ROUND(CAST(SUM(CASE WHEN a.currency_code IN ('810', '643') THEN t.debet_amount_rub ELSE 0 END) AS NUMERIC), 2) AS NUMERIC(23,8)),
            
            SUM(CASE WHEN a.currency_code NOT IN ('810', '643') THEN t.debet_amount_rub ELSE 0 END),
            CAST(ROUND(CAST(SUM(CASE WHEN a.currency_code NOT IN ('810', '643') THEN t.debet_amount_rub ELSE 0 END) AS NUMERIC), 2) AS NUMERIC(23,8)),
            
            SUM(t.debet_amount_rub),
            CAST(ROUND(CAST(SUM(t.debet_amount_rub) AS NUMERIC), 2) AS NUMERIC(23,8)),
            
            -- Обороты по кредиту (явное округление)
            SUM(CASE WHEN a.currency_code IN ('810', '643') THEN t.credit_amount_rub ELSE 0 END),
            CAST(ROUND(CAST(SUM(CASE WHEN a.currency_code IN ('810', '643') THEN t.credit_amount_rub ELSE 0 END) AS NUMERIC), 2) AS NUMERIC(23,8)),
            
            SUM(CASE WHEN a.currency_code NOT IN ('810', '643') THEN t.credit_amount_rub ELSE 0 END),
            CAST(ROUND(CAST(SUM(CASE WHEN a.currency_code NOT IN ('810', '643') THEN t.credit_amount_rub ELSE 0 END) AS NUMERIC), 2) AS NUMERIC(23,8)),
            
            SUM(t.credit_amount_rub),
            CAST(ROUND(CAST(SUM(t.credit_amount_rub) AS NUMERIC), 2) AS NUMERIC(23,8)),
            
            -- Остатки на конец (явное округление)
            SUM(CASE WHEN a.currency_code IN ('810', '643') THEN b_end.balance_out_rub ELSE 0 END),
            CAST(ROUND(CAST(SUM(CASE WHEN a.currency_code IN ('810', '643') THEN b_end.balance_out_rub ELSE 0 END) AS NUMERIC), 2) AS NUMERIC(23,8)),
            
            SUM(CASE WHEN a.currency_code NOT IN ('810', '643') THEN b_end.balance_out_rub ELSE 0 END),
            CAST(ROUND(CAST(SUM(CASE WHEN a.currency_code NOT IN ('810', '643') THEN b_end.balance_out_rub ELSE 0 END) AS NUMERIC), 2) AS NUMERIC(23,8)),
            
            SUM(b_end.balance_out_rub),
            CAST(ROUND(CAST(SUM(b_end.balance_out_rub) AS NUMERIC), 2) AS NUMERIC(23,8))
        FROM 
            DS.MD_ACCOUNT_D a
        LEFT JOIN DS.MD_LEDGER_ACCOUNT_S la ON SUBSTRING(a.account_number, 1, 5) = la.ledger_account::TEXT
        LEFT JOIN DM.DM_ACCOUNT_BALANCE_F b_prev ON a.account_rk = b_prev.account_rk 
            AND b_prev.on_date = (DATE_TRUNC('month', i_OnDate - INTERVAL '1 month') - INTERVAL '1 day')::DATE
        LEFT JOIN DM.DM_ACCOUNT_BALANCE_F b_end ON a.account_rk = b_end.account_rk 
            AND b_end.on_date = (DATE_TRUNC('month', i_OnDate) - INTERVAL '1 day')::DATE
        LEFT JOIN DM.DM_ACCOUNT_TURNOVER_F t ON a.account_rk = t.account_rk 
            AND t.on_date BETWEEN DATE_TRUNC('month', i_OnDate - INTERVAL '1 month')::DATE 
            AND (DATE_TRUNC('month', i_OnDate) - INTERVAL '1 day')::DATE
        WHERE 
            a.data_actual_date <= (DATE_TRUNC('month', i_OnDate) - INTERVAL '1 day')::DATE
            AND a.data_actual_end_date >= DATE_TRUNC('month', i_OnDate - INTERVAL '1 month')::DATE
        GROUP BY 
            la.chapter, SUBSTRING(a.account_number, 1, 5), a.char_type;
        
        GET DIAGNOSTICS v_rows_processed = ROW_COUNT;
        
        -- 3. Запись успешного завершения
        IF v_log_id IS NOT NULL THEN
            BEGIN
                UPDATE logs.etl_logs 
                SET end_time = CURRENT_TIMESTAMP, 
                    status = 'SUCCESS',
                    rows_processed = v_rows_processed
                WHERE log_id = v_log_id;
            EXCEPTION WHEN OTHERS THEN
                RAISE NOTICE 'Не удалось обновить лог при завершении: %', SQLERRM;
            END;
        END IF;
        
    EXCEPTION WHEN OTHERS THEN
        -- 4. Обработка ошибок
        v_error_info := SQLERRM;
        
        IF v_log_id IS NOT NULL THEN
            BEGIN
                UPDATE logs.etl_logs 
                SET end_time = CURRENT_TIMESTAMP, 
                    status = 'FAILED',
                    error_message = LEFT(v_error_info, 255)
                WHERE log_id = v_log_id;
            EXCEPTION WHEN OTHERS THEN
                RAISE NOTICE 'Не удалось записать лог ошибки: %', SQLERRM;
            END;
        END IF;
        
        RAISE EXCEPTION '%', LEFT(v_error_info, 200);
    END;
END;
$$ LANGUAGE plpgsql;