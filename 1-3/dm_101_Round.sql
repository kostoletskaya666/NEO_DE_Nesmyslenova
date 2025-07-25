-- Создание таблицы для 101 формы
CREATE TABLE DM.DM_F101_ROUND_F (
    FROM_DATE DATE NOT NULL,
    TO_DATE DATE NOT NULL,
    CHAPTER CHAR(1),
    LEDGER_ACCOUNT CHAR(5) NOT NULL,
    CHARACTERISTIC CHAR(1),
    BALANCE_IN_RUB NUMERIC(23,8),
    R_BALANCE_IN_RUB NUMERIC(23,8),
    BALANCE_IN_VAL NUMERIC(23,8),
    R_BALANCE_IN_VAL NUMERIC(23,8),
    BALANCE_IN_TOTAL NUMERIC(23,8),
    R_BALANCE_IN_TOTAL NUMERIC(23,8),
    TURN_DEB_RUB NUMERIC(23,8),
    R_TURN_DEB_RUB NUMERIC(23,8),
    TURN_DEB_VAL NUMERIC(23,8),
    R_TURN_DEB_VAL NUMERIC(23,8),
    TURN_DEB_TOTAL NUMERIC(23,8),
    R_TURN_DEB_TOTAL NUMERIC(23,8),
    TURN_CRE_RUB NUMERIC(23,8),
    R_TURN_CRE_RUB NUMERIC(23,8),
    TURN_CRE_VAL NUMERIC(23,8),
    R_TURN_CRE_VAL NUMERIC(23,8),
    TURN_CRE_TOTAL NUMERIC(23,8),
    R_TURN_CRE_TOTAL NUMERIC(23,8),
    BALANCE_OUT_RUB NUMERIC(23,8),
    R_BALANCE_OUT_RUB NUMERIC(23,8),
    BALANCE_OUT_VAL NUMERIC(23,8),
    R_BALANCE_OUT_VAL NUMERIC(23,8),
    BALANCE_OUT_TOTAL NUMERIC(23,8),
    R_BALANCE_OUT_TOTAL NUMERIC(23,8),
    -- Добавляем первичный ключ
    CONSTRAINT pk_dm_f101_round_f PRIMARY KEY (FROM_DATE, LEDGER_ACCOUNT)
);
-- Создание индексов для ускорения поиска
CREATE INDEX idx_dm_f101_round_f_date ON DM.DM_F101_ROUND_F (FROM_DATE);
CREATE INDEX idx_dm_f101_round_f_account ON DM.DM_F101_ROUND_F (LEDGER_ACCOUNT);
CREATE INDEX idx_dm_f101_round_f_chapter ON DM.DM_F101_ROUND_F (CHAPTER);