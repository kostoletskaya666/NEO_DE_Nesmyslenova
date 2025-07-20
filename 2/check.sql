-- Сравнить количество записей
SELECT 
    (SELECT COUNT(*) FROM dm.loan_holiday_info) as old_count,
    (SELECT COUNT(*) FROM dm.loan_holiday_info_corrected) as new_count;

-- Проверить конкретные пропущенные записи
SELECT * FROM rd.loan_holiday 
WHERE deal_rk NOT IN (SELECT deal_rk FROM dm.loan_holiday_info)
LIMIT 100;