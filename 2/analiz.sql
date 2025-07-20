-- Проверить количество записей в источниках и витрине
SELECT 
    (SELECT COUNT(*) FROM rd.deal_info) as deal_count,
    (SELECT COUNT(*) FROM rd.loan_holiday) as holiday_count,
    (SELECT COUNT(*) FROM rd.product) as product_count,
    (SELECT COUNT(*) FROM dm.loan_holiday_info) as dm_count;
    
-- Найти явные расхождения
SELECT 'Сделки без каникул' as issue, COUNT(*) 
FROM rd.deal_info d 
LEFT JOIN rd.loan_holiday lh ON d.deal_rk = lh.deal_rk 
WHERE lh.deal_rk IS NULL
UNION ALL
SELECT 'Каникулы без сделок' as issue, COUNT(*) 
FROM rd.loan_holiday lh 
LEFT JOIN rd.deal_info d ON d.deal_rk = lh.deal_rk 
WHERE d.deal_rk IS NULL;