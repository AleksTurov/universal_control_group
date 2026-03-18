SELECT m.*
FROM DWH.dm_datamart_monthly m
WHERE m.DT = toDate(:DATA_START) -- начало месяца
  AND m.CUST_LEVEL NOT IN ('employee', 'TEST') -- исключаем сотрудников и тестовых

