/*
Сколько новых строк вставилось в дату report_dt.

Параметры:
- :report_dt дата среза в формате YYYY-MM-DD.
*/

SELECT
    assignment_dt,
    count() AS inserted_rows,
    round(avg(is_control), 6) AS control_share_inserted
FROM data_science.ukg_assignment
WHERE assignment_dt = toDate(:report_dt)
GROUP BY assignment_dt;