/*
Глобальный summary по всей assignment-таблице.

Что проверяем:
1) общий размер assignment-базы;
2) глобальную долю control;
3) наличие дублей по subs_id.
*/

SELECT
    count() AS total_assigned,
    sum(is_control) AS control_cnt,
    round(avg(is_control), 6) AS control_share_global,
    count() - uniqExact(subs_id) AS duplicate_rows_cnt
FROM data_science.ukg_assignment;