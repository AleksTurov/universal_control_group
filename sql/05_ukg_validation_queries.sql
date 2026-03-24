/*
Контрольные запросы после monthly run UKG.

Что проверяем:
1) сколько новых строк вставилось в дату report_dt;
2) какая глобальная доля control в полной assignment-таблице;
3) как выглядит бизнесовая разбивка групп из самой assignment-таблицы;
4) нет ли дублей по subs_id.

Параметры форматирования:
- :report_dt дата среза в формате YYYY-MM-DD.
*/

SELECT
    assignment_dt,
    count() AS inserted_rows,
    round(avg(is_control), 6) AS control_share_inserted
FROM data_science.ukg_assignment
WHERE assignment_dt = toDate(:report_dt)
GROUP BY assignment_dt;

SELECT
    count() AS total_assigned,
    sum(is_control) AS control_cnt,
    round(avg(is_control), 6) AS control_share_global
FROM data_science.ukg_assignment;

SELECT
    group_short_name,
    experiment_group_name,
    experiment_group_name_ru,
    experiment_group_description,
    count() AS subs_cnt,
    round(count() / sum(count()) OVER (), 6) AS subs_share
FROM data_science.ukg_assignment
GROUP BY
    group_short_name,
    experiment_group_name,
    experiment_group_name_ru,
    experiment_group_description
ORDER BY subs_cnt DESC;

SELECT
    subs_id,
    count() AS cnt
FROM data_science.ukg_assignment
GROUP BY subs_id
HAVING cnt > 1
LIMIT 100;