/*
Упрощенная разбивка assignment по группам.
*/

SELECT
    group_short_name,
    experiment_group,
    count() AS subs_cnt,
    round(count() / sum(count()) OVER (), 6) AS subs_share
FROM data_science.ukg_assignment
GROUP BY
    group_short_name,
    experiment_group
ORDER BY subs_cnt DESC;