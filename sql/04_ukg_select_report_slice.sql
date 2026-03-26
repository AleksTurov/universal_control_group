/*
Получение текущего monthly slice для Python split-job.

Что делает запрос:
1) берет eligible-базу за report_dt;
2) применяет базовые исключения на уровне SQL;
3) подтягивает уже существующий assignment, чтобы Python добавлял только новых клиентов;
4) возвращает поля, нужные для bucket-логики, stratification и KS-проверок.

Параметры форматирования:
- :DATA_START дата начала месяца в формате YYYY-MM-DD.

Важно:
- здесь нет расчета hash и нет assignment логики;
- split считается только в Python.
*/
WITH
    toDate(:DATA_START) AS data_start,
    addDays(addMonths(data_start, 1), -1) AS data_end,
    eligible_subs AS (
        SELECT DISTINCT
            toUInt64(hs.subscription_id) AS subscription_id
        FROM DWH.hfct_subs_short hs
        WHERE 1 = 1
          AND hs.eff_dt = data_end -- конец месяца
          AND toDate(hs.subs_termination_time) > data_start -- подписка не завершена на начало месяца
          AND dictGetOrDefault(dict.dict_msisdn_operator, 'operator', hs.subs_msisdn, 'beeline') <> 'IZI' -- не IZI
          AND hs.subs_category NOT ILIKE '%employee%' -- не сотрудник
          AND hs.subs_category NOT ILIKE '%test%' -- не тестовый
          AND hs.subscription_id GLOBAL NOT IN ( -- исключаем M2M
              SELECT DISTINCT hsp.subscription_id
              FROM DWH.hfct_subs_packs hsp
              WHERE hsp.package_code = 'M2M'
                AND (
                    hsp.subspack_deactivated_at IS NULL
                    OR hsp.subspack_deactivated_at > data_end
                )
          )
),
    report_slice AS (
        SELECT
          toUInt64(hs.SUBS_ID) AS SUBS_ID,
          hs.ACTIVE_IND AS ACTIVE_IND,
          hs.STATUS AS STATUS,
          hs.CUST_LEVEL AS CUST_LEVEL,
          hs.REGION AS REGION,
          hs.PERIODICITY AS PERIODICITY,
          hs.FLAG_4G AS FLAG_4G,
          hs.FLAG_ABONKA AS FLAG_ABONKA,
          hs.LIFETIME_TOTAL AS LIFETIME_TOTAL,
          hs.REVENUE_TOTAL_INTERCONNECT AS REVENUE_TOTAL_INTERCONNECT,
          hs.REVENUE_TOTAL AS REVENUE_TOTAL,
          hs.USAGE_INTERNET AS USAGE_INTERNET,
          hs.TRANZ_FLAG AS TRANZ_FLAG,
          hs.BALANCE_USER AS BALANCE_USER,
          hs.MULTIPLAY AS MULTIPLAY,
          hs.FIRST_SIM AS FIRST_SIM,
          hs.MY_BEELINE_USER AS MY_BEELINE_USER,
          hs.DAYS_WITHOUT_PAYMENT AS DAYS_WITHOUT_PAYMENT,
          hs.TOTAL_RECHARGE AS TOTAL_RECHARGE,
          hs.BALANCE_END AS BALANCE_END,
          hs.TOTAL_MOU AS TOTAL_MOU
        FROM DWH.dm_datamart_monthly hs
        WHERE 1 = 1
          AND hs.DT = data_start -- в dm_datamart_monthly DT всегда первый день месяца
          AND toUInt64(hs.SUBS_ID) GLOBAL IN (SELECT subscription_id FROM eligible_subs)
        ORDER BY toUInt64(hs.SUBS_ID)
        LIMIT 1 BY toUInt64(hs.SUBS_ID)
    ),
    existing_assignment AS (
        SELECT
            ua.subs_id,
            ua.experiment_group,
            ua.is_control,
            ua.assignment_dt,
            ua.first_seen_dt,
            ua.split_hash,
            ua.ukg_pct,
            ua.ukg_salt,
            ua.assignment_version
        FROM data_science.ukg_assignment ua
        WHERE ua.assignment_dt <= data_start -- берем только существующие назначения на дату среза и ранее
        ORDER BY ua.subs_id, ua.assignment_dt DESC, ua.created_at DESC
        LIMIT 1 BY ua.subs_id
    )

SELECT
  hs.SUBS_ID AS SUBS_ID,
  hs.ACTIVE_IND AS ACTIVE_IND,
  hs.STATUS AS STATUS,
  hs.CUST_LEVEL AS CUST_LEVEL,
  hs.REGION AS REGION,
  hs.PERIODICITY AS PERIODICITY,
  hs.FLAG_4G AS FLAG_4G,
  hs.FLAG_ABONKA AS FLAG_ABONKA,
  hs.LIFETIME_TOTAL AS LIFETIME_TOTAL,
  hs.REVENUE_TOTAL_INTERCONNECT AS REVENUE_TOTAL_INTERCONNECT,
  hs.REVENUE_TOTAL AS REVENUE_TOTAL,
  hs.USAGE_INTERNET AS USAGE_INTERNET,
  hs.TRANZ_FLAG AS TRANZ_FLAG,
  hs.BALANCE_USER AS BALANCE_USER,
  hs.MULTIPLAY AS MULTIPLAY,
  hs.MY_BEELINE_USER AS MY_BEELINE_USER,
  hs.FIRST_SIM AS FIRST_SIM,
  hs.DAYS_WITHOUT_PAYMENT AS DAYS_WITHOUT_PAYMENT,
  hs.TOTAL_RECHARGE AS TOTAL_RECHARGE,
  hs.BALANCE_END AS BALANCE_END,
  hs.TOTAL_MOU AS TOTAL_MOU,
    a.experiment_group AS existing_experiment_group,
    a.is_control AS existing_is_control,
    a.assignment_dt AS existing_assignment_dt,
    a.first_seen_dt AS existing_first_seen_dt,
    a.split_hash AS existing_split_hash,
    a.ukg_pct AS existing_ukg_pct,
    a.ukg_salt AS existing_ukg_salt,
    a.assignment_version AS existing_assignment_version
FROM report_slice hs
GLOBAL LEFT JOIN existing_assignment a
  ON a.subs_id = hs.SUBS_ID;
