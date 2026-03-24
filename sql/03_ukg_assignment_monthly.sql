/*
UKG monthly assignment script (ClickHouse)

What this script does:
1) Creates persistent UKG assignment tables (local + distributed).
2) Adds ONLY new eligible subscribers each month.
3) Keeps historical assignment fixed for previously assigned subscribers.

Required parameters:
- :REPORT_DT (YYYY-MM-DD)
- :UKG_PCT (example 0.10)
- :UKG_SALT (fixed string, do not change without migration)
- :ASSIGNMENT_VERSION (integer, bump when assignment logic changes)
*/

/* -------------------------------------------------------------------------- */
/* 1) DDL: persistent assignment storage                                       */
/* -------------------------------------------------------------------------- */

CREATE TABLE IF NOT EXISTS DWH.ukg_assignment_src
ON CLUSTER edwh
(
    subs_id UInt64,
    first_seen_dt Date,
    assignment_dt Date,
    experiment_group LowCardinality(String),   -- control | test
    is_control UInt8,                           -- 1 control, 0 test
    split_hash UInt64,
    ukg_pct Float64,
    ukg_salt String,
    assignment_version UInt32,
    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(first_seen_dt)
ORDER BY (subs_id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS DWH.ukg_assignment
ON CLUSTER edwh
AS DWH.ukg_assignment_src
ENGINE = Distributed(
    edwh,
    DWH,
    ukg_assignment_src,
    murmurHash3_64(subs_id)
);

/* Optional current snapshot view (one row per subs_id by design). */
CREATE VIEW IF NOT EXISTS DWH.v_ukg_assignment_current AS
SELECT
    subs_id,
    experiment_group,
    is_control,
    assignment_dt,
    first_seen_dt,
    split_hash,
    ukg_pct,
    ukg_salt,
    assignment_version,
    created_at
FROM DWH.ukg_assignment;

/* Final business-facing view with readable group labels. */
CREATE VIEW IF NOT EXISTS DWH.v_ukg_assignment_final AS
SELECT
    subs_id,
    experiment_group,
    multiIf(
        experiment_group = 'control', 'UCG',
        experiment_group = 'test', 'TARGET',
        'UNKNOWN_GROUP'
    ) AS group_short_name,
    multiIf(
        experiment_group = 'control', 'UNIVERSAL_CONTROL_GROUP',
        experiment_group = 'test', 'TARGETABLE_BASE',
        'UNKNOWN_GROUP'
    ) AS experiment_group_name,
    multiIf(
        experiment_group = 'control', 'Универсальная контрольная группа',
        experiment_group = 'test', 'Таргетируемая база',
        'Неизвестная группа'
    ) AS experiment_group_name_ru,
    multiIf(
        experiment_group = 'control', 'Абонент закреплен в универсальной контрольной группе и должен исключаться из всех целевых коммуникаций, где измеряется инкрементальный эффект кампании.',
        experiment_group = 'test', 'Абонент не входит в универсальную контрольную группу и может участвовать в целевых коммуникациях по правилам конкретной кампании.',
        'Группа не распознана, требуется проверка assignment logic.'
    ) AS experiment_group_description,
    is_control,
    assignment_dt,
    first_seen_dt,
    split_hash,
    ukg_pct,
    ukg_salt,
    assignment_version,
    created_at
FROM DWH.v_ukg_assignment_current;

/* -------------------------------------------------------------------------- */
/* 2) Monthly incremental INSERT (ONLY new clients)                            */
/* -------------------------------------------------------------------------- */

INSERT INTO DWH.ukg_assignment
(
    subs_id,
    first_seen_dt,
    assignment_dt,
    experiment_group,
    is_control,
    split_hash,
    ukg_pct,
    ukg_salt,
    assignment_version,
    created_at
)
WITH
    toDate(:REPORT_DT) AS report_dt,
    toFloat64(:UKG_PCT) AS ukg_pct,
    toString(:UKG_SALT) AS ukg_salt,
    toUInt32(:ASSIGNMENT_VERSION) AS assignment_version,

    eligible AS
    (
        /*
        Replace/extend exclusions below with your final business rules.
        Keeping this block explicit makes monthly logic transparent.
        */
        SELECT DISTINCT toUInt64(m.SUBS_ID) AS subs_id
        FROM DWH.dm_datamart_monthly m
        WHERE m.DT = report_dt
          AND m.CUST_LEVEL NOT IN ('employee', 'TEST')
    ),

    new_clients AS
    (
        SELECT e.subs_id
        FROM eligible e
        LEFT JOIN DWH.ukg_assignment a
            ON a.subs_id = e.subs_id
        WHERE a.subs_id IS NULL
    ),

    ranked AS
    (
        SELECT
            n.subs_id,
            murmurHash3_64(concat(toString(n.subs_id), '|', ukg_salt)) AS split_hash,
            row_number() OVER (
                ORDER BY murmurHash3_64(concat(toString(n.subs_id), '|', ukg_salt)), n.subs_id
            ) AS rn,
            count() OVER () AS new_total,
            toUInt64(round(count() OVER () * ukg_pct)) AS control_target
        FROM new_clients n
    )
SELECT
    r.subs_id,
    report_dt AS first_seen_dt,
    report_dt AS assignment_dt,
    if(r.rn <= r.control_target, 'control', 'test') AS experiment_group,
    if(r.rn <= r.control_target, toUInt8(1), toUInt8(0)) AS is_control,
    r.split_hash,
    ukg_pct,
    ukg_salt,
    assignment_version,
    now() AS created_at
FROM ranked r;

/* -------------------------------------------------------------------------- */
/* 3) Validation queries after each monthly run                                */
/* -------------------------------------------------------------------------- */

/* A) How many were added this month? */
SELECT
    assignment_dt,
    count() AS inserted_rows,
    round(avg(is_control), 6) AS control_share_inserted
FROM DWH.ukg_assignment
WHERE assignment_dt = toDate(:REPORT_DT)
GROUP BY assignment_dt;

/* B) Global control share in full assignment table. */
SELECT
    count() AS total_assigned,
    sum(is_control) AS control_cnt,
    round(avg(is_control), 6) AS control_share_global
FROM DWH.ukg_assignment;

/* B2) Business summary by final group labels. */
SELECT
    group_short_name,
    experiment_group_name,
    experiment_group_name_ru,
    experiment_group_description,
    count() AS subs_cnt,
    round(count() / sum(count()) OVER (), 6) AS subs_share
FROM DWH.v_ukg_assignment_final
GROUP BY
    group_short_name,
    experiment_group_name,
    experiment_group_name_ru,
    experiment_group_description
ORDER BY subs_cnt DESC;

/* C) Duplicate safety check (must be zero rows). */
SELECT
    subs_id,
    count() AS cnt
FROM DWH.ukg_assignment
GROUP BY subs_id
HAVING cnt > 1
LIMIT 100;
