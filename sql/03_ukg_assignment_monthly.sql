/*
Инициализация объектов UKG в ClickHouse.

Назначение файла:
1) создать persistent-таблицы assignment на всем кластере;
2) хранить итоговые названия и описания групп прямо в таблице.

Важно:
- в этом файле нет логики split;
- hash, стратификация, SRM и KS считаются в Python job src/app.py;
- файл выполняется из src/database.py при старте monthly job.

Параметры форматирования:
- edwh       имя ClickHouse-кластера;
- data_science имя базы, например DWH.
*/

CREATE TABLE IF NOT EXISTS data_science.ukg_assignment_src
ON CLUSTER edwh
(
    subs_id UInt64 COMMENT 'Идентификатор абонента (уникальный ключ закрепления в UKG).',
    first_seen_dt Date COMMENT 'Дата первого появления абонента в eligible-срезе.',
    assignment_dt Date COMMENT 'Дата назначения абонента в control/test.',
    experiment_group LowCardinality(String) COMMENT 'Базовая группа эксперимента: control | test.',
    group_short_name LowCardinality(String) MATERIALIZED multiIf(
        experiment_group = 'control', 'UCG',
        experiment_group = 'test', 'TARGET',
        'UNKNOWN_GROUP'
    ) COMMENT 'Короткое бизнес-название группы.',
    experiment_group_name LowCardinality(String) MATERIALIZED multiIf(
        experiment_group = 'control', 'UNIVERSAL_CONTROL_GROUP',
        experiment_group = 'test', 'TARGETABLE_BASE',
        'UNKNOWN_GROUP'
    ) COMMENT 'Техническое бизнес-название группы.',
    experiment_group_name_ru String MATERIALIZED multiIf(
        experiment_group = 'control', 'Универсальная контрольная группа',
        experiment_group = 'test', 'Таргетируемая база',
        'Неизвестная группа'
    ) COMMENT 'Русское читаемое название группы.',
    experiment_group_description String MATERIALIZED multiIf(
        experiment_group = 'control', 'Абонент закреплен в универсальной контрольной группе и должен исключаться из всех целевых коммуникаций, где измеряется инкрементальный эффект кампании.',
        experiment_group = 'test', 'Абонент не входит в универсальную контрольную группу и может участвовать в целевых коммуникациях по правилам конкретной кампании.',
        'Группа не распознана, требуется проверка assignment logic.'
    ) COMMENT 'Бизнес-описание назначения группы.',
    is_control UInt8 COMMENT 'Флаг контрольной группы: 1 = control, 0 = test.',
    split_hash UInt64 COMMENT 'Стабильный hash от subs_id и соли, рассчитанный в Python.',
    ukg_pct Float64 COMMENT 'Доля control, использованная в конкретном запуске.',
    ukg_salt String COMMENT 'Соль hash-функции для детерминированного split.',
    assignment_version UInt32 COMMENT 'Версия логики назначения.',
    created_at DateTime DEFAULT now() COMMENT 'Время вставки строки в таблицу assignment.'
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(first_seen_dt)
ORDER BY (subs_id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS data_science.ukg_assignment
ON CLUSTER edwh
AS data_science.ukg_assignment_src
ENGINE = Distributed(
    edwh,
    data_science,
    ukg_assignment_src,
    murmurHash3_64(subs_id)
);
