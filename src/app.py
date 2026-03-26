from __future__ import annotations

import json
import sys
from datetime import date, datetime
from typing import Any

from src.assignment_analysis import build_default_analyzer
from src.stratified_assignment import StratifiedAssigner

from src.config import path_config, ukg_job_config
from src.database import UKGAssignmentRepository, query_df
from src.logger import logger
from src.ukg_job import (
    add_behavior_buckets,
    merge_existing_and_new_assignments,
)


def _parse_report_dt(raw_value: str) -> date:
    """Поддерживает запуск как с YYYY-MM-DD, так и с YYYY-MM."""
    for date_format in ukg_job_config.CLI_DATE_FORMATS:
        try:
            parsed = datetime.strptime(raw_value, date_format)
            if date_format == "%Y-%m":
                parsed = parsed.replace(day=1)
            return parsed.date()
        except ValueError:
            continue

    raise ValueError(f"Некорректный report_dt: {raw_value}. Ожидается один из форматов: {ukg_job_config.CLI_DATE_FORMATS}")


def run_job(
    report_dt: date,
) -> dict[str, Any]:
    """Запускает полный monthly pipeline для UKG."""
    logger.info("Старт UKG monthly job")
    logger.info(
        "Параметры job: report_dt=%s, ukg_pct=%.5f, ukg_salt=%s, assignment_version=%s, dry_run=%s",
        report_dt,
        ukg_job_config.UKG_PCT,
        ukg_job_config.UKG_SALT,
        ukg_job_config.ASSIGNMENT_VERSION,
        ukg_job_config.DRY_RUN,
    )

    report_slice_df = query_df(path_config.SELECT_SLICE_SQL, DATA_START=report_dt)
    logger.info("Eligible slice: %s строк", len(report_slice_df))

    # Добавляем производные бакеты, которые участвуют в стратификации.
    report_slice_df, monetization_col = add_behavior_buckets(report_slice_df)
    logger.info("Источник monetization для ARPU_BUCKET: %s", monetization_col)

    # В новом assignment участвуют только абоненты без исторического закрепления.
    new_clients_df = report_slice_df[report_slice_df["existing_experiment_group"].isna()].copy()
    logger.info("Новых клиентов для assignment: %s", len(new_clients_df))

    logger.info("Core strata колонки: %s", ", ".join(ukg_job_config.CORE_STRATA_COLUMNS))
    assigner = StratifiedAssigner(salt=ukg_job_config.UKG_SALT)
    new_assignment_df = assigner.assign(
        df=new_clients_df,
        id_col="SUBS_ID",
        strata_cols=ukg_job_config.CORE_STRATA_COLUMNS,
        control_share=ukg_job_config.UKG_PCT,
    )

    logger.info("Назначение control/test для новых клиентов выполнено, контроль: %s%%", ukg_job_config.UKG_PCT * 100)
    repository = UKGAssignmentRepository()
    assignment_models = repository.build_models(
        new_assignment_df=new_assignment_df,
        report_dt=report_dt,
        ukg_pct=ukg_job_config.UKG_PCT,
        ukg_salt=ukg_job_config.UKG_SALT,
        assignment_version=ukg_job_config.ASSIGNMENT_VERSION,
    )
    logger.info("ORM-модели для вставки в ClickHouse подготовлены, строк: %s", len(assignment_models))

    # Собираем финальный срез, где старые назначения сохранены, а новые уже добавлены.
    current_assignment_df = merge_existing_and_new_assignments(report_slice_df, new_assignment_df)
    logger.info("Текущий assignment slice собран: %s строк", len(current_assignment_df))

    if ukg_job_config.DRY_RUN:
        logger.info("Dry-run режим: загрузка в ClickHouse пропущена")
    else:
        inserted_rows = repository.insert_rows(assignment_models)
        logger.info("В ClickHouse загружено новых строк: %s", inserted_rows)

    logger.info("KS колонки: %s", ", ".join(ukg_job_config.KS_COLUMNS))
    analyzer = build_default_analyzer(
        report_dt=report_dt,
        srm_alpha=ukg_job_config.SRM_ALPHA,
        ks_alpha=ukg_job_config.KS_ALPHA,
    )
    analysis_result = analyzer.analyze(
        current_assignment_df=current_assignment_df,
        control_share=ukg_job_config.UKG_PCT,
        ks_columns=ukg_job_config.KS_COLUMNS,
        strata_cols=ukg_job_config.CORE_STRATA_COLUMNS,
        split_version=ukg_job_config.DEFAULT_SPLIT_VERSION,
        monetization_col=monetization_col,
        insert_rows=len(assignment_models),
    )
    logger.info("Run summary: %s", json.dumps(analysis_result["summary"], ensure_ascii=False))
    logger.info("Artifacts saved to: %s", analyzer.output_dir)

    logger.info("UKG monthly job завершен успешно")
    return {
        "status": analysis_result["status"],
        "report_dt": report_dt,
        "summary": analysis_result["summary"],
        "checks": analysis_result["checks"],
        "validations": analysis_result["validations"],
        "artifacts": analysis_result["artifacts"],
        "insert_rows": int(len(assignment_models)),
        "dry_run": bool(ukg_job_config.DRY_RUN),
    }


if __name__ == '__main__':
    if len(sys.argv) > 1:
        result = run_job(_parse_report_dt(sys.argv[1]))
        print(json.dumps(result, ensure_ascii=False, default=str))
    else:
        logger.error("Передайте report_dt в формате YYYY-MM-DD или YYYY-MM")
        raise SystemExit(1)