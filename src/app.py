from __future__ import annotations

import json
import os
import sys
from datetime import datetime
from turtle import pd

from stratified_assignment import StratifiedAssigner

from src.config import path_config, ukg_job_config
from src.database import ensure_ukg_tables, insert_dataframe, query_df, query_multiple_dataframes
from src.logger import logger
from src.ukg_job import (
    add_behavior_buckets,
    assign_control_test_with_strata,
    build_insert_frame,
    build_run_summary,
    calculate_srm,

    merge_existing_and_new_assignments,
    run_ks_checks,
) 
assigner = StratifiedAssigner(salt=ukg_job_config.UKG_SALT)




def _warn_failed_checks(srm: dict, ks_report) -> dict:
    '''
    Проверяет результаты SRM и KS, логирует предупреждения, возвращает словарь статусов проверок
    '''
    srm_p_value = float(srm.get("p_value", 1.0) or 1.0)
    srm_failed = srm_p_value < ukg_job_config.srm_alpha
    if srm_failed:
        logger.warning("SRM check не пройден: p_value=%.6f (< %.3f)", srm_p_value, ukg_job_config.srm_alpha)

    ks_failed_features: list[str] = []
    if not ks_report.empty and "ks_significant_0_05" in ks_report.columns:
        ks_failed_features = ks_report.loc[ks_report["ks_significant_0_05"], "feature"].astype(str).tolist()
        if ks_failed_features:
            logger.warning("KS check не пройден по фичам (alpha=%.3f): %s", ukg_job_config.ks_alpha, ", ".join(ks_failed_features))

    return {
        "srm_failed": srm_failed,
        "srm_p_value": srm_p_value,
        "ks_failed_features": ks_failed_features,
    }

def run_job(
    report_dt: datetime.date,
) -> dict:
    logger.info("Старт UKG monthly job")
    logger.info(
        "Параметры job: report_dt=%s, ukg_pct=%.5f, ukg_salt=%s, assignment_version=%s, dry_run=%s",
        report_dt,
        ukg_job_config.UKG_PCT,
        ukg_job_config.UKG_SALT,
        ukg_job_config.ASSIGNMENT_VERSION,
        ukg_job_config.DRY_RUN,
    )

    ensure_ukg_tables()
    logger.info("DDL проверен")

    report_slice_df = query_df(path_config.SELECT_SLICE_SQL, DATA_START=report_dt)
    logger.info("Eligible slice: %s строк", len(report_slice_df))

    # Добавляем поведенческие бакеты и определяем источник монетизации для стратификации. Логируем количество строк после добавления бакетов и выбранный источник монетизации.
    report_slice_df, monetization_col = add_behavior_buckets(report_slice_df)
    logger.info("Источник monetization для ARPU_BUCKET: %s", monetization_col)

    # Выбираем только новых клиентов для назначения control/test, логируем их количество
    new_clients_df = report_slice_df[report_slice_df["existing_experiment_group"].isna()].copy()
    logger.info("Новых клиентов для assignment: %s", len(new_clients_df))
    

    logger.info("Core strata колонки: %s", ", ".join(ukg_job_config.CORE_STRATA_COLUMNS))
    # Распределяем новых клиентов между control и test с учетом страт. Логируем долю клиентов, попавших в control, и первые 5 строк назначенных клиентов для проверки.
    assigner = StratifiedAssigner(salt=ukg_job_config.UKG_SALT)
    new_assignment_df = assigner.assign(
        df=new_clients_df,
        id_col="SUBS_ID",
        strata_cols=ukg_job_config.CORE_STRATA_COLUMNS,
        control_share=ukg_job_config.UKG_PCT,
)

    logger.info("Назначение control/test для новых клиентов выполнено, контроль: %s%%", ukg_job_config.UKG_PCT * 100)
    insert_df = build_insert_frame(
        new_assignment_df=new_assignment_df,
        report_dt=report_dt,
        ukg_pct=ukg_job_config.UKG_PCT,
        ukg_salt=ukg_job_config.UKG_SALT,
        assignment_version=ukg_job_config.ASSIGNMENT_VERSION,
    )
    logger.info("DataFrame для вставки в ClickHouse подготовлен, строк для вставки: %s", len(insert_df))

    current_assignment_df = merge_existing_and_new_assignments(report_slice_df, new_assignment_df)
    srm = calculate_srm(current_assignment_df["experiment_group"], control_share=ukg_job_config.UKG_PCT)
    logger.info("KS колонки: %s", ", ".join(ukg_job_config.KS_COLUMNS))
    ks_report = run_ks_checks(current_assignment_df, group_column="experiment_group", ks_columns=ukg_job_config.KS_COLUMNS)
    check_status = _warn_failed_checks(srm=srm, ks_report=ks_report)

    summary = build_run_summary(
        report_dt=report_dt,
        split_version=ukg_job_config.DEFAULT_SPLIT_VERSION,
        monetization_col=monetization_col,
        insert_df=insert_df,
        current_assignment_df=current_assignment_df,
        srm=srm,
        ks_report=ks_report,
        strata_cols=ukg_job_config.CORE_STRATA_COLUMNS,
    )
    logger.info("Run summary: %s", json.dumps(summary, ensure_ascii=False))

    if ukg_job_config.DRY_RUN:
        logger.info("Dry-run режим: загрузка в ClickHouse пропущена")
    else:
        insert_dataframe(ukg_job_config.TARGET_TABLE, insert_df)
        logger.info("В ClickHouse загружено новых строк: %s", len(insert_df))

    validation_frames = query_multiple_dataframes(path_config.VALIDATION_SQL, report_dt=report_dt)
    validations: dict[str, list[dict]] = {}
    for name, dataframe in zip(path_config.VALIDATION_NAMES, validation_frames):
        records = dataframe.to_dict(orient="records")
        validations[name] = records
        logger.info("Validation %s: %s", name, records)

    duplicate_records = validations.get("duplicate_check", [])
    if duplicate_records:
        logger.warning("Найдены дубли в assignment по subs_id: %s", len(duplicate_records))

    logger.info("UKG monthly job завершен успешно")
    return {
        "status": "warning" if (check_status["srm_failed"] or check_status["ks_failed_features"] or duplicate_records) else "ok",
        "report_dt": report_dt,
        "summary": summary,
        "checks": check_status,
        "validations": validations,
        "insert_rows": int(len(insert_df)),
        "dry_run": bool(ukg_job_config.DRY_RUN),
    }


# %%    
if __name__ == '__main__':
    if len(sys.argv) > 1:
        settlement_date = datetime.datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
        run_job(settlement_date)
    else:
        logger.error("Please provide a settlement date in the format YYYY-MM-DD as a command-line argument.")