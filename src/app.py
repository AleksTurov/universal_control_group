from __future__ import annotations

import json
import os
import sys
from datetime import datetime

from src.config import path_config, ukg_job_config
from src.database import ensure_ukg_tables, insert_dataframe, query_df, query_multiple_dataframes
from src.logger import logger
from src.ukg_job import (
    add_behavior_buckets,
    assign_control_test_with_strata,
    build_insert_frame,
    build_run_summary,
    calculate_srm,
    get_available_ks_columns,
    get_available_strata_columns,
    merge_existing_and_new_assignments,
    normalize_report_slice,
    run_ks_checks,
) 


def _warn_missing_columns(report_slice_df) -> list[str]:
    missing_required = [column for column in ukg_job_config.REQUIRED_SLICE_COLUMNS if column not in report_slice_df.columns]
    if missing_required:
        logger.warning("В срезе отсутствуют обязательные колонки: %s", ", ".join(missing_required))

    missing_core = [column for column in ukg_job_config.CORE_STRATA_COLUMNS if column not in report_slice_df.columns]
    if missing_core:
        logger.warning("Часть core-strata колонок отсутствует в срезе: %s", ", ".join(missing_core))

    missing_ks = [column for column in ukg_job_config.KS_COLUMNS if column not in report_slice_df.columns]
    if missing_ks:
        logger.warning("Часть KS-колонок отсутствует в срезе: %s", ", ".join(missing_ks))

    return missing_required


def _warn_failed_checks(srm: dict, ks_report) -> dict:
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


def _normalize_report_dt(raw_value: str) -> str:
    for fmt in ukg_job_config.cli_date_formats:
        try:
            parsed = datetime.strptime(raw_value, fmt)
            return parsed.replace(day=1).strftime("%Y-%m-%d")
        except ValueError:
            continue
    raise ValueError(
        "report_dt должен быть в формате YYYY-MM или YYYY-MM-DD"
    )


def _resolve_report_dt(argv: list[str] | None = None) -> str:
    args = argv if argv is not None else sys.argv[1:]
    raw_value = args[0] if args else os.getenv(ukg_job_config.report_dt_env_var, "")
    if not raw_value:
        raise ValueError(
            f"Передайте report_dt первым аргументом или через переменную окружения {ukg_job_config.report_dt_env_var}"
        )
    return _normalize_report_dt(raw_value)


def run_job(
    report_dt: str,
) -> dict:
    logger.info("Старт UKG monthly job")
    logger.info(
        "Параметры job: report_dt=%s, ukg_pct=%.5f, ukg_salt=%s, assignment_version=%s, dry_run=%s",
        report_dt,
        ukg_job_config.ukg_pct,
        ukg_job_config.ukg_salt,
        ukg_job_config.assignment_version,
        ukg_job_config.dry_run,
    )

    ensure_ukg_tables()
    logger.info("DDL проверен")

    report_slice_df = query_df(path_config.SELECT_SLICE_SQL, DATA_START=report_dt)
    report_slice_df = normalize_report_slice(report_slice_df)
    logger.info("Eligible slice: %s строк", len(report_slice_df))

    if report_slice_df.empty:
        logger.warning("Eligible slice пуст, job завершен")
        return {"status": "skipped", "reason": "empty_slice", "report_dt": report_dt}

    missing_required = _warn_missing_columns(report_slice_df)
    if missing_required:
        return {
            "status": "failed",
            "reason": "missing_required_columns",
            "missing_required_columns": missing_required,
            "report_dt": report_dt,
        }

    report_slice_df, monetization_col = add_behavior_buckets(report_slice_df)
    strata_cols = get_available_strata_columns(report_slice_df)
    ks_cols = get_available_ks_columns(report_slice_df)

    if not strata_cols:
        logger.warning("Не найдено ни одной колонки для stratification: split будет без strata")
    if not ks_cols:
        logger.warning("Не найдено валидных числовых колонок для KS-проверки")

    logger.info("Источник monetization для ARPU_BUCKET: %s", monetization_col)
    logger.info("Core strata колонки: %s", ", ".join(strata_cols))
    logger.info("KS колонки: %s", ", ".join(ks_cols))

    new_clients_df = report_slice_df[report_slice_df["existing_experiment_group"].isna()].copy()
    logger.info("Новых клиентов для assignment: %s", len(new_clients_df))

    new_assignment_df = assign_control_test_with_strata(
        df=new_clients_df,
        id_col="SUBS_ID",
        strata_cols=strata_cols,
        control_share=ukg_job_config.ukg_pct,
        salt=ukg_job_config.ukg_salt,
    )

    insert_df = build_insert_frame(
        new_assignment_df=new_assignment_df,
        report_dt=report_dt,
        ukg_pct=ukg_job_config.ukg_pct,
        ukg_salt=ukg_job_config.ukg_salt,
        assignment_version=ukg_job_config.assignment_version,
    )

    current_assignment_df = merge_existing_and_new_assignments(report_slice_df, new_assignment_df)
    srm = calculate_srm(current_assignment_df["experiment_group"], control_share=ukg_job_config.ukg_pct)
    ks_report = run_ks_checks(current_assignment_df, group_column="experiment_group", ks_columns=ks_cols)
    check_status = _warn_failed_checks(srm=srm, ks_report=ks_report)

    summary = build_run_summary(
        report_dt=report_dt,
        split_version=ukg_job_config.DEFAULT_SPLIT_VERSION,
        monetization_col=monetization_col,
        insert_df=insert_df,
        current_assignment_df=current_assignment_df,
        srm=srm,
        ks_report=ks_report,
        strata_cols=strata_cols,
    )
    logger.info("Run summary: %s", json.dumps(summary, ensure_ascii=False))

    if ukg_job_config.dry_run:
        logger.info("Dry-run режим: загрузка в ClickHouse пропущена")
    else:
        insert_dataframe(ukg_job_config.target_table, insert_df)
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
        "dry_run": bool(ukg_job_config.dry_run),
    }


if __name__ == "__main__":
    try:
        report_dt = _resolve_report_dt()
        result = run_job(report_dt=report_dt)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        raise SystemExit(1 if result.get("status") == "failed" else 0)
    except Exception as exc:
        logger.exception("UKG monthly job завершился с ошибкой: %s", exc)
        raise SystemExit(1)
    