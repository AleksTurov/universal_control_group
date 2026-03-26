from __future__ import annotations

import numpy as np
import pandas as pd
from src.config import ukg_job_config


def make_zero_aware_fixed_bucket(
    series: pd.Series,
    positive_bins: list[float],
    positive_labels: list[str],
    zero_label: str,
    missing_label: str = "UNKNOWN",
) -> pd.Series:
    '''Создает фиксированные бины для числовой серии: нули отдельно, положительные значения по заданным порогам.'''
    if len(positive_bins) != len(positive_labels) + 1:
        raise ValueError("Число границ positive_bins должно быть на 1 больше числа positive_labels")

    numeric = pd.to_numeric(series, errors="coerce")
    bucket = pd.Series(missing_label, index=series.index, dtype="string")

    # Нули выделяем отдельно, чтобы стабильно отделять zero-поведение.
    zero_mask = numeric.eq(0)
    positive_mask = numeric.gt(0)
    bucket.loc[zero_mask] = zero_label

    # Для положительных значений используем только фиксированные (не monthly) границы.
    if positive_mask.any():
        bucket.loc[positive_mask] = pd.cut(
            numeric.loc[positive_mask],
            bins=positive_bins,
            labels=positive_labels,
            include_lowest=False,
            right=True,
        ).astype("string")

    return bucket.fillna(missing_label)


def add_behavior_buckets(df: pd.DataFrame) -> tuple[pd.DataFrame, str | None]:
    '''Добавляет бакеты для tenure, ARPU и интернет-трафика.'''
    out = df.copy()

    # Бины по жизненному циклу абонента.
    lifetime_num = pd.to_numeric(out["LIFETIME_TOTAL"], errors="coerce")
    out["TENURE_BUCKET"] = pd.cut(
        lifetime_num,
        bins=[-1, 30, 90, 180, 365, 730, 10_000],
        labels=["0-30d", "31-90d", "91-180d", "181-365d", "1-2y", "2y+"],
        include_lowest=True,
    ).astype("string").fillna("UNKNOWN")

    # Для ARPU и трафика используем фиксированные бизнес-бакеты.
    out["ARPU_BUCKET"] = make_zero_aware_fixed_bucket(
        out["REVENUE_TOTAL"],
        positive_bins=ukg_job_config.ARPU_POSITIVE_BINS,
        positive_labels=ukg_job_config.ARPU_POSITIVE_LABELS,
        zero_label="ARPU_ZERO",
    )

    out["TRAFFIC_BUCKET"] = make_zero_aware_fixed_bucket(
        out["USAGE_INTERNET"],
        positive_bins=ukg_job_config.TRAFFIC_POSITIVE_BINS,
        positive_labels=ukg_job_config.TRAFFIC_POSITIVE_LABELS,
        zero_label="TRAFFIC_ZERO",
    )

    return out, "REVENUE_TOTAL"

def merge_existing_and_new_assignments(report_slice_df: pd.DataFrame, new_assignment_df: pd.DataFrame) -> pd.DataFrame:
    '''Объединяет исторические назначения с новыми назначениями текущего месяца.'''
    out = report_slice_df.copy()
    out["experiment_group"] = out.get("existing_experiment_group")
    out["is_control"] = out.get("existing_is_control")
    out["split_hash"] = out.get("existing_split_hash")

    if not new_assignment_df.empty:
        update_columns = ["SUBS_ID", "experiment_group", "is_control", "split_hash", "strata_key"]
        updates = new_assignment_df[update_columns].set_index("SUBS_ID")
        out = out.merge(
            updates,
            left_on="SUBS_ID",
            right_index=True,
            how="left",
            suffixes=("", "_new"),
        )
        out["experiment_group"] = out["experiment_group"].fillna(out["experiment_group_new"])
        out["is_control"] = out["is_control"].fillna(out["is_control_new"])
        out["split_hash"] = out["split_hash"].fillna(out["split_hash_new"])
        out["strata_key"] = out.get("strata_key", out.get("strata_key_new"))
        if "strata_key_new" in out.columns:
            out["strata_key"] = out["strata_key"].fillna(out["strata_key_new"])
        drop_columns = [column for column in out.columns if column.endswith("_new")]
        out = out.drop(columns=drop_columns)

    out["is_control"] = pd.to_numeric(out["is_control"], errors="coerce").fillna(0).astype("uint8")
    return out