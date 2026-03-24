from __future__ import annotations

import hashlib
from typing import Any

import numpy as np
import pandas as pd
from scipy import stats
from src.logger import logger
from src.config import ukg_job_config



def normalize_report_slice(df: pd.DataFrame) -> pd.DataFrame:
    '''Приводит колонки датафрейма к нужным типам и форматам для дальнейшей обработки.'''
    out = df.copy()
    out.columns = [str(column) for column in out.columns]
    if "SUBS_ID" in out.columns:
        out["SUBS_ID"] = pd.to_numeric(out["SUBS_ID"], errors="coerce").astype("Int64")
    return out


def benjamini_hochberg(p_values: pd.Series | list[float]) -> pd.Series:
    '''Проводит коррекцию p-value по методу Бенджамини-Хохберга для контроля FDR при множественных проверках.
    Принимает серию или список p-value и возвращает серию q-value (скорректированных p-value) в том же порядке.
    '''
    p_series = pd.Series(p_values, dtype="float64")
    valid = p_series.dropna()
    result = pd.Series(np.nan, index=p_series.index, dtype="float64")
    if valid.empty:
        return result

    order = np.argsort(valid.to_numpy())
    ranked_index = valid.index[order]
    ranked_values = valid.loc[ranked_index].to_numpy()
    size = len(ranked_values)
    q_values = ranked_values * size / np.arange(1, size + 1)
    q_values = np.minimum.accumulate(q_values[::-1])[::-1]
    q_values = np.clip(q_values, 0, 1)
    result.loc[ranked_index] = q_values
    return result


def calculate_srm(group_series: pd.Series, control_share: float) -> dict[str, Any]:
    '''Проводит SRM-проверку для серии с группами control/test и возвращает словарь с результатами.'''
    counts = group_series.value_counts()
    observed = np.array([counts.get("control", 0), counts.get("test", 0)], dtype="float64")
    total = observed.sum()
    if total == 0:
        return {
            "control_count": 0,
            "test_count": 0,
            "control_share": 0.0,
            "test_share": 0.0,
            "chi2": np.nan,
            "p_value": np.nan,
        }

    expected = np.array([control_share, 1 - control_share], dtype="float64") * total
    chi2 = ((observed - expected) ** 2 / expected).sum()
    p_value = stats.chi2.sf(chi2, df=1)
    return {
        "control_count": int(observed[0]),
        "test_count": int(observed[1]),
        "control_share": float(observed[0] / total),
        "test_share": float(observed[1] / total),
        "chi2": float(chi2),
        "p_value": float(p_value),
    }


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
    '''Добавляет к датафрейму колонки с бинами для поведения абонентов, которые используются в стратификации.'''
    out = df.copy()
    # Если есть колонка с жизненным циклом, то используем ее для выделения бинов по времени с момента первого появления абонента в срезе.
    lifetime_num = pd.to_numeric(out["LIFETIME_TOTAL"], errors="coerce")
    out["TENURE_BUCKET"] = pd.cut(
        lifetime_num,
        bins=[-1, 30, 90, 180, 365, 730, 10_000],
        labels=["0-30d", "31-90d", "91-180d", "181-365d", "1-2y", "2y+"],
        include_lowest=True,
    ).astype("string").fillna("UNKNOWN")

    # Выбираем источник монетизации для стратификации по ARPU. Если есть данные по interconnect, то используем их, 
    # иначе берем общий revenue.
    monetization_col = next((column for column in ukg_job_config.MONETIZATION_SOURCE_PRIORITY if column in out.columns), None)
    if monetization_col is not None:
        out["ARPU_BUCKET"] = make_zero_aware_fixed_bucket(
            out[monetization_col],
            positive_bins=ukg_job_config.ARPU_POSITIVE_BINS,
            positive_labels=ukg_job_config.ARPU_POSITIVE_LABELS,
            zero_label="ARPU_ZERO",
        )
    else:
        out["ARPU_BUCKET"] = "UNKNOWN"

    if "USAGE_INTERNET" in out.columns:
        out["TRAFFIC_BUCKET"] = make_zero_aware_fixed_bucket(
            out["USAGE_INTERNET"],
            positive_bins=ukg_job_config.TRAFFIC_POSITIVE_BINS,
            positive_labels=ukg_job_config.TRAFFIC_POSITIVE_LABELS,
            zero_label="TRAFFIC_ZERO",
        )
    else:
        out["TRAFFIC_BUCKET"] = "UNKNOWN"

    return out, monetization_col


def build_strata_key(df: pd.DataFrame, columns: list[str]) -> pd.Series:
    '''
    Создает ключ страты для датафрейма на основе указанных колонок.
    Если список колонок пуст, возвращает ключ "ALL" для всех строк.
    '''
    if not columns:
        return pd.Series("ALL", index=df.index, dtype="string")
    prepared = df[columns].copy()
    for column in columns:
        prepared[column] = prepared[column].astype("string").fillna("MISSING")
    return prepared.agg("|".join, axis=1).astype("string")


def stable_uint64_hash(values: pd.Series, salt: str) -> pd.Series:
    '''
    Создает стабильный uint64-хеш для серии значений с учетом соли. 
    Используем blake2b с digest_size=8 для получения 64-битного хеша.
    '''
    encoded_values = values.astype("string").fillna("MISSING")
    hashed = [
        int.from_bytes(
            hashlib.blake2b(f"{value}|{salt}".encode("utf-8"), digest_size=8).digest(),
            byteorder="big",
            signed=False,
        )
        for value in encoded_values
    ]
    return pd.Series(hashed, index=values.index, dtype="uint64")


def assign_control_test_with_strata(
    df: pd.DataFrame,
    id_col: str,
    strata_cols: list[str],
    control_share: float,
    salt: str,
) -> pd.DataFrame:
    ''' 
    Проводит стратифицированное рандомизированное распределение между контрольной 
    и тестовой группой с фиксированным share для контроля.
     - df: датафрейм с данными для распределения, должен содержать id_col и strata_cols.
     - id_col: имя колонки с уникальным идентификатором для распределения (например, SUBS_ID).
     - strata_cols: список колонок для стратификации. Чем больше колонок, тем более однородные страты, но меньше данных в каждой страте.
     - control_share: доля контрольной группы (например, 0.1 для 10% в контроле).
     - salt: строка для соления хеша, чтобы обеспечить стабильное распределение между запусками при одинаковых данных.
    Возвращает датафрейм с добавленными колонками: split_hash (стабильный хеш для распределения), strata_key (ключ страты) и experiment_group (control/test).
    '''
    out = df.copy()
    # Если данных для распределения нет, то возвращаем пустой датафрейм с нужными колонками, чтобы не ломать логику дальше по коду.
    if out.empty:
        logger.warning("Нет данных для распределения, возвращаем пустой датафрейм с нужными колонками")
        out["split_hash"] = pd.Series(dtype="uint64")
        out["strata_key"] = pd.Series(dtype="string")
        out["experiment_group"] = pd.Series(dtype="string")
        out["is_control"] = pd.Series(dtype="uint8")
        return out

    out["split_hash"] = stable_uint64_hash(out[id_col], salt)
    out["strata_key"] = build_strata_key(out, strata_cols)

    target_control = int(round(len(out) * control_share))
    alloc = out.groupby("strata_key").size().rename("n").reset_index()
    alloc["target_float"] = alloc["n"] * control_share
    alloc["target_floor"] = np.floor(alloc["target_float"]).astype(int)
    alloc["fractional"] = alloc["target_float"] - alloc["target_floor"]
    alloc["k"] = alloc["target_floor"]

    remaining = target_control - int(alloc["target_floor"].sum())
    if remaining > 0:
        top_strata = alloc.sort_values(
            ["fractional", "n", "strata_key"],
            ascending=[False, False, True],
            kind="mergesort",
        ).head(remaining)["strata_key"]
        alloc.loc[alloc["strata_key"].isin(top_strata), "k"] += 1

    out = out.merge(alloc[["strata_key", "k"]], on="strata_key", how="left")
    out = out.sort_values(["strata_key", "split_hash", id_col], kind="mergesort").copy()
    out["_rank"] = out.groupby("strata_key").cumcount() + 1
    out["experiment_group"] = np.where(out["_rank"] <= out["k"], "control", "test")
    out["is_control"] = out["experiment_group"].eq("control").astype("uint8")
    return out.drop(columns=["_rank", "k"])


def get_available_strata_columns(df: pd.DataFrame) -> list[str]:
    '''Возвращает список колонок для стратификации, которые есть в датафрейме, в порядке приоритета CORE_STRATA_COLUMNS.'''
    return [column for column in ukg_job_config.CORE_STRATA_COLUMNS if column in df.columns]


def get_available_ks_columns(df: pd.DataFrame) -> list[str]:
    '''Возвращает список колонок для KS-проверок, которые есть в датафрейме и являются числовыми, в порядке приоритета KS_COLUMNS.'''
    return [
        column
        for column in ukg_job_config.KS_COLUMNS
        if column in df.columns and pd.api.types.is_numeric_dtype(df[column])
    ]


def merge_existing_and_new_assignments(report_slice_df: pd.DataFrame, new_assignment_df: pd.DataFrame) -> pd.DataFrame:
    '''
    Объединяет существующие назначения (если абонент уже был в срезе в прошлом) с новыми назначениями, сохраняя стабильность для существующих абонентов и добавляя новых.
     - report_slice_df: датафрейм с данными для текущего месяца, который содержит колонки existing_experiment_group, existing_is_control, existing_split_hash для абонентов, которые уже были в срезе в прошлом.
     - new_assignment_df: датафрейм с новыми назначениями для абонентов, которые впервые появились в срезе.
     Возвращает объединенный датафрейм с колонкой experiment_group, которая содержит финальное назначение (control/test) для всех абонентов в текущем месяце, а также is_control и split_hash.
     '''
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


def build_insert_frame(
    new_assignment_df: pd.DataFrame,
    report_dt: str,
    ukg_pct: float,
    ukg_salt: str,
    assignment_version: int,
) -> pd.DataFrame:
    '''
    Строит датафрейм для вставки в таблицу ukg_assignment на основе новых назначений.    
        - new_assignment_df: датафрейм с новыми назначениями для абонентов, которые впервые появились в срезе в этом месяце. Должен содержать колонки SUBS_ID, experiment_group, is_control, split_hash.
        - report_dt: дата отчета для назначения (обычно первый день месяца).
        - ukg_pct: доля контрольной группы, которая сохраняется в колонке для вставки для удобства анализа.
        - ukg_salt: соль для хеша, которая сохраняется в колонке для вставки для удобства анализа.
        - assignment_version: версия логики назначения, которая сохраняется в колонке для вставки для удобства анализа.
    Возвращает датафрейм с колонками subs_id, first_seen_dt, assignment_dt, 
    experiment_group, is_control, split_hash, ukg_pct, ukg_salt, assignment_version, created_at, который готов для вставки в таблицу ukg_assignment.
    
    '''
    if new_assignment_df.empty:
        return pd.DataFrame(
            columns=[
                "subs_id",
                "first_seen_dt",
                "assignment_dt",
                "experiment_group",
                "is_control",
                "split_hash",
                "ukg_pct",
                "ukg_salt",
                "assignment_version",
                "created_at",
            ]
        )

    insert_df = pd.DataFrame(
        {
            "subs_id": pd.to_numeric(new_assignment_df["SUBS_ID"], errors="coerce").astype("uint64"),
            "first_seen_dt": report_dt,
            "assignment_dt": report_dt,
            "experiment_group": new_assignment_df["experiment_group"].astype("string"),
            "is_control": new_assignment_df["is_control"].astype("uint8"),
            "split_hash": new_assignment_df["split_hash"].astype("uint64"),
            "ukg_pct": float(ukg_pct),
            "ukg_salt": str(ukg_salt),
            "assignment_version": int(assignment_version),
            "created_at": pd.Timestamp.utcnow().tz_localize(None),
        }
    )
    return insert_df


def run_ks_checks(
    assignment_df: pd.DataFrame,
    group_column: str,
    ks_columns: list[str],
    sample_per_group: int = 100_000,
) -> pd.DataFrame:
    '''
    Проводит KS-проверки для указанных колонок между контрольной и тестовой группой.
     - assignment_df: датафрейм с назначениями, должен содержать колонку group_column для разделения на контроль и тест, а также колонки из ks_columns для проверки.
     - group_column: имя колонки, которая содержит информацию о группе (например, "experiment_group" с значениями "control"/"test").
     - ks_columns: список имен колонок, для которых нужно провести KS-проверки. Эти колонки должны быть числовыми.
     - sample_per_group: максимальное количество случайных наблюдений из каждой группы для проведения KS-проверки, чтобы ускорить расчет на больших данных. Если в группе меньше наблюдений, используются все наблюдения.
    Возвращает датафрейм с результатами KS-проверок, который содержит колонки feature (имя проверяемой колонки), ks_stat (статистика
    KS), p_value (p-value теста), control_mean, test_mean, control_median, test_median, control_n, test_n, fdr_q_value (скорректированный p-value по методу Бенджамини-Хохберга) 
    и ks_significant_0_05 (булева колонка, которая True, если результат значим при уровне 0.05 после коррекции FDR).'''
    ks_results: list[dict[str, Any]] = []
    control_mask = assignment_df[group_column].eq("control")
    test_mask = assignment_df[group_column].eq("test")

    for column in ks_columns:
        control_values = pd.to_numeric(assignment_df.loc[control_mask, column], errors="coerce").dropna()
        test_values = pd.to_numeric(assignment_df.loc[test_mask, column], errors="coerce").dropna()
        if control_values.empty or test_values.empty:
            continue

        control_sample = control_values.sample(min(sample_per_group, len(control_values)), random_state=42)
        test_sample = test_values.sample(min(sample_per_group, len(test_values)), random_state=42)
        ks_stat, p_value = stats.ks_2samp(control_sample, test_sample, alternative="two-sided", method="auto")
        ks_results.append(
            {
                "feature": column,
                "ks_stat": float(ks_stat),
                "p_value": float(p_value),
                "control_mean": float(control_values.mean()),
                "test_mean": float(test_values.mean()),
                "control_median": float(control_values.median()),
                "test_median": float(test_values.median()),
                "control_n": int(len(control_values)),
                "test_n": int(len(test_values)),
            }
        )

    if not ks_results:
        return pd.DataFrame(
            columns=[
                "feature",
                "ks_stat",
                "p_value",
                "control_mean",
                "test_mean",
                "control_median",
                "test_median",
                "control_n",
                "test_n",
                "fdr_q_value",
                "ks_significant_0_05",
            ]
        )

    ks_report = pd.DataFrame(ks_results).sort_values("p_value").reset_index(drop=True)
    ks_report["fdr_q_value"] = benjamini_hochberg(ks_report["p_value"])
    ks_report["ks_significant_0_05"] = ks_report["fdr_q_value"] < 0.05
    return ks_report


def build_run_summary(
    report_dt: str,
    split_version: str,
    monetization_col: str | None,
    insert_df: pd.DataFrame,
    current_assignment_df: pd.DataFrame,
    srm: dict[str, Any],
    ks_report: pd.DataFrame,
    strata_cols: list[str],
) -> dict[str, Any]:
    '''Строит словарь с ключевыми метриками и информацией о запуске для логирования и мониторинга.'''
    return {
        "report_dt": report_dt,
        "split_version": split_version,
        "monetization_source": monetization_col,
        "eligible_rows_current_month": int(len(current_assignment_df)),
        "new_rows_inserted": int(len(insert_df)),
        "control_count_current_slice": int(srm.get("control_count", 0)),
        "test_count_current_slice": int(srm.get("test_count", 0)),
        "control_share_current_slice": float(srm.get("control_share", 0.0) or 0.0),
        "test_share_current_slice": float(srm.get("test_share", 0.0) or 0.0),
        "srm_p_value_current_slice": float(srm.get("p_value", np.nan)),
        "ks_features_checked": int(len(ks_report)),
        "ks_significant_after_fdr": int(ks_report["ks_significant_0_05"].sum()) if not ks_report.empty else 0,
        "n_core_strata_features": int(len(strata_cols)),
    }