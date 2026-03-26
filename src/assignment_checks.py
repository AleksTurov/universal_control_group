from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from scipy import stats

from src.database import query_df
from src.logger import logger


class AssignmentChecks:
    """Статистические проверки и SQL-валидации для assignment."""

    def __init__(
        self,
        report_dt: str,
        validation_sqls: tuple[Path, ...],
        validation_names: tuple[str, ...],
        srm_alpha: float,
        ks_alpha: float,
    ) -> None:
        self.report_dt = report_dt
        self.validation_sqls = validation_sqls
        self.validation_names = validation_names
        self.srm_alpha = srm_alpha
        self.ks_alpha = ks_alpha

    @staticmethod
    def benjamini_hochberg(p_values: pd.Series | list[float]) -> pd.Series:
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

    @staticmethod
    def calculate_srm(group_series: pd.Series, control_share: float) -> dict[str, Any]:
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

    def run_ks_checks(
        self,
        assignment_df: pd.DataFrame,
        group_column: str,
        ks_columns: list[str] | tuple[str, ...],
        sample_per_group: int = 100_000,
    ) -> pd.DataFrame:
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
        ks_report["fdr_q_value"] = self.benjamini_hochberg(ks_report["p_value"])
        ks_report["ks_significant_0_05"] = ks_report["fdr_q_value"] < self.ks_alpha
        return ks_report

    def evaluate_checks(self, srm: dict[str, Any], ks_report: pd.DataFrame) -> dict[str, Any]:
        srm_p_value = float(srm.get("p_value", 1.0) or 1.0)
        srm_failed = srm_p_value < self.srm_alpha
        if srm_failed:
            logger.warning("SRM check не пройден: p_value=%.6f (< %.3f)", srm_p_value, self.srm_alpha)

        ks_failed_features: list[str] = []
        if not ks_report.empty and "ks_significant_0_05" in ks_report.columns:
            ks_failed_features = ks_report.loc[ks_report["ks_significant_0_05"], "feature"].astype(str).tolist()
            if ks_failed_features:
                logger.warning(
                    "KS check не пройден по фичам (alpha=%.3f): %s",
                    self.ks_alpha,
                    ", ".join(ks_failed_features),
                )

        return {
            "srm_failed": srm_failed,
            "srm_p_value": srm_p_value,
            "ks_failed_features": ks_failed_features,
        }

    def load_validations(self) -> dict[str, list[dict[str, Any]]]:
        validations: dict[str, list[dict[str, Any]]] = {}
        for name, sql_path in zip(self.validation_names, self.validation_sqls):
            dataframe = query_df(sql_path, report_dt=self.report_dt)
            records = dataframe.to_dict(orient="records")
            validations[name] = records
            logger.info("Validation %s: %s", name, records)
        return validations

    def build_run_summary(
        self,
        split_version: str,
        monetization_col: str | None,
        insert_rows: int,
        current_assignment_df: pd.DataFrame,
        srm: dict[str, Any],
        ks_report: pd.DataFrame,
        strata_cols: list[str] | tuple[str, ...],
    ) -> dict[str, Any]:
        return {
            "report_dt": self.report_dt,
            "split_version": split_version,
            "monetization_source": monetization_col,
            "eligible_rows_current_month": int(len(current_assignment_df)),
            "new_rows_inserted": int(insert_rows),
            "control_count_current_slice": int(srm.get("control_count", 0)),
            "test_count_current_slice": int(srm.get("test_count", 0)),
            "control_share_current_slice": float(srm.get("control_share", 0.0) or 0.0),
            "test_share_current_slice": float(srm.get("test_share", 0.0) or 0.0),
            "srm_p_value_current_slice": float(srm.get("p_value", np.nan)),
            "ks_features_checked": int(len(ks_report)),
            "ks_significant_after_fdr": int(ks_report["ks_significant_0_05"].sum()) if not ks_report.empty else 0,
            "n_core_strata_features": int(len(strata_cols)),
        }
