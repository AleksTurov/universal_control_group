from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from src.assignment_checks import AssignmentChecks
from src.assignment_plots import AssignmentPlots
from src.config import analysis_config
from src.logger import logger


class AssignmentAnalyzer:
    """Считает проверки качества assignment и сохраняет артефакты анализа."""

    def __init__(
        self,
        report_dt: Any,
        output_root: Path,
        validation_sqls: tuple[Path, ...],
        validation_names: tuple[str, ...],
        srm_alpha: float,
        ks_alpha: float,
    ) -> None:
        self.report_dt = str(report_dt)
        self.output_dir = output_root / self.report_dt
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.checks = AssignmentChecks(
            report_dt=self.report_dt,
            validation_sqls=validation_sqls,
            validation_names=validation_names,
            srm_alpha=srm_alpha,
            ks_alpha=ks_alpha,
        )
        self.plots = AssignmentPlots(output_dir=self.output_dir, report_dt=self.report_dt)

    def analyze(
        self,
        current_assignment_df: pd.DataFrame,
        control_share: float,
        ks_columns: list[str] | tuple[str, ...],
        strata_cols: list[str] | tuple[str, ...],
        split_version: str,
        monetization_col: str | None,
        insert_rows: int,
        group_column: str = "experiment_group",
    ) -> dict[str, Any]:
        # Статистические проверки считаем на текущем assignment-срезе.
        srm = self.checks.calculate_srm(current_assignment_df[group_column], control_share=control_share)
        ks_report = self.checks.run_ks_checks(current_assignment_df, group_column=group_column, ks_columns=ks_columns)
        checks = self.checks.evaluate_checks(srm=srm, ks_report=ks_report)
        validations = self.checks.load_validations()
        summary = self.checks.build_run_summary(
            split_version=split_version,
            monetization_col=monetization_col,
            insert_rows=insert_rows,
            current_assignment_df=current_assignment_df,
            srm=srm,
            ks_report=ks_report,
            strata_cols=strata_cols,
        )

        # Дубли берем из отдельного summary-запроса по всей assignment-таблице.
        global_summary = validations.get("global_assignment_summary", [])
        duplicate_rows_cnt = 0
        if global_summary:
            duplicate_rows_cnt = int(global_summary[0].get("duplicate_rows_cnt", 0) or 0)
        if duplicate_rows_cnt > 0:
            logger.warning("Найдены дубли в assignment по subs_id: %s", duplicate_rows_cnt)

        artifacts = self.plots.save_artifacts(
            assignment_df=current_assignment_df,
            ks_report=ks_report,
            summary=summary,
            checks=checks,
            validations=validations,
            target_control_share=control_share,
        )

        return {
            "status": "warning" if (checks["srm_failed"] or checks["ks_failed_features"] or duplicate_rows_cnt > 0) else "ok",
            "summary": summary,
            "checks": checks,
            "validations": validations,
            "artifacts": artifacts,
        }


def build_default_analyzer(report_dt: Any, srm_alpha: float, ks_alpha: float) -> AssignmentAnalyzer:
    return AssignmentAnalyzer(
        report_dt=report_dt,
        output_root=analysis_config.OUTPUT_DIR,
        validation_sqls=analysis_config.VALIDATION_SQLS,
        validation_names=analysis_config.VALIDATION_NAMES,
        srm_alpha=srm_alpha,
        ks_alpha=ks_alpha,
    )