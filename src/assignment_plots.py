from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


class AssignmentPlots:
    """Сохраняет JSON/CSV и графики по результатам assignment-анализа."""

    def __init__(self, output_dir: Path, report_dt: str) -> None:
        self.output_dir = output_dir
        self.report_dt = report_dt
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _save_json(self, file_name: str, payload: Any) -> str:
        target = self.output_dir / file_name
        target.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        return str(target)

    def _save_group_distribution_plot(self, assignment_df: pd.DataFrame) -> str | None:
        if assignment_df.empty or "experiment_group" not in assignment_df.columns:
            return None

        counts = assignment_df["experiment_group"].value_counts().sort_index()
        fig, ax = plt.subplots(figsize=(8, 5))
        counts.plot(kind="bar", ax=ax, color=["#2e86ab", "#f18f01"])
        ax.set_title(f"UKG group distribution {self.report_dt}")
        ax.set_xlabel("experiment_group")
        ax.set_ylabel("rows")
        ax.grid(axis="y", alpha=0.25)
        fig.tight_layout()

        target = self.output_dir / "group_distribution.png"
        fig.savefig(target, dpi=160)
        plt.close(fig)
        return str(target)

    def _save_top_strata_plot(self, assignment_df: pd.DataFrame, target_control_share: float) -> str | None:
        if assignment_df.empty or "strata_key" not in assignment_df.columns:
            return None

        strata_view = (
            assignment_df.groupby("strata_key", dropna=False)
            .agg(total_rows=("experiment_group", "size"), control_share=("is_control", "mean"))
            .sort_values("total_rows", ascending=False)
            .head(15)
            .sort_values("control_share")
        )
        if strata_view.empty:
            return None

        fig, ax = plt.subplots(figsize=(12, 7))
        ax.barh(strata_view.index.astype(str), strata_view["control_share"], color="#2a9d8f")
        ax.axvline(target_control_share, color="#d62828", linestyle="--", linewidth=2)
        ax.set_title(f"Control share by top strata {self.report_dt}")
        ax.set_xlabel("control_share")
        ax.set_ylabel("strata_key")
        ax.grid(axis="x", alpha=0.25)
        fig.tight_layout()

        target = self.output_dir / "top_strata_control_share.png"
        fig.savefig(target, dpi=160)
        plt.close(fig)
        return str(target)

    def _save_ks_plot(self, ks_report: pd.DataFrame) -> str | None:
        if ks_report.empty:
            return None

        top_ks = ks_report.sort_values("fdr_q_value", ascending=True).head(15).iloc[::-1]
        fig, ax = plt.subplots(figsize=(10, 7))
        ax.barh(top_ks["feature"], -np.log10(top_ks["fdr_q_value"].clip(lower=1e-12)), color="#6a4c93")
        ax.set_title(f"Top KS signals {self.report_dt}")
        ax.set_xlabel("-log10(FDR q-value)")
        ax.set_ylabel("feature")
        ax.grid(axis="x", alpha=0.25)
        fig.tight_layout()

        target = self.output_dir / "ks_top_signals.png"
        fig.savefig(target, dpi=160)
        plt.close(fig)
        return str(target)

    def save_artifacts(
        self,
        assignment_df: pd.DataFrame,
        ks_report: pd.DataFrame,
        summary: dict[str, Any],
        checks: dict[str, Any],
        validations: dict[str, list[dict[str, Any]]],
        target_control_share: float,
    ) -> dict[str, str]:
        artifacts: dict[str, str] = {
            "summary_json": self._save_json("summary.json", summary),
            "checks_json": self._save_json("checks.json", checks),
            "validations_json": self._save_json("validations.json", validations),
        }

        if not ks_report.empty:
            ks_csv_path = self.output_dir / "ks_report.csv"
            ks_report.to_csv(ks_csv_path, index=False)
            artifacts["ks_report_csv"] = str(ks_csv_path)

        group_distribution = self._save_group_distribution_plot(assignment_df)
        if group_distribution:
            artifacts["group_distribution_png"] = group_distribution

        top_strata = self._save_top_strata_plot(assignment_df, target_control_share)
        if top_strata:
            artifacts["top_strata_control_share_png"] = top_strata

        ks_plot = self._save_ks_plot(ks_report)
        if ks_plot:
            artifacts["ks_top_signals_png"] = ks_plot

        return artifacts
