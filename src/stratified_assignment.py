from __future__ import annotations

import hashlib

import numpy as np
import pandas as pd


class StratifiedAssigner:
    """Детерминированное распределение клиентов между control и test внутри страт."""

    def __init__(self, salt: str) -> None:
        self.salt = salt

    def build_strata_key(self, df: pd.DataFrame, columns: list[str]) -> pd.Series:
        '''Строит ключ страты, склеивая значения указанных колонок.'''
        # Если список стратификационных колонок пустой, считаем весь датафрейм
        # одной общей стратой. Это деградирует до обычного deterministic split.
        if not columns:
            return pd.Series("ALL", index=df.index, dtype="string")

        # Приводим значения к строкам и заменяем пропуски на явный маркер,
        # чтобы strata_key строился одинаково и не ломался на NaN/None.
        prepared = df[columns].copy()
        for column in columns:
            prepared[column] = prepared[column].astype("string").fillna("MISSING")

        # Склеиваем значения колонок в один ключ страты.
        # Например: "BISHKEK|ARPU_0_10|TRAFFIC_ZERO|31-90d"
        return prepared.agg("|".join, axis=1).astype("string")

    def stable_uint64_hash(self, values: pd.Series) -> pd.Series:
        '''
        Строит стабильный 64-битный hash от пары value|salt.
        Один и тот же id при одинаковом salt всегда дает один и тот же hash,
        поэтому порядок внутри страты воспроизводим между запусками.
        '''
        encoded_values = values.astype("string").fillna("MISSING")
        hashed = [
            int.from_bytes(
                hashlib.blake2b(
                    f"{value}|{self.salt}".encode("utf-8"),
                    digest_size=8,
                ).digest(),
                byteorder="big",
                signed=False,
            )
            for value in encoded_values
        ]
        return pd.Series(hashed, index=values.index, dtype="uint64")

    def assign(
        self,
        df: pd.DataFrame,
        id_col: str,
        strata_cols: list[str],
        control_share: float,
    ) -> pd.DataFrame:
        """Распределяет строки между control и test с учетом страт."""
        out = df.copy()

        # Сначала строим ключ страты и стабильный hash для сортировки внутри нее.
        out["split_hash"] = self.stable_uint64_hash(out[id_col])
        out["strata_key"] = self.build_strata_key(out, strata_cols)

        target_control = int(round(len(out) * control_share))

        alloc = out.groupby("strata_key").size().rename("n").reset_index()
        alloc["target_float"] = alloc["n"] * control_share

        alloc["target_floor"] = np.floor(alloc["target_float"]).astype(int)
        alloc["fractional"] = alloc["target_float"] - alloc["target_floor"]
        alloc["k"] = alloc["target_floor"]

        # Остаток распределяем по стратам с максимальной дробной частью.
        remaining = target_control - int(alloc["target_floor"].sum())
        if remaining > 0:
            top_strata = alloc.sort_values(
                ["fractional", "n", "strata_key"],
                ascending=[False, False, True],
                kind="mergesort",
            ).head(remaining)["strata_key"]
            alloc.loc[alloc["strata_key"].isin(top_strata), "k"] += 1

        out = out.merge(alloc[["strata_key", "k"]], on="strata_key", how="left")

        # Внутри страты первые k строк уходят в control, остальные в test.
        out = out.sort_values(
            ["strata_key", "split_hash", id_col],
            kind="mergesort",
        ).copy()
        out["_rank"] = out.groupby("strata_key").cumcount() + 1

        out["experiment_group"] = np.where(
            out["_rank"] <= out["k"],
            "control",
            "test",
        )
        out["is_control"] = out["experiment_group"].eq("control").astype("uint8")

        return out.drop(columns=["_rank", "k"])