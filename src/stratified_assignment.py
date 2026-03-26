from __future__ import annotations

import hashlib

import numpy as np
import pandas as pd


class StratifiedAssigner:
    """
    Детерминированное стратифицированное распределение клиентов между
    control и test.

    Подходит для batch-сценария, где важно:
    - распределять новых клиентов стабильно;
    - держать целевую долю control;
    - воспроизводить assignment между запусками при одинаковом salt.
    """

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
        '''Распределяет строки датафрейма между control и test с учетом страт.
        Параметры:
        - df: входной датафрейм с клиентами для назначения.
        - id_col: колонка с уникальным идентификатором клиента, от которого строится hash.
        - strata_cols: список колонок для стратификации. Чем больше колонок, тем точнее
          будет соблюдаться баланс по ним, но меньше клиентов в каждой страти.
        - control_share: доля клиентов, которая должна попасть в control (например, 0.10 для 10% в control). Должно быть от 0 до 1. 
        Возвращает датафрейм с добавленной колонкой "experiment_group" со значениями "control" или "test".
        '''
        # Работаем с копией, чтобы не мутировать входной датафрейм снаружи.
        out = df.copy()

        # Для каждого клиента считаем:
        # 1) стабильный hash для deterministic порядка;
        # 2) ключ страты, внутри которой будет идти распределение.
        out["split_hash"] = self.stable_uint64_hash(out[id_col])
        out["strata_key"] = self.build_strata_key(out, strata_cols)

        # Целевое число клиентов в control по всему срезу.
        # Используем round, чтобы итоговый размер control был максимально
        # близок к общей доле control_share.
        target_control = int(round(len(out) * control_share))

        # Для каждой страты считаем ожидаемое число control:
        # n_strata * control_share.
        alloc = out.groupby("strata_key").size().rename("n").reset_index()
        alloc["target_float"] = alloc["n"] * control_share

        # Целую часть забираем сразу.
        alloc["target_floor"] = np.floor(alloc["target_float"]).astype(int)
        alloc["fractional"] = alloc["target_float"] - alloc["target_floor"]
        alloc["k"] = alloc["target_floor"]

        # После floor обычно остаются "недовыданные" места в control.
        # Их распределяем по стратам с наибольшей дробной частью.
        # Это стандартная логика largest remainder, она дает общий объем
        # control максимально близкий к target_control.
        remaining = target_control - int(alloc["target_floor"].sum())
        if remaining > 0:
            top_strata = alloc.sort_values(
                ["fractional", "n", "strata_key"],
                ascending=[False, False, True],
                kind="mergesort",
            ).head(remaining)["strata_key"]
            alloc.loc[alloc["strata_key"].isin(top_strata), "k"] += 1

        # Присоединяем k обратно к строкам датафрейма.
        out = out.merge(alloc[["strata_key", "k"]], on="strata_key", how="left")

        # Внутри каждой страты сортируем по стабильному hash.
        # Первые k строк становятся control, остальные test.
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