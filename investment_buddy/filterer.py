import pendulum
from typing import Union
import pandas as pd
import logging
import glob

logger = logging.getLogger(__name__)


class DataFilters(object):
    def __init__(self, as_of_date):
        df_all_org = pd.concat(
            map(pd.read_csv, glob.glob("data/nse/*.csv") + glob.glob("data/bse/*.csv"))
        ).assign(
            date=lambda df: pd.to_datetime(df.date),
            quarter=lambda df: df.date.dt.quarter,
            alt_id=lambda df: df.alt_id.astype("Int64").astype("string"),
            isin=lambda df: df["isin"].astype("string"),
        )

        # to address change in format of data files after July 8, 2024.
        df_nse = df_all_org.query("exchange=='NSE'")
        df_bse = df_all_org.query("exchange=='BSE'")
        df_bse_new = df_bse.query("alt_id.notna()")
        df_replace_dict = df_bse_new.query("date==date.max()")[
            ["symbol", "isin", "alt_id"]
        ].drop_duplicates()
        df_bse_old = (
            df_bse.query("alt_id.isna()")
            .drop(columns=["symbol", "alt_id"])
            .rename(columns={"isin": "alt_id"})
        )
        df_bse_old = df_bse_old.merge(df_replace_dict, how="left", on="alt_id")
        df_all = pd.concat([df_nse, df_bse_new, df_bse_old])
        assert len(df_all) == len(df_all_org)

        self.as_of_date = pd.to_datetime(as_of_date.naive())
        max_date = df_all.date.max()
        self.filter_date = max_date if max_date < self.as_of_date else self.as_of_date
        self.df_all = df_all.query("date <= @self.filter_date")
        self.date_str = f"{self.filter_date.year}{self.filter_date.month:02}{self.filter_date.day:02}"
        logger.info(
            f"Filtering as of {self.date_str} (Either the date provided or the latest one available). "
        )

    def get_prev_data(self):
        quarter_start = self.current_quarter_start(self.filter_date).date()
        quarter_end = self.filter_date.date() - pendulum.duration(days=1)
        month_start = self.filter_date.replace(day=1).date()
        month_end = self.filter_date.date() - pendulum.duration(days=1)
        quarter_period = pendulum.period(quarter_start, quarter_end)
        month_period = pendulum.period(month_start, month_end)
        date_strs = [
            f"{dt.year}{dt.month:02}{dt.day:02}" for dt in quarter_period.range("days")
        ]
        month_days = [
            int(f"{dt.year}{dt.month:02}{dt.day:02}")
            for dt in month_period.range("days")
        ]
        prev_days = [
            f"data/filtered/{d}.xlsx"
            for d in date_strs
            if len(glob.glob(f"data/filtered/{d}.xlsx"))
        ]
        self.df_prev = (
            pd.concat(map(pd.read_excel, prev_days)).assign(
                is_month=lambda df: df.date_str.isin(month_days)
            )
            if prev_days
            else None
        )

    def apply_all_filters(self):
        self.get_prev_data()
        # self.apply_300p_month_filter()
        self.apply_200p_quarter_filter()
        # self.apply_200p_thrice_12mos()
        self.apply_200p_twice_6mos()
        # self.apply_52week_high_filter()
        self.df_all_filtered = (
            pd.concat(
                [
                    # self.df_300p_val_month.assign(filter="300% value over prior month"),
                    self.df_200p_val_quarter.assign(
                        filter="200% value over prior quarter"
                    ),
                    # self.df_52_week_highs.assign(filter="52 week high"),
                    # self.df_200p_val_thrice.assign(filter="200% thrice in 12 months"),
                    self.df_200p_val_twice.assign(filter="200% twice in 6 months"),
                ]
            )
            .groupby(["exchange", "symbol", "isin"])
            .agg({"filter": lambda x: x.str.cat(sep=", ")})
            .reset_index()
            .assign(date_str=self.date_str)
            # in case same isin repeats, only retain NSE entry
            .sort_values(["isin", "exchange"])
            .groupby("isin")
            .last()
            .reset_index()
        )
        self.df_all_filtered = (
            self.df_all_filtered.query("~symbol.str.endswith('ETF)")
            .query("~symbol.str.startswith('ADANI)")
            .query("~symbol.str.startswith('RELIANCE)")
            .query("~symbol.str.startswith('KOTHARI)")
        )

        self.df_all_filtered.to_excel(
            f"data/filtered/{self.date_str}.xlsx", index=False
        )
        logger.info(
            f"Exported results to data/filtered/{self.date_str}.xlsx. There are {self.df_all_filtered.shape[0]} scripts to scrape."
        )

    def current_quarter_start(self, ref):
        if ref.month < 4:
            return pendulum.DateTime(ref.year, 1, 1)
        elif ref.month < 7:
            return pendulum.DateTime(ref.year, 4, 1)
        elif ref.month < 10:
            return pendulum.DateTime(ref.year, 7, 1)
        return pendulum.DateTime(ref.year, 10, 1)

    def apply_300p_month_filter(self):
        prev_month_first = (self.filter_date - pd.DateOffset(months=1)).replace(day=1)
        grouping_vars = ["exchange", "symbol", "isin", "year", "month"]
        self.df_300p_val_month = (
            self.df_all.query("date >= @prev_month_first")
            .assign(value=lambda df: df.close * df.volume)
            .sort_values(grouping_vars + ["day"])
            .groupby(grouping_vars)
            .agg({"value": sum, "volume": sum, "close": lambda x: x.iloc[-1]})
            .reset_index()
            .sort_values(grouping_vars)
            .assign(
                value_lag=lambda df: df.groupby(["exchange", "symbol", "isin"])[
                    "value"
                ].shift(1),
                volume_lag=lambda df: df.groupby(["exchange", "symbol", "isin"])[
                    "volume"
                ].shift(1),
                close_lag=lambda df: df.groupby(["exchange", "symbol", "isin"])[
                    "close"
                ].shift(1),
                value_ratio=lambda df: df.value / df.value_lag,
                volume_ratio=lambda df: df.volume / df.volume_lag,
                close_ratio=lambda df: df.close / df.close_lag,
            )
            .query("value_lag.notna()", engine="python")
            .query("value_ratio>3 & close_ratio>1 & value>2_000_000")
        )
        if self.df_prev is not None:
            self.df_300p_val_month = (
                self.df_300p_val_month.merge(
                    self.df_prev.query(
                        "filter.str.contains('300% value over prior month') & is_month",
                        engine="python",
                    )
                    .loc[:, ["exchange", "symbol", "isin"]]
                    .assign(already_exists=True),
                    "left",
                )
                .query("already_exists.isna()", engine="python")
                .drop(columns="already_exists")
            )

    def previous_quarter_start(self, ref):
        if ref.month < 4:
            return pendulum.DateTime(ref.year - 1, 10, 1)
        elif ref.month < 7:
            return pendulum.DateTime(ref.year, 1, 1)
        elif ref.month < 10:
            return pendulum.DateTime(ref.year, 4, 1)
        return pendulum.DateTime(ref.year, 7, 1)

    def apply_200p_quarter_filter(self):
        prev_quarter_first = pd.to_datetime(
            self.previous_quarter_start(self.filter_date)
        )
        grouping_vars = ["symbol", "isin", "exchange", "year", "quarter"]
        self.df_200p_val_quarter = (
            self.df_all.query("date >= @prev_quarter_first")
            .assign(value=lambda df: df.close * df.volume)
            .sort_values(grouping_vars + ["month", "day"])
            .groupby(grouping_vars)
            .agg({"value": sum, "volume": sum, "close": lambda x: x.iloc[-1]})
            .reset_index()
            .sort_values(grouping_vars)
            .assign(
                value_lag=lambda df: df.groupby(["exchange", "symbol", "isin"])[
                    "value"
                ].shift(1),
                volume_lag=lambda df: df.groupby(["exchange", "symbol", "isin"])[
                    "volume"
                ].shift(1),
                close_lag=lambda df: df.groupby(["exchange", "symbol", "isin"])[
                    "close"
                ].shift(1),
                value_ratio=lambda df: df.value / df.value_lag,
                volume_ratio=lambda df: df.volume / df.volume_lag,
                close_ratio=lambda df: df.close / df.close_lag,
            )
            .query("value_lag.notna()", engine="python")
            .query("value_ratio>2 & close_ratio>1 & value>6_000_000")
        )
        if self.df_prev is not None:
            self.df_200p_val_quarter = (
                self.df_200p_val_quarter.merge(
                    self.df_prev.query(
                        "filter.str.contains('200% value over prior quarter')",
                        engine="python",
                    )
                    .loc[:, ["exchange", "symbol", "isin"]]
                    .assign(already_exists=True),
                    "left",
                )
                .query("already_exists.isna()", engine="python")
                .drop(columns="already_exists")
            )

    def apply_52week_high_filter(self):
        date_52_weeks_prior = self.filter_date - pd.DateOffset(weeks=52)
        grouping_vars = ["exchange", "symbol", "isin"]

        df_52_week_high_vals = (
            self.df_all.query("date >= @date_52_weeks_prior")
            .sort_values(grouping_vars + ["year", "month", "day"])
            .groupby(grouping_vars)
            .agg({"high": max})
        )

        self.df_52_week_highs = (
            self.df_all.query("date == @self.filter_date")
            .merge(
                df_52_week_high_vals,
                how="left",
                on=grouping_vars,
                suffixes=("", "_max"),
            )
            .query("high==high_max")
            .drop(columns="high_max")
        )

    def apply_200p_thrice_12mos(self):
        date_12mos_prior = (self.filter_date - pd.DateOffset(months=12)).replace(day=1)
        grouping_vars = ["exchange", "symbol", "isin", "year", "month"]
        df_200p_val_thrice_filter = (
            self.df_all.query("date >= @date_12mos_prior")
            .assign(value=lambda df: df.close * df.volume)
            .sort_values(grouping_vars + ["day"])
            .groupby(grouping_vars)
            .agg({"value": sum, "volume": sum, "close": lambda x: x.iloc[-1]})
            .reset_index()
            .sort_values(grouping_vars)
            .assign(
                value_lag=lambda df: df.groupby(["exchange", "symbol", "isin"])[
                    "value"
                ].shift(1),
                volume_lag=lambda df: df.groupby(["exchange", "symbol", "isin"])[
                    "volume"
                ].shift(1),
                close_lag=lambda df: df.groupby(["exchange", "symbol", "isin"])[
                    "close"
                ].shift(1),
                value_ratio=lambda df: df.value / df.value_lag,
                volume_ratio=lambda df: df.volume / df.volume_lag,
                close_ratio=lambda df: df.close / df.close_lag,
            )
            .query("value_lag.notna()", engine="python")
            .query("value_ratio>2 & value>2_000_000")  # & close_ratio>1
            .groupby(["exchange", "symbol", "isin"])
            .agg({"close_ratio": "count"})
            .reset_index()
            .rename(columns={"close_ratio": "n_double"})
            .query("n_double>=3")
            .drop(columns="n_double")
        )
        self.df_200p_val_thrice = self.df_all.query("date == @self.filter_date").merge(
            df_200p_val_thrice_filter, how="inner"
        )

        if self.df_prev is not None:
            self.df_200p_val_thrice = (
                self.df_200p_val_thrice.merge(
                    self.df_prev.query(
                        "(filter.str.contains('200% thrice in 12 months') | filter.str.contains('200% twice in 6 months')) & is_month",
                        engine="python",
                    )
                    .loc[:, ["exchange", "symbol", "isin"]]
                    .assign(already_exists=True),
                    "left",
                )
                .query("already_exists.isna()", engine="python")
                .drop(columns="already_exists")
            )

    def apply_200p_twice_6mos(self):
        date_6mos_prior = (self.filter_date - pd.DateOffset(months=6)).replace(day=1)
        grouping_vars = ["exchange", "symbol", "isin", "year", "month"]
        df_200p_val_twice_filter = (
            self.df_all.query("date >= @date_6mos_prior")
            .assign(value=lambda df: df.close * df.volume)
            .sort_values(grouping_vars + ["day"])
            .groupby(grouping_vars)
            .agg({"value": sum, "volume": sum, "close": lambda x: x.iloc[-1]})
            .reset_index()
            .sort_values(grouping_vars)
            .assign(
                value_lag=lambda df: df.groupby(["exchange", "symbol", "isin"])[
                    "value"
                ].shift(1),
                volume_lag=lambda df: df.groupby(["exchange", "symbol", "isin"])[
                    "volume"
                ].shift(1),
                close_lag=lambda df: df.groupby(["exchange", "symbol", "isin"])[
                    "close"
                ].shift(1),
                value_ratio=lambda df: df.value / df.value_lag,
                volume_ratio=lambda df: df.volume / df.volume_lag,
                close_ratio=lambda df: df.close / df.close_lag,
            )
            .query("value_lag.notna()", engine="python")
            .query("value_ratio>2 & value>2_000_000")  # & close_ratio>1
            .groupby(["exchange", "symbol", "isin"])
            .agg({"close_ratio": "count"})
            .reset_index()
            .rename(columns={"close_ratio": "n_double"})
            .query("n_double>=2")
            .drop(columns="n_double")
        )
        self.df_200p_val_twice = self.df_all.query("date == @self.filter_date").merge(
            df_200p_val_twice_filter, how="inner"
        )

        if self.df_prev is not None:
            self.df_200p_val_twice = (
                self.df_200p_val_twice.merge(
                    self.df_prev.query(
                        "filter.str.contains('200% twice in 6 months') & is_month",
                        engine="python",
                    )
                    .loc[:, ["exchange", "symbol", "isin"]]
                    .assign(already_exists=True),
                    "left",
                )
                .query("already_exists.isna()", engine="python")
                .drop(columns="already_exists")
            )

    def __repr__(self):
        return f"DateFilter({self.date_str})"
