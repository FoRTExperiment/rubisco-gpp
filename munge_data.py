import csv
import glob
import os

from itertools import product
from pathlib import Path
import luigi
import numpy as np
import pandas as pd

from nirv.eenirv import ModisPoint


class CosoreSite:
    def __init__(self, cosore_id):
        self._cosore_id = cosore_id
        with open("data/finality_sites.csv") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row["CSR_DATASET"] == self.cosore_id:
                    self._lat = float(row["CSR_LATITUDE"])
                    self._lon = float(row["CSR_LONGITUDE"])
                    self._flux_format = row["FLUX_FORMAT"]
                    self._site_id = row["CSR_SITE_ID"]
                    self._tz = row["CSR_TIMESTAMP_TZ"]

    @property
    def cosore_id(self):
        return self._cosore_id

    @property
    def lat(self):
        return self._lat

    @property
    def lon(self):
        return self._lon

    @property
    def flux_format(self):
        return self._flux_format

    @property
    def site_id(self):
        return self._site_id

    @property
    def tz(self):
        return self._tz


class Config(luigi.Config):
    oneflux_dir = luigi.Parameter(default="/Users/darryl/mnt/data/flux/oneflux")
    cosore_dir = luigi.Parameter(default="/Users/darryl/mnt/data/cosore")
    nirv_dir = luigi.Parameter(default="data/nirv/")
    output_dir = luigi.Parameter(default="/Users/darryl/mnt/flux_finality")


class FluxData(luigi.ExternalTask):
    site_id = luigi.Parameter()
    freq = luigi.Parameter()
    _freq_mapper = {"30T": "H*", "1D": "DD"}

    def output(self):
        oneflux_freq = self._freq_mapper.get(self.freq)
        pat = os.path.join(
            Config().oneflux_dir, f"FLX_{self.site_id}*_{oneflux_freq}_*.csv"
        )
        fname = glob.glob(pat)[0]
        return luigi.LocalTarget(fname)


class AggregateFlux(luigi.Task):
    site_id = luigi.Parameter()
    freq = luigi.Parameter()
    tz = luigi.Parameter()

    def output(self):
        fname = os.path.join(
            Config().output_dir, "fluxes", f"{self.site_id}_{self.freq}.csv"
        )
        return luigi.LocalTarget(fname)

    def requires(self):
        return FluxData(site_id=self.site_id, freq=self.freq)

    def run(self):

        if self.freq == "30T":
            dt_col = "TIMESTAMP_START"
            dt_fmt = "%Y%m%d%H%M"
        else:
            dt_col = "TIMESTAMP"
            dt_fmt = "%Y%m%d"

        measure_vars = [
            "NEE_VUT_REF",
            "GPP_DT_VUT_REF",
            "GPP_NT_VUT_REF",
            "RECO_NT_VUT_REF",
            "RECO_DT_VUT_REF",
            "PPFD_IN",
        ]

        aux_vars = [
            "NEE_VUT_REF_QC",
            # "PPFD_IN_QC",
            dt_col,
        ]

        df = pd.read_csv(self.input().path, usecols=measure_vars + aux_vars)

        # QC varies by freq:
        if self.freq == "30T":
            df = df.loc[df["NEE_VUT_REF_QC"].isin([0, 1]), :]
        elif self.freq == "1D":
            df = df.loc[df["NEE_VUT_REF_QC"] > 0.85, :]
        else:
            raise NotImplementedError

        df = df.replace(-9999, np.nan)  # handles PPFD_IN

        df = df.set_index(pd.to_datetime(df[dt_col], format=dt_fmt))
        df = df.sort_index()
        agg_df = df[measure_vars].resample(self.freq).mean()
        agg_df = agg_df.tz_localize(self.tz)
        agg_df.index.name = "dt"
        agg_df = agg_df.dropna(subset=["NEE_VUT_REF"])
        agg_df["2003":].to_csv(self.output().path, float_format="%.3f", index=True)


class NIRvData(luigi.ExternalTask):
    site_id = luigi.Parameter()
    lat = luigi.FloatParameter()
    lon = luigi.FloatParameter()

    def output(self):
        fname = os.path.join(Config().nirv_dir, f"{self.site_id}.csv")
        return luigi.LocalTarget(fname)

    def run(self):
        Path(Config().nirv_dir).mkdir(
            parents=True, exist_ok=True
        )  # ensure outdir exists
        site_nirv = ModisPoint(self.lat, self.lon, 2003, 2019).run()
        site_nirv.index.name = "dt"
        site_nirv.to_csv(self.output().path, index=True, float_format="%.2f")


class COSOREData(luigi.ExternalTask):
    cosore_id = luigi.Parameter()

    def output(self):
        fname = os.path.join(
            Config().cosore_dir, "datasets", f"data_{self.cosore_id}.csv"
        )
        return luigi.LocalTarget(fname)


class AggregateCOSORE(luigi.Task):
    cosore_id = luigi.Parameter()
    freq = luigi.Parameter()

    def _map_ports(self):
        ports = pd.read_csv(os.path.join(Config().cosore_dir, "ports.csv"))
        port_map = (
            ports.loc[
                (ports["CSR_DATASET"] == self.cosore_id)
                & (ports["CSR_TREATMENT"] == "None")
            ]
            .set_index("CSR_PORT")
            .CSR_MSMT_VAR.to_dict()
        )
        return port_map

    def _get_tz(self):
        with open(os.path.join(Config().cosore_dir, "description.csv")) as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row["CSR_DATASET"] == self.cosore_id:
                    return row["CSR_TIMEZONE"]

    def requires(self):
        return COSOREData(self.cosore_id)

    def output(self):
        fname = os.path.join(
            Config().output_dir, "cosore", f"{self.cosore_id}_{self.freq}.csv"
        )
        return luigi.LocalTarget(fname)

    def run(self):
        df = pd.read_csv(
            self.input().path,
            usecols=["CSR_TIMESTAMP_END", "CSR_PORT", "CSR_FLUX_CO2"],
        )
        df.loc[:, "measurement_type"] = df["CSR_PORT"].map(self._map_ports())
        df.loc[:, "dt"] = pd.to_datetime(df["CSR_TIMESTAMP_END"])
        df = df.pivot_table(
            index="dt",
            columns="measurement_type",
            values="CSR_FLUX_CO2",
            aggfunc="mean",
        )
        df = df.sort_index()

        timesteps = pd.date_range(str(df.index.year.min()), "2020", freq=self.freq)
        agg_df = df.groupby(timesteps.asof).mean()

        agg_df.index.name = "dt"
        agg_df = agg_df.tz_localize(self._get_tz())
        agg_df.to_csv(self.output().path, index=True, float_format="%0.3f")


class SiteMunge(luigi.Task):
    """
    Combine cosore, NIRv from MODIS and EC Fluxes together at daily timescale
    TODO: PPFD average only over daylight hours
    """

    cosore_id = luigi.Parameter()
    freq = luigi.Parameter()

    def requires(self):
        site_info = CosoreSite(self.cosore_id)

        tasks = {
            "cosore": AggregateCOSORE(cosore_id=self.cosore_id, freq=self.freq),
            "nirv": NIRvData(
                site_id=site_info.site_id, lat=site_info.lat, lon=site_info.lon
            ),
            "flux": AggregateFlux(
                site_id=site_info.site_id, tz=site_info.tz, freq=self.freq
            ),
        }
        return tasks

    def output(self):
        fname = Path().joinpath(
            Config().output_dir, f"{self.cosore_id}_{self.freq}.csv"
        )
        return luigi.LocalTarget(fname)

    def run(self):
        site_info = CosoreSite(
            self.cosore_id
        )  # could probably mix-in class but whatever.

        df_dict = {
            k: pd.read_csv(v.path, index_col="dt", parse_dates=["dt"])
            for k, v in self.input().items()
        }

        df = df_dict["flux"].copy()
        df = df.join(df_dict["cosore"])
        resample_nirv = df_dict["nirv"].resample(self.freq).ffill()
        resample_nirv = resample_nirv.tz_localize(df.index.tz)
        df = df.join(resample_nirv)

        start_yr = df_dict["cosore"].index.min().year
        end_yr = df_dict["cosore"].index.max().year

        df[f"{start_yr}":f"{end_yr}"].to_csv(
            self.output().path, index=True, float_format="%.3f"
        )


class Munge(luigi.Task):
    """
    Wrapper task to kickoff pipeline
    """

    def requires(self):
        freqs = ["30T", "1D"]
        cosore_ids = ["d20190424_ZHANG_maple"]

        tasks = []

        for freq, cosore_id in product(freqs, cosore_ids):
            tasks.append(SiteMunge(cosore_id=cosore_id, freq=freq))

        return tasks


if __name__ == "__main__":
    luigi.run()
