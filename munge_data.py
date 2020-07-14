import csv
import os

from pathlib import Path
import luigi
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
    oneflux_dir = luigi.Parameter(default="/Users/darryl/mnt/data/oneflux")
    cosore_dir = luigi.Parameter(default="/Users/darryl/mnt/data/cosore")
    nirv_dir = luigi.Parameter(default="data/nirv/")


class FluxData(luigi.ExternalTask):
    site_id = luigi.Parameter()
    flux_format = luigi.Parameter()

    def output(self):
        if self.flux_format == "oneflux":
            fname = os.path.join(Config().oneflux_dir)

        return luigi.LocalTarget(fname)


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
        site_nirv.to_csv(self.output().path, float_format="%.2f")


class COSOREData(luigi.ExternalTask):
    cosore_id = luigi.Parameter()
    # NOPE just need a map of where the data is and we're done.

    def output(self):
        fname = os.path.join(Config().cosore_dir, f"{cosore_id}.csv")
        return luigi.LocalTarget(fname)


class DailyMunge(luigi.Task):
    """
    Combine cosore, NIRv from MODIS and EC Fluxes together at daily timescale
    """

    cosore_id = luigi.Parameter()

    def requires(self):
        site_info = CosoreSite(self.cosore_id)

        tasks = {
            "cosore": COSOREData(site_id=self.cosore_id),
            "nirv": NIRvData(
                site_id=site_info.site_id, lat=site_info.lat, lon=site_info.lon
            ),
            "flux": FluxData(
                site_id=site_info.site_id, flux_format=site_info.flux_format
            ),
        }
        return tasks

    def output(self):
        return Path().joinpath(Config().output_dir, f"{self.cosore_id}.csv")

    def run(self):
        site_info = CosoreSite(
            self.cosore_id
        )  # could probably mix-in class but whatever.
        cosore = pd.read_csv(self.input()["cosore"])


if __name__ == "__main__":
    luigi.run()
