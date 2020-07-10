import numpy as np
import datetime
import pandas as pd
import ee

ee.Initialize()


class Point(object):
    def __init__(self, lat, lon, yr_start, yr_end):
        self._lat = float(lat)
        self._lon = float(lon)
        self._yr_start = yr_start
        self._yr_end = yr_end

    def _get_geo(self):
        return ee.Geometry.Point([self._lon, self._lat])

    def _collection(self):
        return None

    def _band_map(self):
        """
        Override required
        Dict of {band name: gee name}
        need red, nir, qa
        """
        return {"red": None, "nir": None, "qa": None}

    def _get_qa(self):
        raise NotImplementedError

    def _get_data(self):
        # This is silly...but allows mapping from GE name to 'human name'
        # sometimes QA is none, so the below logic removes QA band if not used.
        ee_band_list = [v for v in self._band_map().values() if v]
        normalized_band_names = [k for k, v in self._band_map().items() if v]

        ann_start = pd.date_range(
            "{}".format(self._yr_start), "{}".format(self._yr_end), freq="AS"
        )
        ann_end = pd.date_range(
            "{}".format(self._yr_start), "{}".format(self._yr_end), freq="A-DEC"
        )

        store = []
        for startTime, endTime in zip(ann_start, ann_end):
            collection = self._collection().filterDate(startTime, endTime)
            geo = self._get_geo()
            info = collection.getRegion(geo.buffer(30), 30).getInfo()
            # extract the header column names
            header = info[0]

            # create a Numpy array of the data
            data = np.array(info[1:])
            if len(data) == 0:
                continue

            # extract the time information; convert to Python datetime objects
            idx_time = header.index("time")
            time = [
                datetime.datetime.fromtimestamp(i / 1000)
                for i in (data[:, idx_time].astype(int))
            ]

            iBands = [
                header.index(ee_band) for ee_band in ee_band_list
            ]  # if b removes Nones from band_list
            yData = data[:, iBands].astype(np.float)  # allows NaN to work later
            df = pd.DataFrame(
                yData, index=time, columns=normalized_band_names,
            ).sort_index()
            store.append(df)
        if len(store) > 0:
            return pd.concat(store)

    def run(self):

        data = self._get_data()
        if self._band_map()["qa"]:
            data = data.dropna(subset=["qa"])  # no data wo qa flag

            data.loc[:, "is_cloudy"] = data["qa"].apply(self._get_qa)
            data = data.loc[data.is_cloudy == 0]
        # post-qa squash any days with more than one obs; it happens!
        data = data.groupby(
            data.index.date
        ).median()  # Can sometimes have multiple values per day
        data.loc[:, "ndvi"] = (data["nir"] - data["red"]) / (data["nir"] + data["red"])
        data.loc[:, "nirv"] = data["ndvi"] * data["nir"]

        data.loc[:, "pct_change_flag"] = data["red"].pct_change() < 5
        data = data.loc[data.pct_change_flag, ["red", "nir", "ndvi", "nirv"]]
        return data


class L5Point(Point):
    def _band_map(self):
        """
        Dict of {band name: gee name}
        need red, nir, qa
        """
        return {"red": "B3", "nir": "B4", "qa": "pixel_qa"}

    @staticmethod
    def _get_qa(qa_flag):
        # cloud shadow bit is set (3)
        qa_flag = int(qa_flag)
        if np.bitwise_and(qa_flag, 1 << 3) != 0:
            return 1
        # cloud confidence & cloud bit
        if (np.bitwise_and(qa_flag, 1 << 5) != 0) and (
            np.bitwise_and(qa_flag, 1 << 7) != 0
        ):
            return 1
        # snow
        if np.bitwise_and(qa_flag, 1 << 4) != 0:
            return 1

        return 0

    def _collection(self):
        return ee.ImageCollection("LANDSAT/LT05/C01/T1_SR")


class L8Point(Point):
    def _band_map(self):
        """
        Dict of {band name: gee name}
        need red, nir, qa
        """
        return {"red": "B4", "nir": "B5", "qa": "pixel_qa"}

    @staticmethod
    def _get_qa(qa_flag):
        # cloud shadow bit is set (3)
        qa_flag = int(qa_flag)
        if np.bitwise_and(qa_flag, 1 << 3) != 0:
            return 1
        # cloud confidence & cloud bit
        if (np.bitwise_and(qa_flag, 1 << 5) != 0) and (
            np.bitwise_and(qa_flag, 1 << 7) != 0
        ):
            return 1
        # snow
        if np.bitwise_and(qa_flag, 1 << 4) != 0:
            return 1

        return 0

    def _collection(self):
        return ee.ImageCollection("LANDSAT/LC08/C01/T1_SR")


class ModisPoint(Point):
    def _band_map(self):
        """
        Dict of {band name: gee name}
        need red, nir, qa
        """
        return {
            "red": "Nadir_Reflectance_Band1",
            "nir": "Nadir_Reflectance_Band2",
            "qa": None,  # always valid: https://developers.google.com/earth-engine/datasets/catalog/MODIS_006_MCD43A4
        }

    def _collection(self):
        return ee.ImageCollection("MODIS/006/MCD43A4")
