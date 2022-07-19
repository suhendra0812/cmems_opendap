from datetime import datetime
import logging
from pathlib import Path

import matplotlib.pyplot as plt
import metpy.calc as mpcalc
import numpy as np
import pandas as pd
import xarray as xr
from pydap.cas.get_cookies import setup_session
from pydap.client import open_url

logger = logging.getLogger(__name__)


def get_data_store(
    url: str, username: str, password: str
) -> xr.backends.PydapDataStore:
    cas_url = "https://cmems-cas.cls.fr/cas/login"
    session = setup_session(cas_url, username, password)
    session.cookies.set("CASTGC", session.cookies.get_dict()["CASTGC"])
    data_store = xr.backends.PydapDataStore(open_url(url, session=session))
    return data_store


def get_depth(data_depths: np.ndarray, given_depth: float):
    depth_index = np.argmin(np.abs(data_depths - given_depth))
    depth_value = data_depths[depth_index]
    return depth_value


def get_time(data_times: np.ndarray, given_time: datetime):
    time_index = np.argmin(np.abs(pd.to_datetime(data_times) - given_time))
    time_value = data_times[time_index]
    return time_value


def filter_dataset(
    ds: xr.Dataset,
    variables: list[str],
    start_date: datetime,
    stop_date: datetime,
    lon_min: float,
    lon_max: float,
    lat_min: float,
    lat_max: float,
    depth_min: float,
    depth_max: float,
) -> xr.Dataset:
    ds = ds[variables]

    lon_query = slice(lon_min, lon_max)
    lat_query = slice(lat_min, lat_max)

    if start_date != stop_date:
        time_query = slice(start_date, stop_date)
    else:
        data_times = ds["time"].values
        time_query = get_time(data_times, start_date)

    if not set(variables).issubset(["ZSD", "VHM0"]):
        if depth_min != depth_max:
            depth_query = slice(depth_min, depth_max)
        else:
            data_depths = ds["depth"].values
            depth_query = get_depth(data_depths, depth_min)
        ds = ds.sel(
            longitude=lon_query,
            latitude=lat_query,
            time=time_query,
            depth=depth_query,
        )
    else:
        logger.warning(f"Depth dimension query is not used in {variables[0]}")
        ds = ds.sel(longitude=lon_query, latitude=lat_query, time=time_query)

    return ds


def calculate_velocity(u: xr.DataArray, v: xr.DataArray) -> xr.DataArray:
    return mpcalc.wind_speed(u, v)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )

    # define username and password
    username = "ssuhendra"
    password = "@Kuningan08121995"

    # read parameter in csv file
    param_df = pd.read_csv("./sources.csv")

    param_dict = dict(
        arus= "sea_water_velocity",
        sst= "thetao",
        salinitas= "so",
        klorofil= "chl",
        ph= "ph",
        gelombang= "VHM0",
        kecerahan= "ZSD",
    )

    # define parameter
    parameter = "arus"
    temporal = "monthly"
    start_date = datetime.fromisoformat("2021-01-01")
    stop_date = datetime.today()
    depth_min = 0  # meter
    depth_max = 0  # meter
    lon_min = 114.35  # degree
    lon_max = 116  # degree
    lat_min = -8.35  # degree
    lat_max = -7  # degree

    temporal_resample_dict = dict(daily="1D", monthly= "1M", annual= "1Y")

    if not parameter in list(param_dict.keys()):
        raise ValueError(
            f"'{parameter}' is not in valid parameter options ({', '.join(param_dict.keys())})"
        )

    variable = param_dict[parameter]

    if parameter == "arus":
        variables = ["uo", "vo"]
    else:
        variables = [variable]

    # filter parameter dataframe based on defined variable
    param_df = param_df[param_df["parameter"] == variable]

    # filter parameter dataframe based on temporal resolution
    if parameter == "gelombang":
        param_df = param_df[param_df["temporal"] == "3-hourly"].iloc[0]
    else:
        param_df = param_df[param_df["temporal"] == temporal].iloc[0]

    # get initial and near-realtime date
    init_date = pd.to_datetime(param_df["init_date"]).to_pydatetime()
    nrt_date = pd.to_datetime(param_df["nrt_date"]).to_pydatetime()

    # check start and stop date to nrt date and populate the opendap url
    urls = []
    if (start_date < nrt_date) and (stop_date < nrt_date):
        url = param_df["opendap_my"]
        urls.append(url)
    elif (start_date >= nrt_date) and (stop_date > nrt_date):
        url = param_df["opendap_nrt"]
        urls.append(url)
    elif (start_date < nrt_date) and (stop_date >= nrt_date):
        url_my = param_df["opendap_my"]
        url_nrt = param_df["opendap_nrt"]
        urls.append(url_my)
        urls.append(url_nrt)
    else:
        raise ValueError("Start date must be less than stop date")

    param_title = param_df["title"]
    value_min = param_df["value_min"]
    value_max = param_df["value_max"]

    ds_list = []
    for url in urls:
        data_store = get_data_store(url, username, password)
        ds = xr.open_dataset(data_store).metpy.parse_cf()
        ds = filter_dataset(
            ds,
            variables,
            start_date,
            stop_date,
            lon_min,
            lon_max,
            lat_min,
            lat_max,
            depth_min,
            depth_max,
        )
        if parameter == "gelombang":
            logger.info(f"Resampling gelombang to {temporal}")
            ds = ds.resample(time=temporal_resample_dict[temporal]).mean()

        ds_list.append(ds)

    if len(ds_list) > 1:
        all_ds = xr.concat(ds_list, dim="time")
    else:
        all_ds = ds_list[0]

    res_deg = 1 / 111.139

    new_lon = np.arange(all_ds["longitude"].min(), all_ds["longitude"].max(), res_deg)
    new_lat = np.arange(all_ds["latitude"].min(), all_ds["latitude"].max(), res_deg)

    gmrt = xr.open_dataset("./GMRT_baliutara.grd").metpy.parse_cf()
    gmrt = gmrt.rename({"lon": "longitude", "lat": "latitude"})
    gmrt = gmrt.interp(longitude=new_lon, latitude=new_lat)
    altitude = gmrt.altitude
    altitude = altitude < 0

    if parameter == "arus":
        all_ds[variable] = calculate_velocity(
            all_ds[variables[0]], all_ds[variables[1]]
        )

    resample_ds = all_ds.interp(longitude=new_lon, latitude=new_lat)
    resample_ds = resample_ds.interpolate_na(
        dim="longitude", method="nearest", fill_value="extrapolate"
    ).interpolate_na(dim="latitude", method="nearest", fill_value="extrapolate")

    resample_ds = resample_ds.where(altitude)

    output_dir = Path("./output") / parameter
    output_dir.mkdir(parents=True, exist_ok=True)

    skip = 10

    for time, ds in resample_ds.groupby("time"):
        time = pd.to_datetime(time).to_pydatetime()

        fig, ax = plt.subplots(figsize=(15, 10))

        cf = ds[variable].plot.contourf(
            cmap="rainbow",
            vmin=value_min,
            vmax=value_max,
            levels=10,
            extend="max",
            cbar_kwargs={"format": "%.1f"},
            ax=ax,
        )

        if parameter == "arus":
            q = ds.isel(
                longitude=slice(None, None, skip),
                latitude=slice(None, None, skip),
            ).plot.quiver(
                x="longitude",
                y="latitude",
                u=variables[0],
                v=variables[1],
                ax=ax,
            )

        c = altitude.plot.contour(cmap="black", ax=ax)

        # cbar = cf.colorbar
        # ms_sym = r"$ms^{-1}$"
        cbar_text = f"{param_title}"
        title = f"{param_title} at {time.strftime('%Y-%m')}"
        # cbar.ax.set_ylabel(cbar_text)
        # ax.set_xlim([lon_min, lon_max])
        # ax.set_ylim([lat_min, lat_max])
        deg_sym = r"$\degree$"
        ax.set_xlabel(f"Longitude ({deg_sym})")
        ax.set_ylabel(f"Latitude ({deg_sym})")
        ax.set_title(title)

        output_path = output_dir / f"{parameter}_{time.strftime('%Y%m')}.png"

        fig.savefig(output_path, bbox_inches="tight")
        logger.info(f"Saved to {output_path}")
