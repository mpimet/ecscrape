#!/usr/bin/env python3
import datetime
import glob
import json
import os
import pathlib
import re
import requests

import gribscan
import healpy as hp
import numcodecs
import numpy as np
import xarray as xr
from scipy.spatial import Delaunay


def get_latest_forecasttime(dt):
    """Return the most recent ECMWF forecast time for a given datetime object."""
    return datetime.datetime(dt.year, dt.month, dt.day, dt.hour // 12 * 12)


def get_griblist(urlpath):
    """Yield relative paths of all GRIB2 files in a ECMWF forecast."""
    ret = requests.get(urlpath)

    if ret.status_code != 200:
        raise Exception(f"Forecast not availablae at: {urlpath}")

    for l in ret.text.split("\n"):
        regex = re.compile('<a href="(.*)">(.*\.grib2)</a>')
        if m := regex.match(l):
            relurl, filename = m.groups()
            yield relurl, filename


def download_file(urlpath, localpath, chunk_size=2**16):
    with requests.get(urlpath, stream=True) as ret:
        with open(localpath, "wb") as fp:
            for buf in ret.iter_content(chunk_size=chunk_size):
                if buf:
                    fp.write(buf)


def download_forecast(fctime, outdir):
    baseurl = "https://data.ecmwf.int"
    date, hour = fctime.strftime("%Y%m%d"), fctime.strftime("%H")

    gribfiles = get_griblist(f"{baseurl}/forecasts/{date}/{hour}z/0p25/oper/")

    for relpath, filename in gribfiles:
        download_file(f"{baseurl}{relpath}", outdir / filename)
        gribscan.write_index((outdir / filename).as_posix(), force=True)


def create_datasets(outdir):
    datasets = gribscan.grib_magic(
        outdir.glob("*.index"),
        magician=gribscan.magician.IFSMagician(),
        global_prefix=outdir,
    )

    for name, ref in datasets.items():
        with open(f"{outdir}/{name}.json", "w") as fp:
            json.dump(ref, fp)

    return [f"reference::{outdir}/{name}.json" for name in datasets.keys()]


def get_latlon_grid(hpz=7, nest=True):
    """Return two-dimensional longitude and latitude grids."""
    lons, lats = hp.pix2ang(
        2**hpz, np.arange(hp.nside2npix(2**hpz)), nest=nest, lonlat=True
    )

    return (lons + 180) % 360 - 180, lats


def get_weights(points, xi):
    """Compute interpolation weights."""
    tri = Delaunay(np.stack(points, axis=-1))  # Compute the triangulation
    targets = np.stack(xi, axis=-1)
    triangles = tri.find_simplex(targets)

    X = tri.transform[triangles, :2]
    Y = targets - tri.transform[triangles, 2]
    b = np.einsum("...jk,...k->...j", X, Y)
    weights = np.concatenate([b, 1 - b.sum(axis=-1)[..., np.newaxis]], axis=-1)
    src_idx = tri.simplices[triangles]
    valid = triangles >= 0

    return {"src_idx": src_idx, "weights": weights, "valid": valid}


def remap(var, src_idx, weights, valid):
    """Apply given interpolation weights."""
    return np.where(valid, (var[src_idx] * weights).sum(axis=-1), np.nan)


def healpix_dataset(dataset, zoom=7):
    grid_lon, grid_lat = get_latlon_grid(hpz=zoom)
    weight_kwargs = get_weights(
        points=(dataset.lon, dataset.lat), xi=(grid_lon, grid_lat)
    )

    ds_remap = (
        xr.apply_ufunc(
            remap,
            dataset,
            kwargs=weight_kwargs,
            input_core_dims=[["value"]],
            output_core_dims=[["cell"]],
            dask="parallelized",
            vectorize=True,
            output_dtypes=["f4"],
            dask_gufunc_kwargs={
                "output_sizes": {"cell": grid_lon.size},
            },
        )
        .chunk(
            {
                "time": 6,
                "cell": 4**7,
            }
        )
        .astype("float16")
    )

    for var in dataset:
        ds_remap[var].attrs = {
            "long_name": dataset[var].attrs["name"],
            "standard_name": dataset[var].attrs.get("cfName", ""),
            "units": dataset[var].attrs["units"],
            "type": "forecast" if dataset[var].attrs["dataType"] == "fc" else "analysis",
            "levtype": dataset[var].attrs["typeOfLevel"],
        }

    ds_remap["time"].attrs["axis"] = "T"

    ds_remap["level"].attrs = {
        "units": "hPa",
        "positive": "down",
        "standard_name": "air_pressure",
        "long_name": "Air pressure at model level",
        "axis": "Z",
    }

    ds_remap["crs"] = xr.DataArray(
        name="crs",
        data=[np.nan],
        dims=("crs",),
        attrs={
            "grid_mapping_name": "healpix",
            "healpix_nside": 2**zoom,
            "healpix_order": "nest",
        },
    )

    return ds_remap


def set_swift_token():
    regex = re.compile('setenv (.*) (.*)$')
    with open(pathlib.Path("~/.swiftenv").expanduser(), "r") as fp:
        for line in fp.readlines():
            if (m := regex.match(line)):
                k, v = m.groups()
                os.environ[k] = v


def main():
    now = datetime.datetime.now()
    fctime = get_latest_forecasttime(now)

    isostr = fctime.strftime("%Y-%m-%dT%HZ")
    outdir = pathlib.Path(f"/scratch/m/m300575/tmp/{isostr}")
    outdir.mkdir(parents=True, exist_ok=True)

    download_forecast(fctime, outdir=outdir)
    datasets = create_datasets(outdir=outdir)

    ecmwf = xr.open_mfdataset(datasets, engine="zarr")

    set_swift_token()
    urlpath = f"swift://swift.dkrz.de/dkrz_948e7d4bbfbb445fbff5315fc433e36a/data_ecmwf/{isostr}.zarr"
    healpix_dataset(ecmwf).to_zarr(urlpath)


if __name__ == "__main__":
    main()
