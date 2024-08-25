#!/usr/bin/env python3
import argparse
import datetime
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
from easygems.remap import compute_weights_delaunay, apply_weights


def get_latest_forecasttime(dt):
    """Return the most recent ECMWF forecast time for a given datetime object."""
    return datetime.datetime(dt.year, dt.month, dt.day, dt.hour // 12 * 12)


def check_urlpath(urlpath):
    """Check if urlpath exists."""
    if requests.get(urlpath).status_code != 200:
        raise Exception(f"Forecast not availablae at: {urlpath}")


def get_griblist(urlpath):
    """Yield relative paths of all GRIB2 files in a ECMWF forecast."""
    for line in requests.get(urlpath).text.split("\n"):
        regex = re.compile(r'<a href="(.*)">(.*\.grib2)</a>')
        if m := regex.match(line):
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

    # On 2024-02-29 ECMWF introduced an additional level to distinguish
    # the classical and an AI forecast.
    try:
        urlpath = f"{baseurl}/forecasts/{date}/{hour}z/0p25/oper/"
        check_urlpath(urlpath)
    except Exception:
        urlpath = f"{baseurl}/forecasts/{date}/{hour}z/ifs/0p25/oper/"
        check_urlpath(urlpath)

    for relpath, filename in get_griblist(urlpath):
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


def bitround(ds, keepbits=13, codec=None):
    def _bitround(var, keepbits, codec=None):
        if codec is None:
            codec = numcodecs.BitRound(keepbits=keepbits)

        return codec.decode(codec.encode(var))

    ds_rounded = xr.apply_ufunc(
        _bitround,
        ds,
        kwargs={"keepbits": keepbits},
        dask="parallelized",
    )
    for var in ds:
        ds_rounded[var].attrs = ds[var].attrs

    return ds_rounded


def healpix_dataset(dataset, zoom=7):
    grid_lon, grid_lat = get_latlon_grid(hpz=zoom)
    weight_kwargs = compute_weights_delaunay(
        points=(dataset.lon, dataset.lat), xi=(grid_lon, grid_lat)
    )

    ds_remap = (
        xr.apply_ufunc(
            apply_weights,
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
        .pipe(bitround)
    )

    for var in dataset:
        ds_remap[var].attrs = {
            "long_name": dataset[var].attrs["name"],
            "standard_name": dataset[var].attrs.get("cfName", ""),
            "units": dataset[var].attrs["units"],
            "type": "forecast"
            if dataset[var].attrs["dataType"] == "fc"
            else "analysis",
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
    regex = re.compile("setenv (.*) (.*)$")
    with open(pathlib.Path("~/.swiftenv").expanduser(), "r") as fp:
        for line in fp.readlines():
            if m := regex.match(line):
                k, v = m.groups()
                os.environ[k] = v


async def get_client(**kwargs):
    import aiohttp
    import aiohttp_retry

    retry_options = aiohttp_retry.ExponentialRetry(
        attempts=3, exceptions={OSError, aiohttp.ServerDisconnectedError}
    )
    retry_client = aiohttp_retry.RetryClient(
        raise_for_status=False, retry_options=retry_options
    )
    return retry_client


def main():
    parser = argparse.ArgumentParser(
        prog="ecScrape",
        description="Download, archive, remap, rechunk and store ECMWF forecasts.",
    )
    parser.add_argument("--time", "-t", type=str, default=None)
    parser.add_argument("--cache", "-c", type=str)
    parser.add_argument("--out", "-o", type=str)

    args = parser.parse_args()

    if args.time is None:
        now = datetime.datetime.now()
        fctime = get_latest_forecasttime(now)
    else:
        fctime = datetime.datetime.fromisoformat(args.time)

    outdir = pathlib.Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    download_forecast(fctime, outdir=outdir)
    datasets = create_datasets(outdir=outdir)

    ecmwf = xr.open_mfdataset(datasets, engine="zarr")

    set_swift_token()
    healpix_dataset(ecmwf).to_zarr(
        args.out,
        storage_options={"get_client": get_client},
    )


if __name__ == "__main__":
    main()
