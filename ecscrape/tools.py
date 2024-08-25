import argparse
import datetime
import pathlib

import xarray as xr

from . import lib


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
        fctime = lib.get_latest_forecasttime(now)
    else:
        fctime = datetime.datetime.fromisoformat(args.time)

    outdir = pathlib.Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    lib.download_forecast(fctime, outdir=outdir)
    datasets = lib.create_datasets(outdir=outdir)

    ecmwf = xr.open_mfdataset(datasets, engine="zarr")

    lib.set_swift_token()
    lib.healpix_dataset(ecmwf).to_zarr(
        args.out,
        storage_options={"get_client": lib.get_client},
    )
