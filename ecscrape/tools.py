import argparse
import datetime
from pathlib import Path

import xarray as xr

from . import lib


def main():
    parser = argparse.ArgumentParser(
        prog="ecScrape",
        description="Download, archive, remap, rechunk and store ECMWF forecasts.",
    )
    parser.add_argument("--time", "-t", type=str, default=None)
    parser.add_argument("--cache", "-c", type=Path)
    parser.add_argument("--store", "-s", type=Path)

    args = parser.parse_args()

    if args.time is None:
        now = datetime.datetime.now(datetime.timezone.utc)
        fctime = lib.get_latest_forecasttime(now)
    else:
        fctime = datetime.datetime.fromisoformat(args.time)

    # Download GRIB2 files into cache (and build indices)
    args.cache.mkdir(parents=True, exist_ok=True)
    lib.download_forecast(fctime, outdir=args.cache)

    # Create reference filesystems from indices
    datasets = lib.create_datasets(outdir=args.cache)

    # Merge datasets and convert to Zarr store
    ecmwf = xr.open_mfdataset(datasets, engine="zarr")
    lib.set_swift_token()
    lib.healpix_dataset(ecmwf).to_zarr(
        args.store,
        storage_options={"get_client": lib.get_client},
    )
