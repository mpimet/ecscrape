import argparse
import datetime
from pathlib import Path

import xarray as xr

from . import lib


class VariableFilter:
    def __init__(self, variables):
        """Returns `True` if GRIB index contains variable from user defined list."""
        self.variables = variables

    def __call__(self, index):
        return index["param"] in self.variables


def main():
    parser = argparse.ArgumentParser(
        prog="ecScrape",
        description="Download, archive, remap, rechunk and store ECMWF forecasts.",
    )
    parser.add_argument("--time", "-t", type=str, default=None)
    parser.add_argument("--cache", "-c", type=Path)
    parser.add_argument("--store", "-s", type=Path)
    parser.add_argument("--model", type=str, default="ifs")
    parser.add_argument("--stream", type=str, default="oper", choices=["oper", "enfo"])
    parser.add_argument("--variables", default=None, type=lambda s: s.split(","))

    args = parser.parse_args()

    if args.time is None:
        now = datetime.datetime.now(datetime.timezone.utc)
        fctime = lib.get_latest_forecasttime(now)
    else:
        fctime = datetime.datetime.fromisoformat(args.time)

    # Download GRIB2 files into cache (and build indices)
    args.cache.mkdir(parents=True, exist_ok=True)
    lib.download_forecast(
        fctime,
        outdir=args.cache,
        model=args.model,
        stream=args.stream,
        grib_filter=VariableFilter(args.variables) if args.variables else None,
    )

    # Create reference filesystems from indices
    datasets = lib.create_datasets(
        outdir=args.cache,
        stream=args.stream,
    )

    # Merge datasets and convert to Zarr store
    ecmwf = xr.open_mfdataset(datasets, engine="zarr")
    lib.set_swift_token()
    lib.healpix_dataset(ecmwf).to_zarr(
        args.store,
        storage_options={"get_client": lib.get_client},
    )
