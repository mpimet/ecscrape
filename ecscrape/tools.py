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
        epilog="For more information on the IFS forecast product, see:\nhttps://www.ecmwf.int/en/forecasts/datasets/open-data",
    )
    parser.add_argument(
        "--time", "-t", type=str, default=None, help="IFS forecast time"
    )
    parser.add_argument(
        "--cachedir",
        "-c",
        type=Path,
        default="./",
        help="directory to store the downloaded GRIB files",
    )
    parser.add_argument(
        "--outdir",
        "-o",
        type=str,
        default="./",
        help="directory/object store to write the Zarr output store to",
    )
    parser.add_argument("--model", type=str, default="ifs", help="model system")
    parser.add_argument(
        "--stream",
        type=str,
        default="oper",
        choices=["oper", "enfo"],
        help="output stream",
    )
    parser.add_argument(
        "--vars",
        dest="variables",
        default=None,
        type=lambda s: s.split(","),
        help="variable list",
    )

    args = parser.parse_args()

    if args.time is None:
        now = datetime.datetime.now(datetime.timezone.utc)
        fctime = lib.get_latest_forecasttime(now)
    else:
        fctime = datetime.datetime.fromisoformat(args.time)

    if args.stream == "oper":
        stem = f"{fctime:%Y-%m-%dT%HZ}"
    else:
        stem = f"{fctime:%Y-%m-%dT%HZ}-{args.stream}"

    cache = args.cachedir / stem
    store = f"{args.outdir}/{stem}.zarr"

    # Download GRIB2 files into cache (and build indices)
    cache.mkdir(parents=True, exist_ok=True)
    lib.download_forecast(
        fctime,
        outdir=cache,
        model=args.model,
        stream=args.stream,
        grib_filter=VariableFilter(args.variables) if args.variables else None,
    )

    # Create reference filesystems from indices
    datasets = lib.create_datasets(
        outdir=cache,
        stream=args.stream,
    )

    # Merge datasets and convert to Zarr store
    ecmwf = xr.open_mfdataset(datasets, engine="zarr")
    lib.healpix_dataset(ecmwf).to_zarr(
        store,
        storage_options={"get_client": lib.get_client},
    )
