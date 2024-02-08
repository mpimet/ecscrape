#!/usr/bin/env python3
import datetime
import glob
import json
import re
import requests
import pathlib

import gribscan
import xarray as xr


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
        download_file(baseurl + relpath, outdir + filename)
        gribscan.write_index(outdir + filename, force=True)
        break


def create_datasets(outdir):
    datasets = gribscan.grib_magic(
        glob.iglob(outdir + "*.index"),
        magician=gribscan.magician.IFSMagician(),
        global_prefix=outdir,
    )

    for name, ref in datasets.items():
        with open(f"{outdir}/{name}.json", "w") as fp:
            json.dump(ref, fp)

    return [f"reference::{outdir}/{name}.json" for name in datasets.keys()]


def main():
    now = datetime.datetime.now()
    fctime = get_latest_forecasttime(now)

    isostr = fctime.strftime("%Y-%m-%dT%HZ")
    outdir = f"/scratch/m/m300575/tmp/{isostr}"

    download_forecast(fctime, outdir=outdir)
    datasets = create_datasets(outdir=outdir)

    return xr.open_mfdataset(datasets)
