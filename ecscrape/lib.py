import datetime
import json
import re
import requests
from requests_toolbelt.multipart import decoder

import fsspec
import gribscan
import healpix as hp
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


def parse_gribindex(content):
    """Parse a JSON lines GRIB2 index."""
    return [json.loads(line) for line in content.decode().splitlines()]


def gribindex2range(index):
    """Convert a GRIB2 index entry into a byte range."""
    start = index["_offset"]
    length = index["_length"]

    return f"{start}-{start + length - 1}"


def get_headers(indices):
    """Convert a list of GRIB2 indices into a byte range request."""
    return {"Range": "bytes=" + ", ".join([gribindex2range(s) for s in indices])}


def download_grib2(urlpath, outfile, mode="wb", grib_filter=None):
    if grib_filter is None:
        r = requests.get(urlpath)
        content = r.content
    else:
        with fsspec.open(urlpath.replace(".grib2", ".index")) as fp:
            index = parse_gribindex(fp.read())

        index = [i for i in index if grib_filter(i)]
        if len(index) == 0:
            # Skip download if no variable is requested
            raise ValueError("No variables to download.")

        headers = get_headers([i for i in index if grib_filter(i)])
        r_multipart = requests.get(urlpath, headers=headers)

        content = b"".join(
            p.content for p in decoder.MultipartDecoder.from_response(r_multipart).parts
        )

    with open(outfile, mode=mode) as fp:
        fp.write(content)


def download_forecast(
    fctime, outdir, model="ifs", resol="0p25", stream="oper", grib_filter=None
):
    baseurl = "https://data.ecmwf.int"
    date, hour = fctime.strftime("%Y%m%d"), fctime.strftime("%H")

    urlpath = f"{baseurl}/forecasts/{date}/{hour}z/{model}/{resol}/{stream}/"
    check_urlpath(urlpath)

    for relpath, filename in get_griblist(urlpath):
        try:
            download_grib2(
                urlpath=f"{baseurl}{relpath}",
                outfile=outdir / filename,
                grib_filter=grib_filter,
            )
        except ValueError:
            continue
        else:
            gribscan.write_index(
                gribfile=f"{outdir}/./{filename}",
                force=True,
            )


def create_datasets(outdir, stream="oper"):
    if stream == "enfo":
        magician = gribscan.magician.EnsembleMagician()
    else:
        magician = gribscan.magician.IFSMagician()

    datasets = gribscan.grib_magic(
        outdir.glob("*.index"),
        magician=magician,
        global_prefix=outdir.resolve(),
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

    if "level" in ds_remap.dims:
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
