# ecScrape

This repository provides a Python script to scrape forecast data from the ECMWF.

The script downloads all GRIB files of the latest forecast, creates GRIB indices,
remaps onto the HEALPix grid, and stores the resulting dataset in a rechunked Zarr store.
