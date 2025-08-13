
#Based on python Code from amstat

"""
This will download all years of the flight data files as .parquet files
"""
import os

import urllib.request

from concurrent.futures import ThreadPoolExecutor


base_url = "https://blobs.duckdb.org/flight-data-partitioned/"

files = [f"Year={year}/data_0.parquet" for year in range(1987, 2025)]


def download_file(f):

    #this should make sure all files are deposited in the Data folder
    newf = os.path.join("Data", f)

    os.makedirs(os.path.dirname(newf), exist_ok=True)

    req = urllib.request.Request(base_url + f, headers={'User-Agent': 'Mozilla/5.0'})

    with urllib.request.urlopen(req) as response, open(newf, 'wb') as out_file:

        out_file.write(response.read())


with ThreadPoolExecutor() as executor:

    executor.map(download_file, files)