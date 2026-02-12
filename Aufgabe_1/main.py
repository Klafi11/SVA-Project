from __future__ import annotations
import os
import io
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tqdm import tqdm

from dotenv import load_dotenv

load_dotenv()


def station_url_for_day(d: date) -> str:
    """Constructs the URL for the station CSV file for a given date.
    Args:
        d (date): Concrete date for which to construct the station CSV URL
    Returns:
        str: URL to the station CSV file for the given date
    """
    return (
        "https://data.tankerkoenig.de/"
        "tankerkoenig-organization/tankerkoenig-data/raw/branch/master/"
        f"stations/{d:%Y}/{d:%m}/{d:%Y-%m-%d}-stations.csv"
    )


def make_session(username: str, password: str, max_workers: int) -> requests.Session:
    """Creates a requests session with retry logic and connection pooling.

    Args:
        username (str): Tankerkönig github Username
        password (str): Tankerkönig github Password
        max_workers (int): concurrent Workers

    Returns:
        requests.Session: Configured requests session with retry and connection pooling
    """
    session = requests.Session()
    session.auth = (username, password)

    retry = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        respect_retry_after_header=True,
        raise_on_status=False,
    )

    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=1,
        pool_maxsize=max_workers,
        pool_block=True,
    )
    session.mount("https://", adapter)
    return session


def fetch_day(d: date, session: requests.Session) -> tuple[date, pd.DataFrame]:
    """Fetches the station data for a specific date using the provided session.

    Args:
        d (date): Concrete date for which to fetch station data
        session (requests.Session): Session with retry and connection pooling configured

    Returns:
        tuple[date, pd.DataFrame]: Tuple containing the date and the corresponding station data as a DataFrame
    """
    url = station_url_for_day(d)
    resp = session.get(url, timeout=(5, 60))
    resp.raise_for_status()
    df = pd.read_csv(io.BytesIO(resp.content))
    df["day"] = pd.Timestamp(d)
    return d, df


def find_southernmost(df: pd.DataFrame) -> pd.Series:
    """Finds the southernmost petrol stations from the given DataFrame.

    Args:
        df (pd.DataFrame): DataFrame containing petrol station data with 'latitude' and 'longitude' columns

    Returns:
        pd.DataFrame: DataFrame containing the southernmost petrol stations
    """

    lat_col = "latitude"
    lon_col = "longitude"

    work = df.copy()
    work[lat_col] = pd.to_numeric(work[lat_col], errors="coerce")
    work[lon_col] = pd.to_numeric(work[lon_col], errors="coerce")

    work = work[
        work[lat_col].notna()
        & work[lon_col].notna()
        & ~((work["latitude"] == 0) & (work["longitude"] == 0))
    ]

    southest_lat = work[lat_col].drop_duplicates().nsmallest(3)

    southest_petrol_stations = (
        work[work[lat_col].isin(southest_lat)]
        .drop_duplicates(subset="uuid")
        .sort_values(by=lat_col)
    )

    southest_petrol_stations = southest_petrol_stations[
        ["uuid", "name", "brand", "street", "city", "latitude", "longitude"]
    ]

    return southest_petrol_stations


def main() -> None:
    """Main function to fetch station and price data, merge them, and find the southernmost petrol stations.

    Raises:
        RuntimeError: If the USERNAME or PASSWORD environment variables are not set.
    """
    username = os.getenv("USERNAME")
    password = os.getenv("PASSWORD")
    if not username or not password:
        raise RuntimeError("USERNAME and PASSWORD env vars must be set")

    start = date.today() - timedelta(days=1)
    days = [start - timedelta(days=i) for i in range(30)]  # last 30 days
    max_workers = 4

    results: list[tuple[date, pd.DataFrame]] = []
    errors: dict[date, Exception] = {}

    # Use a single session for all requests and a thread pool to fetch multiple days in parallel
    with make_session(username, password, max_workers) as session:
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            # dictionary to keep track of which future corresponds to which date key = future, value = date
            futs = {pool.submit(fetch_day, d, session): d for d in days}
            # iterate over the keys
            for fut in tqdm(as_completed(futs), total=len(futs), desc="Fetching days"):
                # day
                d = futs[fut]
                try:
                    # append to results
                    results.append(fut.result())
                except Exception as e:
                    errors[d] = e

    # Concatenate into one big DF
    big_df = pd.concat([df for _, df in results], ignore_index=True)

    # Find southernmost
    southern = find_southernmost(big_df)

    print("\nSouthernmost station row:")
    print(southern)
    print("\nErrors encountered:")

    if not errors:
        print("  None")
    else:
        for d, e in errors.items():
            print(f"  {d}: {e}")


if __name__ == "__main__":
    main()
