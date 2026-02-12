from __future__ import annotations
import os
import io
import requests
import pandas as pd
from datetime import date, timedelta
from dotenv import load_dotenv

# load environment variables from .env file
load_dotenv()


def get_df(
    url: str, username: str, password: str, session: requests.Session
) -> pd.DataFrame:
    """Fetches a CSV file from the given URL using the provided session and credentials, and returns it as a DataFrame.

    Raises:
        RuntimeError: If there is an HTTP error during the request or if CSV parsing fails.

    Returns:
        DataFrame: DataFrame containing the CSV data
    """
    try:
        response = session.get(url, auth=(username, password), timeout=60)
        response.raise_for_status()
    except requests.RequestException as e:
        raise RuntimeError(f"HTTP error while fetching {url}") from e

    try:
        return pd.read_csv(io.StringIO(response.text))
    except Exception as e:
        raise RuntimeError(f"CSV parsing failed for {url}") from e


def main() -> None:
    """Main function to fetch station and price data, merge them, and find the southernmost petrol stations.

    Raises:
        RuntimeError: If the USERNAME or PASSWORD environment variables are not set.
    """
    username = os.getenv("USERNAME")
    password = os.getenv("PASSWORD")

    if not username or not password:
        raise RuntimeError("USERNAME/PASSWORD env vars are not set")

    d = date.today() - timedelta(days=2)  # vorgestern timedelta (days=2)

    url_station = (
        f"https://data.tankerkoenig.de/"
        f"tankerkoenig-organization/tankerkoenig-data/raw/branch/master/"
        f"stations/{d:%Y}/{d:%m}/{d:%Y-%m-%d}-stations.csv"
    )
    url_price = (
        f"https://data.tankerkoenig.de/"
        f"tankerkoenig-organization/tankerkoenig-data/raw/branch/master/"
        f"prices/{d:%Y}/{d:%m}/{d:%Y-%m-%d}-prices.csv"
    )

    # shared session
    with requests.Session() as session:
        stations_data = get_df(url_station, username, password, session)
        prices_data = get_df(url_price, username, password, session)

    # print(stations_data.info())
    # print(prices_data.info())

    # Join the station and price data on the station UUID, filter for valid diesel prices, and find the southernmost stations with the lowest diesel prices
    df = pd.merge(
        stations_data[["uuid", "name", "brand", "post_code", "city"]],
        prices_data[["date", "station_uuid", "diesel"]],
        left_on="uuid",
        right_on="station_uuid",
        how="inner",
    )

    df = df[df["diesel"] > 0]

    min_diesels = df["diesel"].drop_duplicates().nsmallest(3)

    min_df = df[df["diesel"].isin(min_diesels)]

    print(min_df)
    print(min_df.info())


if __name__ == "__main__":
    main()
