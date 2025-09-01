"""Module for extracting data from various sources."""

from typing import Dict

import requests
from pandas import DataFrame, read_csv, read_json, to_datetime


def get_public_holidays(public_holidays_url: str, year: str) -> DataFrame:
    """Get the public holidays for the given year for Brazil.
    Args:
        public_holidays_url (str): url to the public holidays.
        year (str): The year to get the public holidays for.
    Raises:
        SystemExit: If the request fails.
    Returns:
        DataFrame: A dataframe with the public holidays.
    """
    # Implementa esta función.
    # Debes usar la biblioteca requests para obtener los días festivos públicos del año dado.
    # La URL es public_holidays_url/{year}/BR.
    # Debes eliminar las columnas "types" y "counties" del DataFrame.
    # Debes convertir la columna "date" a datetime.
    # Debes lanzar SystemExit si la solicitud falla. Investiga el método raise_for_status
    # de la biblioteca requests.

    try:
        # Construct the URL for the specific year and country (Brazil)
        url = f"{public_holidays_url}/{year}/BR"

        # Make the HTTP request
        response = requests.get(url)

        # Check if the request was successful, raise exception if not
        response.raise_for_status()

        # Convert the JSON response to a DataFrame
        holidays_df = DataFrame(response.json())

        # Remove the "types" and "counties" columns if they exist
        columns_to_drop = ["types", "counties"]
        existing_columns_to_drop = [
            col for col in columns_to_drop if col in holidays_df.columns
        ]
        if existing_columns_to_drop:
            holidays_df = holidays_df.drop(columns=existing_columns_to_drop)

        # Convert the "date" column to datetime
        if "date" in holidays_df.columns:
            holidays_df["date"] = to_datetime(holidays_df["date"])

        return holidays_df

    except requests.RequestException as e:
        # If any request-related error occurs, raise SystemExit
        raise SystemExit(f"Failed to fetch public holidays: {e}") from e


def extract(
    csv_folder: str, csv_table_mapping: Dict[str, str], public_holidays_url: str
) -> Dict[str, DataFrame]:
    """Extract the data from the csv files and load them into the dataframes.
    Args:
        csv_folder (str): The path to the csv's folder.
        csv_table_mapping (Dict[str, str]): The mapping of the csv file names to the
        table names.
        public_holidays_url (str): The url to the public holidays.
    Returns:
        Dict[str, DataFrame]: A dictionary with keys as the table names and values as
        the dataframes.
    """
    dataframes = {
        table_name: read_csv(f"{csv_folder}/{csv_file}")
        for csv_file, table_name in csv_table_mapping.items()
    }

    holidays = get_public_holidays(public_holidays_url, "2017")

    dataframes["public_holidays"] = holidays

    return dataframes
