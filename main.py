def main():
    # Importing the necessary libraries
    from pandas import DataFrame
    from sqlalchemy import create_engine
    from typing import Dict
    from pathlib import Path

    from src.transform import QueryEnum
    from src import config
    from src.transform import run_queries
    from src.extract import extract, get_public_holidays
    from src.load import load


    # Create the database sql file
    Path(config.SQLITE_BD_ABSOLUTE_PATH).touch()

    # Create the database connection
    ENGINE = create_engine(rf"sqlite:///{config.SQLITE_BD_ABSOLUTE_PATH}", echo=False)

    csv_folder = config.DATASET_ROOT_PATH
    public_holidays_url = config.PUBLIC_HOLIDAYS_URL

    # 1. Get the mapping of the csv files to the table names.
    csv_table_mapping = config.get_csv_to_table_mapping()

    # # 2. Extract the data from the csv files, holidays and load them into the dataframes.
    load(data_frames=csv_dataframes, database=ENGINE)


if __name__ == "__main__":
    main()
