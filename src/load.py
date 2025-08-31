"""Module for loading data into databases."""

from typing import Dict

from pandas import DataFrame
from sqlalchemy.engine.base import Engine


def load(data_frames: Dict[str, DataFrame], database: Engine, if_exists: str = "replace"):
    """Load the dataframes into the sqlite database.

    Args:
        data_frames (Dict[str, DataFrame]): A dictionary with keys as the table names
        and values as the dataframes.
        database (Engine): SQLAlchemy database engine.
    """
    # Implementa esta función. Por cada DataFrame en el diccionario, debes
    # usar pandas.DataFrame.to_sql() para cargar el DataFrame en la base de datos
    # como una tabla.
    # Para el nombre de la tabla, utiliza las claves del diccionario `data_frames`.

    for table_name, dataframe in data_frames.items():
        print(f"table: {table_name}, rows: {dataframe.count()}")
        dataframe.to_sql(
            name=table_name, con=database, if_exists=if_exists, index=False
        )
