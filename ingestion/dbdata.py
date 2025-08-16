import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.postgresql import insert as pg_insert

import config
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def connect():
    return create_engine(config.DATABASE_URL)


def create_tables(engine, ddl_path):
    with open(ddl_path, "r", encoding="utf-8") as f, engine.begin() as conn:
        sql = f.read()
        print(sql)
        conn.exec_driver_sql(sql)


def make_on_conflict_do_nothing(conflict_cols):
    def _method(sqltable, conn, keys, data_iter):
        rows = [dict(zip(keys, row)) for row in data_iter]
        if not rows:
            return
        stmt = pg_insert(sqltable.table).values(rows)
        stmt = stmt.on_conflict_do_nothing(index_elements=conflict_cols)
        conn.execute(stmt)
    return _method


def load_data(engine, df, table_name, if_exists="append"):
    try:
        # Normalize 'date' to python date objects
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"]).dt.date
        
        # Decide conflict key based on the table's schema
        if "geography" in df.columns and "date" in df.columns:
            conflict_cols = ["geography", "date"]
        elif "date" in df.columns:
            conflict_cols = ["date"]
        else:
            conflict_cols = []

        logger.info(f"Inserting {len(df)} rows into {table_name}")

        df.to_sql(
            name=table_name, 
            con=engine,
            if_exists=if_exists, 
            index=False,
            method=make_on_conflict_do_nothing(conflict_cols) if conflict_cols else None
        )
        logger.info("Successfully loaded data")
    except Exception as e:
        logger.error(f"Error inserting data into {table_name}: {e}")
        raise e