from statcan_wds import previewDimensions, getTableData
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from wds_data import get_fx_data, get_labour_force_data, get_fuel_price_data, get_trade_data, get_cpi_data
import config
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def to_db(engine, df, table_name, if_exists="replace"):
    try:
        logger.info(f"Inserting {len(fx_data)} rows into {table_name}")
        df.to_sql(name="foreign_exchange", con=engine, if_exists=if_exists, index=False)
        logger.info("Successfully loaded data")
    except:
        logger.error(f"Error inserting data into '{table_name}': {e}")
        

def load_fx_data(engine, startDate, endDate, if_exists="replace"):
    logger.info("Fetching Foreign Exchange data from Bank of Canada")
    fx_data = get_fx_data(codes=config.FX_CODES, startDate=startDate, endDate=endDate)
    to_db(engine, fx_data, table_name="foreign_exchange", if_exists=if_exists)


def load_labour_data(engine, startDate, endDate, if_exists="replace"):
    logger.info("Fetching Labour Force Status data from StatCan")
    lfs_data = get_labour_force_data(specs=config.LFS_SPECS, startDate=startDate, endDate=endDate)
    to_db(engine, lfs_data, table_name="labour_force_status", if_exists=if_exists)


def load_fuel_data(engine, startDate, endDate, if_exists="replace"):
    logger.info("Fetching fuel price data from StatCan")
    fuel_price_data = get_fuel_price_data(specs=config.FUEL_PRICE_SPECS, startDate=startDate, endDate=endDate)
    to_db(engine, fuel_price_data, table_name="fuel_price", if_exists=if_exists)
    

def load_trade_data(engine, startDate, endDate, if_exists="replace"):
    logger.info("Fetching trade data from StatCan")
    trade_data = get_trade_data(specs=config.TRADE_SPECS, startDate=startDate, endDate=endDate)
    to_db(engine, trade_data, table_name="trade_index", if_exists=if_exists)


def load_cpi_data(engine, startDate, endDate, if_exists="replace"):
    logger.info("Fetching food CPI data from StatCan")
    food_cpi_data = get_cpi_data(specs=config.CPI_SPECS, startDate=startDate, endDate=endDate)
    to_db(engine, food_cpi_data, table_name="food_cpi", if_exists=if_exists)


def ingest_data(mode="update", startDate="2000-01-01", endDate="2025-12-31"):
    if mode == "new":
        if_exists = "replace"
    elif mode == "update":
        if_exists = "append"
    else:
        raise ValueError(f"Expected 'new' or 'update' but received: '{mode}'")

    engine = create_engine(config.DATABASE_URL)
    
    load_fx_data(engine, startDate, endDate, if_exists)
    load_labour_data(engine, startDate, endDate, if_exists)
    load_fuel_data(engine, startDate, endDate, if_exists)
    load_trade_data(engine, startDate, endDate, if_exists)
    load_cpi_data(engine, startDate, endDate, if_exists)


if __name__ == "__main__":
    ingest_data(mode, startDate, endDate)
