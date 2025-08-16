from statcan_wds import previewDimensions, getTableData
from wds_data import (
    get_fx_data, 
    get_labour_force_data, 
    get_fuel_price_data, 
    get_trade_data, get_cpi_data
)
from dbdata import connect, create_tables, load_data
import pandas as pd

import config
import logging
import argparse


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
        

def load_fx_data(engine, startDate, endDate, if_exists="replace"):
    logger.info("Fetching Foreign Exchange data from Bank of Canada")
    fx_data = get_fx_data(codes=config.FX_CODES, startDate=startDate, endDate=endDate)
    load_data(engine, fx_data, table_name="foreign_exchange")


def load_labour_data(engine, startDate, endDate, if_exists="replace"):
    logger.info("Fetching Labour Force Status data from StatCan")
    lfs_data = get_labour_force_data(specs=config.LFS_SPECS, startDate=startDate, endDate=endDate)
    load_data(engine, lfs_data, table_name="labour_force_status")


def load_fuel_data(engine, startDate, endDate, if_exists="replace"):
    logger.info("Fetching fuel price data from StatCan")
    fuel_price_data = get_fuel_price_data(specs=config.FUEL_PRICE_SPECS, startDate=startDate, endDate=endDate)
    load_data(engine, fuel_price_data, table_name="fuel_price")
    

def load_trade_data(engine, startDate, endDate, if_exists="replace"):
    logger.info("Fetching trade data from StatCan")
    trade_data = get_trade_data(specs=config.TRADE_SPECS, startDate=startDate, endDate=endDate)
    load_data(engine, trade_data, table_name="trade_index")


def load_cpi_data(engine, startDate, endDate, if_exists="replace"):
    logger.info("Fetching food CPI data from StatCan")
    food_cpi_data = get_cpi_data(specs=config.CPI_SPECS, startDate=startDate, endDate=endDate)
    load_data(engine, food_cpi_data, table_name="food_cpi")


def ingest_data(mode="update", ddl_path="schema.sql", startDate="2000-01-01", endDate="2025-12-31"):
    if mode != "create" and mode != "update":
        raise ValueError(f"Expected 'create' or 'update' but received: '{mode}'")
    
    engine = connect()

    if mode == "create":
        logger.info(f"Creating tables using {ddl_path}")
        create_tables(engine, ddl_path)
        logger.info("Tables created!")
    
    load_fx_data(engine, startDate, endDate)
    load_labour_data(engine, startDate, endDate)
    load_fuel_data(engine, startDate, endDate)
    load_trade_data(engine, startDate, endDate)
    load_cpi_data(engine, startDate, endDate)

    logger.info("Data ingestion completed successfully!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        choices=["create", "update"],
        required=True,
        default="create",
        help="Operation mode: 'create' or 'update'."
    )

    parser.add_argument(
        "--ddl",
        required=True,
        default="schema.sql",
        dest="ddl_path",
        help="Operation mode: 'create' or 'update'."
    )

    parser.add_argument(
        "--start_date",
        default="2025-01-01",
        dest="startDate",
        help="Start date in YYYY-MM-DD format (default: 2025-01-01)."
    )

    parser.add_argument(
        "--end_date",
        default="2025-12-31",
        dest="endDate",
        help="End date in YYYY-MM-DD format (default: 2025-12-31)."
    )

    args = parser.parse_args()

    ingest_data(args.mode, args.ddl_path, args.startDate, args.endDate)
