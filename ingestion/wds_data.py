from statcan_wds import previewDimensions, getTableData
import pandas as pd


def pivot_column(df, index, col, value_col, aggfunc="first"):
    wide = (
        df.pivot_table(
            index=index,
            columns=col,
            values=value_col,
            aggfunc=aggfunc
        )
        .reset_index()
        .rename_axis(None, axis=1)
    )
    return wide


def get_legacy_fx_data(codes, startDate="2000-01-01", endDate="2016-12-31"):
    if codes is None:
        return None
    
    fx_df = None
    for code in codes:
        fx_url = f"https://www.bankofcanada.ca/valet/observations/{code}/csv?start_date={startDate}&end_date={endDate}"
        df = pd.read_csv(fx_url, skiprows=8)
        fx_df = fx_df.merge(df, on="date", how="inner") if fx_df is not None else df
    fx_df.columns = [codes.get(c, c) for c in fx_df.columns]
    return fx_df


def get_current_fx_data(codes, startDate="2017-01-01", skiprows=39):
    if codes is None:
        return None
    
    fx_url = f"https://www.bankofcanada.ca/valet/observations/group/FX_RATES_MONTHLY/csv?start_date={startDate}"
    df = pd.read_csv(fx_url, skiprows=skiprows)
    df = df[["date"] + list(codes.keys())]
    df.columns = [codes.get(c, c) for c in df.columns]
    return df


def get_fx_data(codes, startDate, endDate):
    legacy = get_legacy_fx_data(codes.get("legacy"), startDate, endDate)
    current = get_current_fx_data(codes.get("current"), startDate)
    fx_df = pd.concat([legacy, current], ignore_index=False)
    fx_df["date"] = pd.to_datetime(fx_df["date"])
    return fx_df


def get_labour_force_data(specs, startDate="2000-01-01", endDate="2025-12-31"):
    PID = 14100287
    raw_df = getTableData(pid=PID, series_specs=specs, startRefPeriod=startDate, endRefPeriod=endDate)
    df = pivot_column(df=raw_df, index=["Geography", "REF_DATE"], col="Labour force characteristics", value_col="VALUE")
    df = df.rename(columns={"REF_DATE": "date"})
    df["date"] = pd.to_datetime(df["date"])
    return df


def get_fuel_price_data(specs, startDate="2000-01-01", endDate="2025-12-31"):
    PID = 18100001
    raw_df = getTableData(pid=PID, series_specs=specs, startRefPeriod=startDate, endRefPeriod=endDate)
    df = pivot_column(df=raw_df, index=["Geography", "REF_DATE"], col="Type of fuel", value_col="VALUE")
    df = df.rename(columns={
        "REF_DATE": "date", 
        "Regular unleaded gasoline at self service filling stations": "Gasoline price",
        "Diesel fuel at self service filling stations": "Diesel price"
    })
    df["date"] = pd.to_datetime(df["date"])
    df = df.groupby(["date"], as_index=False).agg({"Gasoline price": "mean", "Diesel price": "mean"})
    df["Geography"] = "Canada"
    return df


def get_trade_data(specs, startDate="2000-01-01", endDate="2025-12-31"):
    CURRENT_PID = 12100168
    ARCHIVED_PID = 12100128
    # Helper function
    def fetch_data(specs, pid, startDate, endDate):
        raw_df = getTableData(pid=pid, series_specs=specs, startRefPeriod=startDate, endRefPeriod=endDate)
        df = pivot_column(df=raw_df, index=["Geography", "REF_DATE"], col="Trade", value_col="VALUE")
        df = df.rename(columns={"REF_DATE": "date"})
        df["date"] = pd.to_datetime(df["date"])
        return df

    archived = None
    current = None
    if pd.to_datetime(startDate) < pd.to_datetime("2017-01-01"):
        date = str(min(pd.to_datetime(endDate),  pd.to_datetime("2016-12-31")).date())
        archived = fetch_data(specs, pid=ARCHIVED_PID, startDate=startDate, endDate=date)
    if pd.to_datetime(endDate) >= pd.to_datetime("2017-01-01"):
        date = str(max(pd.to_datetime(startDate), pd.to_datetime("2017-01-01")).date())
        current = fetch_data(specs, pid=CURRENT_PID, startDate=date, endDate=endDate)
    
    trade_df = pd.concat([archived, current], ignore_index=True)
    
    return trade_df


def get_cpi_data(specs, startDate="2000-01-01", endDate="2025-12-31"):
    PID = 18100006
    raw_df = getTableData(pid=PID, series_specs=specs, startRefPeriod=startDate, endRefPeriod=endDate)
    df = pivot_column(df=raw_df, index=["Geography", "REF_DATE"], col="Products and product groups", value_col="VALUE")
    df = df.rename(columns={"REF_DATE": "date"})
    df["date"] = pd.to_datetime(df["date"])
    return df
