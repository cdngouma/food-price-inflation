# statcan_wds
Small, focused helper for retrieving Statistics Canada CODR/WDS data from Python.
- Inspect a table’s (cube’s) metadata
- Discover dimensions & members
- Build coordinates from human-readable specs
- Resolve coordinates to vector IDs
- Fetch time series over a reference-period range into a tidy pandas DataFrame

> WDS guide: https://www.statcan.gc.ca/en/developers/wds/user-guide

## Installation
```bash
pip install -e .
```
Requires a `pyproject.toml` (provided).

## Dependencies
- Python 3.9+
- `requests`, `pandas`

## Quick start
```python
from statcan_wds._core import previewDimensions, getTableData

PID = 18100006  # CPI 18-10-0006-01

# 1) Peek at dimensions
previewDimensions(PID, target="names")

# 2) Specify series
spec = [
    {"Geography": "Canada"},
    {"Products and product groups": ["All-items", "Food"]},
    {"Seasonal adjustment": "Seasonally adjusted"},
]

# 3) Fetch
df = getTableData(PID, spec, startRefPerid="2022-01-01", endRefPeriod="2025-06-01")
df["REF_DATE"] = pd.to_datetime(df["REF_DATE"])
print(df.head())
```

## Core concepts (WDS summary)
- productId (PID): numeric ID for a StatCan table/cube (e.g., `18100006` for 18-10-0006-01).
- dimension & member: non-time dimensions (e.g., Geography, Trade) with members (e.g., Canada, Imports).
- `coordinate`: dot-separated `memberIds`, one per dimension, ordered by `dimensionPositionId`, padded to 10 slots with `"0"` (e.g., `"1.2.0.0.0.0.0.0.0.0"`).
- `vectorId`: the unique series ID for one fixed combo of non-time dimensions (one coordinate --> one vector --> one time series).

## API (public surface)
`previewDimensions(pid, target="names", dimName=None)`
prints dimension names/positions, or member maps for a given dimension.

`target` can take the following values:
- `full`: returns the full `dimension` object including all `member`
- `names`: returns the dimension names as well as their position. For example:
```json
{
  'Geography': 1, 
  'Labour force characteristics': 2, 
  'Gender': 3, 
  'Age group': 4, 
  'Statistics': 5, 
  'Data type': 6
}
```
- `values`: When target is set to `values`, a valid `dimName` (dimension name) must also be provided. The function `previewDimensions` would then return all possible values for the dimension `dimName`. For example, if we pass `dimName="Geography"`, we get:
```json
{
  'Canada': 1, 
  'Newfoundland and Labrador': 2, 
  'Prince Edward Island': 3, 
  'Nova Scotia': 4, 
  'New Brunswick': 5, 
  'Quebec': 6, 
  'Ontario': 7, 
  'Manitoba': 8, 
  'Saskatchewan': 9, 
  'Alberta': 10, 
  'British Columbia': 11
}
```

`getTableData(pid, series_specs, startRefPerid, endRefPeriod) -> pandas.DataFrame`
- Expands series_specs (cartesian product),
- Builds WDS coordinates,
- Resolves to vector IDs,
- Fetches time series over the requested date range,
- Returns a tidy DataFrame with one row per (series, REF_DATE)

`series_specs` should have the following format format:

```python
[
  {"Geography": ["Quebec", "Canada"]},
  {"Trade": ["Import", "Domestic-exports"]},
  {"North American Product Classification System (NAPCS)": "All merchandise"},
  {"Principal trading partners": "All countries"},
]
```

Returned columns
- One column per selected dimension (ordered by the cube’s `dimensionPositionId`)
- `REF_DATE` (YYYY-MM-DD)
- `VALUE` (numeric)

### Error handling & troubleshooting
- HTTP 4xx/5xx: `requests.raise_for_status()` will raise; network/endpoint issues are surfaced immediately.
- WDS “SUCCESS” but invalid series: If a coordinate doesn’t map to a real series, WDS may return `vectorId = 0`. The module normalizes an “all invalid” batch to `None` in `getVectorIds`, and `getTableData` raises with a clear message.
- Common cause of invalid `coordinates`: Not selecting **one member per required dimension** (don’t assume there’s an “All/Total” with ID `0`; always read the cube’s metadata).

### Disclaimer
This project is not affiliated with Statistics Canada. APIs, endpoints, and table schemas can change; always validate against the official WDS user guide.