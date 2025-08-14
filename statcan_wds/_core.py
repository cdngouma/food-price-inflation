"""
Thin client around StatCan's WDS API (CODR). Utilities to:
- inspect cube metadata
- preview dimensions/members
- expand human-friendly series specs into coordinates
- resolve coordinates to vector IDs
- fetch time-series data over a date range for multiple vectors

Design:
- A "coordinate" is a dot-joined chain of memberIds (one per non-time dimension,
  ordered by dimensionPositionId, padded to 10 slots with '0').
- A "vector" is a single time series (fixed non-time dims) identified by vectorId.

NOTE: This module prints in a few places (preview, debug). Consider returning data
structures instead for library-style usage.
"""

import requests
import pandas as pd
from itertools import product


BASE = "https://www150.statcan.gc.ca/t1/wds/rest"


def getCubeMetadata(pid):
    """
    Fetch cube (table) metadata for a given productId (PID).

    Parameters
    ----------
    pid : int or str
        StatCan productId (e.g., 18100006 for CPI 18-10-0006-01).

    Returns
    -------
    dict
        The "object" field from WDS metadata containing dimensions, members, etc.

    Raises
    ------
    HTTPError
        If HTTP status is not 2xx.
    ValueError
        If the payload structure is unexpected.
    RuntimeError
        If WDS returns non-SUCCESS at the item level.
    """
    url = f"{BASE}/getCubeMetadata"
    payload = [{"productId": pid}]
    response = requests.post(url, json=payload)
    response.raise_for_status()  # surface non-2xx HTTP errors immediately

    data = response.json()
    if not isinstance(data, list) or not data:
        raise ValueError(f"Unexpected payload: {data}")

    item = data[0]
    if item.get("status") != "SUCCESS":
        # WDS-level error even though HTTP was 200
        raise RuntimeError(f"WDS error: {item.get('status')} | {item.get('object')}")
    return item["object"]


def previewDimensions(pid, target="names", dimName=None):
    """
    Quick console preview of a cube's dimensions.

    Parameters
    ----------
    pid : int or str
        productId of the table.
    target : {"names", "values", "full"}
        - "names": prints {dimensionName: position}
        - "values": prints {memberName: memberId} for a given dimName
        - "full": prints full nested mapping {dimName: {position, values{...}}}
    dimName : str or None
        Required when target="values"; the English dimension name to inspect.

    Notes
    -----
    This function prints for exploration; it does not return data structures.
    """
    meta = getCubeMetadata(pid)

    # Build a mapping from dimension English name -> position + {memberName: memberId}
    dimensions = {
        dim["dimensionNameEn"]: {
            "position": dim["dimensionPositionId"],
            "values": {m["memberNameEn"]: m["memberId"] for m in dim["member"]},
        }
        for dim in meta["dimension"]
    }

    if target == "full":
        return dimensions
    elif target == "names":
        return {dim: dimensions[dim].get("position") for dim in dimensions}
    elif target == "values":
        if dimName is None or dimName not in dimensions.keys():
            # NOTE: message reads oddly; probably meant "is NOT a valid dimension."
            raise ValueError(f"{dimName} is a valid dimension.")
        for name, dim in dimensions.items():
            if name == dimName:
                return dim["values"]


def buildCoordinates(pid, series_coords):
    """
    Map human-readable series specs to WDS coordinates.

    Parameters
    ----------
    pid : int or str
        productId of the table.
    series_coords : list[dict]
        Each dict selects exactly one member (by *English name*) per dimension you care about.
        Example item: {"Geography": "Canada", "Trade": "Imports", ...}

    Returns
    -------
    (list[str], dict)
        - coordinates: list of 10-slot coordinate strings (dot-joined memberIds)
        - dim_map: {dimensionNameEn: positionId} for sorting/index alignment later

    Notes
    -----
    - Dimensions not specified in a series dict remain '0' in the coordinate slot.
      If the cube requires a specific member (i.e., no valid 'All/Total'), you will
      later get vectorId=0 from WDS for that coordinate.
    """
    # Get table metadata (dimensions + members)
    meta = getCubeMetadata(pid)

    # Build dimension mapping: {dimName -> {position, values{name -> id}}}
    dim_mapping = {
        dim["dimensionNameEn"]: {
            "position": dim["dimensionPositionId"],
            "values": {m["memberNameEn"]: m["memberId"] for m in dim["member"]},
        }
        for dim in meta["dimension"]
    }

    # Map a dict like {"Geography":"Canada", ...} to a 10-part coordinate string
    coordinates = []
    for series in series_coords:
        coords = ["0"] * 10  # WDS uses up to 10 non-time dimension slots; pad with '0'
        for k, v in series.items():
            dim = dim_mapping.get(k)
            if dim is None:
                # Dimension name not found in this cube
                print(f"Warning: Could not locate dimension '{k}'")
                coords = None
                break
            pos = dim["position"]  # 1-based index
            value = dim["values"].get(v)
            if value is None:
                # Member name not found in this dimension
                print(f"Warning: No '{v}' found for the dimension '{k}'")
                coords = None
                break
            coords[int(pos) - 1] = str(value)  # place memberId in the correct slot
        if coords:
            coordinates.append(".".join(coords))

    # dim_map helps later to order index columns by the cube's dimension positions
    return coordinates, {dim: int(v["position"]) for dim, v in dim_mapping.items()}


def expand_specs(series_spec):
    """
    Expand a compact spec into the cartesian product of choices.

    Parameters
    ----------
    series_spec : list[dict]
        Each dict has exactly one key (dimension name) and either a scalar or list of values.
        Example:
            [
              {"Geography": ["Quebec", "Canada"]},
              {"Trade": ["Import", "Domestic-exports"]},
              {"NAPCS": "All merchandise"}
            ]

    Returns
    -------
    list[dict]
        One dict per combination, e.g.
        [{"Geography":"Quebec","Trade":"Import","NAPCS":"All merchandise"}, ...]
    """
    pairs = []
    for dim in series_spec:
        (k, v), = dim.items()
        vals = v if isinstance(v, (list, tuple)) else [v]
        pairs.append((k, list(vals)))
    keys = [k for k, _ in pairs]
    value_lists = [vals for _, vals in pairs]
    return [dict(zip(keys, combo)) for combo in product(*value_lists)]


def getVectorIds(pid, coords):
    """
    Resolve coordinates to vector IDs (one vector == one time series).

    Parameters
    ----------
    pid : int or str
        productId of the table.
    coords : list[str]
        WDS coordinate strings (dot-joined memberIds).

    Returns
    -------
    dict or None
        {vectorId: SeriesTitleEn}, or None if WDS indicates invalid combinations.

    Notes
    -----
    - If the cube/series combo is invalid, WDS may return vectorId=0; this function
      normalizes that to None for easier error handling upstream.
    """
    # Translate coordinates to vectors via WDS
    if len(coords) > 0:
        vec_payload = [{"productId": pid, "coordinate": c} for c in coords]
    else:
        raise Exception("Invalid coordinates. Please specify all required dimensions")

    series = requests.post(
        f"{BASE}/getSeriesInfoFromCubePidCoord", json=vec_payload
    ).json()

    # Build {vectorId: title}; relies on WDS response structure
    vec_map = { s["object"]["vectorId"]: s["object"]["SeriesTitleEn"] for s in series if s["object"]["vectorId"] != 0 }

    # All-invalid case: a single item with vectorId 0 (common WDS pattern)
    if not vec_map:
        raise Exception(f"Failed to retrieve vectors for coordinates: {coords}. Please specify all required dimensions")

    return vec_map


def getTableData(pid, series_specs, startRefPeriod="2000-01-01", endRefPeriod="2025-12-31"):
    """
    Fetch data for multiple series over a reference-period range and return a tidy DataFrame.

    Parameters
    ----------
    pid : int or str
        productId of the table.
    series_specs : list[dict]
        Compact specification used by `expand_specs`, e.g.
        [
          {"Geography": ["Quebec", "Canada"]},
          {"Trade": ["Import", "Domestic-exports"]},
          {"North American Product Classification System (NAPCS)": "Farm..."},
          {"Principal trading partners": "All countries"}
        ]
    startRefPeriod : str
        Start of reference period range (YYYY-MM-DD).
    endRefPeriod : str
        End of reference period range (YYYY-MM-DD).

    Returns
    -------
    pandas.DataFrame
        Columns:
          - one column per chosen dimension (as index columns)
          - REF_DATE (reference period)
          - VALUE (numeric observation)

    Notes
    -----
    - This uses a multi-vector range endpoint with query parameters (vectorIds=...).
      Make sure the endpoint matches current WDS docs for multi-series range retrieval.
    - Series titles from WDS are split on ';' and aligned to dimension columns ordered
      by actual dimensionPositionId (via dim_map).
    """
    # 1) Expand cartesian product of user specs to per-series human dicts
    expanded_specs = expand_specs(series_specs)

    # 2) Build WDS coordinates for those dicts; also get {dimName: position}
    coords, dim_map = buildCoordinates(pid, expanded_specs)

    # 3) Resolve coordinates to vector IDs (+ readable titles)
    vec_map = getVectorIds(pid, coords)

    # 4) Fetch all vectors across the requested reference-period range
    # Build a CSV of quoted vectorIds per current API style
    vectorIds = ",".join([f'"{v}"' for v in vec_map.keys()])
    series = requests.get(
        f"{BASE}/getDataFromVectorByReferencePeriodRange"
        f"?vectorIds={vectorIds}&startRefPeriod={startRefPeriod}&endReferencePeriod={endRefPeriod}"
    ).json()

    final_df = None

    # 5) For each series payload, construct a small DataFrame and accumulate
    for s in series:
        vId = s["object"].get("vectorId")

        # Determine the order of dimension columns:
        # start from the user-specified dimension names...
        index_cols = [list(spec.keys())[0] for spec in series_specs]
        # ...then sort them by the cube's dimensionPositionId to match WDS title order
        index_cols.sort(key=lambda col: dim_map.get(col, float("inf")))

        # WDS series title encodes selected members separated by ';'
        # We align those parts with our sorted dimension columns.
        index_vals = vec_map[vId].split(";")
        row_index = {k: v for k, v in zip(index_cols, index_vals) if k in dim_map.keys()}

        # Extract datapoints for this vector into rows
        dataPoints = s["object"].get("vectorDataPoint")
        rows = []
        for pt in dataPoints:
            value = pt["value"]
            ref_date = pt["refPer"]
            row = row_index | {"REF_DATE": ref_date, "VALUE": value}
            rows.append(row)

        df = pd.DataFrame(rows)
        final_df = pd.concat([final_df, df], ignore_index=True)

    return final_df
