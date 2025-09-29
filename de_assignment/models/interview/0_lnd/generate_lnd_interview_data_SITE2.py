import math
import random
from datetime import datetime, timedelta
from typing import Dict, Iterable, Iterator, List, Literal, Tuple, Mapping, Sequence


def _parse_iso_datetime(value: str) -> datetime:
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return datetime.fromisoformat(value + " 00:00:00")


def _time_step_for(granularity: Literal["minute", "hour", "day"]) -> timedelta:
    if granularity == "minute":
        return timedelta(minutes=1)
    if granularity == "hour":
        return timedelta(hours=1)
    if granularity == "day":
        return timedelta(days=1)
    raise ValueError("granularity must be one of: minute, hour, day")


def _iter_datetimes(start: datetime, end: datetime, step: timedelta) -> Iterator[datetime]:
    current = start
    while current <= end:
        yield current
        current = current + step


def _generate_value(ts: datetime, min_value: float, max_value: float, rng: random.Random) -> float:
    mid = (min_value + max_value) / 2.0
    span = (max_value - min_value)
    if span <= 0:
        return mid
    amplitude = span * 0.35
    minutes_in_day = ts.hour * 60 + ts.minute
    phase = 2.0 * math.pi * (minutes_in_day / 1440.0)
    noise = rng.uniform(-span * 0.02, span * 0.02)
    value = mid + amplitude * math.sin(phase) + noise
    return max(min_value, min(max_value, value))


def _normalize_datapoints(s: Dict[str, Iterable[float]]) -> Dict[str, Tuple[float, float]]:
    normalized: Dict[str, Tuple[float, float]] = {}
    for name, rng_vals in s.items():
        vals = list(rng_vals)
        if len(vals) != 2:
            raise ValueError(f"datapoint '{name}' must have exactly two values [min, max]")
        mn = float(vals[0])
        mx = float(vals[1])
        if mn >= mx:
            raise ValueError(f"datapoint '{name}' min must be < max")
        normalized[name] = (mn, mx)
    return normalized


def build_dataframe(session,
                    params: Mapping,
                    column_aliases: Mapping[str, str],
                    default_datapoints: Mapping[str, Sequence[float]],
                    require_asset_types: bool = False):
    start = _parse_iso_datetime(str(params.get("start", "2025-01-01")))
    end = _parse_iso_datetime(str(params.get("end", "2025-01-10")))
    granularity: Literal["minute", "hour", "day"] = str(params.get("granularity", "hour")).lower()

    customer = str(params.get("customer", "CG"))
    site = str(params.get("site", "TEST"))

    raw_datapoints = params.get("datapoints", default_datapoints)
    datapoints_in: Dict[str, Iterable[float]] = dict(raw_datapoints)

    value_column = str(params.get("value_col", "metric_value"))
    gaps = float(params.get("gaps", 0.0))
    seed_val = params.get("seed", None)
    seed_int = int(seed_val) if seed_val is not None else None

    if start > end:
        raise ValueError("start must be <= end")

    # Asset list handling
    asset_pairs: List[Tuple[str, str]] = []  # (asset_type, asset_id)
    asset_type_map = params.get("asset_types")
    if require_asset_types and (not isinstance(asset_type_map, dict) or not asset_type_map):
        raise ValueError("interview_params.asset_types (dict of asset_type -> [asset_ids]) is required")

    if isinstance(asset_type_map, dict) and asset_type_map:
        for atype, ids in asset_type_map.items():
            if not isinstance(ids, (list, tuple)):
                raise ValueError("Each entry in asset_types must be a list of asset_ids")
            for asset_id in (ids or []):
                asset_pairs.append((str(atype), str(asset_id)))
    else:
        raw_assets = params.get("asset_ids")
        if not raw_assets:
            raise ValueError("Provide either asset_types mapping or asset_ids list")
        if isinstance(raw_assets, str):
            asset_ids: List[str] = [a.strip() for a in raw_assets.split(",") if a.strip()]
        else:
            asset_ids = list(raw_assets)
        for aid in asset_ids:
            inferred_type = (aid.split("-")[0] or "GEN") if "-" in aid else "GEN"
            asset_pairs.append((inferred_type, aid))

    if not asset_pairs:
        raise ValueError("No assets provided")

    datapoints = _normalize_datapoints(datapoints_in)
    rng = random.Random(seed_int)
    step = _time_step_for(granularity)

    rows: List[Dict[str, str]] = []
    for asset_type, asset_id in asset_pairs:
        for datapoint_name, (mn, mx) in datapoints.items():
            for ts in _iter_datetimes(start, end, step):
                value = _generate_value(ts, mn, mx, rng)
                rows.append({
                    "customer": customer,
                    "site": site,
                    "asset_type": asset_type,
                    "asset_id": asset_id,
                    "ts": ts.strftime("%Y-%m-%d %H:%M:%S"),
                    "datapoint": datapoint_name,
                    "value": f"{value:.3f}",
                })

    if gaps > 0 and rows:
        total = len(rows)
        drop_count = min(int(total * gaps) if gaps < 1 else int(round(gaps)), total - 1)
        if drop_count > 0:
            indices = list(range(total))
            rng.shuffle(indices)
            keep_mask = set(indices[drop_count:])
            rows = [r for i, r in enumerate(rows) if i in keep_mask]

    # Build output rows using column aliases mapping from internal keys
    # Known internal keys: customer, site, asset_type, asset_id, ts, datapoint, value
    output_columns: List[str] = []
    for internal_key, out_col in column_aliases.items():
        if out_col:
            output_columns.append(out_col)

    data_matrix: List[List[str]] = []
    for r in rows:
        out_row: List[str] = []
        for internal_key, out_col in column_aliases.items():
            if not out_col:
                continue  # skip
            if internal_key == "value":
                out_row.append(r["value"])  # str
            else:
                out_row.append(r[internal_key])
        data_matrix.append(out_row)

    df = session.create_dataframe(data_matrix, schema=output_columns)
    return df


def model(dbt, session):
    dbt.config(materialized="table")
    meta = dbt.config.get("meta") or {}
    params = meta.get("interview_params", {})

    column_aliases = {
        "site": "site_code",
        "asset_type": "asset_type",
        "asset_id": "device_id",
        "ts": "ts",
        "datapoint": "datapoint",
        "value": params.get("value_col", "value"),
    }

    return build_dataframe(
        session=session,
        params=params,
        column_aliases=column_aliases,
        default_datapoints={},
        require_asset_types=True,
    )


