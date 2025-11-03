import math
import random
from datetime import datetime, timedelta
from typing import Dict, Iterable, Iterator, List, Literal, Optional, Tuple, Mapping, Sequence


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


def _generate_value(ts: datetime, min_value: float, max_value: float, rng: random.Random, 
                    prev_value: Optional[float], trend_state: Dict, start_ts: datetime,
                    drift_enabled: bool, drift_magnitude: float, drift_period_hours: float,
                    setpoint_offset: float, setpoint_change_speed: float) -> float:
    """Generate natural-looking time series with trends, momentum, and daily patterns."""
    mid = (min_value + max_value) / 2.0
    span = (max_value - min_value)
    if span <= 0:
        return mid
    
    # Initialize state on first call
    if 'trend' not in trend_state:
        trend_state['trend'] = 0.0
        trend_state['trend_velocity'] = 0.0
        trend_state['base_value'] = mid
        trend_state['long_term_drift'] = 0.0
        trend_state['drift_direction'] = rng.choice([-1, 1])
        trend_state['drift_change_counter'] = 0
        trend_state['drift_period_timesteps'] = 0
        trend_state['current_setpoint_offset'] = 0.0
    
    # Daily sinusoidal pattern (20% of range)
    minutes_in_day = ts.hour * 60 + ts.minute
    phase = 2.0 * math.pi * (minutes_in_day / 1440.0)
    daily_pattern = span * 0.2 * math.sin(phase)
    
    # Long-term drift (multi-day cycles) - configurable
    long_term_component = 0.0
    if drift_enabled:
        # Calculate hours since start
        hours_elapsed = (ts - start_ts).total_seconds() / 3600.0
        
        # Calculate period in timesteps (need to know granularity)
        if 'drift_period_timesteps' not in trend_state or trend_state['drift_period_timesteps'] == 0:
            # Estimate based on first few calls
            trend_state['drift_period_timesteps'] = int(drift_period_hours)
        
        # Change drift direction at configured period (with some randomness)
        trend_state['drift_change_counter'] += 1
        period_variance = int(trend_state['drift_period_timesteps'] * 0.2)  # ±20% variance
        if trend_state['drift_change_counter'] > trend_state['drift_period_timesteps'] + rng.randint(-period_variance, period_variance):
            trend_state['drift_direction'] *= -1
            trend_state['drift_change_counter'] = 0
        
        # Slow drift that moves across the range
        # Speed calibrated so it takes ~2 periods to traverse full range
        drift_speed = drift_magnitude / (trend_state['drift_period_timesteps'] * 2)
        trend_state['long_term_drift'] += trend_state['drift_direction'] * drift_speed
        trend_state['long_term_drift'] = max(-drift_magnitude, min(drift_magnitude, trend_state['long_term_drift']))
        long_term_component = span * trend_state['long_term_drift']
    
    # Setpoint change handling - rapid adjustment to new target
    # Gradually move current offset toward target setpoint offset
    offset_diff = setpoint_offset - trend_state['current_setpoint_offset']
    if abs(offset_diff) > 0.001:
        # Fast adjustment when setpoint changes
        adjustment = offset_diff * setpoint_change_speed
        trend_state['current_setpoint_offset'] += adjustment
    else:
        trend_state['current_setpoint_offset'] = setpoint_offset
    
    setpoint_component = span * trend_state['current_setpoint_offset']
    
    # Smooth trend changes using velocity and acceleration
    if rng.random() < 0.01:  # 1% chance to nudge trend
        trend_state['trend_velocity'] += rng.uniform(-span * 0.001, span * 0.001)
    
    # Apply velocity with strong damping
    trend_state['trend'] += trend_state['trend_velocity']
    trend_state['trend_velocity'] *= 0.98  # Damping
    trend_state['trend'] *= 0.999  # Mean reversion
    
    # Limit trend magnitude
    trend_state['trend'] = max(-span * 0.25, min(span * 0.25, trend_state['trend']))
    
    # Update base value slowly (random walk)
    if prev_value is not None:
        # Smooth transition from previous value
        target = mid + daily_pattern + trend_state['trend'] + long_term_component + setpoint_component
        # Move 15% toward target, keeping smooth continuity
        trend_state['base_value'] = prev_value * 0.85 + target * 0.15
    else:
        trend_state['base_value'] = mid
    
    # Small noise
    noise = rng.gauss(0, span * 0.008)
    
    # Combine components
    value = trend_state['base_value'] + noise
    
    # Keep within bounds (will be violated for anomalies later)
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
    anomalies = float(params.get("anomalies", 0.0))
    anomaly_severity = float(params.get("anomaly_severity", 0.10))
    correlation_lag_minutes = int(params.get("correlation_lag_minutes", 60))
    drift_enabled = bool(params.get("drift_enabled", True))
    drift_magnitude = float(params.get("drift_magnitude", 0.4))
    drift_period_hours = float(params.get("drift_period_hours", 168.0))
    setpoint_changes = int(params.get("setpoint_changes", 0))
    setpoint_change_speed = float(params.get("setpoint_change_speed", 0.15))
    setpoint_change_magnitude = float(params.get("setpoint_change_magnitude", 0.3))
    sensor_failures = int(params.get("sensor_failures", 0))
    sensor_failure_duration_hours = float(params.get("sensor_failure_duration_hours", 12.0))
    sensor_failure_type = str(params.get("sensor_failure_type", "erratic"))
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
    
    # Calculate correlation lag in timesteps
    lag_minutes = correlation_lag_minutes
    if granularity == "minute":
        lag_steps = lag_minutes
    elif granularity == "hour":
        lag_steps = max(1, lag_minutes // 60)
    else:  # day
        lag_steps = max(1, lag_minutes // 1440)

    rows: List[Dict[str, str]] = []
    
    # Calculate setpoint change schedule
    total_timesteps = len(list(_iter_datetimes(start, end, step)))
    setpoint_schedule: Dict[int, float] = {}  # timestep -> offset
    
    if setpoint_changes > 0:
        # Distribute setpoint changes evenly across time period
        interval = total_timesteps // (setpoint_changes + 1)
        for i in range(setpoint_changes):
            change_timestep = (i + 1) * interval
            # Random offset within magnitude range
            offset = rng.uniform(-setpoint_change_magnitude, setpoint_change_magnitude)
            setpoint_schedule[change_timestep] = offset
    
    # Calculate duration in timesteps for sensor failures
    if granularity == "minute":
        duration_steps = int(sensor_failure_duration_hours * 60)
    elif granularity == "hour":
        duration_steps = int(sensor_failure_duration_hours)
    else:  # day
        duration_steps = max(1, int(sensor_failure_duration_hours / 24))
    
    # Generate data per asset, with correlation between datapoints
    for asset_type, asset_id in asset_pairs:
        # Store time series for first datapoint to use as correlation source
        correlation_source: List[float] = []
        datapoint_names = list(datapoints.keys())
        
        for dp_idx, datapoint_name in enumerate(datapoint_names):
            mn, mx = datapoints[datapoint_name]
            trend_state: Dict = {}
            prev_value: Optional[float] = None
            current_setpoint_offset = 0.0
            frozen_value: Optional[float] = None
            
            # Generate unique sensor failure schedule for this asset+datapoint combination
            sensor_failure_periods: List[Tuple[int, int, str]] = []
            if sensor_failures > 0:
                # Randomly decide how many failures this sensor will have (0 to max)
                actual_failures = rng.randint(0, sensor_failures)
                
                if actual_failures > 0:
                    # Use different intervals per asset/datapoint to avoid synchronization
                    interval = total_timesteps // (actual_failures + 1)
                    
                    for i in range(actual_failures):
                        # Add randomness to start time so not all sensors fail at same time
                        base_start = (i + 1) * interval
                        random_offset = rng.randint(-interval // 4, interval // 4)
                        start_idx = max(0, min(base_start + random_offset, total_timesteps - duration_steps - 1))
                        
                        # Randomize duration (50% to 100% of max duration)
                        actual_duration = rng.randint(duration_steps // 2, duration_steps)
                        end_idx = min(start_idx + actual_duration, total_timesteps - 1)
                        
                        # Determine failure type
                        if sensor_failure_type == "mixed":
                            failure_mode = rng.choice(["erratic", "zero", "frozen"])
                        else:
                            failure_mode = sensor_failure_type
                        
                        sensor_failure_periods.append((start_idx, end_idx, failure_mode))
            
            for ts_idx, ts in enumerate(_iter_datetimes(start, end, step)):
                # Check if there's a setpoint change at this timestep
                if ts_idx in setpoint_schedule:
                    current_setpoint_offset = setpoint_schedule[ts_idx]
                
                # Check if we're in a sensor failure period
                in_failure = False
                failure_mode = None
                for start_fail, end_fail, mode in sensor_failure_periods:
                    if start_fail <= ts_idx <= end_fail:
                        in_failure = True
                        failure_mode = mode
                        break
                
                if in_failure:
                    # Apply sensor failure behavior
                    if failure_mode == "zero":
                        value = 0.0
                    elif failure_mode == "frozen":
                        if frozen_value is None:
                            # Freeze at current value
                            frozen_value = prev_value if prev_value is not None else (mn + mx) / 2
                        value = frozen_value
                    elif failure_mode == "erratic":
                        # Wild fluctuations across entire possible range
                        span = mx - mn
                        value = rng.uniform(mn - span * 0.5, mx + span * 0.5)
                    else:
                        value = _generate_value(ts, mn, mx, rng, prev_value, trend_state, start,
                                               drift_enabled, drift_magnitude, drift_period_hours,
                                               current_setpoint_offset, setpoint_change_speed)
                else:
                    # Normal operation
                    frozen_value = None  # Reset frozen value when failure ends
                    value = _generate_value(ts, mn, mx, rng, prev_value, trend_state, start,
                                           drift_enabled, drift_magnitude, drift_period_hours,
                                           current_setpoint_offset, setpoint_change_speed)
                    
                    # Apply correlation from first datapoint to subsequent ones
                    if dp_idx > 0 and correlation_source:
                        # Look back by lag_steps
                        source_idx = ts_idx - lag_steps
                        if 0 <= source_idx < len(correlation_source):
                            # Get normalized position of source value in its range
                            source_val = correlation_source[source_idx]
                            source_mn, source_mx = datapoints[datapoint_names[0]]
                            source_normalized = (source_val - source_mn) / (source_mx - source_mn) if source_mx > source_mn else 0.5
                            
                            # Apply correlation: blend current value with correlated target
                            target_val = mn + (mx - mn) * source_normalized
                            value = value * 0.6 + target_val * 0.4  # 40% correlation strength
                
                # Store first datapoint values for correlation
                if dp_idx == 0:
                    correlation_source.append(value)
                
                prev_value = value
                rows.append({
                    "customer": customer,
                    "site": site,
                    "asset_type": asset_type,
                    "asset_id": asset_id,
                    "ts": ts.strftime("%Y-%m-%d %H:%M:%S"),
                    "datapoint": datapoint_name,
                    "value": f"{value:.3f}",
                })

    # Apply gaps (remove rows)
    if gaps > 0 and rows:
        total = len(rows)
        drop_count = min(int(total * gaps) if gaps < 1 else int(round(gaps)), total - 1)
        if drop_count > 0:
            indices = list(range(total))
            rng.shuffle(indices)
            keep_mask = set(indices[drop_count:])
            rows = [r for i, r in enumerate(rows) if i in keep_mask]
    
    # Build a set of row indices that are in sensor failure periods
    # We'll exclude these from anomaly injection
    failure_row_indices = set()
    if sensor_failures > 0:
        for row_idx, row in enumerate(rows):
            # Mark rows that have value 0.0 (zero failures) or are clearly in failure mode
            val = float(row["value"])
            if val == 0.0 or abs(val) > 1000:  # Simple heuristic for failure detection
                failure_row_indices.add(row_idx)
    
    # Apply anomalies (subtle deviations from expected sequence)
    # But NOT during sensor failures
    if anomalies > 0 and rows:
        # Filter out indices that are in failure periods
        valid_indices = [i for i in range(len(rows)) if i not in failure_row_indices]
        
        if valid_indices:
            anomaly_count = min(int(len(valid_indices) * anomalies) if anomalies < 1 else int(round(anomalies)), len(valid_indices))
            if anomaly_count > 0:
                rng.shuffle(valid_indices)
                anomaly_indices = set(valid_indices[:anomaly_count])
                
                for i in anomaly_indices:
                    row = rows[i]
                    datapoint_name = row["datapoint"]
                    
                    # Find the min/max for this datapoint
                    if datapoint_name in datapoints:
                        mn, mx = datapoints[datapoint_name]
                        span = mx - mn
                        current_val = float(row["value"])
                        
                        # Generate anomaly using configurable severity
                        # Severity is fraction of range (e.g., 0.05 = 5% of range)
                        deviation = span * anomaly_severity * rng.uniform(0.5, 1.5)
                        
                        # Randomly go up or down
                        if rng.random() < 0.5:
                            anomaly_val = current_val + deviation
                        else:
                            anomaly_val = current_val - deviation
                        
                        row["value"] = f"{anomaly_val:.3f}"

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


