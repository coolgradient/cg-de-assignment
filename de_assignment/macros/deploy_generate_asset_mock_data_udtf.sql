{% macro deploy_generate_asset_mock_data_udtf() %}

-- Snowflake UDTF for synthetic data generation
-- Call this macro to deploy the UDF: dbt run-operation deploy_generate_asset_mock_data_udtf

CREATE OR REPLACE FUNCTION {{ target.schema }}.generate_asset_mock_data_udtf(
    start_date VARCHAR,
    end_date VARCHAR,
    granularity VARCHAR,
    customer_code VARCHAR,
    site_code VARCHAR,
    asset_types_json VARCHAR,
    datapoints_json VARCHAR,
    gaps NUMBER(38,10),
    anomalies NUMBER(38,10),
    anomaly_severity NUMBER(38,10),
    correlation_lag_minutes NUMBER(38,0),
    drift_enabled BOOLEAN,
    drift_magnitude NUMBER(38,10),
    drift_period_hours NUMBER(38,10),
    setpoint_changes NUMBER(38,0),
    setpoint_change_speed NUMBER(38,10),
    setpoint_change_magnitude NUMBER(38,10),
    sensor_failures NUMBER(38,0),
    sensor_failure_duration_hours NUMBER(38,10),
    sensor_failure_type VARCHAR,
    seed_value NUMBER(38,0)
)
RETURNS TABLE (
    customer_short_code VARCHAR,
    dc_site_code VARCHAR,
    asset_type VARCHAR,
    asset_id VARCHAR,
    event_dts TIMESTAMP,
    datapoint VARCHAR,
    metric_value FLOAT
)
LANGUAGE PYTHON
RUNTIME_VERSION = 3.11
PACKAGES = ('numpy')
HANDLER = 'SyntheticDataGenerator'
AS $$
import math
import random
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

class SyntheticDataGenerator:
    def __init__(self):
        self.rows = []
        self.start_date = None
        self.end_date = None
        self.granularity = None
        self.customer_code = None
        self.site_code = None
        self.asset_types_json = None
        self.datapoints_json = None
        self.gaps = None
        self.anomalies = None
        self.anomaly_severity = None
        self.correlation_lag_minutes = None
        self.drift_enabled = None
        self.drift_magnitude = None
        self.drift_period_hours = None
        self.setpoint_changes = None
        self.setpoint_change_speed = None
        self.setpoint_change_magnitude = None
        self.sensor_failures = None
        self.sensor_failure_duration_hours = None
        self.sensor_failure_type = None
        self.seed_value = None
        
    def _parse_iso_datetime(self, value: str) -> datetime:
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return datetime.fromisoformat(value + " 00:00:00")
    
    def _time_step_for(self, granularity: str) -> timedelta:
        if granularity == "minute":
            return timedelta(minutes=1)
        if granularity.endswith("minute") or granularity.endswith("minutes"):
            try:
                minutes = int(granularity.replace("minute", "").replace("minutes", "").strip())
                return timedelta(minutes=minutes)
            except ValueError:
                pass
        if granularity == "hour":
            return timedelta(hours=1)
        if granularity == "day":
            return timedelta(days=1)
        raise ValueError("granularity must be one of: minute, Nminute/Nminutes, hour, day")
    
    def _generate_value(self, ts: datetime, min_value: float, max_value: float, 
                       rng: random.Random, prev_value: Optional[float], 
                       trend_state: Dict, start_ts: datetime,
                       drift_enabled: bool, drift_magnitude: float, 
                       drift_period_hours: float, setpoint_offset: float, 
                       setpoint_change_speed: float) -> float:
        mid = (min_value + max_value) / 2.0
        span = (max_value - min_value)
        if span <= 0:
            return mid
        
        if 'trend' not in trend_state:
            trend_state['trend'] = 0.0
            trend_state['trend_velocity'] = 0.0
            trend_state['base_value'] = mid
            trend_state['long_term_drift'] = 0.0
            trend_state['drift_direction'] = rng.choice([-1, 1])
            trend_state['drift_change_counter'] = 0
            trend_state['drift_period_timesteps'] = 0
            trend_state['current_setpoint_offset'] = 0.0
        
        minutes_in_day = ts.hour * 60 + ts.minute
        phase = 2.0 * math.pi * (minutes_in_day / 1440.0)
        daily_pattern = span * 0.2 * math.sin(phase)
        
        long_term_component = 0.0
        if drift_enabled:
            if 'drift_period_timesteps' not in trend_state or trend_state['drift_period_timesteps'] == 0:
                trend_state['drift_period_timesteps'] = int(drift_period_hours)
            
            trend_state['drift_change_counter'] += 1
            period_variance = int(trend_state['drift_period_timesteps'] * 0.2)
            if trend_state['drift_change_counter'] > trend_state['drift_period_timesteps'] + rng.randint(-period_variance, period_variance):
                trend_state['drift_direction'] *= -1
                trend_state['drift_change_counter'] = 0
            
            drift_speed = drift_magnitude / (trend_state['drift_period_timesteps'] * 2)
            trend_state['long_term_drift'] += trend_state['drift_direction'] * drift_speed
            trend_state['long_term_drift'] = max(-drift_magnitude, min(drift_magnitude, trend_state['long_term_drift']))
            long_term_component = span * trend_state['long_term_drift']
        
        offset_diff = setpoint_offset - trend_state['current_setpoint_offset']
        if abs(offset_diff) > 0.001:
            adjustment = offset_diff * setpoint_change_speed
            trend_state['current_setpoint_offset'] += adjustment
        else:
            trend_state['current_setpoint_offset'] = setpoint_offset
        
        setpoint_component = span * trend_state['current_setpoint_offset']
        
        if rng.random() < 0.01:
            trend_state['trend_velocity'] += rng.uniform(-span * 0.001, span * 0.001)
        
        trend_state['trend'] += trend_state['trend_velocity']
        trend_state['trend_velocity'] *= 0.98
        trend_state['trend'] *= 0.999
        trend_state['trend'] = max(-span * 0.25, min(span * 0.25, trend_state['trend']))
        
        if prev_value is not None:
            target = mid + daily_pattern + trend_state['trend'] + long_term_component + setpoint_component
            trend_state['base_value'] = prev_value * 0.85 + target * 0.15
        else:
            trend_state['base_value'] = mid
        
        noise = rng.gauss(0, span * 0.008)
        value = trend_state['base_value'] + noise
        
        return max(min_value, min(max_value, value))
    
    def process(self, start_date, end_date, granularity, customer_code, site_code,
                asset_types_json, datapoints_json, gaps, anomalies, anomaly_severity,
                correlation_lag_minutes, drift_enabled, drift_magnitude, drift_period_hours,
                setpoint_changes, setpoint_change_speed, setpoint_change_magnitude,
                sensor_failures, sensor_failure_duration_hours, sensor_failure_type, seed_value):
        self.start_date = start_date
        self.end_date = end_date
        self.granularity = granularity
        self.customer_code = customer_code
        self.site_code = site_code
        self.asset_types_json = asset_types_json
        self.datapoints_json = datapoints_json
        self.gaps = gaps
        self.anomalies = anomalies
        self.anomaly_severity = anomaly_severity
        self.correlation_lag_minutes = correlation_lag_minutes
        self.drift_enabled = drift_enabled
        self.drift_magnitude = drift_magnitude
        self.drift_period_hours = drift_period_hours
        self.setpoint_changes = setpoint_changes
        self.setpoint_change_speed = setpoint_change_speed
        self.setpoint_change_magnitude = setpoint_change_magnitude
        self.sensor_failures = sensor_failures
        self.sensor_failure_duration_hours = sensor_failure_duration_hours
        self.sensor_failure_type = sensor_failure_type
        self.seed_value = seed_value
    
    def end_partition(self):
        start = self._parse_iso_datetime(self.start_date)
        end = self._parse_iso_datetime(self.end_date)
        granularity = self.granularity.lower()
        customer = self.customer_code
        site = self.site_code
        
        asset_types = json.loads(self.asset_types_json)
        datapoints_raw = json.loads(self.datapoints_json)
        datapoints = {k: (float(v[0]), float(v[1])) for k, v in datapoints_raw.items()}
        
        gaps_pct = float(self.gaps)
        anomalies_pct = float(self.anomalies)
        anomaly_sev = float(self.anomaly_severity)
        corr_lag = int(self.correlation_lag_minutes)
        drift_en = bool(self.drift_enabled)
        drift_mag = float(self.drift_magnitude)
        drift_per = float(self.drift_period_hours)
        setpt_chg = int(self.setpoint_changes)
        setpt_spd = float(self.setpoint_change_speed)
        setpt_mag = float(self.setpoint_change_magnitude)
        sens_fail = int(self.sensor_failures)
        sens_dur = float(self.sensor_failure_duration_hours)
        sens_type = str(self.sensor_failure_type)
        seed = int(self.seed_value) if self.seed_value is not None else None
        
        rng = random.Random(seed)
        step = self._time_step_for(granularity)
        minutes_per_step = step.total_seconds() / 60
        lag_steps = max(1, int(corr_lag / minutes_per_step))
        
        asset_pairs = []
        for atype, ids in asset_types.items():
            for asset_id in ids:
                asset_pairs.append((atype, asset_id))
        
        timestamps = []
        current = start
        while current <= end:
            timestamps.append(current)
            current = current + step
        
        total_timesteps = len(timestamps)
        
        setpoint_schedule = {}
        if setpt_chg > 0:
            interval = total_timesteps // (setpt_chg + 1)
            for i in range(setpt_chg):
                change_timestep = (i + 1) * interval
                offset = rng.uniform(-setpt_mag, setpt_mag)
                setpoint_schedule[change_timestep] = offset
        
        duration_steps = max(1, int((sens_dur * 60) / minutes_per_step))
        asset_type_correlation = {}
        
        rows = []
        for asset_type, asset_id in asset_pairs:
            for datapoint_name, (mn, mx) in datapoints.items():
                trend_state = {}
                prev_value = None
                current_setpoint_offset = 0.0
                frozen_value = None
                
                sensor_failure_periods = []
                if sens_fail > 0:
                    actual_failures = rng.randint(0, sens_fail)
                    if actual_failures > 0:
                        interval = total_timesteps // (actual_failures + 1)
                        for i in range(actual_failures):
                            base_start = (i + 1) * interval
                            random_offset = rng.randint(-interval // 4, interval // 4)
                            start_idx = max(0, min(base_start + random_offset, total_timesteps - duration_steps - 1))
                            actual_duration = rng.randint(duration_steps // 2, duration_steps)
                            end_idx = min(start_idx + actual_duration, total_timesteps - 1)
                            
                            if sens_type == "mixed":
                                failure_mode = rng.choice(["erratic", "zero", "frozen"])
                            else:
                                failure_mode = sens_type
                            
                            sensor_failure_periods.append((start_idx, end_idx, failure_mode))
                
                for ts_idx, ts in enumerate(timestamps):
                    if ts_idx in setpoint_schedule:
                        current_setpoint_offset = setpoint_schedule[ts_idx]
                    
                    in_failure = False
                    failure_mode = None
                    for start_fail, end_fail, mode in sensor_failure_periods:
                        if start_fail <= ts_idx <= end_fail:
                            in_failure = True
                            failure_mode = mode
                            break
                    
                    if in_failure:
                        if failure_mode == "zero":
                            value = 0.0
                        elif failure_mode == "frozen":
                            if frozen_value is None:
                                frozen_value = prev_value if prev_value is not None else (mn + mx) / 2
                            value = frozen_value
                        elif failure_mode == "erratic":
                            span = mx - mn
                            value = rng.uniform(mn - span * 0.5, mx + span * 0.5)
                        else:
                            value = self._generate_value(ts, mn, mx, rng, prev_value, trend_state, start,
                                                        drift_en, drift_mag, drift_per,
                                                        current_setpoint_offset, setpt_spd)
                    else:
                        frozen_value = None
                        value = self._generate_value(ts, mn, mx, rng, prev_value, trend_state, start,
                                                     drift_en, drift_mag, drift_per,
                                                     current_setpoint_offset, setpt_spd)
                        
                        correlation_key = (asset_type, datapoint_name)
                        if correlation_key in asset_type_correlation:
                            source_series = asset_type_correlation[correlation_key]
                            source_idx = ts_idx - lag_steps
                            if 0 <= source_idx < len(source_series):
                                source_val = source_series[source_idx]
                                source_normalized = (source_val - mn) / (mx - mn) if mx > mn else 0.5
                                target_val = mn + (mx - mn) * source_normalized
                                value = value * 0.7 + target_val * 0.3
                    
                    correlation_key = (asset_type, datapoint_name)
                    if correlation_key not in asset_type_correlation:
                        asset_type_correlation[correlation_key] = []
                    if len(asset_type_correlation[correlation_key]) == ts_idx:
                        asset_type_correlation[correlation_key].append(value)
                    
                    prev_value = value
                    rows.append({
                        "customer": customer,
                        "site": site,
                        "asset_type": asset_type,
                        "asset_id": asset_id,
                        "ts": ts,
                        "datapoint": datapoint_name,
                        "value": value,
                    })
        
        if gaps_pct > 0 and rows:
            total = len(rows)
            drop_count = min(int(total * gaps_pct), total - 1)
            if drop_count > 0:
                indices = list(range(total))
                rng.shuffle(indices)
                keep_mask = set(indices[drop_count:])
                rows = [r for i, r in enumerate(rows) if i in keep_mask]
        
        failure_row_indices = set()
        if sens_fail > 0:
            for row_idx, row in enumerate(rows):
                val = row["value"]
                if val == 0.0 or abs(val) > 1000:
                    failure_row_indices.add(row_idx)
        
        if anomalies_pct > 0 and rows:
            valid_indices = [i for i in range(len(rows)) if i not in failure_row_indices]
            if valid_indices:
                anomaly_count = min(int(len(valid_indices) * anomalies_pct), len(valid_indices))
                if anomaly_count > 0:
                    rng.shuffle(valid_indices)
                    anomaly_indices = set(valid_indices[:anomaly_count])
                    
                    for i in anomaly_indices:
                        row = rows[i]
                        datapoint_name = row["datapoint"]
                        if datapoint_name in datapoints:
                            mn, mx = datapoints[datapoint_name]
                            span = mx - mn
                            current_val = row["value"]
                            deviation = span * anomaly_sev * rng.uniform(0.5, 1.5)
                            if rng.random() < 0.5:
                                row["value"] = current_val + deviation
                            else:
                                row["value"] = current_val - deviation
        
        for row in rows:
            yield (
                row["customer"],
                row["site"],
                row["asset_type"],
                row["asset_id"],
                row["ts"],
                row["datapoint"],
                row["value"]
            )
$$

{% endmacro %}

