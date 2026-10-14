-- Dataset A: latest known positions for a Point Map and state table.
SELECT device_id, latitude, longitude, speed_kmh, is_moving,
       event_time AS source_event_time, received_at AS last_received_at
FROM fleetpulse.gold.device_state;

-- Dataset B: fleet state counts. "Stationary" does NOT prove engine-off.
SELECT COUNT(*) AS fleet_devices,
       SUM(CASE WHEN is_moving THEN 1 ELSE 0 END) AS moving_devices,
       SUM(CASE WHEN NOT is_moving THEN 1 ELSE 0 END) AS stationary_devices
FROM fleetpulse.gold.device_state;

-- Dataset C: hourly GPS readings and average movement speed.
SELECT date_trunc('hour', event_time) AS hour_bucket,
       COUNT(*) AS gps_readings,
       AVG(speed_kmh) AS avg_speed_kmh
FROM fleetpulse.silver.gps_events
GROUP BY 1
ORDER BY 1;

-- Dataset D: most recent readings per device, for inspection.
SELECT device_id, event_time, received_at, speed_kmh, latitude, longitude
FROM fleetpulse.silver.gps_events
ORDER BY event_time DESC
LIMIT 100;
