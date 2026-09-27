-- Watches needing attention right now: low battery or low blood oxygen in their latest event
SELECT device_id, event_time, battery, battery_low, spo2, spo2_status
FROM (
  SELECT *, row_number() OVER (PARTITION BY device_id ORDER BY event_time DESC) AS rn
  FROM silver_events
  WHERE dt = date_format(current_date, '%Y-%m-%d')
)
WHERE rn = 1 AND (battery_low OR spo2_status <> 'normal')
ORDER BY battery;
