-- Every abnormal heart-rate event of the last 24 hours, most recent first
SELECT event_time, device_id, heart_rate, heart_rate_zone, spo2, spo2_status
FROM silver_events
WHERE dt >= date_format(current_date - INTERVAL '1' DAY, '%Y-%m-%d')
  AND is_abnormal_hr
ORDER BY event_time DESC
LIMIT 50;
