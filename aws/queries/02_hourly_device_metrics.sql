-- Gold: per device per hour metrics over the last 24 hours
SELECT dt, hour, device_id, event_count, avg_heart_rate, min_heart_rate, max_heart_rate,
       abnormal_hr_count, total_steps, avg_spo2, last_battery
FROM gold_device_hourly
WHERE dt >= date_format(current_date - INTERVAL '1' DAY, '%Y-%m-%d')
ORDER BY dt DESC, hour DESC, device_id
LIMIT 100;
