-- Latest 20 validated events (silver), today (UTC)
SELECT event_time, device_id, heart_rate, heart_rate_zone, steps, spo2, spo2_status, battery,
       end_to_end_latency_ms
FROM silver_events
WHERE dt = date_format(current_date, '%Y-%m-%d')
ORDER BY event_time DESC
LIMIT 20;
