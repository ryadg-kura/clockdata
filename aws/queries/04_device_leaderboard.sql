-- Today's leaderboard: steps, average heart rate and alerts per device
SELECT device_id,
       sum(total_steps)                                             AS steps_today,
       round(sum(avg_heart_rate * event_count) / sum(event_count), 1) AS avg_heart_rate,
       max(max_heart_rate)                                          AS peak_heart_rate,
       sum(abnormal_hr_count)                                       AS abnormal_events,
       min(min_spo2)                                                AS lowest_spo2
FROM gold_device_hourly
WHERE dt = date_format(current_date, '%Y-%m-%d')
GROUP BY device_id
ORDER BY steps_today DESC;
