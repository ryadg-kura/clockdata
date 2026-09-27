-- Data quality per hour: accepted vs rejected records, and why records were rejected
WITH received AS (
  SELECT dt, hour, count(*) AS received FROM bronze_events
  WHERE dt = date_format(current_date, '%Y-%m-%d') GROUP BY dt, hour
), rejected AS (
  SELECT dt, hour, count(*) AS rejected, array_join(array_distinct(flatten(array_agg(errors))), '; ') AS reasons
  FROM quarantine_events
  WHERE dt = date_format(current_date, '%Y-%m-%d') GROUP BY dt, hour
)
SELECT r.dt, r.hour, r.received, coalesce(q.rejected, 0) AS rejected,
       round(100.0 * coalesce(q.rejected, 0) / r.received, 2) AS rejected_pct, q.reasons
FROM received r LEFT JOIN rejected q ON r.dt = q.dt AND r.hour = q.hour
ORDER BY r.hour DESC;
