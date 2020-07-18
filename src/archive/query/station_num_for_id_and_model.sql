SELECT station_num, init_time
FROM files
WHERE id = ?1 AND model = ?2
ORDER BY init_time DESC
LIMIT 1
