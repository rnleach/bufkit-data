SELECT 
	sites.station_num, 
	files.id, 
	files.model, 
	sites.name, 
	sites.state, 
	sites.notes, 
	sites.tz_offset_sec, 
    files.lat,
    files.lon,
	COUNT(files.station_num)
FROM sites LEFT JOIN files ON files.station_num = sites.station_num
GROUP BY sites.station_num, id, model, lat, lon
