SELECT 
	sites.station_num, 
	files.id, 
	files.model, 
	sites.name, 
	sites.state, 
	sites.notes, 
	sites.tz_offset_sec, 
	COUNT(*)
FROM files JOIN sites ON files.station_num = sites.station_num
GROUP BY files.station_num, id, model
