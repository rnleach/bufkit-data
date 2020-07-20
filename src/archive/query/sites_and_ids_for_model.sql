DROP TABLE IF EXISTS temp_ids;

CREATE TEMP TABLE temp_ids AS
    SELECT files.id, files.station_num
    FROM files JOIN (
        SELECT files.station_num, MAX(files.init_time) as maxtime
        FROM files 
        GROUP BY files.station_num) as maxs
    ON maxs.station_num = files.station_num AND files.init_time = maxs.maxtime
    WHERE files.model = ?1;
        
SELECT 
    sites.station_num,
    sites.name, 
    sites.state, 
    sites.notes, 
    sites.tz_offset_sec, 
    sites.auto_download, t
    emp_ids.id
FROM sites JOIN temp_ids ON temp_ids.station_num = sites.station_num
