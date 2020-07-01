INSERT INTO sites 
(
    station_num, 
    name,
    state,
    notes,
    tz_offset_sec,
    auto_download,
    mean_lat,
    mean_lon
)
VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
