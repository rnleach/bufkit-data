INSERT INTO sites 
(
    station_num, 
    name,
    state,
    notes,
    tz_offset_sec,
    auto_download
)
VALUES (?1, ?2, ?3, ?4, ?5, ?6)
