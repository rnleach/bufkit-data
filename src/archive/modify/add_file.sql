INSERT OR REPLACE INTO files
    (
        station_num, 
        model, 
        init_time,
        end_time,
        file_name,
        id,
        lat,
        lon,
        elevation_m
    )
VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
