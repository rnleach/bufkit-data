INSERT OR REPLACE INTO files
    (
        station_num, 
        model, 
        init_time,
        end_time,
        file_name
    )
VALUES (?1, ?2, ?3, ?4, ?5)
