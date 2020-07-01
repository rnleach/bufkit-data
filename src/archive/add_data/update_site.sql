UPDATE sites
SET (
    state,
    name,
    notes,
    auto_download,
    tz_offset_sec
) = (?2, ?3, ?4, ?5, ?6)
WHERE station_num = ?1
