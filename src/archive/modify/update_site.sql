UPDATE sites
SET (
    state,
    name,
    notes,
    tz_offset_sec,
    auto_download
) = (?2, ?3, ?4, ?5, ?6)
WHERE station_num = ?1
