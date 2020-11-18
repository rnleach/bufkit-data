UPDATE sites
SET (
    state,
    name,
    notes,
    tz_offset_sec
) = (?2, ?3, ?4, ?5)
WHERE station_num = ?1
