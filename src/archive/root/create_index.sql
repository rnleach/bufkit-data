BEGIN;

CREATE TABLE files (
    station_num INT         NOT NULL,
    model       TEXT        NOT NULL,
    init_time   TEXT        NOT NULL,
    end_time    TEXT        NOT NULL,
    file_name   TEXT UNIQUE NOT NULL,
    FOREIGN KEY (station_num) REFERENCES sites(station_num)
);

CREATE TABLE sites (
    station_num   INT  UNIQUE  NOT NULL, -- External identifier, WMO#, USAF#
    name          TEXT DEFAULT NULL,     -- common name
    state         TEXT DEFAULT NULL,     -- State/Providence code
    notes         TEXT DEFAULT NULL,     -- Human readable notes
    tz_offset_sec INT  DEFAULT 0,        -- Offset from UTC in seconds
    auto_download INT  DEFAULT 0,        -- Download this site automatically
    mean_lat      REAL         NOT NULL, -- Mean of latitudes for this location over time.
    mean_lon      REAL         NOT NULL, -- Mean of longitudes for this location over time.
    PRIMARY KEY (station_num)
);

CREATE TABLE site_ids (
    station_num INT  UNIQUE NOT NULL,
    id          TEXT UNIQUE NOT NULL,
    PRIMARY KEY (station_num),
    FOREIGN KEY (station_num) REFERENCES sites(station_num)
);

CREATE TABLE coords (
    station_num INT NOT NULL,
    lat REAL NOT NULL,
    lon REAL NOT NULL
);


-- For fast searches by file name.
CREATE UNIQUE INDEX fname ON files(file_name);

-- For fast searches by metadata.
CREATE UNIQUE INDEX no_dups_files ON files(model, station_num, init_time);

-- For fast searches including end times
CREATE INDEX time_ranges ON files(model, station_num, init_time, end_time);

-- Don't repeat a lat-lon set of coords for a site.
CREATE INDEX unique_coords ON coords(station_num, lat, lon);

COMMIT;
