BEGIN;

CREATE TABLE sites (
    site          TEXT UNIQUE NOT NULL, -- External identifier, WMO#, ICAO id...
    name          TEXT DEFUALT NULL,    -- common name
    state         TEXT DEDAULT NULL,    -- State/Providence code
    notes         TEXT DEFAULT NULL,    -- Human readable notes
    tz_offset_sec INT  DEFAULT 0,       -- Offset from UTC in seconds
    auto_download INT  DEFAULT 0,
    PRIMARY KEY (site)
);

CREATE TABLE files (
    site        TEXT        NOT NULL,
    model       TEXT        NOT NULL,
    init_time   TEXT        NOT NULL,
    end_time    TEXT        NOT NULL,
    file_name   TEXT UNIQUE NOT NULL,
    FOREIGN KEY (site)     REFERENCES sites(site)
);

-- For fast searches by file name.
CREATE UNIQUE INDEX fname ON files(file_name);  

-- For fast searches by metadata.
CREATE UNIQUE INDEX no_dups_files ON files(model, site, init_time); 

COMMIT;