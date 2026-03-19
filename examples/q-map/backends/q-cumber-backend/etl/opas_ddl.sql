-- OPAS ISPRA air quality monitoring — PostGIS tables
-- Run once against the q-cumber PostGIS database.

CREATE SCHEMA IF NOT EXISTS qvt;

-- Stations: 699 monitoring points across Italy
CREATE TABLE IF NOT EXISTS qvt.opas_stations (
    id              integer PRIMARY KEY,
    eu_code         text,
    name            text NOT NULL,
    area_class      text,          -- urbana, suburbana, rurale, ...
    active          boolean,
    typology        text,          -- Chimica, Meteo, Idrometrica, ...
    network         text,          -- ArpaVDA RMQA, ArpaL RMQA, ...
    measure_type    text,          -- continua, periodica, ...
    cadence         text,          -- oraria, giornaliera, ...
    municipality    text,
    municipality_istat text,
    province        text,
    province_istat  text,
    province_code   text,
    region          text,
    region_istat    text,
    altitude        integer,
    startup_date    timestamptz,
    dismiss_date    timestamptz,
    geom            geometry(Point, 4326),
    synced_at       timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS opas_stations_geom_gix
    ON qvt.opas_stations USING GIST (geom);
CREATE INDEX IF NOT EXISTS opas_stations_region_idx
    ON qvt.opas_stations (region_istat);

-- Latest air quality measurements (one row per station × parameter)
CREATE TABLE IF NOT EXISTS qvt.opas_measurements (
    station_id      integer NOT NULL,
    station_name    text,
    region          text,
    region_istat    text,
    parameter_id    integer NOT NULL,
    parameter_name  text,
    parameter_unit  text,
    series_id       integer NOT NULL,
    measure_value   double precision,
    measure_time    timestamptz,
    quality_code    integer,
    geom            geometry(Point, 4326),
    synced_at       timestamptz DEFAULT now(),
    PRIMARY KEY (station_id, parameter_id)
);

CREATE INDEX IF NOT EXISTS opas_measurements_geom_gix
    ON qvt.opas_measurements USING GIST (geom);
CREATE INDEX IF NOT EXISTS opas_measurements_param_idx
    ON qvt.opas_measurements (parameter_id);
CREATE INDEX IF NOT EXISTS opas_measurements_region_idx
    ON qvt.opas_measurements (region_istat);

-- Hourly time series (partitioned by month)
CREATE TABLE IF NOT EXISTS qvt.opas_hourly (
    station_id      integer NOT NULL,
    parameter_id    integer NOT NULL,
    measure_time    timestamptz NOT NULL,
    station_name    text,
    region          text,
    region_istat    text,
    parameter_name  text,
    parameter_unit  text,
    series_id       integer,
    measure_value   double precision,
    validity_code   integer,      -- final_validity_code: 0=valid, 1=pending, -1=invalid
    geom            geometry(Point, 4326),
    PRIMARY KEY (station_id, parameter_id, measure_time)
) PARTITION BY RANGE (measure_time);

-- Partitions: Jan 2025 → Dec 2026 (extend as needed)
CREATE TABLE IF NOT EXISTS qvt.opas_hourly_2025_01 PARTITION OF qvt.opas_hourly
    FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');
CREATE TABLE IF NOT EXISTS qvt.opas_hourly_2025_02 PARTITION OF qvt.opas_hourly
    FOR VALUES FROM ('2025-02-01') TO ('2025-03-01');
CREATE TABLE IF NOT EXISTS qvt.opas_hourly_2025_03 PARTITION OF qvt.opas_hourly
    FOR VALUES FROM ('2025-03-01') TO ('2025-04-01');
CREATE TABLE IF NOT EXISTS qvt.opas_hourly_2025_04 PARTITION OF qvt.opas_hourly
    FOR VALUES FROM ('2025-04-01') TO ('2025-05-01');
CREATE TABLE IF NOT EXISTS qvt.opas_hourly_2025_05 PARTITION OF qvt.opas_hourly
    FOR VALUES FROM ('2025-05-01') TO ('2025-06-01');
CREATE TABLE IF NOT EXISTS qvt.opas_hourly_2025_06 PARTITION OF qvt.opas_hourly
    FOR VALUES FROM ('2025-06-01') TO ('2025-07-01');
CREATE TABLE IF NOT EXISTS qvt.opas_hourly_2025_07 PARTITION OF qvt.opas_hourly
    FOR VALUES FROM ('2025-07-01') TO ('2025-08-01');
CREATE TABLE IF NOT EXISTS qvt.opas_hourly_2025_08 PARTITION OF qvt.opas_hourly
    FOR VALUES FROM ('2025-08-01') TO ('2025-09-01');
CREATE TABLE IF NOT EXISTS qvt.opas_hourly_2025_09 PARTITION OF qvt.opas_hourly
    FOR VALUES FROM ('2025-09-01') TO ('2025-10-01');
CREATE TABLE IF NOT EXISTS qvt.opas_hourly_2025_10 PARTITION OF qvt.opas_hourly
    FOR VALUES FROM ('2025-10-01') TO ('2025-11-01');
CREATE TABLE IF NOT EXISTS qvt.opas_hourly_2025_11 PARTITION OF qvt.opas_hourly
    FOR VALUES FROM ('2025-11-01') TO ('2025-12-01');
CREATE TABLE IF NOT EXISTS qvt.opas_hourly_2025_12 PARTITION OF qvt.opas_hourly
    FOR VALUES FROM ('2025-12-01') TO ('2026-01-01');
CREATE TABLE IF NOT EXISTS qvt.opas_hourly_2026_01 PARTITION OF qvt.opas_hourly
    FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');
CREATE TABLE IF NOT EXISTS qvt.opas_hourly_2026_02 PARTITION OF qvt.opas_hourly
    FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');
CREATE TABLE IF NOT EXISTS qvt.opas_hourly_2026_03 PARTITION OF qvt.opas_hourly
    FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');
CREATE TABLE IF NOT EXISTS qvt.opas_hourly_2026_04 PARTITION OF qvt.opas_hourly
    FOR VALUES FROM ('2026-04-01') TO ('2026-05-01');
CREATE TABLE IF NOT EXISTS qvt.opas_hourly_2026_05 PARTITION OF qvt.opas_hourly
    FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');
CREATE TABLE IF NOT EXISTS qvt.opas_hourly_2026_06 PARTITION OF qvt.opas_hourly
    FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');
CREATE TABLE IF NOT EXISTS qvt.opas_hourly_2026_07 PARTITION OF qvt.opas_hourly
    FOR VALUES FROM ('2026-07-01') TO ('2026-08-01');
CREATE TABLE IF NOT EXISTS qvt.opas_hourly_2026_08 PARTITION OF qvt.opas_hourly
    FOR VALUES FROM ('2026-08-01') TO ('2026-09-01');
CREATE TABLE IF NOT EXISTS qvt.opas_hourly_2026_09 PARTITION OF qvt.opas_hourly
    FOR VALUES FROM ('2026-09-01') TO ('2026-10-01');
CREATE TABLE IF NOT EXISTS qvt.opas_hourly_2026_10 PARTITION OF qvt.opas_hourly
    FOR VALUES FROM ('2026-10-01') TO ('2026-11-01');
CREATE TABLE IF NOT EXISTS qvt.opas_hourly_2026_11 PARTITION OF qvt.opas_hourly
    FOR VALUES FROM ('2026-11-01') TO ('2026-12-01');
CREATE TABLE IF NOT EXISTS qvt.opas_hourly_2026_12 PARTITION OF qvt.opas_hourly
    FOR VALUES FROM ('2026-12-01') TO ('2027-01-01');

CREATE INDEX IF NOT EXISTS opas_hourly_geom_gix
    ON qvt.opas_hourly USING GIST (geom);
CREATE INDEX IF NOT EXISTS opas_hourly_param_idx
    ON qvt.opas_hourly (parameter_id, measure_time);
CREATE INDEX IF NOT EXISTS opas_hourly_station_time_idx
    ON qvt.opas_hourly (station_id, measure_time);
CREATE INDEX IF NOT EXISTS opas_hourly_region_idx
    ON qvt.opas_hourly (region_istat, measure_time);
