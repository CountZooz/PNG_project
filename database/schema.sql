-- Create necessary schemas
CREATE SCHEMA IF NOT EXISTS wialon_data;
CREATE SCHEMA IF NOT EXISTS fuel_management;

-- Units table (vehicles and bowsers)
CREATE TABLE wialon_data.units (
    unit_id BIGINT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    type VARCHAR(50),  -- 'truck' or 'bowser'
    wialon_group_id BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    active BOOLEAN DEFAULT TRUE,
    custom_fields JSONB
);

-- Sensors table
CREATE TABLE wialon_data.sensors (
    sensor_id BIGINT PRIMARY KEY,
    unit_id BIGINT REFERENCES wialon_data.units(unit_id),
    name VARCHAR(255) NOT NULL,
    type INTEGER NOT NULL,  -- 1=Digital, 2=Analog, 3=Engine, etc.
    parameter VARCHAR(100),
    formula VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    active BOOLEAN DEFAULT TRUE,
    UNIQUE (unit_id, name)
);

-- Sensor readings table
CREATE TABLE wialon_data.sensor_readings (
    reading_id SERIAL PRIMARY KEY,
    unit_id BIGINT NOT NULL,
    unit_name VARCHAR(255) NOT NULL,
    sensor_id BIGINT NOT NULL,
    sensor_name VARCHAR(255) NOT NULL,
    sensor_type INTEGER NOT NULL,
    value DOUBLE PRECISION,
    timestamp TIMESTAMP NOT NULL,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    speed DOUBLE PRECISION,
    processed BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    collection_method VARCHAR(20) DEFAULT 'scheduled'
);
CREATE INDEX idx_sensor_readings_timestamp ON wialon_data.sensor_readings(timestamp);
CREATE INDEX idx_sensor_readings_unit_id ON wialon_data.sensor_readings(unit_id);
CREATE INDEX idx_sensor_readings_sensor_id ON wialon_data.sensor_readings(sensor_id);

-- Geozones table
CREATE TABLE wialon_data.geozones (
    geozone_id BIGINT PRIMARY KEY,
    resource_id BIGINT NOT NULL,
    name VARCHAR(255) NOT NULL,
    type INTEGER NOT NULL,  -- 1=Circle, 2=Polygon, etc.
    center_latitude DOUBLE PRECISION,
    center_longitude DOUBLE PRECISION,
    radius DOUBLE PRECISION,
    points JSONB,  -- For polygon geozones
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    active BOOLEAN DEFAULT TRUE
);

-- Geozone events table
CREATE TABLE wialon_data.geozone_events (
    event_id SERIAL PRIMARY KEY,
    unit_id BIGINT REFERENCES wialon_data.units(unit_id),
    geozone_id BIGINT REFERENCES wialon_data.geozones(geozone_id),
    event_type VARCHAR(10) NOT NULL,  -- 'enter' or 'exit'
    timestamp TIMESTAMP NOT NULL,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    processed BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- iButton events table
CREATE TABLE wialon_data.ibutton_events (
    event_id SERIAL PRIMARY KEY,
    unit_id BIGINT REFERENCES wialon_data.units(unit_id),
    ibutton_code VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    processed BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Fuel Management schema tables
CREATE TABLE fuel_management.drivers (
    driver_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    role VARCHAR(100),
    ibutton_code VARCHAR(50) UNIQUE,
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE fuel_management.vehicles (
    vehicle_id VARCHAR(50) PRIMARY KEY,
    wialon_unit_id BIGINT UNIQUE,
    name VARCHAR(255) NOT NULL,
    registration VARCHAR(50),
    fuel_capacity DOUBLE PRECISION,
    standard_burn_rate DOUBLE PRECISION,
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE fuel_management.bowsers (
    bowser_id VARCHAR(50) PRIMARY KEY,
    wialon_unit_id BIGINT UNIQUE,
    name VARCHAR(255) NOT NULL,
    capacity DOUBLE PRECISION,
    critical_level DOUBLE PRECISION,
    geozone_id BIGINT,
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE fuel_management.vehicle_status (
    unit_id BIGINT PRIMARY KEY,
    fuel_level DOUBLE PRECISION,
    odometer DOUBLE PRECISION,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    timestamp TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE fuel_management.bowser_status (
    unit_id BIGINT PRIMARY KEY,
    fuel_level DOUBLE PRECISION,
    total_dispensed DOUBLE PRECISION,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    timestamp TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE fuel_management.transactions (
    transaction_id VARCHAR(50) PRIMARY KEY,
    driver_id VARCHAR(50) REFERENCES fuel_management.drivers(driver_id),
    vehicle_id VARCHAR(50) REFERENCES fuel_management.vehicles(vehicle_id),
    bowser_id VARCHAR(50) REFERENCES fuel_management.bowsers(bowser_id),
    timestamp TIMESTAMP NOT NULL,
    dispensed_amount DOUBLE PRECISION,
    received_amount DOUBLE PRECISION,
    discrepancy DOUBLE PRECISION,
    discrepancy_percentage DOUBLE PRECISION,
    odometer_before DOUBLE PRECISION,
    odometer_after DOUBLE PRECISION,
    odo_distance DOUBLE PRECISION,
    gps_distance DOUBLE PRECISION,
    distance_discrepancy_percent DOUBLE PRECISION,
    trip_start TIMESTAMP,
    trip_end TIMESTAMP,
    trip_duration INTEGER,  -- In seconds
    status VARCHAR(50),
    verification_method VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE fuel_management.fuel_events (
    event_id SERIAL PRIMARY KEY,
    unit_id BIGINT NOT NULL,
    event_type VARCHAR(20) NOT NULL,  -- 'dispensed' or 'received'
    timestamp TIMESTAMP NOT NULL,
    amount DOUBLE PRECISION,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    transaction_id VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE fuel_management.authentication_events (
    event_id SERIAL PRIMARY KEY,
    driver_id VARCHAR(50),
    vehicle_id VARCHAR(50),
    ibutton_code VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE fuel_management.vehicle_bowser_proximity (
    event_id SERIAL PRIMARY KEY,
    vehicle_id VARCHAR(50),
    bowser_id VARCHAR(50),
    event_type VARCHAR(10) NOT NULL,  -- 'enter' or 'exit'
    timestamp TIMESTAMP NOT NULL,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE fuel_management.odometer_readings (
    reading_id SERIAL PRIMARY KEY,
    vehicle_id VARCHAR(50),
    reading DOUBLE PRECISION NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    input_method VARCHAR(50),
    verified BOOLEAN DEFAULT FALSE,
    verification_photo TEXT,
    transaction_id VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);