-- load_staging.sql
-- Insert a few sample rows into staging_water for testing

INSERT INTO staging_water (sensor_id, sensor_type, measure_date, measure_value, location) VALUES ('S-A1','pH', DATE '2024-01-05', 7.1, 'Location A');
INSERT INTO staging_water (sensor_id, sensor_type, measure_date, measure_value, location) VALUES ('S-A1','pH', DATE '2024-02-10', 6.9, 'Location A');
INSERT INTO staging_water (sensor_id, sensor_type, measure_date, measure_value, location) VALUES ('S-B2','Nitrate', DATE '2024-01-20', 3.2, 'Location B');
INSERT INTO staging_water (sensor_id, sensor_type, measure_date, measure_value, location) VALUES ('S-C3','pH', DATE '2024-03-15', 7.4, 'Location C');
INSERT INTO staging_water (sensor_id, sensor_type, measure_date, measure_value, location) VALUES ('S-B2','Nitrate', DATE '2024-02-05', 2.8, 'Location B');

COMMIT;
