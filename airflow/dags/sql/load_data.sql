ALTER ROLE eyaya; -- to allow superuser to store data
COPY trafic_data(track_id, vehicle_type, traveled_d, avg_speed,lat, lon, speed, loan_acc, lat_acc, timer)
FROM './data/cleaned_data.csv'
DELIMITER ','
CSV HEADER;
