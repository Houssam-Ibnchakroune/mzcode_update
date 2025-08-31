### The list of water sensors measured by type by month ###

SELECT 
    s.measurementMonth, s.sensorType, s.numberOfSensors FROM
    (
    SELECT 
        t.measurementMonth,
        EXTRACT(MONTH FROM TO_DATE(measurementMonth, 'MM')) monthNumber,
        s.sensorType, 
        COUNT(measurement) numberOfSensors
    FROM 
        dimSensorTable s
        INNER JOIN factMeasurementsTable f ON s.sensorID = f.sensorID
        INNER JOIN dimTimeTable t ON f.timeID = t.timeID
    GROUP BY 
        s.sensorType, t.measurementMonth 
    ) s 
ORDER BY 
    s.monthNumber, s.sensorType;
    


------------------------------------------------------------


### The number of sensor measurements collected by type by week ###

SELECT 
    t.measurementWeek, 
    s.sensorType, 
    COUNT(measurement) numberOfMeasurements
FROM 
    dimSensorTable s
    INNER JOIN factMeasurementsTable f ON s.sensorID = f.sensorID
    INNER JOIN dimTimeTable t ON f.timeID = t.timeID
GROUP BY 
    s.sensorType, t.measurementWeek
ORDER BY
    t.measurementWeek, s.sensorType;
    


---------------------------------------------------------------


### The number of measurements made by location by month ###

SELECT 
    s.measurementMonth, s.measurementLocation, s.numberOfMeasurements FROM
    (
    SELECT 
        t.measurementMonth, 
        l.measurementLocation, 
        EXTRACT(MONTH FROM TO_DATE(measurementMonth, 'MM')) monthNumber,
        COUNT(measurement) numberOfMeasurements
    FROM 
        dimLocationTable l 
        INNER JOIN factMeasurementsTable f ON l.locationID = f.locationID
        INNER JOIN dimTimeTable t ON f.timeID = t.timeID
    GROUP BY 
        l.measurementLocation, t.measurementMonth
    ) s
ORDER BY 
    s.monthNumber, s.measurementLocation;
    


----------------------------------------------------------


### The average number of measurements covered for pH by year ###

SELECT 
    t.measurementYear, 
    COUNT(measurement) numberOfMeasurements
FROM 
    dimSensorTable s 
    INNER JOIN factMeasurementsTable f ON s.sensorID = f.sensorID
    INNER JOIN dimTimeTable t ON f.timeID = t.timeID
WHERE 
    s.sensorType = 'pH'
GROUP BY 
    t.measurementYear
ORDER BY 
    t.measurementYear;
    


--------------------------------------------------------------


### The average value of nitrate measurements by locations by year ###

SELECT 
    t.measurementYear,
    l.measurementLocation,
    ROUND(AVG(measurement), 2) averageValuesOfNitrate
FROM 
    dimLocationTable l 
    INNER JOIN factMeasurementsTable f ON l.locationID = f.locationID
    INNER JOIN dimSensorTable s ON f.sensorID = s.sensorID
    INNER JOIN dimTimeTable t ON f.timeID = t.timeID
WHERE 
    s.sensorType = 'Nitrate-N'
GROUP BY 
    l.measurementLocation, t.measurementYear
ORDER BY 
    t.measurementYear, l.measurementLocation;
    


-----------------------------------------------------------------
