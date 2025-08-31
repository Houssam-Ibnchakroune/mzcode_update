### TO CREATE DIMENSION TABLES AND FACT TABLE AND LOAD THEM ###

--- To create 6 dimension tables and a fact table ---

CREATE TABLE dimLocationTable (
    locationID INTEGER GENERATED ALWAYS AS IDENTITY,
    locationMetadata VARCHAR2(255),
    locationNotation VARCHAR2(255),
    measurementLocation VARCHAR2(255),
    locationEasting INTEGER, 
    locationNorthing INTEGER,
    CONSTRAINT pk_locationID PRIMARY KEY (locationID)
    );

CREATE TABLE dimPurposeTable (
    purposeID INTEGER GENERATED ALWAYS AS IDENTITY,
    samplingPurpose VARCHAR2(255),
    CONSTRAINT pk_purposeID PRIMARY KEY (purposeID)
    );
    
CREATE TABLE dimComplianceTable (
    complianceID INTEGER GENERATED ALWAYS AS IDENTITY,
    sampleCompliance INTEGER,
    CONSTRAINT pk_complianceID PRIMARY KEY (complianceID)
    );
    
CREATE TABLE dimSampleTable (
    sampleID INTEGER GENERATED ALWAYS AS IDENTITY,
    samplingMaterial VARCHAR2(255),
    CONSTRAINT pk_sampleID PRIMARY KEY (sampleID)
    );

CREATE TABLE dimSensorTable (
    sensorID INTEGER GENERATED ALWAYS AS IDENTITY,
    sensorType VARCHAR2(255),
    sensorTypeDefinition VARCHAR2(255),
    sensorNotation INTEGER,
    sensorUnit VARCHAR2(255),
    CONSTRAINT pk_sensorID PRIMARY KEY (sensorID)
    );

CREATE TABLE dimTimeTable (
    timeID INTEGER GENERATED ALWAYS AS IDENTITY,
    measurementDate VARCHAR2(255),
    measurementDay INTEGER,
    measurementWeek INTEGER,
    measurementMonth VARCHAR2(255),
    measurementYear INTEGER,
    CONSTRAINT pk_timeID PRIMARY KEY (timeID)
    );

CREATE TABLE factMeasurementsTable (
    measurementID INTEGER GENERATED ALWAYS AS IDENTITY,
    locationID INTEGER REFERENCES dimLocationTable(locationID),
    purposeID INTEGER REFERENCES dimPurposeTable(purposeID),
    complianceID INTEGER REFERENCES dimComplianceTable(complianceID),
    sampleID INTEGER REFERENCES dimSampleTable(sampleID),
    timeID INTEGER REFERENCES dimTimeTable(timeID),
    sensorID INTEGER REFERENCES dimSensorTable(sensorID),
    measurement FLOAT,
    measurementMetadata VARCHAR2(255),
    CONSTRAINT pk_measurementID PRIMARY KEY (measurementID)
    );


---------------------------------------------------


--- Cursor to populate the LOCATION dimension table ---

DECLARE CURSOR c_location IS
    SELECT DISTINCT 
        samplesamplingPoint, 
        samplesamplingPointnotation, 
        samplesamplingPointlabel,  
        samplesamplingPointeasting, 
        samplesamplingPointnorthing
    FROM dw_water_quality;
BEGIN
    FOR c_loc IN c_location LOOP
        INSERT INTO dimLocationTable (
            locationMetadata, 
            locationNotation, 
            measurementLocation,  
            locationEasting, 
            locationNorthing
            )
        VALUES ( 
            c_loc.samplesamplingPoint, 
            c_loc.samplesamplingPointnotation, 
            c_loc.samplesamplingPointlabel, 
            c_loc.samplesamplingPointeasting, 
            c_loc.samplesamplingPointnorthing
            );
    END LOOP;
END;



--- Cursor to populate the PURPOSE dimension table ---

DECLARE CURSOR c_purpose IS
    SELECT DISTINCT 
        samplepurposelabel
    FROM dw_water_quality;
BEGIN
    FOR c_pur IN c_purpose LOOP
        INSERT INTO dimPurposeTable (
            samplingPurpose
            )
        VALUES (
            c_pur.samplepurposelabel
            );
    END LOOP;
END;



--- Cursor to populate the COMPLIANCE dimension table ---

DECLARE CURSOR c_compliance IS
    SELECT DISTINCT 
        sampleisComplianceSample
    FROM dw_water_quality;
BEGIN
    FOR c_com IN c_compliance LOOP
        INSERT INTO dimComplianceTable (
            sampleCompliance
            )
        VALUES ( 
            c_com.sampleisComplianceSample
            );
    END LOOP;
END;



--- Cursor to populate the SAMPLE dimension table ---

DECLARE CURSOR c_sample IS
    SELECT DISTINCT 
        samplesampledMaterialTypelabel
    FROM dw_water_quality;
BEGIN
    FOR c_sam IN c_sample LOOP
        INSERT INTO dimSampleTable (
            samplingMaterial
            )
        VALUES (
            c_sam.samplesampledMaterialTypelabel
            );
    END LOOP;
END;



--- Cursor to populate the SENSOR dimension table ---

DECLARE CURSOR c_sensor IS
    SELECT DISTINCT 
        determinandlabel, 
        determinanddefinition, 
        determinandnotation, 
        determinandunitlabel
    FROM dw_water_quality;
BEGIN
    FOR c_sen IN c_sensor LOOP
        INSERT INTO dimSensorTable (
            sensorType, 
            sensorTypeDefinition, 
            sensorNotation, 
            sensorUnit
            )
        VALUES (
            c_sen.determinandlabel, 
            c_sen.determinanddefinition, 
            c_sen.determinandnotation, 
            c_sen.determinandunitlabel
            );
    END LOOP;
END;



--- Cursor to populate the TIME dimension table ---

DECLARE CURSOR c_time IS
    SELECT DISTINCT 
        samplesampleDateTime
    FROM dw_water_quality;
    timeDay INTEGER;
    timeWeek INTEGER;
    timeMonth VARCHAR2(255);
    timeYear INTEGER;
BEGIN
    FOR c_tim IN c_time LOOP
    timeDay := EXTRACT(DAY FROM TO_DATE(SUBSTR(c_tim.samplesampleDateTime, 1, 10), 'YYYY-MM-DD'));
    timeWeek := TO_CHAR(TO_DATE(SUBSTR(c_tim.samplesampleDateTime, 1, 10), 'YYYY-MM-DD'), 'IW');
    timeMonth := TO_CHAR(TO_DATE(SUBSTR(c_tim.samplesampleDateTime, 1, 10), 'YYYY-MM-DD'), 'MONTH');
    timeYear := EXTRACT(YEAR FROM TO_DATE(SUBSTR(c_tim.samplesampleDateTime, 1, 10), 'YYYY-MM-DD'));
        INSERT INTO dimTimeTable (
            measurementDate, 
            measurementDay, 
            measurementWeek, 
            measurementMonth,
            measurementYear
            )
        VALUES (
            c_tim.samplesampleDateTime, 
            timeDay,
            timeWeek,
            timeMonth,
            timeYear
            );
    END LOOP;
END;



--- Cursor to populate the FACT table ---

DECLARE CURSOR c_fact IS
    SELECT
        l.locationID,
        p.purposeID,
        c.complianceID,
        sm.sampleID,
        t.timeID, 
        s.sensorID,
        wq.result,
        wq.ID_,
        wq.samplesamplingPointlabel,
        wq.determinandlabel,
        wq.samplesampleDateTime
    FROM dw_water_quality wq
        INNER JOIN dimLocationTable l 
    ON wq.samplesamplingPointlabel = l.measurementLocation
            INNER JOIN dimPurposeTable p 
    ON wq.samplepurposelabel = p.samplingPurpose
        INNER JOIN dimComplianceTable c 
    ON wq.sampleisComplianceSample = c.sampleCompliance
        INNER JOIN dimSampleTable sm 
    ON wq.samplesampledMaterialTypelabel = sm.samplingMaterial
        INNER JOIN dimSensorTable s 
    ON wq.determinandlabel = s.sensorType
        INNER JOIN dimTimeTable t 
    ON wq.samplesampleDateTime = t.measurementDate;
BEGIN
    FOR c_fac IN c_fact LOOP
        INSERT INTO factMeasurementsTable (
            locationID,
            purposeID,
            complianceID,
            sampleID,
            timeID,
            sensorID,
            measurement,
            measurementMetadata
            )
        VALUES (
            c_fac.locationID,
            c_fac.purposeID,
            c_fac.complianceID,
            c_fac.sampleID,
            c_fac.timeID, 
            c_fac.sensorID, 
            c_fac.result,
            c_fac.ID_
            );
    END LOOP;
END;

--------------------------------------------------------

##### Check each of the tables

SELECT * FROM factMeasurementsTable;    

SELECT * FROM dimLocationTable;    

SELECT * FROM dimPurposeTable;    

SELECT * FROM dimComplianceTable;    

SELECT * FROM dimSampleTable;    

SELECT * FROM dimSensorTable;   

SELECT * FROM dimTimeTable;   


---------------------------------------------------------
