=================== BEGIN FILE ===========================

### create the staging table called dw_water_quality

DROP TABLE dw_water_quality;

CREATE TABLE dw_water_quality (
ID1 INTEGER,
ID INTEGER,
id_ VARCHAR2(255),
samplesamplingPoint VARCHAR2(255),
samplesamplingPointnotation VARCHAR2(255),
samplesamplingPointlabel VARCHAR2(255),
samplesampleDateTime VARCHAR2(255),
determinandlabel VARCHAR2(255),
determinanddefinition VARCHAR2(255),
determinandnotation INTEGER,
resultQualifiernotation VARCHAR2(255),
result FLOAT,
codedResultInterpretationinterpretation VARCHAR2(255),
determinandunitlabel VARCHAR2(255),
samplesampledMaterialTypelabel VARCHAR2(255),
sampleisComplianceSample VARCHAR2(255),
samplepurposelabel VARCHAR2(255),
samplesamplingPointeasting INTEGER,
samplesamplingPointnorthing INTEGER
);

SELECT * FROM dw_water_quality;


======================== END OF FILE ============================
