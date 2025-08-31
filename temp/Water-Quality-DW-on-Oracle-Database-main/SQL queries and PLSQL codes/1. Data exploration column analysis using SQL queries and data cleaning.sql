######   DATA EXPLORATION  ########


### EXAMINE THE dw_water_quality STAGING TABLE THAT HAS BEEN PREPARED ###

SELECT * FROM dw_water_quality;



---------------------------------------------
COLUMN EXPLORATION
----------------------------------------------

### CHECK THE COLUMN FOR DISTINCT VALUES ###

SELECT DISTINCT ID1 
    FROM dw_water_quality;

---------------------------------------------


### CHECK THE COLUMN FOR NULL VALUES ###

SELECT * FROM dw_water_quality
    WHERE ID1 IS NULL;


--------------------------------------------------------
DRILLING DOWN
---------------------------------------------------------


### CHECK THE determinandlabel COLUMN FOR OUTLIERS ###

SELECT determinandlabel, COUNT(determinandlabel) cnt
    FROM dw_water_quality
GROUP BY determinandlabel 
ORDER BY cnt;

# Up to 90 'sensors' out of 164 appear less than three times. 
# This observation will be ignored for this project.


--------------------------------------------------


### EXAMINE THE determinandunitlabel COLUMN IN THE STAGING TABLE ###

SELECT determinandunitlabel, COUNT(determinandunitlabel) cnt
    FROM dw_water_quality
GROUP BY determinandunitlabel
ORDER BY cnt;

# 5 out of 12 units appear in less than 50 records.


------------------------------------------------------


### CHECK THE determinandunitlabel COLUMN FOR TOTAL OUTLIERS ###

SELECT * FROM dw_water_quality
    WHERE determinandunitlabel = 'unitless'
        UNION
SELECT * FROM dw_water_quality
    WHERE determinandunitlabel = 'text'
        UNION
SELECT * FROM dw_water_quality
    WHERE determinandunitlabel = 'coded';

# These records are to be deleted because they seem not to be important considering 
# their corresponding values in the determinanddefinition column.


-------------------------------------------------------


### CHECK THE determinanddefinition COLUMN INFO OF determinandunitlabel COLUMN ENTRIES THAT LOOKS ODD ###

SELECT DISTINCT s.determinanddefinition FROM
    (
    SELECT * FROM dw_water_quality
        WHERE determinandunitlabel = 'unitless'
            UNION
    SELECT * FROM dw_water_quality
        WHERE determinandunitlabel = 'text'	
            UNION
    SELECT * FROM dw_water_quality
        WHERE determinandunitlabel = 'coded'
    ) s;

# The result summary below shows the reason why these records seem not to be important.


-------------------------------------------------------
DATA CLEANING
-------------------------------------------------------

### DATA CLEANING: TO REMOVE UNNECESSARY RECORDS ###

## Deleting is done using the distinct column "ID_" as a unique key column!

DELETE FROM dw_water_quality 
    WHERE ID_ IN
        (
        SELECT s.ID_ FROM
            (
            SELECT * FROM dw_water_quality
                WHERE determinandunitlabel = 'unitless'
                    UNION
            SELECT * FROM dw_water_quality
                WHERE determinandunitlabel = 'text'	
                    UNION
            SELECT * FROM dw_water_quality
                WHERE determinandunitlabel = 'coded'
            ) s
        );


-------------------------------------------------


### EXAMINE THE RESIDUAL STAGING TABLE SO FAR ###


SELECT * FROM dw_water_quality;



------------------------------------------------
