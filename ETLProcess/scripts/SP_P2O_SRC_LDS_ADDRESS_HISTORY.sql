
CREATE PROCEDURE              CDMH_STAGING.SP_P2O_SRC_LDS_ADDRESS_HISTORY 
(
  DATAPARTNERID IN NUMBER 
, MANIFESTID IN NUMBER 
, RECORDCOUNT OUT NUMBER
) AS 
/********************************************************************************************************
     Name:      SP_P2O_SRC_LDS_ADDRESS_HISTORY
     Purpose:    Loading from the NATIVE_PCORNET51_CDM.LDS_ADDRESS_HISTORY Table into CDMH_STAGING.ST_OMOP53_LOCATION
               
     Source:
     Revisions:
     Ver          Date        Author               Description
     0.1       5/16/2020     SHONG                Initial Version
     0.2       6/25/2020     SNAREDLA             Added logic to pick latest address record for PATID

*********************************************************************************************************/
BEGIN
      DELETE FROM CDMH_STAGING.ST_OMOP53_LOCATION WHERE data_partner_id=DATAPARTNERID AND DOMAIN_SOURCE='PCORNET_LDS_ADDRESS_HISTORY';
      COMMIT;

    INSERT INTO CDMH_STAGING.ST_OMOP53_LOCATION (
    DATA_PARTNER_ID
    ,MANIFEST_ID
    ,LOCATION_ID
    ,ADDRESS_1
    ,ADDRESS_2
    ,CITY
    ,STATE
    ,ZIP
    ,COUNTY
    ,LOCATION_SOURCE_VALUE
    ,DOMAIN_SOURCE
    )
    SELECT 
    DATAPARTNERID AS DATA_PARTNER_ID, 
    MANIFESTID as MANIFEST_ID, 
    mp.N3cds_Domain_Map_Id AS LOCATION_ID,
    null as ADDRESS_1,
    null as ADDRESS_2, 
    ADDRESS_CITY as city,
    ADDRESS_STATE AS state,
    ADDRESS_ZIP5 as zip,
    null as county, 
    null as LOCATION_SOURCE_VALUE,
    'PCORNET_LDS_ADDRESS_HISTORY' AS DOMAIN_SOURCE
    FROM (WITH cte_addr as (
                  SELECT ADDRESSID,
                  PATID,
                  ADDRESS_CITY,
                  ADDRESS_STATE,
                  ADDRESS_ZIP5,
                  ADDRESS_PERIOD_START,
                  ADDRESS_PERIOD_END
                 ,Row_Number() Over (Partition By PATID Order By ADDRESS_PERIOD_END Desc) as addr_rank
                  FROM NATIVE_PCORNET51_CDM.LDS_ADDRESS_HISTORY) 
                    SELECT * FROM cte_addr where addr_rank=1 )addr
    JOIN CDMH_STAGING.PERSON_CLEAN pc on addr.PATID=pc.PERSON_ID and pc.DATA_PARTNER_ID=DATAPARTNERID
    JOIN CDMH_STAGING.N3cds_Domain_Map mp on Mp.Source_Id = addr.PATID 
                                        AND Mp.Domain_Name='LDS_ADDRESS_HISTORY' 
                                        AND mp.DATA_PARTNER_ID=DATAPARTNERID ;
    RECORDCOUNT:=SQL%ROWCOUNT;
    COMMIT;

    DBMS_OUTPUT.put_line(RECORDCOUNT || 'PCORnet LDS_ADDRESS_HISTORY source data inserted to Location staging table, ST_OMOP53_LOCATION, successfully.'); 

END SP_P2O_SRC_LDS_ADDRESS_HISTORY;
