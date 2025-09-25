CREATE TABLE `ri.foundry.main.dataset.48e954fe-58fe-4e94-ae5f-a93ba1f74382` AS
    --inpatient dx codes
    --dx codes are in the following columns
    -- gender race ethnicity xwalk
    -- gender is sex at birth
    -- BENE_BIRTH_DT
    -- race follows the OMB required categories: White, Black or African American, American Indian or Alaska Native, Asian, and Native Hawaiian or Other Pacific Islander.
    -- Code	Code value--https://resdac.org/cms-data/variables/beneficiary-race-code-base
    -- 0	Unknown
    -- 1	White --
    -- 2	Black
    -- 3	Other
    -- 4	Asian
    -- 5	Hispanic
    -- 6	North American Native
    -- gender code - https://resdac.org/cms-data/variables/sex
    -- Code	Code value
    -- 0	Unknown-- 1	Male -- 2Female
    with xwalk as (
        
    SELECT distinct 
    'mbsf' as domain,
    'SEX_IDENT_CD' as column_name, 
    m.SEX_IDENT_CD as src_code,
    'Gender' as src_vocab_id,  
    case when m.SEX_IDENT_CD = 1 then 8507 --male
         when m.SEX_IDENT_CD = 2 then 8532 --female
         else 0
         end as target_concept_id
    FROM `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/02 - schema applied/mbsf` m
    UNION
-----------8557	4	Native Hawaiian or Other Pacific Islander?
-- 5	Hispanic--38003563/ or not Hispanic 38003564
-- RTI_RACE_CD may be more cleaner column for race and ethnicity
---- 8657 =North American Native or Alaska Native/ American Indian = 38003572/
--     SELECT DISTINCT 
--     'mbsf' as domain,
--     'RTI_RACE_CD' as column_name, 
--     RTI_RACE_CD as src_code,
--     'Race' as src_vocab_id, 
--     case when m.RTI_RACE_CD = 1 then 8527 --white
--          when m.RTI_RACE_CD = 2 then 8516 --black
--          when m.RTI_RACE_CD = 0 then 8532 --Other
--          when m.RTI_RACE_CD = 4 then 8515 --Asian
--          when m.RTI_RACE_CD = 5 then 0 --5	Hispanic--38003563 ethnicity is overloaded in the race column, in this case we will use 0 for race. Decision from 9/13/22 CMS meeting 
--          when m.RTI_RACE_CD = 6 then 8657	-- North American Native/ American Indian - 38003572//8657	1	American Indian or Alaska Native
--          else 0
--          end as target_concept_id 
--     FROM `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/02 - schema applied/mbsf` m
--     UNION

--     SELECT DISTINCT 
--     'mbsf' as domain,
--     'BENE_RACE_CD' as column_name, 
--      BENE_RACE_CD as src_code,
--     'Ethnicity' as src_vocab_id, 
--     case when m.RTI_RACE_CD = 5 then 38003563 --Hispanic
--     else 38003564 --not Hispanic
--     end as target_concept_id
--     FROM `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/02 - schema applied/mbsf` m
--     UNION
    
    SELECT DISTINCT
    'mbsf' as domain,
    'BENE_RACE_CD' as column_name, 
    BENE_RACE_CD as src_code,
    'Race' as src_vocab_id, 
    case when m.BENE_RACE_CD = 1 then 8527 --white
         when m.BENE_RACE_CD = 2 then 8516 --black
         when m.BENE_RACE_CD = 0 then 0 --Unknown
         when m.BENE_RACE_CD = 4 then 8515 --Asian
         when m.BENE_RACE_CD = 5 then 0 --5	Hispanic--38003563 ethnicity is overloaded in the race column, in this case we will use 0 for race. Decision from 9/13/22 CMS meeting 
         when m.BENE_RACE_CD = 6 then 8657	-- North American Native or American Indian - 38003572//8657	1	American Indian or Alaska Native
         else 0
         end as target_concept_id 
    FROM `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/02 - schema applied/mbsf` m
    
    )

    select * from xwalk