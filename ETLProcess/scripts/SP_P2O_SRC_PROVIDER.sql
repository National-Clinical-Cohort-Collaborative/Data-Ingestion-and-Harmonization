
CREATE PROCEDURE              CDMH_STAGING.SP_P2O_SRC_PROVIDER (
    datapartnerid   IN    NUMBER,
    manifestid      IN    NUMBER,
    recordcount     OUT   NUMBER
) AS
/********************************************************************************************************
     Name:      sp_p2o_src_provider
     Purpose:    Loading The NATIVE_PCORNET51_CDM.PROVIDER Table into 
                1. CDMH_STAGING.st_omop53_provider

     Edit History:
     Ver          Date        Author               Description
       0.1         8/30/20     SHONG               Intial Version.
       
*********************************************************************************************************/
BEGIN
    DELETE FROM cdmh_staging.st_omop53_provider
    WHERE
        data_partner_id = datapartnerid
        AND domain_source = 'PCORNET_PROVIDER';

    COMMIT;
    INSERT INTO cdmh_staging.st_omop53_provider (
        data_partner_id,
        manifest_id,
        provider_id,
        provider_name,
        npi,
        dea,
        specialty_concept_id,
        care_site_id,
        year_of_birth,
        gender_concept_id,
        provider_source_value,
        specialty_source_value,
        specialty_source_concept_id,
        gender_source_value,
        gender_source_concept_id,
        domain_source
    )
        SELECT
            datapartnerid                AS data_partner_id,
            manifestid                   AS manifest_id,
            mp.n3cds_domain_map_id       AS provider_id,
            NULL AS provider_name,
            NULL AS npi,
            NULL AS dea,
            NULL AS specialty_concept_id,
            NULL AS care_site_id,
            NULL AS year_of_birth,
            gx.target_concept_id         AS gender_concept_id,
            NULL AS provider_source_value,
            provider_specialty_primary   AS specialty_source_value,
            NULL AS specialty_source_concept_id,
            provider_sex                 AS gender_source_value,
            NULL AS gender_source_concept_id,
            'PCORNET_PROVIDER' AS domain_source
        FROM
            "NATIVE_PCORNET51_CDM"."PROVIDER"
            JOIN cdmh_staging.n3cds_domain_map   mp ON mp.source_id = provider.providerid
                                                     AND mp.domain_name = 'PROVIDER'
                                                     AND mp.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.gender_xwalk       gx ON gx.cdm_tbl = 'DEMOGRAPHIC'
                                                      AND gx.src_gender = provider.provider_sex;

    recordcount := SQL%rowcount;
    dbms_output.put_line(recordcount || ' PCORnet PROVIDER source data inserted to staging table, ST_OMOP53_PROVIDER, successfully.'
    ); 
--
    COMMIT;
END sp_p2o_src_provider;
