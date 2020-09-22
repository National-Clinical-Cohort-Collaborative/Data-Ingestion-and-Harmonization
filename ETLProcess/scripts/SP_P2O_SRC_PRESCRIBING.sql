
CREATE PROCEDURE                                        CDMH_STAGING.SP_P2O_SRC_PRESCRIBING (
    datapartnerid   IN    NUMBER,
    manifestid      IN    NUMBER,
    recordcount     OUT   NUMBER
) AS 
/********************************************************************************************************
     Name:      SP_P2O_SRC_PRESCRIBING
     Purpose:    Loading The NATIVE_PCORNET51_CDM.PRESCRIBING Table into DRUG_EXPORURE
     Source:
     Revisions:
     Ver          Date        Author               Description
     0.1       5/16/2020     SHONG Initial Version
	   0.2       7/8/2020      SNAREDLA          Fixed the logic issue with DRUG_EXPOSURE_END_DATE
       0.3       7/15/2020     SNAREDLA          Fixed the logic issue with DRUG_EXPOSURE_END_DATE 
     0.4         9/9/2020      DIH               Updated the *_type_concept_id logic
     
*********************************************************************************************************/
BEGIN
    DELETE FROM cdmh_staging.st_omop53_drug_exposure
    WHERE
        data_partner_id = datapartnerid
        AND domain_source = 'PCORNET_PRESCRIBING';

    COMMIT;
    --insert into 
    INSERT INTO cdmh_staging.st_omop53_drug_exposure (  --INSERT INTO CDMH_STAGING.ST_OMOP53_DRUG_EXPOSURE
        data_partner_id,
        manifest_id,
        drug_exposure_id,
        person_id,
        drug_concept_id,
        drug_exposure_start_date,
        drug_exposure_start_datetime,
        drug_exposure_end_date,
        drug_exposure_end_datetime,
        verbatim_end_date,
        drug_type_concept_id,
        stop_reason,
        refills,
        quantity,
        days_supply,
        sig,
        route_concept_id,
        lot_number,
        provider_id,
        visit_occurrence_id,
        visit_detail_id,
        drug_source_value,
        drug_source_concept_id,
        route_source_value,
        dose_unit_source_value,
        domain_source
    ) --26
  --PRESCRIBING
        SELECT
            datapartnerid               AS datapartnerid,
            manifestid                  AS manifestid,
            mp.n3cds_domain_map_id      AS drug_exposure_id,
            p.n3cds_domain_map_id       AS person_id,
            xw.target_concept_id        drug_concept_id,
            nvl(rx_start_date, rx_order_date) AS drug_exposure_start_date,
            nvl(rx_start_date, rx_order_date) AS drug_exposure_start_datetime, 
--    nvl(rx_end_date, rx_order_date) as drug_exposure_end_date, 
--    nvl(rx_end_date,rx_order_date) as drug_exposure_end_datetime,

--CY: obo CB: if the date is null set the drug_exposure_end_date to rx_start_date + rx_days_supply -1. 
--If rx_days_supply is null, set the drug_exposure_end_date = rx_start_date.
            CASE
                WHEN rx_end_date IS NULL THEN
                    CASE
                        WHEN nvl(rx_days_supply, 0) = 0 THEN
                            nvl(rx_start_date, rx_order_date)
                        ELSE
                            nvl(rx_start_date, rx_order_date) + rx_days_supply - 1
                    END
                ELSE
                    rx_end_date
            END AS drug_exposure_end_date,
            CASE
                WHEN rx_end_date IS NULL THEN
                    CASE
                        WHEN nvl(rx_days_supply, 0) = 0 THEN
                            nvl(rx_start_date, rx_order_date)
                        ELSE
                            nvl(rx_start_date, rx_order_date) + rx_days_supply - 1
                    END
                ELSE
                    rx_end_date
            END AS drug_exposure_end_datetime,
            pr.rx_end_date              AS verbatim_end_date,
--            38000177 AS drug_type_concept_id,
            32817 AS drug_type_concept_id,
            NULL AS stop_reason,
            NULL AS refills,
            NULL AS quantity,
            rx_days_supply              AS days_supply,
            rx_frequency                AS sig,
            mx.target_concept_id        AS route_concept_id,
            NULL AS lot_number,
            prv.n3cds_domain_map_id     AS provider_id,
            e.n3cds_domain_map_id       AS visit_occurrence_id,
            NULL AS visit_detail_id,
            rxnorm_cui                  AS drug_source_value,
            xw.source_code_concept_id   AS drug_source_concept_id, --- drug source concept id if it is prescribing
            rx_route                    AS route_source_value,
            rx_dose_ordered_unit        AS dose_unit_source_value,
            'PCORNET_PRESCRIBING' AS domain_source
        FROM
            native_pcornet51_cdm.prescribing       pr
            JOIN cdmh_staging.person_clean              pc ON pr.patid = pc.person_id
                                                 AND pc.data_partner_id = datapartnerid
            JOIN cdmh_staging.n3cds_domain_map          mp ON pr.prescribingid = mp.source_id
                                                     AND mp.domain_name = 'PRESCRIBING'
                                                     AND mp.target_domain_id = 'Drug'
                                                     AND mp.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.n3cds_domain_map          p ON p.source_id = pr.patid
                                                         AND p.domain_name = 'PERSON'
                                                         AND p.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.n3cds_domain_map          e ON e.source_id = pr.encounterid
                                                         AND e.domain_name = 'ENCOUNTER'
                                                         AND e.target_domain_id = 'Visit'
                                                         AND e.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.n3cds_domain_map          prv ON prv.source_id = pr.rx_providerid
                                                           AND prv.domain_name = 'PROVIDER'
                                                           AND prv.data_partner_id = datapartnerid
            JOIN cdmh_staging.p2o_code_xwalk_standard   xw ON rxnorm_cui = xw.src_code
                                                                 AND xw.cdm_tbl = 'PRESCRIBING'
                                                                 AND xw.target_domain_id = 'Drug'
                                                                 AND xw.target_concept_id = mp.target_concept_id
            LEFT JOIN cdmh_staging.p2o_medadmin_term_xwalk   mx ON src_cdm_column = 'RX_ROUTE'
                                                                 AND mx.src_code = pr.rx_route
            LEFT JOIN cdmh_staging.p2o_medadmin_term_xwalk   u ON pr.rx_dose_ordered_unit = u.src_code
                                                                AND u.src_cdm_column = 'RX_DOSE_ORDERED_UNIT';

    recordcount := SQL%rowcount;
    COMMIT;
    dbms_output.put_line(recordcount || ' PCORnet PRESCRIBING source data inserted to DRUG_EXPOSURE staging table, ST_OMOP53_DRUG_EXPOSURE, successfully.'
    );
END sp_p2o_src_prescribing;
