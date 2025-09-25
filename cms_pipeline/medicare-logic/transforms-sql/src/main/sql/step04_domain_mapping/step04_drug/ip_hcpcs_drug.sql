CREATE TABLE `ri.foundry.main.dataset.d718778c-54bd-4106-bbed-7ee2672cc194` AS
 --/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/04 - domain_mapping/drug/ip_hcpcs_drug
    with ip_hcpcs (
    select distinct 
    CLAIM_ID
    , BID
    ,HCPSCD
    ,ADMSN_DT 
    ,DSCHRGDT 
    ,THRU_DT
    ,PROVIDER
    ,SGMT_NUM 
    ,TYPE_ADM 
    ,'IP' as source_domain
    FROM  `ri.foundry.main.dataset.774bb207-ecda-4ce6-922f-20e71cb4f55c` ip 
    ),
    -- TODO: ip hcpcs is only returning sgmt_num = 1, is this valid?
    ip_hcpcs_drug as ( --- HCPSCD01-45 reference the long datasets -- 25 rows from IP
    SELECT
     CAST(ip.BID as long) as person_id
    , cxw.target_concept_id as drug_concept_id
    , TO_DATE(ip.ADMSN_DT, 'ddMMMyyyy') as drug_exposure_start_date
    , TO_DATE(ip.DSCHRGDT, 'ddMMMyyyy') as drug_exposure_end_date
    , 32810 as drug_type_concept_id --32810 claim type
    , CAST(ip.PROVIDER AS long) AS provider_id
    , CAST(HCPSCD as string) as  drug_source_value
    , cxw.source_concept_id as drug_source_concept_id
    , CAST(ip.PROVIDER as long) as cms_provider_id  
    ,'IP' as source_domain
    , ip.TYPE_ADM
    , ipv.visit_concept_id
    FROM ip_hcpcs ip
    LEFT JOIN `ri.foundry.main.dataset.d410cfbe-a82a-47e7-ab1a-41dcde8e9e16` cxw ON ip.HCPSCD = cxw.cms_src_code AND cxw.target_domain_id = 'Drug' 
    left join `/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/03 - prepared/ip_visits` ipv 
    on ip.BID = ipv.BID AND ip.CLAIM_ID =ipv.CLAIM_ID AND ip.PROVIDER = ipv.PROVIDER and ip.ADMSN_DT=ipv.ADMSN_DT and ip.DSCHRGDT=ipv.DSCHRGDT and ip.THRU_DT = ipv.THRU_DT and ip.TYPE_ADM = ipv.TYPE_ADM
    WHERE ip.BID is not null and ip.CLAIM_ID is not null and ip.HCPSCD is not null and cxw.target_concept_id is not null 
    ),
    ip_hcpcs_drug_with_visit as (
    select distinct -- order of these columns are important for the drug_exposure, must be in the following order 
    CAST(d.person_id as long) as person_id
    , drug_concept_id
    , drug_exposure_start_date
    ,  CAST(drug_exposure_start_date AS timestamp)drug_exposure_start_datetime
    , drug_exposure_end_date
    , CAST(drug_exposure_end_date AS timestamp) as drug_exposure_end_datetime
    , cast( null as date ) as verbatim_end_date
    , drug_type_concept_id
    , CAST(null AS string) AS stop_reason
    , CAST( null as Integer)  as refills ---this is 2 character string need to converts to int 01 to 57
    , CAST(null AS float) as quantity
    , CAST(null as Integer) as days_supply
    , CAST(null AS string) AS sig
    , CAST(null as int) as route_concept_id
    , CAST(null AS string) AS lot_number
    , CAST(d.provider_id AS long) AS provider_id
    , visit_occurrence_id
    , v.visit_concept_id
    , CAST(null AS long) AS visit_detail_id
    , d.drug_source_value
    , d.drug_source_concept_id
    , d.cms_provider_id  
    , cast(null as string) route_source_value
    , cast(null as string ) dose_unit_source_value
    , d.TYPE_ADM as place_of_service_source_value --ip.TYPE_ADM
    , d.source_domain
    FROM ip_hcpcs_drug d
    left JOIN `ri.foundry.main.dataset.9909b93e-c79b-482a-b542-06be55f4a33a` v
    on d.person_id = v.person_id
    and d.provider_id = v.provider_id
    and v.visit_start_date = d.drug_exposure_start_date
    and v.visit_end_date = d.drug_exposure_end_date
    and v.visit_concept_id = d.visit_concept_id-- it rollup based on the CMS place of visit. 
    and v.source_domain= d.source_domain
    )
    select * from ip_hcpcs_drug_with_visit
    --select  * from ip_hcpcs_drug

