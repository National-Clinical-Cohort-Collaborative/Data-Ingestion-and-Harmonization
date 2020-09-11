/**
project : N3C DI&H
Date: 5/16/2020
Author: Stephanie Hong
Description : person can not have more than one patid, 
---gender, ethnicity, race which restriction to apply
--TBD can a person have more than one gender then look at the gender_source value to determine which one to choose.
--TBD can a person have more than one ethnicity( hispanice or not hispanic ) and race? ( can not be both set to unknown and choose one)
--TBD can a person have more than one race yes this is allowed. Does this mean we have two patid with two different race?

A person should have at least one record in one of the table below:
observation_period
visit_occurrence, observation, condition_occurrence, procedure_occurrence, drug_exposure, device_expore, measurement, note
do we need to impose a cut_off_date_begin starting jan 01, 2018?

**/



truncate table CDMsrc.PERSON_CLEAN ;

insert into CDMsrc.PERSON_CLEAN
select PERSON_ID_SEQ.nextval,  pop.person_id, current_timestamp
from 
(   select distinct person_id, gender_concept_id, ethnicity_concept_id --,race_concept_id,  -- can a person have more than one gender, eth, race
    FROM CDMsrc.PERSON  pm
    where exists
    (
        select 1 from ( 
            select 1 from CDMsrc.visit_occurrence vo where vo.person_id = pm.person_id
            union all
            select 1 from CDMsrc.observation o where o.person_id = pm.person_id 
            union all 
            select 1 from CDMsrc.observation_period op where o.person_id = pm.person_id
            union all 
            select 1 from CDMsrc.condition_occurrence co where co.person_id = pm.person_id
            union all 
            select 1 from CDMsrc.procedure_occurrence po where po.person_id = pm.person_id
            union all 
            select 1 from CDMsrc.drug_exposure de where de.person_id = pm.person_id
            union all 
            select 1 from CDMsrc.device_exposure dev where dev.person_id = pm.person_id
            union all 
            select 1 from CDMsrc.measurement ms where ms.person_id = pm.person_id
            union all 
            select 1 from CDMsrc.note nt where nt.person_id = pm.person_id
            union all 
            select 1 from CDMsrc.location lt where lt.person_id = pm.person_id
            union all
            select 1 from CDMsrc.payer_plan_period ppp where ppp.person_id = pm.person_id

        )
    )
) pop
; 
