
# Skilled Nursing import and visit rollup

The strategy for SN starts by importing claims from the SN file to
the visit_detail table. Then since a single visit gets divided on 
month boundaries because of billing, a rollup process is used to 
create fewer, more cohesive visits in the visit_occurrence table.
This is done in a few steps that roughly mirror work done for macro
visits. They are listed here and compared to that work,starting 
with a review of macro visits.


## Macro Visit
Macro visits exist to collect in-patient (IP) and related out-patient
(OP) visits together when the OP visits are understood as componenets
of the IP visit. Unlike this SN work, the macrovisit data starts out
 in visit_occcurrence.

1. macro_visit.sql 
  This creates the date-ranges or spans of the macro visit by going through
  a series of CTEs that identify breaks between them and grouping visits into
  the macro visits.
1. macro_visit_with_micro_visit.sql
  This links the visit_occurrences to the macro_visit by including
  the visit_occurrence_id into the macro_visit
1. visit_occurrence_with_macro_visit.sql
  This does the reverse, where visit_occurrences get a FK to the macro visit.
1. macrovisit_check.sql


## SN visit_detail to visit_occcurrence rollup
SN gets loaded into visit_detail and rolled-up to visit_occurrence.
1. The process starts with the (previously written) conversion of the SN
 file to one of three normalized long files, sn_prcdrcd_long, 
 sn_diagnosis_long, or sn_hcpscd_long.
1. Start with sn_hcpscd_long, prcdrcd_long and sn_diagnosis_long so that 
the data is easily accessed and different REVCNTRCDs can be treated 
separately. Creating visits from all three covers the  chance that 
subsequent months have different codes and visit types.
1. sn_visit_detail_prelim.sql is basically an OMOPificiation of the SN data
    - The cardinality here is the number of sn visits when divided on month 
    boundaries, as they come in (~650k)
1. sn_visit_spans.sql identifies the spans of the longer visits that will 
  end up in visit_occurrence.
    - This compares to macro_visit.sql
    - The cardinality here is the number of merged visits (~300k)
1. sn_visit_spans_with_detail_id.sql joins back to the visit_detail rows 
  and includes the sn_visit_spans_with_detail_id.
    - This compares to macro_visit_with_micro_visit.sql
    - The cardinality here is back to the number of visit_detail
1. sn_visit_occurence builds out the visit_ocurrence rows using 
  sn_visit_spans_with_detail_id, creating a visit_occurrence_id PK.
    - The cardinality here is the same as spans.
1. sn_visit_detail.sql takes what has been started (and is nearly complete) 
   in sn_visit_detail_prelim, and adds the FK to visit_occurrences now that 
   we have PKs there.
    - This compares to visit_occurrence_with_macro_visit.sql
    - The cardinality remains the same.

## Differences between detail rollups and macro visits
Macro visits don't have a visit_concept_id, meaning the contained 
visit_ocurrences can be of different types. This makes perfect sense in the 
context where you have an over-arching IP visit containing other OP visits.

In contrast, visit_occurrence does have a visit_concept_id, and so do the 
visit_details. This also mostly makes sense in the context because all 
we're doing is re-connecting parts of the same visit that were divided by
monthly billing boundaries. Consideration must be paid to the multiple sources
of visit_detail rows (sn_hcpscd_long, sn_prcdrcd_long, sn_diagnosis_long)
that could possibly have the same dates, yet different visit_concept_ids.
So including visit_concept_id in the natural key for a visit_occurrence
makes sense. 

## Natural Primary Keys
- visit_detail: (person_id, provider_id, visit_detail_date, 
                 visit_detail_end_date)
    - add visit_detail_type_concept_id??
    - end date is included because you get all kinds of visits
    - Should it include the visit_detail_concept_id? That is, can a person 
        have simultaneous SN visits in ER and NIHV? An analysis of the 
        visit_detail_prelim table in the workbook noted below has columns 
        four_count and five_count for the PK as described here and one that 
        includes the concept_id. They come up the same, so observationally or 
        empirically you don't need the fifth column. Logically, the answer to 
        question above is no.
- visit_occurrence: (person_id, provider_id, visit_start_date)
    - add visit_type_concept_id??
    - SN visits don't overlap, so you don't/shouldn't need a visit_end_date to 
      distinguish two visits that start on the same day.
    - counts are in the workbook as for visit_detail, etc.
- Domains are in cells in the linked workbook with names prefixed "TEST". In
  most cases the PK are person_id, provider_id, start_date, end_date and a 
  domain concept_id. Observation and procedure_occurrence have a single date
  (end date for observation and start_date for procedure_occurrence),
  which can make joining to the visit_detail table an less exact open join. 
  Condition_occurrence, for the same person, provider, date-range and concept
  can have multiple rows because of the conditions status. That is, a patient
  can simultaneously have admission, primary and secondary conditions. If they
  are the same, the rows look like duplicates until you include this status
  in your PK.

## Open Questions, TODOs after getting a clearer picture before implementing 
- Are ADMSN_DT and THRU_DT always the right choice? DSCHRG_DT is out there too.
- Should all rows filter on PMT_AMT > 0? If not which and why?
- Importing Domain data to visit_occurrences rather than the visit_detail.
  The domain data as coded has spans that match the visit_detail rows, not
  the visit_occurrence rows. They are not rolled-up.

## Big Picture Visit Type and domain concept id flow
 There are numerous sources for visits and domain rows that ultimately come
 from the SN file. As a way to deal with its denormalized form, we use each of
 the "long" files sn_hcpscd_long, sn_prcdrcd_long and sn_diagnosis_long. There
 are also many destinations: the visit_occurrence table and the domain tables:
 observation, procedure_occurrence, device_exposure and condition_occurrence.
 Visits are created first, and then domain tables which are joined to visits.
 At the center of domain row creation is the cms_code_xwalk table
 that not only  translates codes from their incoming code systems (often ICD10)
 to OMOP code systems, but identifies the domain table to be used when routing
 the data.

 The code is structured into three parts. The visits are created in 
 visit_detail, then rolled-up to visit_occurrence.sql. Then the domain tables 
 are populated with individual scripts named after them.

 Each of the domain scripts has sections for each CMS file that would populate
 it. That is, a CMS file or source domain may appear in multiple (OMOP) domain
 scripts. In the case of SN, there may be as many as three, one for each of 
 the "long" files (sn_hcpcscd_long, sn_diagnosis_long and sn_prcdrcd_long).

 The domain scripts follow a pattern where the code comes in two parts, two 
 CTEs. The first translates from CMS names to OMOP names and types, 
 converting dates, filtering on the vocabulary for the source_concept_id, and
 identifying the target domain. The second joins with the visits previously 
 created. The join criteria are person, provider, start and end date in most 
 cases. They also  filter on the visit_concept_id or <domain>_concept_id, 
 target_domain and  source_domain (source domain is the CMS file, 'SN' in 
 this case).

 Q: why not use each of three long SN source files in each of the domain 
   tables? Observation uses all three. Procedure uses sn_hcpscd_long and 
   sn_prcdrcd_long.  Device uses just sn_hcpscd_long and Condition uses only 
   sn_diagnosis_long.
 A: It must have been clear when creating the xwalk table, that they weren't 
 all needed, that certain CMS files never route to all the domain tables.


### visits
SN file, person/BID, provider, start_date and end_date are used to create 
visits. The visit concept id defaults to 42898160 (non-hospital institutional 
visit). In some cases the REVCNTR code is used to identify ER visits instead.
(as in the question above)

### domains
- Observation from sn_hcpscd_long
  - target_domain is Observation (from mapping HCPSCD field through the 
    crosswalk)
  - src_vocabulary_code is HIPPS
  - join to visit_occurrence is on person_id, provider, end_date and 
     visit_type_concept_id = 42898160 ?
- Observation from sn_hcpscd_long (from mapping HCPSCD field through the 
  xwalk)
  - target_domain is Observation
  - src_vocabulary_code is not HIPPS
  - join to visit_occurrence is on person_id, provider, end_date and 
    visit_type_concept_id = 42898160 ?
- Observation from sn_diagnosis_long (from mapping DX field through the 
    crosswalk)
  - target_domain is Observation
  - src_vocabulary_code is unrestricted
  - join to visit_occurrence is on person_id, provider, end_date and 
    visit_type_concept_id = 42898160
- Procedure_occurrence from sn_hcpscd_long
  - target_domain is Procedure (from mapping HCPSCD field through the xwalk)
  - src_vocabulary_code any of HCPCS, ICD10PCS, CPT4
  - join to visit_occurrence is on person_id, provider, start_date, end_date
    and visit_type_concept_id = 42898160 or 9203
- Procedure_occurrence from sn_prcdrcd_long
  - target_domain is Procedure (from mapping PRCDRCD field through the xwalk)
  - src_vocabulary_code is ICD10PCS
  - join to visit_occurrence is on person_id, provider, start_date, 
    and visit_type_concept_id = 42898160
- Device_exposure from sn_hcpscd_long (from mapping HCPSCD field through the 
    crosswalk)
  - target_domain is Device
  - src_vocabulary_code is sn_hcpscd_long
  - join to visit_occurrence is on person_id, provider, start_date,  
    and visit_type_concept_id = 42898160 ?
- Condition_occurrence from sn_diagnosis_long (from mapping DX field through 
    the crosswalk)
  - target_domain is Condition
  - src_vocabulary_code is unrestricted
  - join to visit_occurrence is on person_id, provider, start_date, end_date
    and visit_type_concept_id = 42898160
  - more detail here using the dx_col to finesse primary vs admitted 
    diagnosis into the condition_status_concept_id


## Issues
- provider_id has null values that break joins and throw-off counts. As of
  this writing, I coalesce provider_id to zero so the joins work. Counts in
  a workbook mentioned below show much more consistency with this fix.
- Does this happen with other columns? will it? TBD
    - as part of the workbook, I have counted rows and distinct combinations
      of various fields when looking for primary keys, and only found this 
      one. So at least those columns seem OK, and they're the most likely to
      matter as far as joins etc. go.
- coalescing nulls in visit_detail_prelim is a good idea, but using the empty 
  string, '', for dates and datetimes might not be the best, because the 
  conversion will just result in a null again causing trouble with natural 
  PKs...


## Tricky Parts

### windowing functions
I found the windowing functions rank, lag and sum in macro_visit.sql and 
sn_visit_spans.sql to be tricky and wrote some 
[simple exercises](https://github.com/chrisroederucdenver/Overlapping-Dates)
to make more sense of them. 

## Tests, Counts and Explorations
Tests and explorations are in a 
[workbook](https://unite.nih.gov/workspace/vector/view/ri.vector.main.workbook.ece9e970-fa6b-40a9-a063-ed9031a72e6a?branch=master)
in the enclave.
- TODO migrate this to DQP or some CI/CD place, possibly using one of dbt, 
  deequ, or GreatExpectations.