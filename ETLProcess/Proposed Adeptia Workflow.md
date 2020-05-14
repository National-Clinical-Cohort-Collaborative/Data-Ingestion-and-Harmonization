## Proposed Adeptia Workflow - last updated May 13 2020

1. SFTP ---> Sites specific Directory -->Unzip --> manifest.csv & datacount.csv at the root directory & datafiles sub-folder\*.csv files
2. Queue up - Setup dataSet processing queue in order not to over write each other CDM Native data at the staging area
3. Load data to the CDM Native tables - Note, data load will fail if the data does not conform to the CDM table structure or data types.
4. Load DataCount/Manifest --- table
5. Do Comparison - DataCount.Rowcount if counts mis-match generate DIH_datacount_report
6. Populate global domain id table with N3C ids - using siteid,domain, domain_sourceid, n3cid (siteid prepend sourceid),datecreated to prevent data colliding from multiple-sites.
7. Native CDM Data Validation - Check list TBD - generate DQCDM_report
8. Map to OMOP5.3.1 target tables and fields using static and dynamicmapping view/lookup tables
9. Locate and Fix/remap failed mappings
    a. correct Mappings / try to re-map for those fields with 0 concept ids and nulls
    b. make the data conform to OMOP standard vocabulary as much as possible
    c. have as many valid values as we can inform (e.g. correct LOINC codes from lab text names);
    d. contains no overtly obsolete or invalid data.
    e. provide feedback loop with step 10  
10. OMOP Approved Data Quality Checks - generate DQOMOP_report - provide feeback loop to step 9
11. Contribute current instance of dataSet to the N3C data store
12. Create a backup of the current DataSet
