## Proposed Adeptia Workflow - last updated May 12 2020

1. SFTP ---> Sites specific Directory -->Unzip --> manifest.csv & datacount.csv & datafiles sub-folder\*.csv files
2. Queue up - dataSite data processing in order not to over write each other CDM Native data staging place
3. Load data to the CDM Native tables - Note, data load will fail if the data does not conform to the CDM structure
4. Load DataCount/Manifest --- table
5. Do Comparison - DataCount.Rowcount
6. Populate global domain id table with N3C ids - using siteid,domain, domain_sourceid, n3cid (siteid prepend sourceid),datecreated to previent data colliding with each other. 
7. Native CDM Data Validation
8. Map to OMOP5.3.1 target/using static and dynamicmap view-lookup/
9. Locate and Fix/remap failed mappings
    a. correct Mappings / re-populate using  OMOP Concept table and ones with 0 concept ids
    b. make the data conform to OMOP
    c. have as many valid values as we can inform (e.g. LOINC coding from text lab names);
    d. contains no overtly obsolete or invalid data.
    e. feedback with step 10  
10. OMOP Approved Data Quality Checks / Review
11. Merge data instance to the N3C data store
