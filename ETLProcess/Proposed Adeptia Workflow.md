## Proposed Adeptia Workflow (By May 12 2020)

1. SFTP ---> Local Dir--->Unzip --Done
2. File
a. **Manifest**/**Data_count** --- Corresponding Tables
b. DataFiles ----> Results_File (BLOB) ---> Query_Repr --Done
3. Create a queue to process one site data at a time/Results_File (BLOB) ---- Staging/CDM (Modify/test)
4. Load DataCount/Manifest --- table
5. Do Comparison - Row count
6. Populate global domain id table
7. JHU DQ review/R code execute (Native CDM)
8. Mapping to OMOP5.3 /using dynamicMap view-lookup/
9. Locate and Fix/remap failed mappings
    a. correct Mappings / re-populate using  OMOP Concept table and ones with 0 concept ids
    b. make the data conform to OMOP
   c. have as many valid values as we can inform (e.g. LOINC coding from text lab names);
   d. contains no overtly obsolete or invalid data.
   e. feedback with step 10  
10. OMOP Achilles Review
