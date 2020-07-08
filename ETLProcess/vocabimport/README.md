
We are using the CPT4 utility for CDM v5 in order to update Vocabulary table with CPT codes.
This utility will import the CPT4 vocabulary into concept.csv.
UMLS account is required and can be obtained from : 
https://utslogin.nlm.nih.gov/cas/login.

The following command is used to update the CPT4 vocabulary.
java -Dumls-user=%1 -Dumls-password=%2 -jar cpt4.jar 5
