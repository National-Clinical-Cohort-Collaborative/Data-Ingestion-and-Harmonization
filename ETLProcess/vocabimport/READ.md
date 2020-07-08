CPT4 codes were missing from our Vocabulary Table :
We are using the CPT4 utility for CDM v5 in order to update Vocabulary table with CPT codes.
This utility will import the CPT4 vocabulary into concept.csv.
UMLS account: 
https://utslogin.nlm.nih.gov/cas/login.

java -Dumls-user=%1 -Dumls-password=%2 -jar cpt4.jar 5
