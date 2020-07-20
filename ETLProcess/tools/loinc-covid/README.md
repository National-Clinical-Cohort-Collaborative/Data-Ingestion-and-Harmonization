# LOINC CODE FIX

It uses the covid19 testnorm tool developed by UTexas Health to correct LOINC code in native PCORNET CDM submitted to N3C.

## Prerequisite:

  * Python 3.6 and up
  * Local Oracle libraries or client


## Installation:   

Install the required python packages using

  pip install -r requirements.txt

Update loinc_update.py. Put the correct username and password and make sure the
table names and oracle connection string is correct. 

## Usage:

  python loinc_update.py <manifest_id> <data_partner_id>

