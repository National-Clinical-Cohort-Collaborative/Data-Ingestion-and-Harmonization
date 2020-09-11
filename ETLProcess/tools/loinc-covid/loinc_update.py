import cx_Oracle
from pprint import pprint
from TestNorm.covid19_test_norm import load_rules_data, get_loinc_codes
import sys

if len(sys.argv) != 3:
    print('usage: update_loinc.py manifest_id partner_id')
    sys.exit(1)

(_, manifest_id, partner_id) = sys.argv

# Oralce username and password
username = ''
password = ''

# Oracle DSN and port.
dsn = 'server:1512/ORCL'  # Add the correct Data connection string. 
port = 1512
encoding = 'utf-8'

# Change this to the correct table where the Native LO
lab_result_cm_table = 'NATIVE_PCORNET51_CDM.LAB_RESULT_CM'

# Change this to the correct table for storing logs
log_table = 'JHU_DJIAO.LOINC_LAB_RESULT_LOG'

csv = None
connection = None
rules_data = load_rules_data()
update_sql = (f'update {lab_result_cm_table} SET LAB_LOINC = :loinc_code WHERE RAW_LAB_NAME = :name AND LAB_LOINC IS NULL')
copy_sql = (f'INSERT INTO {log_table} (LAB_RESULT_CM_ID, PATID, ENCOUNTERID, RAW_LAB_NAME, NEW_LAB_LOINC, MANIFEST_ID, DATA_PARTNER_ID) '
        ' SELECT LAB_RESULT_CM_ID, PATID, ENCOUNTERID, RAW_LAB_NAME, :loinc_code, :manifest_id, :partner_id '
        f' FROM {lab_result_cm_table} '
        ' WHERE RAW_LAB_NAME=:name AND LAB_LOINC IS NULL ')

loinc_map = {}
total = 0

try:
    connection = cx_Oracle.connect(
            username,
            password,
            dsn,
            encoding=encoding)
    print(connection.version)
    cursor = connection.cursor()
    rows = cursor.execute(f"select distinct LAB_LOINC, RAW_LAB_NAME, COUNT(*) AS cnt FROM {lab_result_cm_table} WHERE LAB_LOINC IS NULL GROUP BY LAB_LOINC, RAW_LAB_NAME ORDER BY cnt DESC")
    rows = cursor.fetchall()
    for row in rows:
        _, name, count = row
        if name is None:
            continue
        if 'SARS-CoV-2' not in name:
            continue
        if 'Comment' in name or 'Source' in name:
            continue
        loinc_codes = get_loinc_codes(name, rules_data)
        if len(loinc_codes['loinc']['Codes']) > 0:
            loinc_code = loinc_codes['loinc']['Codes'][0]
            cursor.execute(copy_sql, [loinc_code, manifest_id, partner_id, name])
            connection.commit()
            cursor.execute(update_sql, [loinc_code, name])
            connection.commit()
            print(f'mapped {count} rows. "{name}" -> "{loinc_code}"')
            total += count
except cx_Oracle.Error as error:
    print(error)
finally:
    if connection:
        connection.close()

print('Rows updated:', total)
