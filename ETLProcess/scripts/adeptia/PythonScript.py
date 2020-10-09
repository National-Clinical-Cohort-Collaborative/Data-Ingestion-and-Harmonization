# Python Pre-Processing Script
# 1. Converts comma-delimited to pipe delimited csv files
# 2. When null values are found, it replaces null values with empty values (within double quotes)
# 3. Adds double quotes for a field if not present 
# 4. Checks and skips if empty files are found during processing
# 5. Removes special chars and line terminators (except \n) other than UTF-8 encoding

import csv
import os
import sys
import subprocess
import itertools
import shutil

if len(sys.argv) != 2:
    raise ValueError('Please provide input directory')
directory = sys.argv[1]
#print(f'Input dir is {dirin}')
#print(f'Output dir is {dirout}')

def get_all_files(directory):
    for filename in os.listdir(directory):
        if(os.path.isdir(directory + filename +"/")):
            new_dir = directory + filename +"/"
            get_all_files(new_dir)
        else:
            if os.path.getsize(directory + filename) > 0:
                delimiter = is_comma_separated(directory, filename)
                containsQuotes = contains_quotes(directory, filename)
                #pre_processing(directory, filename)
                if delimiter == ',':
                    #remove_temp_files(directory, filename)
                    add_quotes(directory, filename, "temp_"+filename, containsQuotes, delimiter)
                    remove_temp_files(directory, filename)
                elif (containsQuotes):
                    #remove_temp_files(directory, filename)
                    shutil.copy(directory + filename, directory + "temp_"+filename)
                    remove_temp_files(directory, filename)
                else:
                    #remove_temp_files(directory, filename)
                    add_quotes(directory, filename, "temp_"+filename, containsQuotes, delimiter)
                    remove_temp_files(directory, filename)
                replace_null(directory, filename)
                remove_temp_files(directory, "temp_"+filename)
                print(f"Successfully pre-processed the file {filename}")
    print(f"Successfully pre-processed all the files in the specified directory {directory}")
    return;

def is_comma_separated(directory, filename):
    head = ''.join(itertools.islice(open(directory + filename), 1))
    headers = csv.Sniffer().has_header(head)
    dialect = csv.Sniffer().sniff(head)
    return dialect.delimiter

def contains_quotes(directory, filename):
    head = ''.join(itertools.islice(open(directory + filename), 1))
    headers = csv.Sniffer().has_header(head)
    if head[0] == "\"":
        return True
    return False

def add_quotes(directory, inputfilename, outputfilename, containsQuotes, delimiter):
    with open(directory + inputfilename) as csvfile, \
        open(directory + outputfilename, 'w', newline='') as outputfile:
        if (containsQuotes == False and delimiter != ','):
            reader1 = csv.DictReader(csvfile, delimiter='|')
            writer1 = csv.DictWriter(outputfile, reader1.fieldnames, delimiter='|', dialect='unix', lineterminator='\n')
            writer1.writeheader()
            writer1.writerows(reader1)
        else:
            reader = csv.DictReader(csvfile, doublequote=True)
            writer = csv.DictWriter(outputfile, reader.fieldnames, delimiter='|', doublequote=True, quoting=csv.QUOTE_ALL, lineterminator='\n')
            writer.writeheader()
            writer.writerows(reader)

def replace_null(directory, filename):
    file = open(directory + "temp_"+filename, "r", encoding="utf8", errors='ignore')
    file = ''.join([i for i in file]).replace("null", "").replace("NULL", "").replace("Null", "")
    writer = open(directory + filename, "w")
    writer.writelines(file)
    writer.close()

def remove_temp_files(directory, filename):
    if os.path.exists(directory + filename):
        os.remove(directory + filename)
    else:
        print("The file does not exist")

get_all_files(directory);
