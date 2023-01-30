import csv
import tempfile
import shutil
from transforms.api import TransformInput, TransformOutput
from pyspark.sql import Row
from act.act_schemas import complete_domain_schema_dict, schema_dict_to_struct
from act.site_specific_utils import get_site_dialect_params


def parse_input(ctx, my_input: TransformInput, error_df: TransformOutput, site_id: int, domain: str, regex: str):

    def process_file(file_status):
        # Copy contents of file from Foundry into temp file
        with tempfile.NamedTemporaryFile() as t:
            with my_input.filesystem().open(file_status.path, 'rb') as f_bytes:
                shutil.copyfileobj(f_bytes, t)
                t.flush()

            # Read the csv, line by line, and use csv.Sniffer to infer the delimiter
            # Write any improperly formatted rows to the errors DataFrame
            with open(t.name, newline="", encoding="utf8", errors='ignore') as f:
                with error_df.filesystem().open('error_rows', 'w', newline='') as writeback:
                    dialect = csv.Sniffer().sniff(f.read(1024))
                    f.seek(0)
                    dialect_params = get_site_dialect_params(site_id, domain)
                    r = csv.reader(f, delimiter=dialect.delimiter, **dialect_params)
                    w = csv.writer(writeback)

                    # Construct a pyspark.Row from our header row
                    header = next(r)
                    MyRow = Row(*header)
                    expected_num_fields = len(header)

                    error_encountered = False
                    for i, row in enumerate(r):
                        if len(row) == expected_num_fields:
                            # Properly formatted row
                            yield MyRow(*row)
                        elif not row:
                            continue  # ignore empty rows/extra newlines
                        else:
                            # Improperly formatted row
                            if not error_encountered:
                                # Create header for output csv
                                w.writerow(["row_number", "row_contents"])
                                error_encountered = True
                            # Write to a csv file in the errors dataset, recording the row number and malformed row
                            malformed_row = "|".join(row)
                            w.writerow([str(i), malformed_row])

    files_df = my_input.filesystem().files(regex=regex)
    processed_rdd = files_df.rdd.flatMap(process_file)

    if processed_rdd.isEmpty():
        # Get OrderedDict that specifies this domain's schema
        schema_dict = complete_domain_schema_dict[domain]
        # Create StructType for the schema with all types as strings
        struct_schema = schema_dict_to_struct(schema_dict, all_string_type=True)
        # Create empty dataset with proper columns, all string types
        processed_df = processed_rdd.toDF(struct_schema)
    else:
        # csv file for the domain is empty
        # Create dataset with whatever columns the site gave us, all string types
        processed_df = processed_rdd.toDF()

    return processed_df
