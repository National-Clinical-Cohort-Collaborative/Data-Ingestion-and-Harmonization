import csv
from pyspark.sql import Row, functions as F, types as T

header_col = "__is_header__"
errorCols = ["row_number", "error_type", "error_details"]
ErrorRow = Row(header_col, *errorCols)


def required_parse(filename_input, payload_input, domain, clean_output, error_output, all_cols, required_cols):
    parse(filename_input, payload_input, domain, clean_output, error_output, all_cols, required_cols)


def optional_parse(filename_input, payload_input, domain, clean_output, error_output, all_cols, required_cols):
    parse(filename_input, payload_input, domain, clean_output, error_output, all_cols, required_cols)


def cached_parse(filename_input, payload_input, domain, clean_output, error_output, all_cols, required_cols):
    regexPattern = "(?i).*" + domain + "\\.csv"
    fs = payload_input.filesystem()
    files_df = fs.files(regex=regexPattern)

    if files_df.count() > 0:
        parse(filename_input, payload_input, domain, clean_output, error_output, all_cols, required_cols)
    else:
        clean_output.abort()
        error_output.abort()


def metadata_parse(payload_input, filename, clean_output, error_output, all_cols, required_cols):
    regex = "(?i).*" + filename + "\\.csv"
    clean_df, error_df = parse_csv(payload_input, regex, all_cols, required_cols)

    clean_output.write_dataframe(clean_df)
    error_output.write_dataframe(error_df)


# API above, functionality below


def parse(filename_input, payload_input, domain, clean_output, error_output, all_cols, required_cols):
    regex = "(?i).*" + domain + "\\.csv"
    clean_df, error_df = parse_csv(payload_input, regex, all_cols, required_cols)

    payload_filename = filename_input.dataframe().where(F.col("newest_payload") == True).head().payload  # noqa
    clean_df = clean_df.withColumn("payload", F.lit(payload_filename))

    clean_output.write_dataframe(clean_df)
    error_output.write_dataframe(error_df)


def parse_csv(payload_input, regex, all_cols, required_cols):
    # Get the correct file from the unzipped payload
    files_df = payload_input.filesystem().files(regex=regex)

    # Parse the CSV into clean rows and error rows
    parser = CsvParser(payload_input, all_cols, required_cols)
    result_rdd = files_df.rdd.flatMap(parser)

    # The idea behind caching here was that it would make sure Spark didn't parse the CSV twice, once for the clean
    # rows and once for the error rows. However some CSVs are greater than 100G, and this line always caused an OOM
    # for those. After removing this line, we could parse the 100G even with a small profile.
    # result_rdd = result_rdd.cache()

    # Separate into good and bad rows
    clean_df = rddToDf(result_rdd, "clean", T.StructType([T.StructField(col, T.StringType()) for col in all_cols]))
    error_df = rddToDf(result_rdd, "error", T.StructType([T.StructField(col, T.StringType()) for col in errorCols]))

    # Return
    return (clean_df, error_df)


def rddToDf(inputRdd, rowType, schema):
    # Filter by type and get the rows
    resultRdd = inputRdd.filter(lambda row: row[0] == rowType)
    resultRdd = resultRdd.map(lambda row: row[1])

    if resultRdd.isEmpty():
        # Convert to DF using the given schema
        resultDf = resultRdd.toDF(schema)
    else:
        # Convert to DF using the RDD Rows' schema
        resultDf = resultRdd.toDF()

        # Drop the header row - get only the data rows. This is needed to ensure we get the right schema.
        resultDf = resultDf.filter(resultDf[header_col] == False).drop(header_col)

    return resultDf


class CsvParser():
    def __init__(self, rawInput, all_cols, required_cols):
        self.rawInput = rawInput
        self.all_cols = all_cols
        self.required_cols = required_cols

    def __call__(self, csvFilePath):
        try:
            dialect = self.determineDialect(csvFilePath)
        except Exception as e:
            yield ("error", ErrorRow(False, "0", "Could not determine the CSV dialect", repr(e)))
            return

        with self.rawInput.filesystem().open(csvFilePath.path, errors='ignore') as csvFile:
            csvReader = csv.reader(csvFile, dialect=dialect)
            yield from self.parseHeader(csvReader)
            yield from self.parseFile(csvReader)

    def determineDialect(self, csvFilePath):
        with self.rawInput.filesystem().open(csvFilePath.path, errors='ignore') as csvFile:
            dialect = csv.Sniffer().sniff(csvFile.readline(), delimiters=",|")

        return dialect

    def parseHeader(self, csvReader):
        header = next(csvReader)
        header = [x.strip().strip("\ufeff").upper() for x in header]
        header = [*filter(lambda col: col, header)]  # Remove empty headers

        self.CleanRow = Row(header_col, *header)
        self.expected_num_fields = len(header)

        yield ("clean", self.CleanRow(True, *header))
        yield ("error", ErrorRow(True, "", "", ""))

        warningDetails = {
            "all columns": self.all_cols,
            "required columns": self.required_cols,
            "header": header
        }

        # Throw warning for every column in the required schema but not in the header
        for col in self.required_cols:
            if col not in header:
                message = f"Header did not contain required column `{col}`"
                yield ("error", ErrorRow(False, "0", message, warningDetails))

        # Throw warning for every column in the header but not in the schema
        for col in header:
            if col not in self.all_cols:
                message = f"Header contained unexpected extra column `{col}`"
                yield ("error", ErrorRow(False, "0", message, warningDetails))

    def parseFile(self, csvReader):
        i = 0
        while True:
            i += 1

            nextError = False
            try:
                row = next(csvReader)
            except StopIteration:
                break
            except Exception as e:
                nextError = [str(i), "Unparsable row", repr(e)]

            # Hit an error parsing
            if nextError:
                yield ("error", ErrorRow(False, *nextError))

            # Properly formatted row
            elif len(row) == self.expected_num_fields:
                yield ("clean", self.CleanRow(False, *row))

            # Ignore empty rows/extra newlines
            elif not row:
                continue

            # Improperly formatted row
            else:
                message = f"Incorrect number of fields. Expected {str(self.expected_num_fields)} but found {str(len(row))}."
                yield ("error", ErrorRow(False, str(i), message, str(row)))
