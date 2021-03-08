# dialect parameters based on csv module: https://docs.python.org/3/library/csv.html#dialects-and-formatting-parameters
# these override whatever the csv.Sniffer detects for the csv file
site_csv_dialect_dict = {
}


def get_site_dialect_params(site_id, domain):
    # Default to using backslash as escapechar
    return site_csv_dialect_dict.get((site_id, domain), {})
