from cms.configs import de_base, de_dates, de_dsb, de_hh_and_spo, de_mc, de_mfp, de_wvr, ip, lt, ot, rx
from ..repo_path import repo_path
import logging

'''
Purpose of this file:
This should be the single location to define paths that are relative to the location of this repository.
By having a single location rather than hard-coded paths throughout, folder structure is easier to manage and oversee.
This setup is important for Marketplace to work - take care if changing it.
'''

zipMDCDInput = "ri.foundry.main.dataset.dc3d5c78-5eb7-4861-988a-06d278baa97e"
zipFilename = []
zipProviderFilename = []
zipTokenFilename = ["Medicaid_2023_04_03.zip"]

this_repo_path = repo_path
root = "/".join(list(this_repo_path.split('/')[0:4])) + "/pipeline/"
root_project = "/".join(list(this_repo_path.split('/')[0:3]))
logging.warn(repo_path)
logging.warn(root)

metadata = root + "metadata/"
transform = root + "transform/"
staging = root + "staging/"

provider = root_project + "/analysis-provider-characterization/"
parsed_path = transform + '01 - parsed/'
union_path = transform + '01 - union/'

datasets = [
    de_base, de_dates, de_dsb, de_hh_and_spo, de_mc, de_mfp, de_wvr, ip, lt, ot, rx
]

# Step 3
melted_columns = [
    "src_domain",
    "src_code_type",
    "src_vocab_code",
    "pkey",
    "index",
    "src_column",
    "src_code",
    "start_date_key",
    "start_date_value",
    "end_date_key",
    "end_date_value"
]

taf_de_base = [
    'taf_de_base_2017.csv',
    'taf_de_base_2018.csv',
    'taf_de_base_2019.csv',
    'taf_de_base_2020.csv'
]

taf_de_dates = [
    'taf_de_dates_2016.csv',
    'taf_de_dates_2017.csv',
    'taf_de_dates_2018.csv',
    'taf_de_dates_2019.csv',
    'taf_de_dates_2020.csv'
]

taf_de_dsb = [
    'taf_de_dsb_2016.csv',
    'taf_de_dsb_2017.csv',
    'taf_de_dsb_2018.csv',
    'taf_de_dsb_2019.csv',
    'taf_de_dsb_2020.csv'
]

taf_de_hh_and_spo = [
          'taf_de_hh_and_spo_2016.csv',
          'taf_de_hh_and_spo_2017.csv',
          'taf_de_hh_and_spo_2018.csv',
          'taf_de_hh_and_spo_2019.csv',
          'taf_de_hh_and_spo_2020.csv'
          ]

taf_de_mc = ['taf_de_mc_2016.csv',
             'taf_de_mc_2017.csv',
             'taf_de_mc_2018.csv',
             'taf_de_mc_2019.csv',
             'taf_de_mc_2020.csv'
             ]

taf_de_mfp = ['taf_de_mfp_2016.csv',
              'taf_de_mfp_2017.csv',
              'taf_de_mfp_2018.csv',
              'taf_de_mfp_2019.csv',
              'taf_de_mfp_2020.csv'
              ]

taf_de_wvr = ['taf_de_wvr_2016.csv',
              'taf_de_wvr_2017.csv',
              'taf_de_wvr_2018.csv',
              'taf_de_wvr_2019.csv',
              'taf_de_wvr_2020.csv'
              ]

taf_ip = ['taf_ip_2016.csv',
          'taf_ip_2017.csv',
          'taf_ip_2018.csv',
          'taf_ip_2019.csv',
          'taf_ip_2020.csv'
          ]

taf_lt = ['taf_lt_2016.csv',
          'taf_lt_2017.csv',
          'taf_lt_2018.csv',
          'taf_lt_2019.csv',
          'taf_lt_2020.csv'
          ]

taf_ot = ['taf_ot_2016.csv',
          'taf_ot_2017.csv',
          'taf_ot_2018.csv',
          'taf_ot_2019.csv',
          'taf_ot_2020.csv'
          ]

taf_rx = ['taf_rx_2016.csv',
          'taf_rx_2017.csv',
          'taf_rx_2018.csv',
          'taf_rx_2019.csv',
          'taf_rx_2020.csv'
          ]

input_file_list = taf_ip + taf_lt + taf_ot + taf_rx + taf_de_base + taf_de_dates + taf_de_dsb + taf_de_hh_and_spo + taf_de_mc + taf_de_mfp + taf_de_wvr