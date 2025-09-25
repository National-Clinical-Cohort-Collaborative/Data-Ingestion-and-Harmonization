from cms.configs import dm, hh, hs, ip, mbsf, opl, pb, pde, sn
from ..repo_path import repo_path
import logging

'''
Purpose of this file:
This should be the single location to define paths that are relative to the location of this repository.
By having a single location rather than hard-coded paths throughout, folder structure is easier to manage and oversee.
This setup is important for Marketplace to work - take care if changing it.
'''

# Medicare dataset containing zips
zipInput = "ri.foundry.main.dataset.2680f254-12e4-43ac-a225-7f5ea6992a20"
# Provider dataset containing zips
zipProviderInput = "ri.foundry.main.dataset.d694070a-7d2d-4b70-91a0-dbbdcbe89d89"

this_repo_path = repo_path
root = "/".join(list(this_repo_path.split('/')[0:4])) + "/"
root_project = "/".join(list(this_repo_path.split('/')[0:3]))
logging.warn(repo_path)
logging.warn(root)

transform = root + "transform/"
staging = root + "staging/"

provider = root_project + "/analysis-provider-characterization/"
pp = root_project + "/medicaid/pipeline/step01_parsed/"

datasets = [
    dm,
    hh,
    hs,
    ip,
    mbsf,
    opl,
    pb,
    pde,
    sn
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
