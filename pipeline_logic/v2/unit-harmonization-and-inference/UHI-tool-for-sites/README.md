# Measurement Unit Harmonization and Inference Tool for Sites
Measurement unit harmonization and inference pipeline scripts, developed for the National COVID Cohort Collaborative (N3C) and adapted for site-level implementation. Our aim in offering this code is to help improve measurement data quality, data completeness, and unit harmonization at the site-level, which in turn faciliates research and gives value back to our current and prospective data partner sites.

## Software versions
Versions of software used for validation and pipeline steps are found in the file [pipeline-code/transforms-python/conda-versions.run.linux-64.lock](https://github.com/kbradwell/Data-Ingestion-and-Harmonization/blob/master/pipeline_logic/v2/unit-harmonization-and-inference/UHI-tool-for-sites/pipeline-code/transforms-python/conda-versions.run.linux-64.lock)

## Unit inference threshold validation and original pipeline implementation
The original development of this tool is described in the publication https://www.ncbi.nlm.nih.gov/pmc/articles/PMC9196692/, along with the Git repository https://github.com/kbradwell/N3C-units


Citation:  
Bradwell KR, Wooldridge JT, Amor B, Bennett TD, Anand A, Bremer C, Yoo YJ, Qian Z, Johnson SG, Pfaff ER, Girvin AT, Manna A, Niehaus EA, Hong SS, Zhang XT, Zhu RL, Bissell M, Qureshi N, Saltz J, Haendel MA, Chute CG, Lehmann HP, Moffitt RA; N3C Consortium. Harmonizing units and values of quantitative data elements in a very large nationally pooled electronic health record (EHR) dataset. J Am Med Inform Assoc. 2022 Jun 14;29(7):1172-1182. doi: 10.1093/jamia/ocac054. PMID: 35435957; PMCID: PMC9196692.

## Unit inference and unit harmonization
Rows of patient data that are missing measurement units, as well as rows of data that contain invalid units of measure for a lab, undergo unit inference.
Unit harmonization is then performed on the inferred and known units, in order to ensure a common measurement unit for analysis per lab.

Details:  
Reference distributions ([filtered_percentiles_table](https://github.com/kbradwell/Data-Ingestion-and-Harmonization/blob/master/pipeline_logic/v2/unit-harmonization-and-inference/UHI-tool-for-sites/pipeline-input/filtered_percentiles_table.xlsx
)) for each measured variable, in a single canonical unit ([canonical_units_of_measure](https://github.com/kbradwell/Data-Ingestion-and-Harmonization/blob/master/pipeline_logic/v2/unit-harmonization-and-inference/UHI-tool-for-sites/pipeline-input/canonical_units_of_measure.xlsx
)), are used along with a dictionary of conversions ([conversionsDictionary](https://github.com/kbradwell/Data-Ingestion-and-Harmonization/blob/master/pipeline_logic/v2/unit-harmonization-and-inference/UHI-tool-for-sites/pipeline-input/code-to-update-input/conversionsDictionary.py
)) to determine the most likely unit for measurement concept ~ data partner (set as a constant) ~ 'null' concept combination where units are missing, the "test" set. Examples of 'null' concepts are 'Null', 'No Matching Concept', 'Other', 'No information'. Additionally, any concept within the invalid units list ([invalid_units_of_measure](https://github.com/kbradwell/Data-Ingestion-and-Harmonization/blob/master/pipeline_logic/v2/unit-harmonization-and-inference/UHI-tool-for-sites/pipeline-input/invalid_units_of_measure.xlsx
)) is treated as Null. Unit inference is achieved by first obtaining p-values from a KS test comparing the reference and test distributions using the script [get_unit_inference_calculation_file_site_tool](https://github.com/kbradwell/Data-Ingestion-and-Harmonization/blob/master/pipeline_logic/v2/unit-harmonization-and-inference/UHI-tool-for-sites/pipeline-code/transforms-python/src/get_unit_inference_calculation_file_site_tool.py). The p-values are then filtered using the script [get_unit_inference_file_site_tool](https://github.com/kbradwell/Data-Ingestion-and-Harmonization/blob/master/pipeline_logic/v2/unit-harmonization-and-inference/UHI-tool-for-sites/pipeline-code/transforms-python/src/get_unit_inference_file_site_tool.py). Finally, an intermediate mapping table of required conversions ([get_unit_mapping_file](https://github.com/kbradwell/Data-Ingestion-and-Harmonization/blob/master/pipeline_logic/v2/unit-harmonization-and-inference/UHI-tool-for-sites/pipeline-code/transforms-python/src/get_unit_mapping_file.py
)), and a final augmented measurement table are produced ([harmonization_pipeline](https://github.com/kbradwell/Data-Ingestion-and-Harmonization/blob/master/pipeline_logic/v2/unit-harmonization-and-inference/UHI-tool-for-sites/pipeline-code/transforms-python/src/harmonization_pipeline.py
)), that contains the following new columns:  
unit_concept_id_or_inferred_unit_concept_id  
harmonized_unit_concept_id  
harmonized_value_as_number  

Please reach out to kbradwell@palantir.com for help adapting this code at your site.
