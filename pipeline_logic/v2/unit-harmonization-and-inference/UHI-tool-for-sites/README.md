# Measurement Unit Harmonization and Inference Tool for Sites
Measurement unit harmonization and inference pipeline scripts, developed for the National COVID Cohort Collaborative (N3C) and adapted for site-level implementation. Our aim in offering this code is to help improve measurement data quality, data completeness, and unit harmonization at the site-level, which in turn faciliates research and gives value back to our current and prospective data partner sites.

## Software versions
Versions of software used for validation and pipeline steps are found in the file [pipeline-code/transforms-python/conda-versions.run.linux-64.lock](https://github.com/kbradwell/Data-Ingestion-and-Harmonization/blob/master/pipeline_logic/v2/unit-harmonization-and-inference/UHI-tool-for-sites/pipeline-code/transforms-python/conda-versions.run.linux-64.lock)

## Unit inference threshold validation and original pipeline implementation
The original development of this tool is described in the publication https://www.ncbi.nlm.nih.gov/pmc/articles/PMC9196692/, along with the Git repository https://github.com/kbradwell/N3C-units


Citation:  
Bradwell KR, Wooldridge JT, Amor B, Bennett TD, Anand A, Bremer C, Yoo YJ, Qian Z, Johnson SG, Pfaff ER, Girvin AT, Manna A, Niehaus EA, Hong SS, Zhang XT, Zhu RL, Bissell M, Qureshi N, Saltz J, Haendel MA, Chute CG, Lehmann HP, Moffitt RA; N3C Consortium. Harmonizing units and values of quantitative data elements in a very large nationally pooled electronic health record (EHR) dataset. J Am Med Inform Assoc. 2022 Jun 14;29(7):1172-1182. doi: 10.1093/jamia/ocac054. PMID: 35435957; PMCID: PMC9196692.

## Unit inference and unit harmonization

#### Overview

Rows of patient data that are missing measurement units, as well as rows of data that contain invalid units of measure for a lab, undergo unit inference.
Unit harmonization is then performed on the inferred and known units, in order to ensure a common measurement unit for analysis per lab.

#### Details

##### Unit Inference

Unit inference utilizes the Kolmogorov–Smirnov (KS) test to compare i) distributions of values that are missing units to ii) reference distributions from a set of pooled values for the lab across all the sites of N3C. The results are then filtered to ensure we are confident in only one inferred unit being present, with very little overlap to value distributions from other units. These inferred units per measurement concept for a data partner site are then added to a new column in the N3C measurement dataset rows, to enter as input for the unit harmonization process.
##### Unit Harmonization

The unit harmonization process involves using a dictionary of conversions for each measurement concept and unit to a target (canonical) unit. These conversions are applied using the following input: i) known or inferred units of the measurement, ii) measurement concept for the measurement and iii) the value as number for the measurement. This results in a new column in the measurement table, harmonized_value_as_number, and the canonical units used as a target for harmonization are recorded in another new column harmonized_unit_concept_id.

##### Workflow

Reference distributions ([filtered_percentiles_table](https://github.com/kbradwell/Data-Ingestion-and-Harmonization/blob/master/pipeline_logic/v2/unit-harmonization-and-inference/UHI-tool-for-sites/pipeline-input/filtered_percentiles_table.xlsx
)) for each measured variable, in a single canonical unit ([canonical_units_of_measure](https://github.com/kbradwell/Data-Ingestion-and-Harmonization/blob/master/pipeline_logic/v2/unit-harmonization-and-inference/UHI-tool-for-sites/pipeline-input/canonical_units_of_measure.xlsx
)), are used along with a dictionary of conversions ([conversionsDictionary](https://github.com/kbradwell/Data-Ingestion-and-Harmonization/blob/master/pipeline_logic/v2/unit-harmonization-and-inference/UHI-tool-for-sites/pipeline-input/code-to-update-input/conversionsDictionary.py
)) to determine the most likely unit for measurement concept ~ data partner (set as a constant) ~ 'null' concept combination where units are missing, the "test" set. Examples of 'null' concepts are 'Null', 'No Matching Concept', 'Other', 'No information'. Additionally, any concept within the invalid units list ([invalid_units_of_measure](https://github.com/kbradwell/Data-Ingestion-and-Harmonization/blob/master/pipeline_logic/v2/unit-harmonization-and-inference/UHI-tool-for-sites/pipeline-input/invalid_units_of_measure.xlsx
)) is treated as Null. Unit inference is achieved by first obtaining p-values from a KS test comparing the reference and test distributions using the script [get_unit_inference_calculation_file_site_tool](https://github.com/kbradwell/Data-Ingestion-and-Harmonization/blob/master/pipeline_logic/v2/unit-harmonization-and-inference/UHI-tool-for-sites/pipeline-code/transforms-python/src/get_unit_inference_calculation_file_site_tool.py). The p-values are then filtered using the script [get_unit_inference_file_site_tool](https://github.com/kbradwell/Data-Ingestion-and-Harmonization/blob/master/pipeline_logic/v2/unit-harmonization-and-inference/UHI-tool-for-sites/pipeline-code/transforms-python/src/get_unit_inference_file_site_tool.py). Finally, an intermediate mapping table of required conversions ([get_unit_mapping_file](https://github.com/kbradwell/Data-Ingestion-and-Harmonization/blob/master/pipeline_logic/v2/unit-harmonization-and-inference/UHI-tool-for-sites/pipeline-code/transforms-python/src/get_unit_mapping_file.py
)), and a final augmented measurement table are produced ([harmonization_pipeline](https://github.com/kbradwell/Data-Ingestion-and-Harmonization/blob/master/pipeline_logic/v2/unit-harmonization-and-inference/UHI-tool-for-sites/pipeline-code/transforms-python/src/harmonization_pipeline.py
)), that contains the following new columns:
  * **unit_concept_id_or_inferred_unit_concept_id**: the known measurement unit from the sites or the inferred unit in cases where the unit needed to be inferred
  * **harmonized_unit_concept_id**: canonical measurement unit ID
  * **harmonized_value_as_number**: harmonized value in the canonical units

#### Steps for reuse

1. Run [get_unit_inference_calculation_file_site_tool](https://github.com/kbradwell/Data-Ingestion-and-Harmonization/blob/master/pipeline_logic/v2/unit-harmonization-and-inference/UHI-tool-for-sites/pipeline-code/transforms-python/src/get_unit_inference_calculation_file_site_tool.py), by replacing the input PATH in the code with your local PATH for the following input:
    - [filtered_percentiles_table](https://github.com/kbradwell/Data-Ingestion-and-Harmonization/blob/master/pipeline_logic/v2/unit-harmonization-and-inference/UHI-tool-for-sites/pipeline-input/filtered_percentiles_table)
    - your OMOP measurement table
    - [canonical_units_of_measure](https://github.com/kbradwell/Data-Ingestion-and-Harmonization/blob/master/pipeline_logic/v2/unit-harmonization-and-inference/UHI-tool-for-sites/pipeline-input/canonical_units_of_measure)
    - A Python Dict of conversions created using [conversionsDictionary](https://github.com/kbradwell/Data-Ingestion-and-Harmonization/blob/master/pipeline_logic/v2/unit-harmonization-and-inference/UHI-tool-for-sites/pipeline-input/code-to-update-input/conversionsDictionary.py
)
    - [invalid_units_of_measure](https://github.com/kbradwell/Data-Ingestion-and-Harmonization/blob/master/pipeline_logic/v2/unit-harmonization-and-inference/UHI-tool-for-sites/pipeline-input/invalid_units_of_measure)
    - A lookup table of concepts to concept sets (found in [Supplementary Table S1](https://oup.silverchair-cdn.com/oup/backfile/Content_public/Journal/jamia/29/7/10.1093_jamia_ocac054/1/ocac054_supplementary_data.zip?Expires=1678171960&Signature=dnG5NMJW~to9xVMbOthBzA-py87iPSPx5rp0EADjWNlHK9gKkW3KVQP9JXo4tWw6pwWyopoBr8UJUmYtFCaan0mfUnuUjK1fEaLRKDkFtWNmy~Q9Jp7k6VAt0wAIoOb7EclIWeT5VHiGzmqW7Y8v8k8zir7R52Fp6i7WzeNXDGlfB0ToOPQGyhHaRfCctxAcGsGDQFM5vCDFPGU7UK1EdsQMcHQ69EcnJjP-wroIN49FvMsKIDBYYVgYEv4MzzBpcSWZD300RThwDtalx27RhghW6zZkHVbrEVCKBe~vxopmPduEcxCkq~XXUXCgYc6vG09XFCpelJCLdcAyDijGIA__&Key-Pair-Id=APKAIE5G5CRDK6RD3PGA) within our publication [here](https://academic.oup.com/jamia/article/29/7/1172/6569865#360557274))
2. Run [get_unit_inference_file_site_tool](https://github.com/kbradwell/Data-Ingestion-and-Harmonization/blob/master/pipeline_logic/v2/unit-harmonization-and-inference/UHI-tool-for-sites/pipeline-code/transforms-python/src/get_unit_inference_file_site_tool.py) using the following input:
    - The output table of p-values from Step #1
    - [canonical_units_of_measure](https://github.com/kbradwell/Data-Ingestion-and-Harmonization/blob/master/pipeline_logic/v2/unit-harmonization-and-inference/UHI-tool-for-sites/pipeline-input/canonical_units_of_measure)
3. Run [get_unit_mapping_file](https://github.com/kbradwell/Data-Ingestion-and-Harmonization/blob/master/pipeline_logic/v2/unit-harmonization-and-inference/UHI-tool-for-sites/pipeline-code/transforms-python/src/get_unit_mapping_file.py
) using the following input:
    - your OMOP measurement table
    - [canonical_units_of_measure](https://github.com/kbradwell/Data-Ingestion-and-Harmonization/blob/master/pipeline_logic/v2/unit-harmonization-and-inference/UHI-tool-for-sites/pipeline-input/canonical_units_of_measure)
    - [invalid_units_of_measure](https://github.com/kbradwell/Data-Ingestion-and-Harmonization/blob/master/pipeline_logic/v2/unit-harmonization-and-inference/UHI-tool-for-sites/pipeline-input/invalid_units_of_measure)
    - [Lookup table of concepts to concept sets](https://oup.silverchair-cdn.com/oup/backfile/Content_public/Journal/jamia/29/7/10.1093_jamia_ocac054/1/ocac054_supplementary_data.zip?Expires=1678171960&Signature=dnG5NMJW~to9xVMbOthBzA-py87iPSPx5rp0EADjWNlHK9gKkW3KVQP9JXo4tWw6pwWyopoBr8UJUmYtFCaan0mfUnuUjK1fEaLRKDkFtWNmy~Q9Jp7k6VAt0wAIoOb7EclIWeT5VHiGzmqW7Y8v8k8zir7R52Fp6i7WzeNXDGlfB0ToOPQGyhHaRfCctxAcGsGDQFM5vCDFPGU7UK1EdsQMcHQ69EcnJjP-wroIN49FvMsKIDBYYVgYEv4MzzBpcSWZD300RThwDtalx27RhghW6zZkHVbrEVCKBe~vxopmPduEcxCkq~XXUXCgYc6vG09XFCpelJCLdcAyDijGIA__&Key-Pair-Id=APKAIE5G5CRDK6RD3PGA)
    - The output table from step #2 (inferred units)
4. Run [harmonization_pipeline](https://github.com/kbradwell/Data-Ingestion-and-Harmonization/blob/master/pipeline_logic/v2/unit-harmonization-and-inference/UHI-tool-for-sites/pipeline-code/transforms-python/src/harmonization_pipeline.py
) using the following input:
    - your OMOP measurement table
    - [canonical_units_of_measure](https://github.com/kbradwell/Data-Ingestion-and-Harmonization/blob/master/pipeline_logic/v2/unit-harmonization-and-inference/UHI-tool-for-sites/pipeline-input/canonical_units_of_measure)
    - [invalid_units_of_measure](https://github.com/kbradwell/Data-Ingestion-and-Harmonization/blob/master/pipeline_logic/v2/unit-harmonization-and-inference/UHI-tool-for-sites/pipeline-input/invalid_units_of_measure)
    - [Lookup table of concepts to concept sets](https://oup.silverchair-cdn.com/oup/backfile/Content_public/Journal/jamia/29/7/10.1093_jamia_ocac054/1/ocac054_supplementary_data.zip?Expires=1678171960&Signature=dnG5NMJW~to9xVMbOthBzA-py87iPSPx5rp0EADjWNlHK9gKkW3KVQP9JXo4tWw6pwWyopoBr8UJUmYtFCaan0mfUnuUjK1fEaLRKDkFtWNmy~Q9Jp7k6VAt0wAIoOb7EclIWeT5VHiGzmqW7Y8v8k8zir7R52Fp6i7WzeNXDGlfB0ToOPQGyhHaRfCctxAcGsGDQFM5vCDFPGU7UK1EdsQMcHQ69EcnJjP-wroIN49FvMsKIDBYYVgYEv4MzzBpcSWZD300RThwDtalx27RhghW6zZkHVbrEVCKBe~vxopmPduEcxCkq~XXUXCgYc6vG09XFCpelJCLdcAyDijGIA__&Key-Pair-Id=APKAIE5G5CRDK6RD3PGA)
    - The output table from step #2 (inferred units)
    - The output table from step #3 (unit mappings)
    - Python Dict of conversions created using [conversionsDictionary](https://github.com/kbradwell/Data-Ingestion-and-Harmonization/blob/master/pipeline_logic/v2/unit-harmonization-and-inference/UHI-tool-for-sites/pipeline-input/code-to-update-input/conversionsDictionary.py)

For help adapting this code to your site, please contact the N3C Helpdesk (https://covid.cd2h.org/support) or Kate Bradwell (kbradwell@palantir.com).
