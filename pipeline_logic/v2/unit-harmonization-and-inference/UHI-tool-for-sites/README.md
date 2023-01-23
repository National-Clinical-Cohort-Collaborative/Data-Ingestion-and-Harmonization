
# Measurement Unit Harmonization and Inference Tool for Sites
Measurement unit harmonization and inference pipeline scripts, developed for the National COVID Cohort Collaborative (N3C) and adapted for site-level implementation.

## Software versions
Versions of software used for validation and pipeline steps are found in the file pipeline-code/transforms-python/conda-versions.run.linux-64.lock

## Unit inference threshold validation and original pipeline implementation
The original development of this tool is described in the publication https://www.ncbi.nlm.nih.gov/pmc/articles/PMC9196692/, along with the Git repository https://github.com/kbradwell/N3C-units

Citation:
Bradwell KR, Wooldridge JT, Amor B, Bennett TD, Anand A, Bremer C, Yoo YJ, Qian Z, Johnson SG, Pfaff ER, Girvin AT, Manna A, Niehaus EA, Hong SS, Zhang XT, Zhu RL, Bissell M, Qureshi N, Saltz J, Haendel MA, Chute CG, Lehmann HP, Moffitt RA; N3C Consortium. Harmonizing units and values of quantitative data elements in a very large nationally pooled electronic health record (EHR) dataset. J Am Med Inform Assoc. 2022 Jun 14;29(7):1172-1182. doi: 10.1093/jamia/ocac054. PMID: 35435957; PMCID: PMC9196692.

## Unit inference and unit harmonization
Any rows of patient data that are missing units, as well as rows of data that contain invalid units of measure for a lab, undergo unit inference.
Unit harmonization is then performed on the inferred and known units, in order to ensure a common measurement unit for analysis per lab.
