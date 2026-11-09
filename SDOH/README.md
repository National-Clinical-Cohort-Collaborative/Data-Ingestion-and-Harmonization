# Available SDOH Survey Concept Sets
The following concept sets support harmonization and analysis of Social Determinants of Health (SDOH) survey instruments within the OMOP Common Data Model. Each concept set is published as a machine-readable JSON artifact on Zenodo with a persistent DOI, and is also provided here as a CSV file for use in data harmonization and analysis workflows.

- **CSV files:** [`csv/`](csv/), one file per concept set (one row per concept)
- **PDF of all concept sets:** [`SDOH_concept_sets.pdf`](SDOH_concept_sets.pdf), a printable summary and full concept listing for all 22 sets

| Domain | Concept Set Description | Data Engineering Use | Data Analysis Use | CSV | JSON DOI |
|---|---|---|---|---|---|
| **Food Insecurity** | All questions for food insecurity | ✓ | | [CSV](csv/FoodInsecurityQuestions-v1.csv) | https://doi.org/10.5281/zenodo.18790811 |
| | All answers for food insecurity questions | ✓ | | [CSV](csv/FoodInsecurityAnswers-v1.csv) | https://doi.org/10.5281/zenodo.18791269 |
| | Positively framed food insecurity questions | ✓ | ✓ | [CSV](csv/FoodInsecurityQuestionsPositivelyFramed-v1.csv) | https://zenodo.org/records/17807444 |
| | Social need indicated answers for positively framed questions | | ✓ | [CSV](csv/FoodInsecurityAnswer-PositivelyFramed-SocialNeedIndicated-v1.csv) | https://doi.org/10.5281/zenodo.18791953 |
| | Social need not-indicated answers for positively framed questions | | ✓ | [CSV](csv/FoodInsecurityAnswer-PositivelyFramed-SocialNeedNotIndicated-v1.csv) | https://doi.org/10.5281/zenodo.18793312 |
| | Negatively framed food insecurity questions | ✓ | ✓ | [CSV](csv/FoodInsecurityQuestionsNegativelyFramed-v1.csv) | https://doi.org/10.5281/zenodo.18793729 |
| | Social need indicated answers for negatively framed questions | | ✓ | [CSV](csv/FoodInsecurityAnswer-NegativelyFramed-SocialNeedIndicated-v1.csv) | https://doi.org/10.5281/zenodo.18794159 |
| | Social need not-indicated answers for negatively framed questions | | ✓ | [CSV](csv/FoodInsecurityAnswer-NegativelyFramed-SocialNeedNotIndicated-v1.csv) | https://doi.org/10.5281/zenodo.18794229 |
| **Housing Instability** | All questions for housing instability | ✓ | | [CSV](csv/HousingInstabilityQuestions-v1.csv) | https://doi.org/10.5281/zenodo.18779019 |
| | All answers for housing instability questions | ✓ | | [CSV](csv/HousingInstabilityAnswers-v1.csv) | https://doi.org/10.5281/zenodo.18779334 |
| | Positively framed housing instability questions | ✓ | ✓ | [CSV](csv/HousingInstabilityQuestionsPositivelyFramed-v1.csv) | https://doi.org/10.5281/zenodo.18779195 |
| | Social need indicated answers for positively framed questions | | ✓ | [CSV](csv/HousingInstabilityAnswers-PositivelyFramed-SocialNeedIndicated-v1.csv) | https://doi.org/10.5281/zenodo.18779425 |
| | Social need not-indicated answers for positively framed questions | | ✓ | [CSV](csv/HousingInstabilityAnswers-PositivelyFramed-SocialNeedNotIndicated-v1.csv) | https://doi.org/10.5281/zenodo.18790194 |
| **Transportation Barriers** | All questions for transportation barriers | ✓ | | [CSV](csv/TransportationBarrierQuestions-v1.csv) | https://doi.org/10.5281/zenodo.18776975 |
| | All answers for transportation barrier questions | ✓ | | [CSV](csv/TransportationBarrierAnswers-v1.csv) | https://doi.org/10.5281/zenodo.18777935 |
| | Positively framed transportation barrier questions | ✓ | ✓ | [CSV](csv/TransportationBarrierQuestionsPositivelyFramed-v1.csv) | https://doi.org/10.5281/zenodo.18776429 |
| | Social need indicated answers for positively framed questions | | ✓ | [CSV](csv/TransportationBarrierAnswer-PositivelyFramed-SocialNeedIndicated-v1.csv) | https://doi.org/10.5281/zenodo.18778299 |
| | Social need not-indicated answers for positively framed questions | | ✓ | [CSV](csv/TransportationBarrierAnswer-PositivelyFramed-SocialNeedNotIndicated-v1.csv) | https://doi.org/10.5281/zenodo.18778600 |
| | Negatively framed transportation barrier questions | ✓ | ✓ | [CSV](csv/TransportationBarrierQuestionsNegativelyFramed-v1.csv) | https://doi.org/10.5281/zenodo.18777668 |
| | Social need indicated answers for negatively framed questions | | ✓ | [CSV](csv/TransportationBarrierAnswer-NegativelyFramed-SocialNeedIndicated-v1.csv) | https://doi.org/10.5281/zenodo.18778723 |
| | Social need not-indicated answers for negatively framed questions | | ✓ | [CSV](csv/TransportationBarrierAnswer-NegativelyFramed-SocialNeedNotIndicated-v1.csv) | https://doi.org/10.5281/zenodo.18778862 |
| **Null / Missing** | “I don’t know” or “Prefer not to answer” responses | ✓ | ✓ | [CSV](csv/FlavorsOfNullResponses-v1.csv) | https://doi.org/10.5281/zenodo.18794406 |

## Notes

- **Data Engineering Use:** Concept sets used for ETL pipelines, survey harmonization, and mapping workflows.
- **Data Analysis Use:** Concept sets used for cohort definition, analytic queries, and identification of social needs.
- **CSV:** Generated from the Zenodo JSON files. Columns: `concept_set_name`, `concept_set_version`, `vocabulary_version`, `source_file`, `concept_id`, `concept_name`, `domain_id`, `vocabulary_id`, `concept_class_id`, `concept_code`, `standard_concept`, `invalid_reason`, `valid_start_date`, `valid_end_date`, `is_excluded`, `include_descendants`, `include_mapped`.
- **PDF:** Generated from the same CSV files; lists each concept set with its DOI, version, vocabulary version (v5.0 27-AUG-25) and concepts.
- **JSON DOI:** Persistent DOI linking to the machine-readable concept set artifact hosted on Zenodo. The Zenodo record is the version of record; if a CSV or the PDF differs from it, the Zenodo JSON takes precedence.
- **License:** CC-BY 4.0
