# Available SDOH Survey Concept Sets in JSON format
The following concept sets support harmonization and analysis of Social Determinants of Health (SDOH) survey instruments within the OMOP Common Data Model. Each concept set is published as a machine-readable JSON artifact with a persistent DOI.

| Domain | Concept Set Description | Data Engineering Use | Data Analysis Use | JSON DOI |
|---|---|---|---|---|
| **Food Insecurity** | All questions for food insecurity | ✓ |  | https://doi.org/10.5281/zenodo.18790811 |
|  | All answers for food insecurity questions | ✓ |  | https://doi.org/10.5281/zenodo.18791269 |
|  | Positively framed food insecurity questions | ✓ | ✓ | https://doi.org/10.5281/zenodo.17807444 |
|  | Social need indicated answers for positively framed questions |  | ✓ | https://doi.org/10.5281/zenodo.18791953 |
|  | Social need not-indicated answers for positively framed questions |  | ✓ | https://doi.org/10.5281/zenodo.18793312 |
|  | Negatively framed food insecurity questions | ✓ | ✓ | https://doi.org/10.5281/zenodo.18793729 |
|  | Social need indicated answers for negatively framed questions |  | ✓ | https://doi.org/10.5281/zenodo.18794159 |
|  | Social need not-indicated answers for negatively framed questions |  | ✓ | https://doi.org/10.5281/zenodo.18794229 |
| **Housing Instability** | All questions for housing instability | ✓ |  | https://doi.org/10.5281/zenodo.18779019 |
|  | All answers for housing instability questions | ✓ |  | https://doi.org/10.5281/zenodo.18779334 |
|  | Positively framed housing instability questions | ✓ | ✓ | https://doi.org/10.5281/zenodo.18779195 |
|  | Social need indicated answers for positively framed questions |  | ✓ | https://doi.org/10.5281/zenodo.18779425 |
|  | Social need not-indicated answers for positively framed questions |  | ✓ | https://doi.org/10.5281/zenodo.18790194 |
| **Transportation Barriers** | All questions for transportation barriers | ✓ |  | https://doi.org/10.5281/zenodo.18776975 |
|  | All answers for transportation barrier questions | ✓ |  | https://doi.org/10.5281/zenodo.18777935 |
|  | Positively framed transportation barrier questions | ✓ | ✓ | https://doi.org/10.5281/zenodo.18776429 |
|  | Social need indicated answers for positively framed questions |  | ✓ | https://doi.org/10.5281/zenodo.18778299 |
|  | Social need not-indicated answers for positively framed questions |  | ✓ | https://doi.org/10.5281/zenodo.18778600 |
|  | Negatively framed transportation barrier questions | ✓ | ✓ | https://doi.org/10.5281/zenodo.18777668 |
|  | Social need indicated answers for negatively framed questions |  | ✓ | https://doi.org/10.5281/zenodo.18778723 |
|  | Social need not-indicated answers for negatively framed questions |  | ✓ | https://doi.org/10.5281/zenodo.18778862 |
| **Null / Missing** | “I don’t know” or “Prefer not to answer” responses | ✓ | ✓ | https://doi.org/10.5281/zenodo.18794406 |

## Notes

- **Data Engineering Use:** Concept sets used for ETL pipelines, survey harmonization, and mapping workflows.
- **Data Analysis Use:** Concept sets used for cohort definition, analytic queries, and identification of social needs.
- **JSON DOI:** Persistent DOI linking to the machine-readable concept set artifact hosted on Zenodo.
- **License:** CC-BY 4.0
