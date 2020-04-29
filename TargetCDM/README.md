
This repository contains the initial version documentation for the target **OMOP v 5.3.1 CDM**. The documentation is designed for the N3C project but may be useful for other research projects as well.


# Files Included in the repository


## [OMOP CDM v5 COVID.pdf](https://github.com/National-COVID-Cohort-Collaborative/Data-Ingestion-and-Harmonization/blob/master/TargetCDM/OMOP%20CDM%20v5%20COVID.pdf "OMOP CDM v5 COVID.pdf")

The pdf version of the targetOMOP v 5.3.1 document.

## [OMOP_CDM_v5.3.1 COVID.xlsx](https://github.com/National-COVID-Cohort-Collaborative/Data-Ingestion-and-Harmonization/blob/master/TargetCDM/OMOP_CDM_v5.3.1%20COVID.xlsx "OMOP_CDM_v5.3.1 COVID.xlsx")

The spreadsheets for target domains and data fields, collapsed by OMOP domains.


# Comments welcome

Comments and discussions are welcome and appreciated. Feel free to put your thoughts into issues to this repo of thie workstream.([https://github.com/National-COVID-Cohort-Collaborative/Data-Ingestion-and-Harmonization](https://github.com/National-COVID-Cohort-Collaborative/Data-Ingestion-and-Harmonization)) This is the way we determined to keep track of such valuable information during this rush hour, and may trigger insightful discussions from the community before the final implementation.
<br>
@Ken Gursing:

> In reality PCORI, ACT and TRINETX are smaller than OMOP so most of the information from those models will be blank. The OMOP COVID-19 tablesand fields removed were one’s that the NIH had a hard time with or were not part of a Limited Data Set (date and zip codes are allowed)
> 
> Altered Tables
> 
> The Tables impacted are LOCATION, PROVIDER and PERSON all the other tables I left untouched I also added a new table called VISIT_DETAILS this table was added in 5.3.1 Finally I included two tables I doubt will have information Specimen and Death. I would be fine if we removed them
> 
> FYI Death was removed in OMOP 6.0
<br>
@Christopher G. Chute:

> 1. My immediate impressions are:<br>
>     a.  We should preserve City and State. Zip may not always be reported, having at least City and State would be a backup.<br>
>     b.  I agree that we should delete the specimen table for this purpose. To my knowledge, N3C will have no specimen content. It is true we may want to link in future to repositories that have such information, but there appears to be no purpose for it in the N3C LDS.<br>
>     c.  I would keep the death table. Clearly it is a key outcome. While it may exist in other locations, it remains the ultimate outcome.<br>
> 2.  I agree the OMOP is more the superset relative to the others and that this will result in many missing values. We agreed we would be model agnostic in our recommendation of how folks can contribute data.<br>
> It is overwhelmingly clear that OMOP is preferred, though it seems we are choosing not to point that out. I agree with that decision.
