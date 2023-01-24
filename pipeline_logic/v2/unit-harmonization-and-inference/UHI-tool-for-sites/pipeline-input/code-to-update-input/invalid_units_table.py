from transforms.api import transform, Output
import pandas as pd

@transform(
    output=Output("/UNITE/Unit Harmonization/invalid_units_of_measure")
)

def my_compute_function(output):
    '''
    Below is a list of invalid units from sites per codeset.
    These units will be nulled out during the unit inference and harmonization process such that unit inference is performed.
    '''
    invalid_units = [
        {'measured_variable':'White blood cell count, x10E3/uL','codeset_id':'138719030','omop_unit_concept_id':'8792','omop_unit_concept_name':'Kelvin per microliter'},
        {'measured_variable':'White blood cell count, x10E3/uL','codeset_id':'138719030','omop_unit_concept_id':'8903','omop_unit_concept_name':'Kelvin per cubic millimeter'},
        {'measured_variable':'Neutrophils (absolute), x10E3/uL','codeset_id':'881397271','omop_unit_concept_id':'8792','omop_unit_concept_name':'Kelvin per microliter'},
        {'measured_variable':'Lymphocytes (absolute), x10E3/uL','codeset_id':'627523060','omop_unit_concept_id':'8792','omop_unit_concept_name':'Kelvin per microliter'},
        {'measured_variable':'Platelet count, x10E3/uL','codeset_id':'167697906','omop_unit_concept_id':'8792','omop_unit_concept_name':'Kelvin per microliter'}

    ]

    df = pd.DataFrame(data={
        'measured_variable': list(map(lambda p: p["measured_variable"], invalid_units)),
        'codeset_id': list(map(lambda p: p["codeset_id"], invalid_units)),
        'omop_unit_concept_id': list(map(lambda p: p["omop_unit_concept_id"], invalid_units)),
        'omop_unit_concept_name': list(map(lambda p: p["omop_unit_concept_name"], invalid_units))
    })

    return output.write_pandas(df)
