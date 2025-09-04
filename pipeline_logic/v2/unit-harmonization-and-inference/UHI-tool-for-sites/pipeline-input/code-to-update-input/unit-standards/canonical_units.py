from transforms.api import transform, Output
import pandas as pd

@transform(
    output=Output("/UNITE/Unit Harmonization/canonical_units_of_measure")
)

def my_compute_function(output):
    canonical_units = [
        {'measured_variable':'Albumin (g/dL)','codeset_id':'104464584','omop_unit_concept_id':'8713','omop_unit_concept_name':'gram per deciliter','max_acceptable_value':'10','min_acceptable_value':'0'},
        {'measured_variable':'ALT (SGPT), IU/L','codeset_id':'538737057','omop_unit_concept_id':'8923','omop_unit_concept_name':'international unit per liter','max_acceptable_value':'50000','min_acceptable_value':'0'},
        {'measured_variable':'AST (SGOT), IU/L','codeset_id':'248480621','omop_unit_concept_id':'8923','omop_unit_concept_name':'international unit per liter','max_acceptable_value':'50000','min_acceptable_value':'0'},
        {'measured_variable':'Bilirubin (total), mg/dL','codeset_id':'586434833','omop_unit_concept_id':'8840','omop_unit_concept_name':'milligram per deciliter','max_acceptable_value':'100','min_acceptable_value':'0'},
        {'measured_variable':'Bilirubin (Conjugated/Direct), mg/dL','codeset_id':'50252641','omop_unit_concept_id':'8840','omop_unit_concept_name':'milligram per deciliter','max_acceptable_value':'100','min_acceptable_value':'0'},
        {'measured_variable':'Bilirubin - (Unconjugated/Indirect), mg/dL','codeset_id':'59108731','omop_unit_concept_id':'8840','omop_unit_concept_name':'milligram per deciliter','max_acceptable_value':'100','min_acceptable_value':'0'},
        {'measured_variable':'BMI','codeset_id':'65622096','omop_unit_concept_id':'9531','omop_unit_concept_name':'kilogram per square meter','max_acceptable_value':'100','min_acceptable_value':'1'},
        {'measured_variable':'BNP, pg/mL','codeset_id':'703853936','omop_unit_concept_id':'8845','omop_unit_concept_name':'picogram per milliliter','max_acceptable_value':'30000','min_acceptable_value':'0'},
        {'measured_variable':'Body weight','codeset_id':'776390058','omop_unit_concept_id':'9529','omop_unit_concept_name':'kilogram','max_acceptable_value':'500','min_acceptable_value':'0.1'},
        {'measured_variable':'BUN, mg/dL','codeset_id':'139231433','omop_unit_concept_id':'8840','omop_unit_concept_name':'milligram per deciliter','max_acceptable_value':'500','min_acceptable_value':'0'},
        {'measured_variable':'BUN/Creatinine ratio','codeset_id':'165765962','omop_unit_concept_id':None,'omop_unit_concept_name':'','max_acceptable_value':'400','min_acceptable_value':'0'},
        {'measured_variable':'c-reactive protein CRP, mg/L','codeset_id':'371622342','omop_unit_concept_id':'8751','omop_unit_concept_name':'milligram per liter','max_acceptable_value':'1000','min_acceptable_value':'0'},
        {'measured_variable':'CD4 Cell Count (absolute)','codeset_id':'24299640','omop_unit_concept_id':'8784','omop_unit_concept_name':'cells per microliter','max_acceptable_value':'5000','min_acceptable_value':'0'},
        {'measured_variable':'CD4 Cell Count (relative)','codeset_id':'254892302','omop_unit_concept_id':'8554','omop_unit_concept_name':'percent','max_acceptable_value':'100','min_acceptable_value':'0'},
        {'measured_variable':'CD8 Cell Count (absolute)','codeset_id':'301181605','omop_unit_concept_id':'8784','omop_unit_concept_name':'cells per microliter','max_acceptable_value':'5000','min_acceptable_value':'0'},
        {'measured_variable':'CD8 Cell Count (relative)','codeset_id':'225952304','omop_unit_concept_id':'8554','omop_unit_concept_name':'percent','max_acceptable_value':'100','min_acceptable_value':'0'},
        {'measured_variable':'CD4/CD8 Ratio','codeset_id':'410508887','omop_unit_concept_id':'8523','omop_unit_concept_name':'ratio','max_acceptable_value':'100','min_acceptable_value':'0'},
        {'measured_variable':'Chloride, mmol/L','codeset_id':'733538531','omop_unit_concept_id':'8753','omop_unit_concept_name':'millimole per liter','max_acceptable_value':'200','min_acceptable_value':'50'},
        {'measured_variable':'Creatinine, mg/dL','codeset_id':'615348047','omop_unit_concept_id':'8840','omop_unit_concept_name':'milligram per deciliter','max_acceptable_value':'30','min_acceptable_value':'0'},
        {'measured_variable':'D-Dimer, mg/L FEU','codeset_id':'475972797','omop_unit_concept_id':'44783461','omop_unit_concept_name':'Milligram per liter fibrinogen equivalent unit','max_acceptable_value':'500','min_acceptable_value':'0'},
        {'measured_variable':'Diastolic blood pressure','codeset_id':'658772723','omop_unit_concept_id':'8876','omop_unit_concept_name':'millimeter mercury column','max_acceptable_value':'400','min_acceptable_value':'0'},
        {'measured_variable':'Eosinophils (absolute)','codeset_id':'296867571','omop_unit_concept_id':'8848','omop_unit_concept_name':'thousand per microliter','max_acceptable_value':'200','min_acceptable_value':'0'},
        {'measured_variable':'Eosinophils (relative)','codeset_id':'309381426','omop_unit_concept_id':'8554','omop_unit_concept_name':'percent','max_acceptable_value':'100','min_acceptable_value':'0'},
        {'measured_variable':'Erythrocyte Sed. Rate, mm/hr','codeset_id':'905545362','omop_unit_concept_id':'8752','omop_unit_concept_name':'millimeter per hour','max_acceptable_value':'300','min_acceptable_value':'0'},
        {'measured_variable':'Ferritin, ng/mL','codeset_id':'317388455','omop_unit_concept_id':'8842','omop_unit_concept_name':'nanogram per milliliter','max_acceptable_value':'100000','min_acceptable_value':'0'},
        {'measured_variable':'Fibrinogen, pg/mL','codeset_id':'27035875','omop_unit_concept_id':'8840','omop_unit_concept_name':'milligram per deciliter','max_acceptable_value':'20000','min_acceptable_value':'0'},
        {'measured_variable':'GCS Eye','codeset_id':'481397778','omop_unit_concept_id':None,'omop_unit_concept_name':'','max_acceptable_value':'4','min_acceptable_value':'1'},
        {'measured_variable':'GCS Motor','codeset_id':'706664851','omop_unit_concept_id':None,'omop_unit_concept_name':'','max_acceptable_value':'6','min_acceptable_value':'1'},
        {'measured_variable':'GCS Verbal','codeset_id':'660016502','omop_unit_concept_id':None,'omop_unit_concept_name':'','max_acceptable_value':'5','min_acceptable_value':'1'},
        {'measured_variable':'Glucose, mg/dL','codeset_id':'59698832','omop_unit_concept_id':'8840','omop_unit_concept_name':'milligram per deciliter','max_acceptable_value':'1000','min_acceptable_value':'0'},
        {'measured_variable':'Heart rate','codeset_id':'596956209','omop_unit_concept_id':'8483','omop_unit_concept_name':'counts per minute','max_acceptable_value':'500','min_acceptable_value':'0'},
        {'measured_variable':'Height','codeset_id':'754731201','omop_unit_concept_id':'9546','omop_unit_concept_name':'meter','max_acceptable_value':'3','min_acceptable_value':'0'},
        {'measured_variable':'Hemoglobin A1c, %','codeset_id':'381434987','omop_unit_concept_id':'8554','omop_unit_concept_name':'percent','max_acceptable_value':'25','min_acceptable_value':'2.0000001'},
        {'measured_variable':'Hemoglobin, g/dL','codeset_id':'28177118','omop_unit_concept_id':'8713','omop_unit_concept_name':'gram per deciliter','max_acceptable_value':'30','min_acceptable_value':'1'},
        {'measured_variable':'HIV Viral Load','codeset_id':'776696816','omop_unit_concept_id':'8799','omop_unit_concept_name':'copies per milliliter','max_acceptable_value':'10000000000','min_acceptable_value':'0'},
        {'measured_variable':'IL-6, pg/mL','codeset_id':'928163892','omop_unit_concept_id':'8845','omop_unit_concept_name':'picogram per milliliter','max_acceptable_value':'60000','min_acceptable_value':'0'},
        {'measured_variable':'Lactate, U/L','codeset_id':'400660753','omop_unit_concept_id':'8753','omop_unit_concept_name':'millimole per liter','max_acceptable_value':'100','min_acceptable_value':'0'},
        {'measured_variable':'Lactate Dehydrogenase (LDH), units/L','codeset_id':'10450810','omop_unit_concept_id':'8645','omop_unit_concept_name':'unit per liter','max_acceptable_value':'1000000','min_acceptable_value':'0'},
        {'measured_variable':'Lymphocytes (absolute), x10E3/uL','codeset_id':'627523060','omop_unit_concept_id':'8848','omop_unit_concept_name':'thousand per microliter','max_acceptable_value':'5000','min_acceptable_value':'0'},
        {'measured_variable':'Lymphocytes (relative), %','codeset_id':'45416701','omop_unit_concept_id':'8554','omop_unit_concept_name':'percent','max_acceptable_value':'100','min_acceptable_value':'0'},
        {'measured_variable':'Mean arterial pressure','codeset_id':'709075479','omop_unit_concept_id':'8876','omop_unit_concept_name':'millimeter mercury column','max_acceptable_value':'300','min_acceptable_value':'20'},
        {'measured_variable':'Neutrophils (absolute), x10E3/uL','codeset_id':'881397271','omop_unit_concept_id':'8848','omop_unit_concept_name':'thousand per microliter','max_acceptable_value':'5000','min_acceptable_value':'0'},
        {'measured_variable':'Neutrophils (relative), %','codeset_id':'304813348','omop_unit_concept_id':'8554','omop_unit_concept_name':'percent','max_acceptable_value':'100','min_acceptable_value':'0'},
        {'measured_variable':'NT pro BNP, pg/mL','codeset_id':'561166072','omop_unit_concept_id':'8845','omop_unit_concept_name':'picogram per milliliter','max_acceptable_value':'1000000','min_acceptable_value':'0'},
        {'measured_variable':'pH','codeset_id':'845428728','omop_unit_concept_id':'8482','omop_unit_concept_name':'pH','max_acceptable_value':'8.5','min_acceptable_value':'5'},
        {'measured_variable':'Platelet count, x10E3/uL','codeset_id':'167697906','omop_unit_concept_id':'8848','omop_unit_concept_name':'thousand per microliter','max_acceptable_value':'4000','min_acceptable_value':'0'},
        {'measured_variable':'Potassium, mmol/L','codeset_id':'622316047','omop_unit_concept_id':'8753','omop_unit_concept_name':'millimole per liter','max_acceptable_value':'20','min_acceptable_value':'0'},
        {'measured_variable':'Procalcitonin, ng/mL','codeset_id':'610397248','omop_unit_concept_id':'8842','omop_unit_concept_name':'nanogram per milliliter','max_acceptable_value':'5000','min_acceptable_value':'0'},
        {'measured_variable':'Respiratory rate','codeset_id':'286601963','omop_unit_concept_id':'8483','omop_unit_concept_name':'counts per minute','max_acceptable_value':'200','min_acceptable_value':'0'},
        {'measured_variable':'Sodium, mmol/L','codeset_id':'887473517','omop_unit_concept_id':'8753','omop_unit_concept_name':'millimole per liter','max_acceptable_value':'250','min_acceptable_value':'50'},
        {'measured_variable':'SpO2','codeset_id':'780678652','omop_unit_concept_id':'8554','omop_unit_concept_name':'percent','max_acceptable_value':'100','min_acceptable_value':'0'},
        {'measured_variable':'Systolic blood pressure','codeset_id':'186465804','omop_unit_concept_id':'8876','omop_unit_concept_name':'millimeter mercury column','max_acceptable_value':'400','min_acceptable_value':'0'},
        {'measured_variable':'Temperature','codeset_id':'656562966','omop_unit_concept_id':'586323','omop_unit_concept_name':'degree Celsius','max_acceptable_value':'45','min_acceptable_value':'25'},
        {'measured_variable':'Troponin all types, ng/mL','codeset_id':'623367088','omop_unit_concept_id':'8842','omop_unit_concept_name':'nanogram per milliliter','max_acceptable_value':'100','min_acceptable_value':'0'},
        {'measured_variable':'White blood cell count, x10E3/uL','codeset_id':'138719030','omop_unit_concept_id':'8848','omop_unit_concept_name':'thousand per microliter','max_acceptable_value':'10000','min_acceptable_value':'0'}

    ]

    df = pd.DataFrame(data={
        'measured_variable': list(map(lambda p: p["measured_variable"], canonical_units)),
        'codeset_id': list(map(lambda p: p["codeset_id"], canonical_units)),
        'omop_unit_concept_id': list(map(lambda p: p["omop_unit_concept_id"], canonical_units)),
        'omop_unit_concept_name': list(map(lambda p: p["omop_unit_concept_name"], canonical_units)),
        'max_acceptable_value': list(map(lambda p: p["max_acceptable_value"], canonical_units)),
        'min_acceptable_value': list(map(lambda p: p["min_acceptable_value"], canonical_units))
    })

    return output.write_pandas(df)