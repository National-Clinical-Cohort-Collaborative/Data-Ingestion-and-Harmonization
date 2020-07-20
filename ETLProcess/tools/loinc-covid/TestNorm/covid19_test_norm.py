#from pathlib import Path
#from tensorflow.contrib import predictor
import os
#import time
import pandas as pd
from .utils import pre_tokenize, contains, has_valid_value

import copy

loinc_ner_dict = {'Component': {'Covid19': [], 'Covid19_Related': [], 'RNA': [], 'Sequence': [], 'Antigen': [], 'Growth':[], 'Antibody': [],'Interpretation': [] }, 
                  'System': {'Blood':[], 'Respiratory': [], 'NP': [], 'Saliva': [], 'Other': [], },
                  'Method': {'RNA': [], 'Sequence': [], 'Antigen': [], 'Growth':[], 'Antibody': [], 'Panel': [] },
                  'Quan_Qual': {'Quantitative': [], 'Qualitative': []},
                  'Institution': {'Manufacturer': []},
                 }

def load_rules_data():
    """Load rules data"""
    cur_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(cur_dir, '../data')
    covid19_lexicons_fn = 'Covid19_lexicons.csv'
    covid19_testkits_fn = 'LIVD-SARS-CoV-2-2020-06-02_LOINC-MAPPING.csv' #downloaded and extracted from https://www.cdc.gov/csels/dls/sars-cov-2-livd-codes.html and https://loinc.org/sars-coronavirus-2/
    loinc_sarscov2_labtests_fn = 'Loinc_Sarscov2_Export_20200603.csv' # downloaded from https://loinc.org/sars-coronavirus-2

    rules_data = dict()
    rules_ner_dict = copy.deepcopy(loinc_ner_dict)
    trip_non_alnum = True # trip non-alnum charater and replace with space

    # load LOINC Sarscov2 data
    loinc_sarscov2_labtests_pfn = os.path.join(data_dir, loinc_sarscov2_labtests_fn)
    if os.path.exists(loinc_sarscov2_labtests_pfn):
        df_loinc_sarscov2_labtests = pd.read_csv(loinc_sarscov2_labtests_pfn)
    else:
        raise Exception('Can not find {}'.format(loinc_sarscov2_labtests_pfn))    
    df_loinc_sarscov2_labtests = df_loinc_sarscov2_labtests[df_loinc_sarscov2_labtests.LOINC_NUM.astype(bool)]
    rules_data['df_loinc_sarscov2_labtests'] = df_loinc_sarscov2_labtests[df_loinc_sarscov2_labtests.LOINC_NUM.astype(bool)]

    # load LOINC_IVD_test_kits
    covid19_testkits_pfn = os.path.join(data_dir, covid19_testkits_fn)
    if os.path.exists(covid19_testkits_pfn):
        df_covid19_testkits = pd.read_csv(covid19_testkits_pfn)
    else:
        raise Exception('Can not find {}'.format(covid19_testkits_pfn))
    df_covid19_testkits.fillna('', inplace=True)
    #df_covid19_testkits = df_covid19_testkits.apply(lambda x: x.str.lower().str.strip() if isinstance(x, str) else x)     
    df_covid19_testkits['Manufacturer'] = df_covid19_testkits['Manufacturer'].apply(lambda x: pre_tokenize(x.lower(), trip_non_alnum))
    df_covid19_testkits['Model'] = df_covid19_testkits['Model'].apply(lambda x: pre_tokenize(x.lower(), trip_non_alnum))
    df_covid19_testkits['Vendor Analyte Name'] = df_covid19_testkits['Vendor Analyte Name'].apply(lambda x: pre_tokenize(x.lower(), trip_non_alnum))
    df_covid19_testkits = df_covid19_testkits[df_covid19_testkits.Manufacturer.astype(bool) | df_covid19_testkits['LOINC Code'].astype(bool)]
    rules_data['df_covid19_testkits'] = df_covid19_testkits

    # load covid19_lexicons
    covid19_lexicons_pfn = os.path.join(data_dir, covid19_lexicons_fn)
    if os.path.exists(covid19_lexicons_pfn):
        df_covid19_lexicons = pd.read_csv(covid19_lexicons_pfn)
    else:
        raise Exception('Can not find {}'.format(covid19_lexicons_pfn))
    df_covid19_lexicons = df_covid19_lexicons[df_covid19_lexicons.KEY.astype(bool)]
    rules_data['df_covid19_lexicons'] = df_covid19_lexicons

    #5 system specimen categories: Saliva, NP, Respiratory, Blood, Other
    system_saliva = df_covid19_lexicons[df_covid19_lexicons.KEY == 'Saliva'].VALUES.tolist() 
    system_saliva = set(map(lambda x: pre_tokenize(x.lower(), trip_non_alnum), system_saliva))
    rules_ner_dict['System']['Saliva'] = list(system_saliva)
   
    system_np = df_covid19_lexicons[df_covid19_lexicons.KEY == 'NP'].VALUES.tolist() 
    system_np = set(map(lambda x: pre_tokenize(x.lower(), trip_non_alnum), system_np))
    rules_ner_dict['System']['NP'] = list(system_np)    
   
    system_respiratory = df_covid19_lexicons[df_covid19_lexicons.KEY == 'Respiratory'].VALUES.tolist() 
    system_respiratory = set(map(lambda x: pre_tokenize(x.lower(), trip_non_alnum), system_respiratory))
    rules_ner_dict['System']['Respiratory'] = list(system_respiratory)
   
    system_blood = df_covid19_lexicons[df_covid19_lexicons.KEY == 'Blood'].VALUES.tolist() 
    system_blood = set(map(lambda x: pre_tokenize(x.lower(), trip_non_alnum), system_blood))
    rules_ner_dict['System']['Blood'] = list(system_blood)
   
    system_other = df_covid19_lexicons[df_covid19_lexicons.KEY == 'Other'].VALUES.tolist() 
    system_other = set(map(lambda x: pre_tokenize(x.lower(), trip_non_alnum), system_other))
    rules_ner_dict['System']['Other'] = list(system_other)
    
    # from https://loinc.org/sars-coronavirus-2/
    #institution
    #manufacturer = df_covid19_lexicons[df_covid19_lexicons.KEY == 'Manufacturer'].VALUES.tolist()
    #manufacturer = set(map(lambda x: pre_tokenize(x.lower(), trip_non_alnum), manufacturer))
    # manufacturer comes from df_covid19_testkits
    manufacturer = df_covid19_testkits['Manufacturer'].tolist()
    manufacturer = set(map(lambda x: pre_tokenize(x.lower(), trip_non_alnum), manufacturer))
    rules_ner_dict['Institution']['Manufacturer'] = list(manufacturer)
   
    covid19_name = df_covid19_lexicons[df_covid19_lexicons.KEY == 'Covid19'].VALUES.tolist()
    covid19_name = set(map(lambda x: pre_tokenize(x.lower(), trip_non_alnum), covid19_name))
    rules_ner_dict['Component']['Covid19'] = list(covid19_name)
    
    covid19_related_name = df_covid19_lexicons[df_covid19_lexicons.KEY == 'Covid19_Related'].VALUES.tolist()
    covid19_related_name = set(map(lambda x: pre_tokenize(x.lower(), trip_non_alnum), covid19_related_name))
    rules_ner_dict['Component']['Covid19_Related'] = list(covid19_related_name)
    
    # Tests looking for SARS-CoV-2 nucleic acids (RNA)
    rna_comp = df_covid19_lexicons[df_covid19_lexicons.KEY == 'RNA_Comp'].VALUES.tolist()    
    rna_comp =  set([pre_tokenize(item.lower(), trip_non_alnum) for item in rna_comp])
    rules_ner_dict['Component']['RNA'] = list(rna_comp)

    rna_method = df_covid19_lexicons[df_covid19_lexicons.KEY == 'RNA_Method'].VALUES.tolist()
    rna_method =  set([pre_tokenize(item.lower(), trip_non_alnum) for item in rna_method])
    rules_ner_dict['Method']['RNA'] = list(rna_method)
    
    # Test to determine the SARS-CoV-2 sequence
    sequence_comp = df_covid19_lexicons[df_covid19_lexicons.KEY == 'Sequence_Comp'].VALUES.tolist()
    sequence_comp = set([pre_tokenize(item.lower(), trip_non_alnum) for item in sequence_comp])
    rules_ner_dict['Component']['Sequence'] = list(sequence_comp)
    
    sequence_method = df_covid19_lexicons[df_covid19_lexicons.KEY == 'Sequence_Method'].VALUES.tolist()
    sequence_method = set([pre_tokenize(item.lower(), trip_non_alnum) for item in sequence_method])
    rules_ner_dict['Method']['Sequence'] = list(sequence_method)
    
    # Test looking for SARS-CoV-2 antigen
    antigen_comp = df_covid19_lexicons[df_covid19_lexicons.KEY == 'Antigen_Comp'].VALUES.tolist()
    antigen_comp = set([pre_tokenize(item.lower(), trip_non_alnum) for item in antigen_comp])
    rules_ner_dict['Component']['Antigen'] = list(antigen_comp)

    antigen_method = df_covid19_lexicons[df_covid19_lexicons.KEY == 'Antigen_Method'].VALUES.tolist()
    antigen_method = set([pre_tokenize(item.lower(), trip_non_alnum) for item in antigen_method])
    rules_ner_dict['Method']['Antigen'] = list(antigen_method)
    
    # Test looking for growth of SARS-CoV-2
    growth_comp = df_covid19_lexicons[df_covid19_lexicons.KEY == 'Growth_Comp'].VALUES.tolist()
    growth_comp = set([pre_tokenize(item.lower(), trip_non_alnum) for item in growth_comp])
    rules_ner_dict['Component']['Growth'] = list(growth_comp)    
    
    growth_method = df_covid19_lexicons[df_covid19_lexicons.KEY == 'Growth_Method'].VALUES.tolist()
    growth_method = set([pre_tokenize(item.lower(), trip_non_alnum) for item in growth_method])
    rules_ner_dict['Method']['Growth'] = list(growth_method)
    
    # Tests looking for antibodies to SARS-CoV-2
    antibody_comp = df_covid19_lexicons[df_covid19_lexicons.KEY == 'Antibody_Comp'].VALUES.tolist()
    antibody_comp = set([pre_tokenize(item.lower(), trip_non_alnum) for item in antibody_comp])
    rules_ner_dict['Component']['Antibody'] = list(antibody_comp)
   
    antibody_method = df_covid19_lexicons[df_covid19_lexicons.KEY == 'Antibody_Method'].VALUES.tolist()
    antibody_method = set([pre_tokenize(item.lower(), trip_non_alnum) for item in antibody_method])
    rules_ner_dict['Method']['Antibody'] = list(antibody_method)

    interpretation_comp = df_covid19_lexicons[df_covid19_lexicons.KEY == 'Interpretation_Comp'].VALUES.tolist()
    interpretation_comp = set([pre_tokenize(item.lower(), trip_non_alnum) for item in interpretation_comp])
    rules_ner_dict['Component']['Interpretation'] = list(interpretation_comp)
    
    quantitative = df_covid19_lexicons[df_covid19_lexicons.KEY == 'Quantitative'].VALUES.tolist()
    quantitative = set(map(lambda x: pre_tokenize(x.lower(), trip_non_alnum), quantitative))
    rules_ner_dict['Quan_Qual']['Quantitative'] = list(quantitative)    

    qualitative = df_covid19_lexicons[df_covid19_lexicons.KEY == 'Qualitative'].VALUES.tolist()
    qualitative = list(map(lambda x: pre_tokenize(x.lower(), trip_non_alnum), qualitative))
    rules_ner_dict['Quan_Qual']['Qualitative'] = list(qualitative)    
        
    panel_method = df_covid19_lexicons[df_covid19_lexicons.KEY == 'Panel_Method'].VALUES.tolist()
    panel_method = set([pre_tokenize(item.lower(), trip_non_alnum) for item in panel_method])
    rules_ner_dict['Method']['Panel'] = list(panel_method) 
    rules_data['ner_dict'] = rules_ner_dict

    return rules_data

def disambiguate_ners(ner_list_1, ner_list_2):
    """Disambiguate ners between two list of ners, i.e., to removing ambiguation between two ner list if there're strict-parital-overlap between them    
       If the same item exists in both ners, leave them and let root axle to disambiguate them (e.g., 'rapid' method for RNA, Antigen, and/or Antibody)

    Args:
        ner_list_1, ner_list_2: list of ner strings

    Returns:
        new ner_list_1 and ner_list_2 without strict-parital-overlap ambiguation
    """    
    if (type(ner_list_1) != list) or  (type(ner_list_2) != list):
        print('Disambiguate_ners: input parameters not list type and do nothing!')    
        return
    ners_1 = set(ner_list_1)    
    ners_2 = set(ner_list_2)
    del_ner_1 = set()
    del_ner_2 = set()
    for ner1 in ners_1:
        for ner2 in ners_2:
            # if ner2 is the same as ner1, there should be typos in the lexicons file. Remove ner2 in ner_list_2 only
            if ner2 == ner1:
                print('Ner {} are in two different sub-categories under the same root axle, keep them.'.format(ner2))
                #del_ner_2.add(ner2) # leave it and let root axle to disambiguate them
            elif contains(ner1, ner2):
                # ner2 is partial of ner1, delete ner2
                del_ner_2.add(ner2)
            elif contains(ner2, ner1): 
                del_ner_1.add(ner1)    
    for ner in ner_list_1:
        if ner in del_ner_1:
            ner_list_1.remove(ner)
    for ner in ner_list_2:
        if ner in del_ner_2:
            ner_list_2.remove(ner)    

def get_ner_dict_by_rule(query_text, rules_ner_dict):
    """Get NER dict by rules data."""
    query_ner_dict = copy.deepcopy(loinc_ner_dict)
    
    query_ner_dict['Institution']['Manufacturer'] = list(set(contains(query_text, rules_ner_dict['Institution']['Manufacturer'])))    
    
    # for Covid19&Covdi19_Related, use partial-match instedad of whole-match in contains fucntion to relax the condition
    query_ner_dict['Component']['Covid19'] = contains(query_text, rules_ner_dict['Component']['Covid19'], False) #list(set(sars_cov_2_pt) & set(ner_all))
    query_ner_dict['Component']['Covid19_Related'] = contains(query_text, rules_ner_dict['Component']['Covid19_Related'], False)
    # since covid19_related values may be parital of covid19 values (e.g., 'Coronavirus' and 'SARS Coronavirus 2'), need to disambiguate them
    disambiguate_ners(query_ner_dict['Component']['Covid19'], query_ner_dict['Component']['Covid19_Related'])    
    if query_ner_dict['Component']['Covid19'] and query_ner_dict['Component']['Covid19_Related']:
        # sometimes the common lexicons not including all the entity names for Covid19 which are partial-overlapped with Covid19_Related
        if contains(query_text, 'SARS-like') or contains(query_text, 'SARS-related') or contains(query_text, 'PAN-SARS'):
            query_ner_dict['Component']['Covid19'] = [] # clear Covid19, keep Covid19_Related
        else:
            query_ner_dict['Component']['Covid19_Related'] = [] # clear Covid19_Related, keep Covid19
    query_ner_dict['Component']['RNA'] = contains(query_text, rules_ner_dict['Component']['RNA']) 
    query_ner_dict['Component']['Sequence'] = contains(query_text, rules_ner_dict['Component']['Sequence'])
    query_ner_dict['Component']['Antigen'] = contains(query_text, rules_ner_dict['Component']['Antigen'])
    query_ner_dict['Component']['Growth'] = contains(query_text, rules_ner_dict['Component']['Growth']) 
    query_ner_dict['Component']['Antibody'] = contains(query_text, rules_ner_dict['Component']['Antibody'])
    query_ner_dict['Component']['Interpretation'] = contains(query_text, rules_ner_dict['Component']['Interpretation'])    
    
    query_ner_dict['Quan_Qual']['Qualitative'] = contains(query_text, rules_ner_dict['Quan_Qual']['Qualitative'])
    query_ner_dict['Quan_Qual']['Quantitative'] = contains(query_text, rules_ner_dict['Quan_Qual']['Quantitative'])    

    query_ner_dict['System']['Saliva'] = contains(query_text, rules_ner_dict['System']['Saliva'])
    query_ner_dict['System']['NP'] = contains(query_text, rules_ner_dict['System']['NP'])
    query_ner_dict['System']['Respiratory'] = contains(query_text, rules_ner_dict['System']['Respiratory'])
    query_ner_dict['System']['Blood'] = contains(query_text, rules_ner_dict['System']['Blood'])
    query_ner_dict['System']['Other'] = contains(query_text, rules_ner_dict['System']['Other']) 
    disambiguate_ners(query_ner_dict['System']['Respiratory'], query_ner_dict['System']['NP'] )

    query_ner_dict['Method']['RNA'] = contains(query_text, rules_ner_dict['Method']['RNA'])    
    query_ner_dict['Method']['Sequence'] = contains(query_text, rules_ner_dict['Method']['Sequence']) 
    query_ner_dict['Method']['Antigen'] = contains(query_text, rules_ner_dict['Method']['Antigen'])    
    query_ner_dict['Method']['Growth'] = contains(query_text, rules_ner_dict['Method']['Growth'])    
    query_ner_dict['Method']['Antibody'] = contains(query_text, rules_ner_dict['Method']['Antibody'])        
    # disambiguate from RNA and Antigen/Antibody, e.g, for 'rapid' method in 'covid19 rapid ia', may belong to RNA, Antigen, and Antibody
    disambiguate_ners(query_ner_dict['Method']['RNA'], query_ner_dict['Method']['Antigen'])
    disambiguate_ners(query_ner_dict['Method']['RNA'], query_ner_dict['Method']['Antibody'])

    # for panel, list separately in method dict
    query_ner_dict['Method']['Panel'] = contains(query_text, rules_ner_dict['Method']['Panel'])        

    # check whether IA is immnunoassay or iowa based on specimen and method， e.g.: COVID-19 (State Health Lab)*ne,ia    NARES
    if contains(query_text, 'IA'):
        if query_ner_dict['System']['Respiratory'] or query_ner_dict['System']['NP']:
            # for respiratory specimen, do not use IA (immunoassay) method, therefore, take 'IA' as abbreviation of something like 'IOWA' other than 'IA' method
            query_ner_dict['Method']['Antigen'] = [item for item in query_ner_dict['Method']['Antigen'] if item.lower() != 'ia']
            query_ner_dict['Method']['Antibody'] = [item for item in query_ner_dict['Method']['Antigen'] if item.lower() != 'ia']
    
    # check antigen and antibody method
    if query_ner_dict['Method']['Antigen'] and query_ner_dict['Method']['Antibody']:
        # if both antigen and antibody method detected simultaneiously, choose antigen if antigen component also found, else choose antibody
        if query_ner_dict['Component']['Antigen']:
            query_ner_dict['Method']['Antibody'] = []
        else:
            query_ner_dict['Method']['Antigen'] = []

    return query_ner_dict

def get_loinc_codes_as_rna_naa(source, ner_dict, default_specimen=''):
    """Get loinc codes as for RNA purpose with NAA method.    

    Args:
        source: input covid19 testing names
        ner_dict: NER dict from source and rules_data
        default_specimen:  default specimen as 'NP', 'Respiratory', etc., '' by default

    Returns:
        Appropriate LOINC codes in list
    """
    loinc_codes = []
    if contains(source, 'non-probe-based') or contains(source, 'non-probe') or contains(source, 'melt curve analysis'):        
        if ner_dict['System']['NP']:
            loinc_codes = ['94565-9']
        else:
            # no NP specimen detected, taken as NP by default under 'RNA->NAA with non-probe-based detection' chain.
            # may update later if more LOINC code rules available on LOINC website
            loinc_codes = ['94565-9']
    else:
        if ner_dict['System']['Saliva'] or (default_specimen == 'Saliva'):
            loinc_codes = ['94845-5']
        elif ner_dict['System']['Respiratory'] or ner_dict['System']['NP'] or (default_specimen == 'Respiratory'):
            # The order matters, especially for panel, SARS-related CoV, etc. 
            # NP is a special respiratory specimen, for sars-cov-2, different codes maybe used, for sars-related, the same codes can be assigned
            if ner_dict['Method']['Panel']:
                loinc_codes = ['94531-1']
            elif ner_dict['Component']['Covid19_Related']:
                if contains(source, 'MERS'):                            
                    loinc_codes = ['94532-9']
                else:                             
                    loinc_codes = ['94502-2']                         
            else:
                if (ner_dict['System']['NP'] and (not ner_dict['System']['Respiratory'])): # COVID-19 (WSLH)    NASOPHARYNGEAL SWAB AND OROPHARYNGEAL SWAB
                    if (contains(source, 'N gene') or contains(source, 'N')):
                        loinc_codes = ['94760-6']
                    else:
                        loinc_codes = ['94759-8']
                else:
                    if contains(source, 'N gene') or contains(source, 'N'):
                        loinc_codes = ['94533-7']
                    elif contains(source, 'E gene') or contains(source, 'E'):
                        loinc_codes = ['94758-0']
                    elif contains(source, 'RdRp gene')  or contains(source, 'RdRp'):
                        if ner_dict['Quan_Qual']['Qualitative']:
                            loinc_codes = ['94534-5']
                        elif ner_dict['Quan_Qual']['Quantitative']:
                            loinc_codes = ['94646-7']
                        else:
                            loinc_codes = ['94534-5'] # assume qualitative by default at present
                    elif contains(source, 'ORF1ab region') or contains(source, 'ORF1ab'):
                        if ner_dict['Quan_Qual']['Qualitative']:
                            loinc_codes = ['94559-2']
                        elif ner_dict['Quan_Qual']['Quantitative']:
                            loinc_codes = ['94644-2']
                        else:
                            loinc_codes = ['94559-2'] 
                    elif contains(source, 'S gene') or contains(source, 'S'):
                        if ner_dict['Quan_Qual']['Qualitative']:
                            loinc_codes = ['94640-0']
                        elif ner_dict['Quan_Qual']['Quantitative']:
                            loinc_codes = ['94642-6']
                        else:
                            loinc_codes = ['94640-0']   
                    else:
                        loinc_codes = ['94500-6']                
        elif ner_dict['System']['Blood'] or (default_specimen == 'Blood'):
            if contains(source, 'E gene') or contains(source, 'E'):
                loinc_codes = ['94765-5']
            elif contains(source, 'N gene') or contains(source, 'N'):
                loinc_codes = ['94766-3']
            elif contains(source, 'S gene') or contains(source, 'S'):
                loinc_codes = ['94767-1']
            else:
                loinc_codes = ['94660-8']
        elif ner_dict['System']['Other'] or (default_specimen == 'Other'):
            # the order matters, especially for panel, SARS-related CoV, SARS-like CoV...
            if ner_dict['Method']['Panel']:
                loinc_codes = ['94306-8']
            elif contains(source, 'SARS-related'):
                loinc_codes = ['94647-5']
            elif contains(source, 'SARS-like') and (contains(source, 'N gene') or contains(source, 'N')):
                if ner_dict['Quan_Qual']['Qualitative']:
                    loinc_codes = ['94310-0']
                elif ner_dict['Quan_Qual']['Quantitative']:
                    loinc_codes = ['94313-4']
                else:
                    loinc_codes = ['94310-0']
            elif (contains(source, 'N gene') or contains(source, 'N')) and contains(source, 'CDC primer-probe set N1'):
                if ner_dict['Quan_Qual']['Qualitative']:
                    loinc_codes = ['94307-6']
                elif ner_dict['Quan_Qual']['Quantitative']:
                    loinc_codes = ['94311-8']
                else:
                    loinc_codes = ['94307-6']    
            elif (contains(source, 'N gene') or contains(source, 'N')) and contains(source, 'CDC primer-probe set N2'):
                if ner_dict['Quan_Qual']['Qualitative']:
                    loinc_codes = ['94308-4']
                elif ner_dict['Quan_Qual']['Quantitative']:
                    loinc_codes = ['94312-6']
                else:
                    loinc_codes = ['94308-4']                                                                                
            elif contains(source, 'N gene') or contains(source, 'N'):
                if ner_dict['Quan_Qual']['Qualitative']:
                    loinc_codes = ['94316-7']
                elif ner_dict['Quan_Qual']['Quantitative']:
                    loinc_codes = ['94510-5']
                else:
                    loinc_codes = ['94316-7']
            elif contains(source, 'E gene') or contains(source, 'E'):
                if ner_dict['Quan_Qual']['Qualitative']:
                    loinc_codes = ['94315-9']
                elif ner_dict['Quan_Qual']['Quantitative']:
                    loinc_codes = ['94509-7']
                else:
                    loinc_codes = ['94315-9']
            elif contains(source, 'RdRp gene') or contains(source, 'RdRp'):
                if ner_dict['Quan_Qual']['Qualitative']:
                    loinc_codes = ['94314-2']
                elif ner_dict['Quan_Qual']['Quantitative']:
                    loinc_codes = ['94645-9']
                else:
                    loinc_codes = ['94314-2']
            elif contains(source, 'ORF1ab region') or contains(source, 'ORF1ab'):
                if  ner_dict['Quan_Qual']['Qualitative']:
                    loinc_codes = ['94639-2']
                elif ner_dict['Quan_Qual']['Quantitative']:
                    loinc_codes = ['94511-3']
                else:
                    loinc_codes = ['94639-2']
            elif contains(source, 'S gene') or contains(source, 'S'):
                if ner_dict['Quan_Qual']['Qualitative']:
                    loinc_codes = ['94641-8']
                elif ner_dict['Quan_Qual']['Quantitative']:
                    loinc_codes = ['94643-4']
                else:
                    loinc_codes = ['94641-8']            
            else:
                if ner_dict['Quan_Qual']['Qualitative']:
                    loinc_codes = ['94309-2']
                elif ner_dict['Quan_Qual']['Quantitative']:
                    loinc_codes = ['94819-0']
                else:
                    loinc_codes = ['94309-2']
    return loinc_codes

def get_loinc_codes_as_rna_sequencing(source, ner_dict):
    loinc_codes = []
    if ner_dict['System']['Saliva']:
        loinc_codes = ['94822-4']
    else:
        # no Saliva detected, taken as Saliva by default under 'RNA->Sequencing' chain
        # may update later if more LOINC code rules available on LOINC website
        loinc_codes = ['94822-4']
    return loinc_codes
  
def get_loinc_codes_as_antibody_rapid_ia(source, ner_dict):
    loinc_codes = []
    if ner_dict['Method']['Panel']:                        
        loinc_codes = ['94503-0']
    elif contains(source, 'IgA'):
        loinc_codes = ['94768-9']
    elif contains(source, 'IgG'):
        loinc_codes = ['94507-1']
    elif contains(source, 'IgM'):
        loinc_codes = ['94508-9']
    else:
        loinc_codes = ['94768-9', '94507-1', '94508-9']     
    return loinc_codes           

def get_loinc_codes_as_antibody_ia_non_specific(source, ner_dict):
    if ner_dict['Quan_Qual']['Qualitative']:
        loinc_codes = ['94762-2']
    elif ner_dict['Quan_Qual']['Quantitative']:
        loinc_codes = ['94769-7']
    else:
        loinc_codes = ['94762-2'] # assume qualitative by default at present
    return loinc_codes

def get_loinc_codes_as_antibody_ia(source, ner_dict):
    if ner_dict['Method']['Panel']:                        
        loinc_codes = ['94504-8']
    elif (contains(source, 'IgA') and contains(source, 'IgG') and contains(source, 'IgM')) or contains(source, 'Total Antiboy') or contains(source, 'Total Ab'):
        # taken as SARS-CoV-2 antibody (non-specific)
        loinc_codes = get_loinc_codes_as_antibody_ia_non_specific(source, ner_dict)
    elif contains(source, 'IgG') and contains(source, 'IgM'):
        loinc_codes = ['94547-7']                            
    elif contains(source, 'IgA'):
        if ner_dict['Quan_Qual']['Qualitative']:
            loinc_codes = ['94562-6']
        elif ner_dict['Quan_Qual']['Quantitative']:
            loinc_codes = ['94720-0']
        else:
            loinc_codes = ['94562-6']
    elif contains(source, 'IgG'):
        if ner_dict['Quan_Qual']['Qualitative']:
            loinc_codes = ['94563-4']
        elif ner_dict['Quan_Qual']['Quantitative']:
            loinc_codes = ['94505-5']
        else:
            loinc_codes = ['94563-4']
    elif contains(source, 'IgM'):
        if ner_dict['Quan_Qual']['Qualitative']:
            loinc_codes = ['94564-2']
        elif ner_dict['Quan_Qual']['Quantitative']:
            loinc_codes = ['94506-3']
        else:
            loinc_codes = ['94564-2']
    elif ner_dict['Component']['Interpretation']: #contains(source, 'Interpretation') or contains(source, 'recent infection') or contains(source, 'past infection'):
        loinc_codes = ['94661-6']
    else:
        loinc_codes = get_loinc_codes_as_antibody_ia_non_specific(source, ner_dict)
    return loinc_codes

def get_loinc_codes_from_rna(source, ner_dict):
    loinc_codes = []
    if ner_dict['Component']['RNA'] \
       or (ner_dict['Method']['RNA'] and (not ner_dict['Component']['Sequence'])
            and (not ner_dict['Component']['Growth']) and (not ner_dict['Component']['Antibody'])
            and (not ner_dict['Component']['Antigen']) and (not ner_dict['Component']['Interpretation'])): # sometimes, sites data may miss comp or method keyword
        if ner_dict['Method']['RNA']:
            if contains(source, 'Sequencing'):
                loinc_codes = get_loinc_codes_as_rna_sequencing(source, ner_dict)
            else:
                #if contains(source, 'NAA') or contains(source, 'Nucleic acid amplification'):
                loinc_codes = get_loinc_codes_as_rna_naa(source, ner_dict)
        if not loinc_codes:
            # some sites data may missing method information            
            if ner_dict['Component']['Covid19_Related']:
                if contains(source, 'MERS'):
                    loinc_codes = ['94532-9'] 
                else:
                    loinc_codes = ['94502-2'] 
            elif not has_valid_value(ner_dict['Method']):
                if not has_valid_value(ner_dict['System']):
                    # missing specimen (System), taken as Unspecified specimen (Other) by default now
                    default_specimen = 'Other'
                    #if contains(source, 'CDC primer-probe set N1') or contains(source, 'CDC primer-probe set N2'):
                        # missing specimen (System), choose unspecified specimen (Other) if contains CDC primer-probe set N1|N2
                    #    default_specimen = 'Other'
                    #else:
                    #    default_specimen = 'Respiratory'
                    loinc_codes = get_loinc_codes_as_rna_naa(source, ner_dict, default_specimen) 
                else:
                    loinc_codes = get_loinc_codes_as_rna_naa(source, ner_dict) 
            else:
                loinc_codes = get_default_loinc_codes(source, ner_dict)        
    else:
        loinc_codes = []
    return loinc_codes

def get_loinc_codes_from_sequence(source, ner_dict):
    loinc_codes = []
    if ner_dict['Component']['Sequence'] \
       or (ner_dict['Method']['Sequence'] and (not ner_dict['Component']['RNA']) 
            and (not ner_dict['Method']['Antigen']) and (not ner_dict['Component']['Growth']) 
            and (not ner_dict['Component']['Antibody']) and (not ner_dict['Component']['Interpretation'])): # sometimes, sites data may miss comp or method keyword
        if ner_dict['Method']['Sequence']:
            if ner_dict['Quan_Qual']['Qualitative'] or not ner_dict['Quan_Qual']['Quantitative']:
                loinc_codes = ['94764-8']
        if not loinc_codes:
            print('Missing more specific information in: {}, however, assign default Sequence codes.'.format(source))
            loinc_codes = ['94764-8'] #get_default_loinc_codes(source, ner_dict)
    else:
        loinc_codes = []
    return loinc_codes

def get_loinc_codes_from_antigen(source, ner_dict):
    loinc_codes = []
    if ner_dict['Component']['Antigen'] \
       or (ner_dict['Method']['Antigen'] and (not ner_dict['Component']['RNA']) 
            and (not ner_dict['Component']['Sequence']) and (not ner_dict['Component']['Growth']) 
            and (not ner_dict['Component']['Antibody']) and (not ner_dict['Component']['Interpretation'])): # sometimes, sites data may miss comp or method keyword        
        #SARS coronavirus 2 Ag [Presence] in Respiratory specimen by Rapid immunoassay
        if ner_dict['Component']['Covid19_Related']:
            #SARS-CoV+SARS-CoV-2 (COVID19) Ag [Presence] in Respiratory specimen by Rapid
            if contains(source, 'SARS-CoV+SARS-CoV-2'):
                loinc_codes = ['95209-3']
            else:
                # also assign '95209-3' for COVID19-Related component by default
                print('Assign LOINC codes as SARS-CoV+SARS-CoV-2 due to containing COVID19-Related component.')
                loinc_codes = ['95209-3']
        elif ner_dict['Quan_Qual']['Qualitative'] or not ner_dict['Quan_Qual']['Quantitative']:
            loinc_codes = ['94558-4']
        if not loinc_codes:
            print('Assign as default antigen with Respiratory specimen and Rapid IA method and qualitative: 94558-4')
            loinc_codes = ['94558-4']
    else:
        loinc_codes = []
    return loinc_codes

def get_loinc_codes_from_growth(source, ner_dict):
    loinc_codes = []
    if ner_dict['Component']['Growth'] \
        or (ner_dict['Method']['Growth'] and (not ner_dict['Component']['RNA']) 
            and (not ner_dict['Component']['Sequence']) and (not ner_dict['Component']['Antigen']) 
            and (not ner_dict['Component']['Antibody']) and (not ner_dict['Component']['Interpretation'])): # sometimes, sites data may miss comp or method keyword
            #SARS coronavirus 2 [Presence] in Unspecified specimen by Organism specific culture
        if ner_dict['Quan_Qual']['Qualitative'] or not ner_dict['Quan_Qual']['Quantitative']:
                loinc_codes = ['94763-0']
        if not loinc_codes:
            print('Missing more specific information in: {}, however, assign default Growth codes.'.format(source))
            loinc_codes = ['94763-0'] #get_default_loinc_codes(source, ner_dict['Quan_Qual'])
    else:
        loinc_codes = []
    return loinc_codes

def get_loinc_codes_from_antibody(source, ner_dict):
    loinc_codes = []
    if ner_dict['Component']['Antibody'] \
       or (ner_dict['Method']['Antibody'] and (not ner_dict['Component']['RNA'])
            and (not ner_dict['Component']['Sequence']) and (not ner_dict['Component']['Antigen'])
            and (not ner_dict['Component']['Growth']) and (not ner_dict['Component']['Interpretation'])): # sometimes, sites data may miss comp or method keyword
        if ner_dict['Method']['Antibody']:
            if contains(source, 'Rapid immunoassay') or contains(source, 'Rapid IA') or contains(source, 'Rapid'):
                #SARS coronavirus 2 IgA Ab [Presence] in Serum, Plasma or Blood by Rapid immunoassay
                # if ner_dict['System']['Blood']:
                # for antibody testing, using blood specimen by default
                loinc_codes = get_loinc_codes_as_antibody_rapid_ia(source, ner_dict)
            elif contains(source, 'Immunoassay') or contains(source, 'IA'):
                #SARS coronavirus 2 Ab [Presence] in Serum or Plasma by Immunoassay
                if True: #ner_dict['System']['Blood'], for antibody testing, using blood specimen by default
                    loinc_codes = get_loinc_codes_as_antibody_ia(source, ner_dict)
        if not loinc_codes:            
            # some sites data may missing certain information, e.g, missing method, take it as immunoassay by default.                
            loinc_codes = get_loinc_codes_as_antibody_ia(source, ner_dict)                         
    else:
        loinc_codes = []
    return loinc_codes

def get_default_loinc_codes(source, ner_dict):
    loinc_codes = []
    if ner_dict['Quan_Qual']['Qualitative'] or (not ner_dict['Quan_Qual']['Quantitative']):
        # take as qualitative if not quantitative
        if ner_dict['Component']['Covid19_Related']:
            if contains(source, 'MERS'):
                loinc_codes = ['94532-9']
            else:
                loinc_codes = ['94502-2']
        elif ner_dict['Component']['Covid19']:
            if not has_valid_value(ner_dict['System']):
                # no specimen recognized in source, taken as Unspecified specimen by default
                loinc_codes = ['94309-2']
            else:
                # any other case, taken as respiratory specimen by default
                loinc_codes = ['94500-6']
    else:
        #quantiative, but not specify gene, return []
        loinc_codes = []
    return loinc_codes

def get_loinc_codes_by_purpose(query_text, ner_dict):
    """Get loinc codes from query_text or ner_dict, according to rules in https://loinc.org/sars-coronavirus-2/,     

    Args:
        query_text: input query string
        ner_dict: ner_dict based on query string and rules_data

    Returns:
        Appropriate LOINC codes in list
    """    
    loinc_codes = []
    if ner_dict['Component']['Covid19'] or ner_dict['Component']['Covid19_Related']:
        # 1. get_loinc_codes_from_rna
        if loinc_codes:
            return loinc_codes
        else:
            loinc_codes = get_loinc_codes_from_rna(query_text, ner_dict)
        # 2. get_loinc_codes_from_sequence
        if loinc_codes:
            return loinc_codes
        else:
            loinc_codes = get_loinc_codes_from_sequence(query_text, ner_dict)
        # 3. get_loinc_codes_from_antigen
        if loinc_codes:
            return loinc_codes
        else:
            loinc_codes = get_loinc_codes_from_antigen(query_text, ner_dict)
        # 4. get_loinc_codes_from_growth
        if loinc_codes:
            return loinc_codes
        else:
            loinc_codes = get_loinc_codes_from_growth(query_text, ner_dict)            
        # 5. get_loinc_codes_from_antibody
        if loinc_codes:
            return loinc_codes
        else:
            loinc_codes = get_loinc_codes_from_antibody(query_text, ner_dict)
        # 6. get loinc codes by default
        if loinc_codes:
            return loinc_codes
        else:
            # some sites data may miss certain information            
            if (not (ner_dict['Component']['RNA'] or ner_dict['Component']['Sequence'] or ner_dict['Component']['Antigen'] or ner_dict['Component']['Growth'] or ner_dict['Component']['Antibody'])) and \
               (not (ner_dict['Method']['RNA'] or ner_dict['Method']['Sequence'] or ner_dict['Method']['Antigen'] or ner_dict['Method']['Growth'] or ner_dict['Method']['Antibody'])):
                ## no comp and method, assume it as RNA and IA
                if has_valid_value(ner_dict['System']):
                    loinc_codes = get_loinc_codes_as_rna_naa(query_text, ner_dict)
                else:
                    # no comp, method, and specimen, get_default_loinc_codes
                    loinc_codes = get_default_loinc_codes(query_text, ner_dict)            
    else:
        loinc_codes = []

    return loinc_codes

def get_loinc_codes_by_institution(query_text, ner_dict, rules_data):
    loinc_codes = []
    #ner_all = ner_dict['All']

    df_covid19_testkits = rules_data['df_covid19_testkits'] #.apply(lambda x: x.str.lower().str.strip() if isinstance(x, object) else x) 
    #rules_inst_set = set(df_covid19_testkits.Manufacturer.tolist())
    inter_manufacturer = ner_dict['Institution']['Manufacturer'] #contains(query_text, rules_inst_set) # list(set(ner_inst) & rules_inst_set)
    if inter_manufacturer:
        df_inter_manuf = df_covid19_testkits[df_covid19_testkits.Manufacturer.isin(inter_manufacturer)]
        rules_model = df_inter_manuf['Model'].tolist()
        inter_model = contains(query_text, rules_model) #list(set(rules_testkits_pt) & set(ner_comp_syst_meth))
        rules_analyte = df_inter_manuf['Vendor Analyte Name'].tolist()
        inter_analyte = contains(query_text, rules_analyte) #list(set(rules_testkits_pt) & set(ner_comp_syst_meth))
        if inter_model:
            if inter_analyte:
                loinc_codes = df_inter_manuf[df_inter_manuf['Model'].isin(inter_model) & df_inter_manuf['Vendor Analyte Name'].isin(inter_analyte)]['LOINC Code'].drop_duplicates().tolist()
            else:
                loinc_codes = df_inter_manuf[df_inter_manuf['Model'].isin(inter_model)]['LOINC Code'].drop_duplicates().tolist() #[i for x in (df_covid19_testkits[df_covid19_testkits['Testkit PT'] == testkit].LOINC.tolist() for testkit in intersect) for i in x]    
    return loinc_codes

def get_loinc_codes(query_text, rules_data, query_ner=False):
    """Get appropriate LOINC codes."""
    loinc_codes = []
    #ner_dict = get_ner_dict_ml(query_ner['query'], query_ner['ner']) 
    ner_dict = get_ner_dict_by_rule(query_text, rules_data['ner_dict']) 
    #judge IVD test kit with intitution      
    loinc_codes = get_loinc_codes_by_institution(query_text, ner_dict, rules_data)
    if not loinc_codes:
        loinc_codes = get_loinc_codes_by_purpose(query_text, ner_dict) # from https://loinc.org/sars-coronavirus-2/    
    loinc_output = {'loinc':{'Codes':[], 'Long Common Names': []}}
    df_loinc_sarscov2_labtests = rules_data['df_loinc_sarscov2_labtests']
    for code in loinc_codes:
        loinc_long_name = ''
        loinc_long_name = df_loinc_sarscov2_labtests[df_loinc_sarscov2_labtests.LOINC_NUM == code]['Long Common Name'].values[0]
        loinc_output['loinc']['Codes'].append(code)
        loinc_output['loinc']['Long Common Names'].append(loinc_long_name)
    loinc_output['ner_dict'] = ner_dict
    return loinc_output
