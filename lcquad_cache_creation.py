import time
import json
import re
import sys
from SPARQLWrapper import SPARQLWrapper, JSON
import itertools
from collections import OrderedDict
from sparqlQueries import dict_sparqlQueries,mu_prop_lcquad, lcquad_templates, ask_triple, dict_lcquad_predicates, get_query_results, write_to_file

F_corechains_cache = open("data/lcquad_cache_corechain.txt", "w")
F_cache_error = open("data/lcquad_cache_corechain_error.txt", "w")
F_cache_terminal = open("data/lcquad_cache_terminal.txt", "w")
F_cache_time = open("data/lcquad_cache_time.txt", "w")
F_cache_has_no_ans = open("data/lcquad_cache_has_no_ans.txt", "w")


oneTE_templates = lcquad_templates()

#! -------- One Hope corechain Cashe
def corechains_oneHop_cache(entityIds_arr, prop_dir, specialUse=None):
    prop_sign = "+"
    if(prop_dir == "left"):
        prop_sign = "-"
    corechains_labels = []  
    corechains_ids = []
    lcquad_props = dict_lcquad_predicates(prop_dir)
    query = dict_sparqlQueries["query_" + prop_dir + "_oneHope"] % {
            'target_resource': entityIds_arr[0], 'filter_in': ''}
    
    if len(entityIds_arr) == 2:
        query = dict_sparqlQueries["query_" + prop_dir + "_twoTE"] % {
            'target_resource': entityIds_arr[0], 'target_resource2': entityIds_arr[1], 'filter_in': ''}
    
    error_msg = ', '.join(entityIds_arr) + "\t" + prop_sign
    write_queryMsg = [F_cache_error, error_msg]

    i=0
    results = get_query_results(query, write_queryMsg)
    if len(results) > 1:
        for result in results["results"]["bindings"]:
            prop_oneHop = result['property']['value'].replace(
                'http://www.wikidata.org/entity/', '')
            if(prop_oneHop and prop_oneHop in lcquad_props["lcquad_props"]):
                i += 1
                cc_label = prop_sign + result['propertyLabel']['value']
                cc_id = prop_sign + prop_oneHop
                cc_line = ', '.join(entityIds_arr).replace('wd:', '') + "\t" + prop_sign + "\t" + cc_label + "\t" + cc_id

                #------ create corechains
                corechains_labels.append(cc_label)
                corechains_ids.append(cc_id)
                if not specialUse:
                    write_to_file(F_corechains_cache, cc_line)
    # -------- if timeout
    if len(results) == 1:
        write_to_file(F_cache_terminal,"fixig error using filters / One Hop " + prop_sign)
        print("fixig error using filters / One Hop " + prop_sign)
        corechains_labels, corechains_ids = corechains_oneHop_cache_fix(
            entityIds_arr, prop_dir, specialUse)
    if not specialUse:
        write_to_file(F_cache_terminal, prop_sign + ' ' + str(len(corechains_labels)))
        print(prop_sign, len(corechains_labels))
    corechains = list(zip(corechains_ids,corechains_labels))
    return corechains
#! -------- One Hope corechain Cashe fixing using filters 
def corechains_oneHop_cache_fix(entityIds_arr, prop_dir, specialUse=None):
    prop_sign = "+"
    if(prop_dir == "left"):
        prop_sign = "-"
    
    corechains_labels = []
    corechains_ids = []
    lcquad_props = dict_lcquad_predicates(prop_dir)
    j = 0
    for filter in lcquad_props["lcquad_props_filters"]:
        j += 1
        query = dict_sparqlQueries["query_" + prop_dir + "_oneHope"] % {
                'target_resource': entityIds_arr[0], 'filter_in': filter}
        
        if len(entityIds_arr) == 2:
            query = dict_sparqlQueries["query_" + prop_dir + "_twoTE"] % {
                'target_resource': entityIds_arr[0], 'target_resource2': entityIds_arr[1], 'filter_in': filter}
        
        error_msg = ', '.join(entityIds_arr) + "\t" + prop_sign + "\t" + "filter:" + str(j)
        write_queryMsg = [F_cache_error, error_msg]
        i = 0
        results = get_query_results(query, write_queryMsg)
        if results:
            for result in results["results"]["bindings"]:
                i += 1
                prop_oneHop = result['property']['value'].replace(
                    'http://www.wikidata.org/entity/', '')

                cc_label = prop_sign + result['propertyLabel']['value']
                cc_id = prop_sign + prop_oneHop
                if len(entityIds_arr) == 1:
                    cc_line = entityIds_arr[0].replace('wd:', '') + "\t" + prop_sign + "\t" + cc_label + "\t" + cc_id
                
                if len(entityIds_arr) == 2:
                    cc_line = ', '.join(entityIds_arr).replace(
                        'wd:', '') + "\t" + prop_sign + "\t" + cc_label + "\t" + cc_id

                #------ create corechains
                corechains_labels.append(cc_label)
                corechains_ids.append(cc_id)
                if not specialUse:
                    write_to_file(F_corechains_cache, cc_line)
   
    if len(entityIds_arr) == 1:
        if(prop_dir == "left"):
            #*get corechains of excluded predicates
            for prop in lcquad_props["exclude_props"]:
                #prop[0]: predicate is , prop[1]: predicate label
                if(ask_triple(entityIds_arr[0].replace('wd:', ''), prop[0], prop_dir)):

                    cc_label = prop_sign + prop[1]
                    cc_id = prop_sign + prop[0]
                    cc_line = entityIds_arr[0].replace(
                        'wd:', '') + "\t" + prop_sign + "\t" + cc_label + "\t" + cc_id

                    #------ create corechains
                    corechains_labels.append(cc_label)
                    corechains_ids.append(cc_id)
                    write_to_file(F_corechains_cache, cc_line)

    return corechains_labels, corechains_ids

#! -------- Quilifiers corechain Cashe
def quilifiers_corechains_cache(entityIds_arr, prop_dir, specialUse=None):
    prop_sign = "+"
    if(prop_dir == "left"):
        prop_sign = "-"
    corechains_labels = []
    corechains_ids = []
    hyper_sign = "*"
    ccSign = prop_sign + hyper_sign
    lcquad_props = dict_lcquad_predicates(prop_dir)
    
    if len(entityIds_arr) == 1: # TE P OBJ PQ Qual -> [+-P *PQ]
        query = dict_sparqlQueries["query_only_hyperRel_" + prop_dir] % {
            'target_resource': entityIds_arr[0], 'target_prop': "?p", "target_resource2": '?obj', "target_qualifier": '?qualifier', 'filter_in': ''}

    if len(entityIds_arr) == 2 and specialUse == 'NoQualifier':  # TE1 P TE2 PQ Qual -> [+-P *PQ]
        query = dict_sparqlQueries["query_only_hyperRel_" + prop_dir] % {
            'target_resource': entityIds_arr[0], 'target_prop': "?p", "target_resource2": entityIds_arr[1], "target_qualifier": '?qualifier', 'filter_in': ''}
    
    if len(entityIds_arr) == 2 and specialUse == 'TE2Qualifier':  # TE1 P OBJ PQ TE2 -> [+-P, *PQ]  {comma means the answer between}
        query = dict_sparqlQueries["query_only_hyperRel_" + prop_dir] % {
            'target_resource': entityIds_arr[0], 'target_prop': "?p", "target_resource2": '?obj', "target_qualifier": entityIds_arr[1], 'filter_in': ''}
        ccSign = prop_sign + ',' + hyper_sign
    
    if len(entityIds_arr) == 2 and specialUse == 'TE1Qualifier':  # TE2 P OBJ PQ TE1 ->  [*PQ +-P]  [not in the LCQuAd2 Tem]
        query = dict_sparqlQueries["query_only_hyperRel_" + prop_dir] % {
            'target_resource': entityIds_arr[1], 'target_prop': "?p", "target_resource2": '?obj', "target_qualifier": entityIds_arr[0], 'filter_in': ''}
        ccSign = hyper_sign + prop_sign
        
    # val1 P val2 PQ1 TE1, PQ2 TE2 ->  [*PQ1 *PQ2 +-P]  [not in the LCQuAd2 Tem]
    if len(entityIds_arr) == 2 and specialUse == 'TwoQualifier':
        query = dict_sparqlQueries["query_twoTE_as_qualifiers"] % {
            'target_resource': entityIds_arr[0], "target_resource2": entityIds_arr[1],'filter_in': ''}
        ccSign = hyper_sign + hyper_sign + prop_sign

    error_msg = ', '.join(entityIds_arr) + "\t" + ccSign
    write_queryMsg = [F_cache_error, error_msg]
    i = 0
    results = get_query_results(query, write_queryMsg)
    if len(results) > 1:
        for result in results["results"]["bindings"]:
            prop_oneHop = result['property']['value'].replace(
                'http://www.wikidata.org/entity/', '')
            if(prop_oneHop and prop_oneHop in lcquad_props["lcquad_props"]):
                i += 1
                #------ create dataset of right/left corechains
                cc_hyper_label = ""
                cc_hyper_id = ""
                
                cc_line = ''

                cc_label = prop_sign + result['propertyLabel']['value']
                cc_id = prop_sign + prop_oneHop
                cc_hyper_id = hyper_sign + result['hyperq']['value'].replace('http://www.wikidata.org/entity/', '')
                cc_hyper_label = hyper_sign + result['hyperqLabel']['value']
                
                # TE1 P TE2 PQ Qual -> [+-P *PQ]
                if len(entityIds_arr) == 2 and specialUse == 'NoQualifier':  
                    cc_line = ', '.join(entityIds_arr).replace('wd:', '') + "\t" + ccSign + "\t" + cc_label + ' ' + cc_hyper_label + "\t" + cc_id + ' ' + cc_hyper_id
                    #------ create corechains
                    corechains_labels.append(cc_label + ' ' + cc_hyper_label)
                    corechains_ids.append(cc_id + ' ' + cc_hyper_id)

                # TE1 P OBJ PQ TE2 -> [+-P, *PQ]  {comma means the answer between}
                elif len(entityIds_arr) == 2 and specialUse == 'TE2Qualifier':
                    cc_line = ', '.join(entityIds_arr).replace('wd:', '') + "\t" + ccSign + "\t" + cc_label + ', ' + cc_hyper_label + "\t" + cc_id + ', ' + cc_hyper_id
                    #------ create corechains
                    corechains_labels.append(cc_label + ", " + cc_hyper_label)
                    corechains_ids.append(cc_id + ", " + cc_hyper_id)

                # TE2 P OBJ PQ TE1 ->  [*PQ +-P]  [not in the LCQuAd2 Tem]
                elif len(entityIds_arr) == 2 and specialUse == 'TE1Qualifier':
                    cc_line = ', '.join(entityIds_arr).replace('wd:', '') + "\t" + ccSign + "\t" + cc_hyper_label + ' ' + cc_label + "\t" + cc_hyper_id + ' ' + cc_id
                    #------ create corechains
                    corechains_labels.append(cc_hyper_label + ' ' + cc_label)
                    corechains_ids.append(cc_hyper_id + ' ' + cc_id)

                # val1 P val2 PQ1 TE1, PQ2 TE2 ->  [*PQ1 *PQ2 +-P]  [not in the LCQuAd2 Tem]
                elif len(entityIds_arr) == 2 and specialUse == 'TwoQualifier':
                    cc_hyper_id2 = hyper_sign + result['hyperq2']['value'].replace('http://www.wikidata.org/entity/', '')
                    cc_hyper_label2 = hyper_sign + result['hyperq2Label']['value']
                    cc_line = ', '.join(entityIds_arr).replace('wd:', '') + "\t" + ccSign + "\t" + cc_hyper_label + ' ' + cc_hyper_label2 + ' ' + cc_label + "\t" + cc_hyper_id + ' ' + cc_hyper_id2 + ' ' + cc_id
                    #------ create corechains
                    corechains_labels.append(cc_hyper_label + ' ' + cc_hyper_label2 + ' ' + cc_label)
                    corechains_ids.append(cc_hyper_id + ' ' + cc_hyper_id2 + ' ' + cc_id)
              
                # TE P OBJ PQ Qual -> [+-P *PQ]:
                elif len(entityIds_arr) == 1:  
                    cc_line = entityIds_arr[0].replace('wd:', '') + "\t" + ccSign + "\t" + cc_label + ' ' + cc_hyper_label + "\t" + cc_id + ' ' + cc_hyper_id
                    #------ create corechains
                    corechains_labels.append(cc_label + ' ' + cc_hyper_label)
                    corechains_ids.append(cc_id + ' ' + cc_hyper_id)

                write_to_file(F_corechains_cache, cc_line)
    # -------- if timeout
    if len(results) == 1:
        write_to_file(F_cache_terminal, "fixig error using filters - hyperFunc: " + ccSign)
        print("fixig error using filters - hyperFunc: " + ccSign)
        corechains_labels, corechains_ids = quilifiers_corechains_cache_fix(entityIds_arr, prop_dir, specialUse)

    write_to_file(F_cache_terminal, ccSign + ' ' + str(len(corechains_labels)))
    print(ccSign , len(corechains_labels))
    corechains = list(zip(corechains_ids,corechains_labels))
    return corechains

#! -------- Quilifiers corechain Cashe fixing using filters
def quilifiers_corechains_cache_fix(entityIds_arr, prop_dir, specialUse=None):
    prop_sign = "+"
    if(prop_dir == "left"):
        prop_sign = "-"
    corechains_labels = []
    corechains_ids = []
    hyper_sign = "*"
    lcquad_props_filters = dict_lcquad_predicates(prop_dir)
    j = 0
    for filter in lcquad_props_filters["lcquad_props_filters"]:
        j += 1
        if len(entityIds_arr) == 1:  # TE P OBJ PQ Qual -> [+-P *PQ]
            query = dict_sparqlQueries["query_only_hyperRel_" + prop_dir] % {
                'target_resource': entityIds_arr[0], 'target_prop': "?p", "target_resource2": '?obj', "target_qualifier": '?qualifier', 'filter_in': filter}
            error_msg = entityIds_arr[0] + "\t" + prop_sign + hyper_sign + "\t" + "filter:" + str(j)
            # TE1 P TE2 PQ Qual -> [+-P *PQ]
        if len(entityIds_arr) == 2 and specialUse == 'NoQualifier':
            query = dict_sparqlQueries["query_only_hyperRel_" + prop_dir] % {
                'target_resource': entityIds_arr[0], 'target_prop': "?p", "target_resource2": entityIds_arr[1], "target_qualifier": '?qualifier', 'filter_in': filter}
            error_msg = ', '.join(entityIds_arr) + "\t" + \
                prop_sign + hyper_sign + "\t" + "filter:" + str(j)

        # TE1 P OBJ PQ TE2 -> [+-P, *PQ]  {comma means the answer between}
        if len(entityIds_arr) == 2 and specialUse == 'TE2Qualifier':
            query = dict_sparqlQueries["query_only_hyperRel_" + prop_dir] % {
                'target_resource': entityIds_arr[0], 'target_prop': "?p", "target_resource2": '?obj', "target_qualifier": entityIds_arr[1], 'filter_in': filter}
            error_msg = ', '.join(entityIds_arr) + "\t" + \
                prop_sign + ',' + hyper_sign + "\t" + "filter:" + str(j)

        # TE2 P OBJ PQ TE1 ->  [*PQ +-P]  [not in the LCQuAd2 Tem]
        if len(entityIds_arr) == 2 and specialUse == 'TE1Qualifier':
            query = dict_sparqlQueries["query_only_hyperRel_" + prop_dir] % {
                'target_resource': entityIds_arr[1], 'target_prop': "?p", "target_resource2": '?obj', "target_qualifier": entityIds_arr[0], 'filter_in': filter}
            error_msg = ', '.join(entityIds_arr) + "\t" + \
                hyper_sign + prop_sign + "\t" + "filter:" + str(j)

        # val1 P val2 PQ1 TE1, PQ2 TE2 ->  [*PQ1 *PQ2 +-P]  [not in the LCQuAd2 Tem]
        if len(entityIds_arr) == 2 and specialUse == 'TwoQualifier':
            query = dict_sparqlQueries["query_twoTE_as_qualifiers"] % {
                'target_resource': entityIds_arr[0], "target_resource2": entityIds_arr[1], 'filter_in': filter}
            error_msg = ', '.join(entityIds_arr) + "\t" + \
                hyper_sign + hyper_sign + prop_sign + "\t" + "filter:" + str(j)
        
        write_queryMsg = [F_cache_error, error_msg]
        i = 0
        results = get_query_results(query, write_queryMsg)
        if results:
            for result in results["results"]["bindings"]:
                i += 1
                #------ create dataset of right/left corechains
                cc_hyper_label = ""
                cc_hyper_id = ""
                prop_oneHop = result['property']['value'].replace(
                'http://www.wikidata.org/entity/', '')

                cc_label = prop_sign + result['propertyLabel']['value']
                cc_id = prop_sign + prop_oneHop
                cc_hyper_id = hyper_sign + result['hyperq']['value'].replace('http://www.wikidata.org/entity/', '')
                cc_hyper_label = hyper_sign + result['hyperqLabel']['value']
                
                # TE1 P OBJ PQ TE2 -> [+-P, *PQ]  {comma means the answer between}
                if len(entityIds_arr) == 2 and specialUse == 'TE2Qualifier':
                    cc_line = ', '.join(entityIds_arr).replace(
                        'wd:', '') + "\t" + prop_sign + "," + hyper_sign + "\t" + cc_label + ', ' + cc_hyper_label + "\t" + cc_id + ', ' + cc_hyper_id
                    #------ create corechains
                    corechains_labels.append(cc_label + ", " + cc_hyper_label)
                    corechains_ids.append(cc_id + ", " + cc_hyper_id)

                # TE2 P OBJ PQ TE1 ->  [*PQ +-P]  [not in the LCQuAd2 Tem]
                elif len(entityIds_arr) == 2 and specialUse == 'TE1Qualifier':
                    cc_line = ', '.join(entityIds_arr).replace(
                        'wd:', '') + "\t" + hyper_sign + prop_sign + "\t" + cc_hyper_label + ' ' + cc_label + "\t" + cc_hyper_id + ' ' + cc_id
                    #------ create corechains
                    corechains_labels.append(cc_hyper_label + ' ' + cc_label)
                    corechains_ids.append(cc_hyper_id + ' ' + cc_id)

                # val1 P val2 PQ1 TE1, PQ2 TE2 ->  [*PQ1 *PQ2 +-P]  [not in the LCQuAd2 Tem]
                elif len(entityIds_arr) == 2 and specialUse == 'TwoQualifier':
                    cc_hyper_id2 = hyper_sign + \
                        result['hyperq2']['value'].replace(
                            'http://www.wikidata.org/entity/', '')
                    cc_hyper_label2 = hyper_sign + \
                        result['hyperq2Label']['value']
                    cc_line = ', '.join(entityIds_arr).replace(
                        'wd:', '') + "\t" + hyper_sign + hyper_sign + prop_sign + "\t" + cc_hyper_label + ' ' + cc_hyper_label2 + ' ' + cc_label + "\t" + cc_hyper_id + ' ' + cc_hyper_id2 + ' ' + cc_id
                    #------ create corechains
                    corechains_labels.append(
                        cc_hyper_label + ' ' + cc_hyper_label2 + ' ' + cc_label)
                    corechains_ids.append(
                        cc_hyper_id + ' ' + cc_hyper_id2 + ' ' + cc_id)

                # TE P OBJ PQ Qual -> [+-P *PQ]:
                elif len(entityIds_arr) == 1:
                    cc_line = entityIds_arr[0].replace(
                        'wd:', '') + "\t" + prop_sign + hyper_sign + "\t" + cc_label + ' ' + cc_hyper_label + "\t" + cc_id + ' ' + cc_hyper_id
                    #------ create corechains
                    corechains_labels.append(cc_label + ' ' + cc_hyper_label)
                    corechains_ids.append(cc_id + ' ' + cc_hyper_id)

                write_to_file(F_corechains_cache, cc_line)

    return corechains_labels, corechains_ids


#! -------- Two Hops corechain Cashe
def corechains_twoHops_cache(entityIds_arr, prop_dir):
    prop_sign1 = ""
    prop_sign2 = ""
    twoHop_dir = "right" #used once just to create the filters, val: right OR left

    #prop_dir val: RR RL LR LL
    if(prop_dir[0] == "R"):
        prop_sign1 = "+"
    else:
        prop_sign1 = "-"
    
    if(prop_dir[1] == "R"):
        prop_sign2 = "+"
    else:
        prop_sign2 = "-"
        twoHop_dir = 'left'
    
    ccSign = prop_sign1 + prop_sign2

    corechains_labels = []
    corechains_ids = []
    lcquad_props = dict_lcquad_predicates(twoHop_dir)

    selectQ = "?p1 ?p2 "
    directClaim = "FILTER(STRSTARTS(str(?p1), 'http://www.wikidata.org/prop/direct/')) .  FILTER(STRSTARTS(str(?p2), 'http://www.wikidata.org/prop/direct/')) . "

    if(len(entityIds_arr) == 1):
        query = dict_sparqlQueries["query_" + prop_dir + "_twoHops"] % {
            'target_resource': entityIds_arr[0], 'selectQ': selectQ, 'target_resource2': '?obj2', 'directClaim': directClaim}
    if(len(entityIds_arr) == 2):
        query = dict_sparqlQueries["query_" + prop_dir + "_twoHops"] % {
            'target_resource': entityIds_arr[0], 'selectQ': selectQ, 'target_resource2': entityIds_arr[1], 'directClaim': directClaim} 
    error_msg = ', '.join(entityIds_arr) + "\t" + ccSign
    write_queryMsg = [F_cache_error, error_msg]    
    i = 0
    results = get_query_results(query, write_queryMsg)
    if len(results) > 1:
        for result in results["results"]["bindings"]:
            prop1_id = result['p1']['value'].replace('http://www.wikidata.org/entity/', '')
            prop1_id = prop1_id.replace('http://www.wikidata.org/prop/direct/', '')

            prop2_id = result['p2']['value'].replace('http://www.wikidata.org/entity/', '')
            prop2_id = prop2_id.replace('http://www.wikidata.org/prop/direct/', '')

            if((prop1_id and prop1_id in lcquad_props["lcquad_props"]) and (prop2_id and prop2_id in lcquad_props["lcquad_props"])):
                i += 1
                prop1_lbl = mu_prop_lcquad(prop1_id, 'id') #get label of oneHopID
                prop2_lbl = mu_prop_lcquad(prop2_id, 'id')
                
                cc_oneHopID = prop_sign1 + prop1_id
                cc_oneHopLabel = prop_sign1 + prop1_lbl

                cc_twoHopID = prop_sign2 + prop2_id
                cc_twoHopLabel = prop_sign2 + prop2_lbl
                
                cc_label = cc_oneHopLabel + " " + cc_twoHopLabel
                cc_id = cc_oneHopID + " " + cc_twoHopID

                corechains_labels.append(cc_label)
                corechains_ids.append(cc_id)

    corechains = list(zip(corechains_ids, corechains_labels))
    for cc in corechains:
        cc_line = ', '.join(entityIds_arr).replace('wd:', '') + "\t" + ccSign + "\t" + cc[1] + "\t" + cc[0]
        #------ create corechains
        write_to_file(F_corechains_cache, cc_line)
    # -------- if timeout
    if len(results) == 1:
        write_to_file(F_cache_terminal, "fixing error by changin the query " + ccSign)       
        print("fixig error by changing the query" + ccSign)       
        corechains = corechains_twoHops_cache_fix(entityIds_arr, prop_dir)
    
    write_to_file(F_cache_terminal, ccSign + ' ' + str(len(corechains))) 
    print(ccSign, len(corechains)) 
     
    if(len(corechains) == 0 and len(results) != 1):
        write_to_file(F_cache_has_no_ans, ', '.join(entityIds_arr).replace('wd:', '') + "\t" + ccSign + "\t" + "LCQuAD2 issue")
    elif(len(corechains) == 0):
        write_to_file(F_cache_has_no_ans, ', '.join(entityIds_arr).replace('wd:', '') + "\t" + ccSign + "\t" + "SPARQL issue")

    return corechains

#! -------- Two Hops corechain Cashe fixing using filters
def corechains_twoHops_cache_fix(entityIds_arr, prop_dir, specialUse=None):
    prop_sign1 = ""
    prop_sign2 = ""
    oneHop_dir = "right"
    twoHop_dir = "right" #used once just to create the filters, val: right OR left

    #prop_dir val: RR RL LR LL
    if(prop_dir[0] == "R"):
        prop_sign1 = "+"
    else:
        prop_sign1 = "-"
        oneHop_dir = 'left'
    
    if(prop_dir[1] == "R"):
        prop_sign2 = "+"
    else:
        prop_sign2 = "-"
        twoHop_dir = 'left'
    
    ccSign = prop_sign1 + prop_sign2

    corechains_labels = []
    corechains_ids = []
    corechains = []

    #-specialUse
    ccProd = []

    lcquad_props = dict_lcquad_predicates(twoHop_dir)

    selectQ = '?obj1 ?p1 ?p2 '
    directClaim = "FILTER(STRSTARTS(str(?p1), 'http://www.wikidata.org/prop/direct/')) .  FILTER(STRSTARTS(str(?p2), 'http://www.wikidata.org/prop/direct/')) . "

    if(len(entityIds_arr) == 1):
        query = dict_sparqlQueries["query_" + prop_dir + "_twoHops"] % {
            'target_resource': entityIds_arr[0], 'selectQ': selectQ, 'target_resource2': '?obj2', 'directClaim': directClaim}
    if(len(entityIds_arr) == 2):
        query = dict_sparqlQueries["query_" + prop_dir + "_twoHops"] % {
            'target_resource': entityIds_arr[0], 'selectQ': selectQ, 'target_resource2': entityIds_arr[1], 'directClaim': directClaim} 
    error_msg = ', '.join(entityIds_arr) + "\t" + ccSign
    write_queryMsg = [F_cache_error, error_msg]
    
    i = 0
    results = get_query_results(query, write_queryMsg)
    if len(results) > 1:
        for result in results["results"]["bindings"]:
            prop1_id = result['p1']['value'].replace('http://www.wikidata.org/entity/', '')
            prop1_id = prop1_id.replace('http://www.wikidata.org/prop/direct/', '')

            prop2_id = result['p2']['value'].replace('http://www.wikidata.org/entity/', '')
            prop2_id = prop2_id.replace('http://www.wikidata.org/prop/direct/', '')

            if((prop1_id and prop1_id in lcquad_props["lcquad_props"]) and (prop2_id and prop2_id in lcquad_props["lcquad_props"])):
                i += 1
                prop1_lbl = mu_prop_lcquad(prop1_id, 'id') #get label of oneHopID
                prop2_lbl = mu_prop_lcquad(prop2_id, 'id')
                
                cc_oneHopID = prop_sign1 + prop1_id
                cc_oneHopLabel = prop_sign1 + prop1_lbl

                cc_twoHopID = prop_sign2 + prop2_id
                cc_twoHopLabel = prop_sign2 + prop2_lbl
                
                cc_label = cc_oneHopLabel + " " + cc_twoHopLabel
                cc_id = cc_oneHopID + " " + cc_twoHopID

                corechains_labels.append(cc_label)
                corechains_ids.append(cc_id)

                # if specialUse == 'temp13-14':
                #     #--> [ [P1, P1Label, P2, P2Label , OBJ1]
                #     entity_between = result['obj1']['value'].replace('http://www.wikidata.org/entity/', '')
                #     ccProd= [cc_oneHopID, cc_oneHopLabel, cc_twoHopID, cc_twoHopLabel, entity_between]
                #     corechains.append(ccProd)

        # if specialUse == 'temp13-14':
        #     #--> [ [P1, P1Label, P2, P2Label , OBJ1]
        #     return corechains #! skip here
        # else:
        corechains = list(zip(corechains_ids, corechains_labels))
        #remove duplicates
        corechains = list(OrderedDict.fromkeys(corechains))
        for cc in corechains:
            cc_line = ', '.join(entityIds_arr).replace('wd:', '') + "\t" + ccSign + "\t" + cc[1] + "\t" + cc[0]
            #------ create corechains
            write_to_file(F_corechains_cache, cc_line)
    if len(results) == 1:
        specialUse_txt = 'withoutProd'
        terminal_txt = 'fixing the error by using prod func'
        
        # if specialUse == 'temp13-14':
        #     specialUse_txt = None
        #     terminal_txt = 'fixing the error of prod by using prod func 2'
        #     ccSign = prop_sign1 + '-/+,' + prop_sign2
        
        write_to_file(F_cache_terminal, terminal_txt + ccSign)    
        print(terminal_txt + ccSign)
        corechains = generate_prod_twoTE_corechain(entityIds_arr, prop_dir, specialUse_txt)
    
    return corechains

#! -------- Two Topic Entities Two Hops corechain Cashe 
def get_ent_between_twoTE(entityIds_arr, prop_dir):
    prop_sign1 = ""
    prop_sign2 = ""

    #prop_dir val: RR RL LR LL
    if(prop_dir[0] == "R"):
        prop_sign1 = "+"
    else:
        prop_sign1 = "-"

    if(prop_dir[1] == "R"):
        prop_sign2 = "+"
    else:
        prop_sign2 = "-"
        twoHop_dir = 'left'
    ccSign = prop_sign1 + prop_sign2

    all_entities_between = []
    error_msg = ', '.join(entityIds_arr) + "\t" + ccSign
    write_queryMsg = [F_cache_error, error_msg]
    if(len(entityIds_arr) == 1):  
        query = dict_sparqlQueries["query_" + prop_dir + "_twoTE"] % {
            'target_resource': entityIds_arr[0], 'target_prop': '?p1', 'target_resource2': '?obj2'}
    else: # for {P1 P2} corechains
        query = dict_sparqlQueries["query_" + prop_dir + "_twoTE"] % {
            'target_resource': entityIds_arr[0], 'target_prop': '?p1', 'target_resource2': entityIds_arr[1]}
    
    i = 0
    results = get_query_results(query, write_queryMsg)
    if len(results) > 1:
        for result in results["results"]["bindings"]:
            entity_between = result['obj1']['value'].replace(
                'http://www.wikidata.org/entity/', '')
            if(entity_between[0] == 'Q'):
                all_entities_between.append(entity_between)

    return all_entities_between

def corechain_product_list_itself(entityIds_arr, oneHop_arr):
    ccSign = ''
    prod_lists_labels = [] 
    prod_lists_ids = []

    #product of list1 with itslef {ids} and {labels}
    i = 0
    for cc in oneHop_arr:
        #cc - > [id, label]
        # prod the predicate with itself ex-> +P17, +P17
        prod_lists_ids.append(cc[0] + ", " + cc[0])
        prod_lists_labels.append(cc[1] + ", " + cc[1])

        #prod the predicate with others:
        for j in range(i+1, len(oneHop_arr)):
            prod_lists_ids.append(cc[0] + ", " + oneHop_arr[j][0])
            prod_lists_labels.append(cc[1] + ", " + oneHop_arr[j][1])
            
        i += 1

    i = 0
    for cc_lbl in prod_lists_labels:
        sign2_index = prod_lists_ids[i].index(', ')
        ccSign = prod_lists_ids[i][0] + "," + prod_lists_ids[i][sign2_index+2]
        line = ','.join(entityIds_arr).replace(
            'wd:', '') + "\t" + ccSign + "\t" + cc_lbl + "\t" + prod_lists_ids [i]
        write_to_file(F_corechains_cache, line)
        i += 1
    
    corechains = list(zip(prod_lists_ids, prod_lists_labels))
    write_to_file(F_cache_terminal, ccSign + ' ' + str(len(corechains)))
    print(ccSign, len(corechains))
    return corechains

#* retrieve topic entites of specific template from LCQuAD 2.0 dataset
def get_lcquad_entities(templateIDs):
    f = open('data/lcquad2_dataset/lcquad2_test_23Nov.json', 'r') #! changed to test in test data/ and train in case train data
    lcquad_data = f.read()  # will append in the list out
    obj = json.loads(lcquad_data)
    i = 0
    all_TE_of_temp = []
    for q_data in obj:
        if q_data['template_id'] in templateIDs:
            # put all the entities of the query in an array
            entities = re.findall(r'\bwd:\w+', q_data['sparql_wikidata'])
            # remove duplicate and keep the order
            entities = list(OrderedDict.fromkeys(entities))
            all_TE_of_temp.append(entities)
    return all_TE_of_temp

#! generate corecahins for two hops (by creating filter first) 
# #----TE1 P1 OBJ1 P2 OBJ2, OBJ1 P3 TE2 ---> P1 P2, P3
def generate_twoHops_corechains_product(entityIds_arr, prop_dir, prod_dir):
    prop_sign1 = ""
    prop_sign2 = ""
    prop_sign3 = '+'
    twoHop_dir = "right" #used once just to create the filters, val: right OR left

    #prop_dir val: RR RL LR LL
    if(prop_dir[0] == "R"):
        prop_sign1 = "+"
    else:
        prop_sign1 = "-"
    
    if(prop_dir[1] == "R"):
        prop_sign2 = "+"
    else:
        prop_sign2 = "-"
        twoHop_dir = 'left'

    corechains_labels = []
    corechains_ids = []
    corechains = []

    lcquad_props = dict_lcquad_predicates(twoHop_dir)
    prod_statement = '?obj1 ?p3 ?obj3.'
    if prod_dir == 'left':
        prod_statement = '?obj3 ?p3 ?obj1.'
        prop_sign3 = '-'

    ccSign = prop_sign1 + prop_sign2 + ',' + prop_sign3

    if(len(entityIds_arr) == 1):
        query = dict_sparqlQueries["query_" + prop_dir + "_prod_twoHops"] % {
            'target_resource': entityIds_arr[0], 'prod_statement': prod_statement, 'target_resource2': '?obj2', 'emb_filter': ''}
    if(len(entityIds_arr) == 2):
        query = dict_sparqlQueries["query_" + prop_dir + "_prod_twoHops"] % {
            'target_resource': entityIds_arr[0], 'prod_statement': prod_statement, 'target_resource2': entityIds_arr[1], 'emb_filter': ''} 
    error_msg = ', '.join(entityIds_arr) + "\t" + ccSign
    write_queryMsg = [F_cache_error, error_msg]
    
    i = 0
    results = get_query_results(query, write_queryMsg)
    if len(results) > 1:
        for result in results["results"]["bindings"]:
            prop1_id = result['p1']['value'].replace('http://www.wikidata.org/entity/', '')
            prop1_id = prop1_id.replace('http://www.wikidata.org/prop/direct/', '')

            prop2_id = result['p2']['value'].replace('http://www.wikidata.org/entity/', '')
            prop2_id = prop2_id.replace('http://www.wikidata.org/prop/direct/', '')

            prop3_id = result['p3']['value'].replace('http://www.wikidata.org/entity/', '')
            prop3_id = prop3_id.replace('http://www.wikidata.org/prop/direct/', '')

            if((prop1_id and prop1_id in lcquad_props["lcquad_props"]) and (prop2_id and prop2_id in lcquad_props["lcquad_props"]) and (prop3_id and prop3_id in lcquad_props["lcquad_props"])):
                i += 1
                prop1_lbl = mu_prop_lcquad(prop1_id, 'id') #get label of oneHopID
                prop2_lbl = mu_prop_lcquad(prop2_id, 'id')
                prop3_lbl = mu_prop_lcquad(prop3_id, 'id')
                
                cc_oneHopID = prop_sign1 + prop1_id
                cc_oneHopLabel = prop_sign1 + prop1_lbl

                cc_twoHopID = prop_sign2 + prop2_id
                cc_twoHopLabel = prop_sign2 + prop2_lbl

                cc_prodHopID = prop_sign3 + prop3_id
                cc_prodHopLabel = prop_sign3 + prop3_lbl
                
                cc_label = cc_oneHopLabel + " " + cc_twoHopLabel+ ', ' + cc_prodHopLabel
                cc_id = cc_oneHopID + " " + cc_twoHopID + ', ' + cc_prodHopID

                corechains_labels.append(cc_label)
                corechains_ids.append(cc_id)


        corechains = list(zip(corechains_ids, corechains_labels))
        #remove duplicates
        corechains = list(OrderedDict.fromkeys(corechains))
        for cc in corechains:
            cc_line = ', '.join(entityIds_arr).replace('wd:', '') + "\t" + ccSign + "\t" + cc[1] + "\t" + cc[0]
            #------ create corechains
            write_to_file(F_corechains_cache, cc_line)
    if len(results) == 1:
        terminal_txt = 'fixing the error by using prod func'
        write_to_file(F_cache_terminal, terminal_txt + ccSign)    
        print(terminal_txt + ccSign)
        corechains = generate_prod_twoTE_corechain(entityIds_arr, prop_dir)

    if(len(corechains) == 0):
        write_to_file(F_cache_has_no_ans, ', '.join(entityIds_arr).replace('wd:', '') + "\t" + ccSign + "\t" + "LCQuAD2 issue")
    print(ccSign, len(corechains))
    return corechains 

#! generate product corechains for two TE with two hops: # #----TE1 P1 OBJ1 P2 OBJ2, OBJ1 P3 TE2
#* THIS FUNCTION IS USED AS A SECONDARY PROD FUNC WHERE THE DIFF IS FIRST RETRIEVING ALL THE ENT BETWEEN
def generate_prod_twoTE_corechain(entityIds_arr, prop_dir, specialUse=None):
    #prop_dir = R OR L
    dirOneHop = ''
    dirTwoHop = ''
    if(prop_dir[0] == "R"):
        prop_sign1 = "+"
        dirOneHop = 'right'
    else:
        prop_sign1 = "-"
        dirOneHop = 'left'

    if(prop_dir[1] == "R"):
        prop_sign2 = "+"
        dirTwoHop = 'right'
    else:
        prop_sign2 = "-"
        dirTwoHop = 'left'

    #middle sign is alway right (future work should be both)
    ccSign = prop_sign1 + '+' + ',' + prop_sign2

    if specialUse == 'withoutProd':
        ccSign = prop_sign1 + prop_sign2

    all_twoTE_twoHops_labels = []
    all_twoTE_twoHops_ids = []
    r_all_twoHops_labels = []
    r_all_twoHops_ids = []
    entities_bet_twoTE = get_ent_between_twoTE(entityIds_arr, prop_dir)
    for ent in entities_bet_twoTE:
        ent = 'wd:' + ent
        r_oneHop_cc = corechains_oneHop_cache([entityIds_arr[0], ent], dirOneHop, 'specialUse') #specialUse means to not write to the txt file
        if len(entityIds_arr) == 2:
            r_twoHops_cc = corechains_oneHop_cache([ent, entityIds_arr[1]], dirTwoHop, 'specialUse')#specialUse means to not write to the txt file
        
        #generate oneHop for ent
        r_ent = []
        if not specialUse:
            r_ent = corechains_oneHop_cache([ent], 'right', 'specialUse')#specialUse means to not write to the txt file      
        
        if specialUse == 'withoutProd' and len(entityIds_arr) == 1:
            r_ent = corechains_oneHop_cache([ent], dirTwoHop, 'specialUse')#specialUse means to not write to the txt file      
        

        if not specialUse:# to the prduct to generate {P1 P2,P3}
            # product twohops of ent with each other of the same direction 
            for cc1 in r_ent:
                # cc1 -> [id, label]
                for cc2 in r_twoHops_cc: 
                    # cc2 -> [id, label]
                    r_all_twoHops_ids.append(cc1[0] + ", " + cc2[0])
                    r_all_twoHops_labels.append(cc1[1] + ", " + cc2[1])

            r_all_twoHops = list(zip(r_all_twoHops_ids, r_all_twoHops_labels))
            # product of one hop with the result of the product above
            for cc1 in r_oneHop_cc:
                for cc2 in r_all_twoHops:
                    # cc2 -> [id, label]
                    # no comma between becuase its TwoHops
                    all_twoTE_twoHops_ids.append(cc1[0] + " " + cc2[0])
                    all_twoTE_twoHops_labels.append(cc1[1] + " " + cc2[1])
       
        elif specialUse == 'withoutProd':#to generate {P1 P2}  
            if len(entityIds_arr) == 1: #if its one TE
                for cc1 in r_oneHop_cc:
                    for cc2 in r_ent:
                        # cc2 -> [id, label]
                        # no comma between becuase its TwoHops
                        all_twoTE_twoHops_ids.append(cc1[0] + " " + cc2[0])
                        all_twoTE_twoHops_labels.append(cc1[1] + " " + cc2[1])
            elif len(entityIds_arr) == 2: #if its Two TE
                for cc1 in r_oneHop_cc:
                    for cc2 in r_twoHops_cc:
                        # cc2 -> [id, label]
                        # no comma between becuase its TwoHops
                        all_twoTE_twoHops_ids.append(cc1[0] + " " + cc2[0])
                        all_twoTE_twoHops_labels.append(cc1[1] + " " + cc2[1])

    #remove duplicate and keep the order
    all_twoTE_twoHops_labels = list(OrderedDict.fromkeys(all_twoTE_twoHops_labels))
    all_twoTE_twoHops_ids = list(OrderedDict.fromkeys(all_twoTE_twoHops_ids))
    #write to the file:
    i = 0
    for cc_label in all_twoTE_twoHops_labels:
        #split on space to get the sign of the middle predicat
        # ex: to get the sign of -P22 in ->  +P110 -P22, +P215 ....result is -
        # split_ccId = all_twoTE_twoHops_ids[i].split() 
        # ccSign = prop_sign1 + split_ccId[1][0] + ',' + prop_sign2

        cc_line = ', '.join(entityIds_arr).replace(
            'wd:', '') + "\t" + ccSign + "\t" + cc_label + "\t" + all_twoTE_twoHops_ids[i]
        write_to_file(F_corechains_cache, cc_line)
        i += 1
    # new_ccSign = prop_sign1 + '(+/-)' + ',' + prop_sign2

    corechains = list(zip(all_twoTE_twoHops_ids, all_twoTE_twoHops_labels))
    if not specialUse:
        write_to_file(F_cache_terminal, ccSign + ' ' + str(len(corechains)))
        print(ccSign, len(corechains))
        if(len(corechains) == 0):
            write_to_file(F_cache_has_no_ans, ', '.join(entityIds_arr).replace('wd:', '') + "\t" + ccSign )

    return corechains

#! genrate {ProdNoQualifier} corechains: TE1 P TE2 PQ1 Qual1, PQ2 Qul2 -> [+-P *PQ1, *PQ2] 
def generate_prod_quilifiers_corechain(entityIds_arr, qualifier_arr, prop_dir):
    prop_sign = "+"
    if(prop_dir == "left"):
        prop_sign = "-"
    
    ccSign = prop_sign + '*,*'
    oneHopId_arr = []
    oneHopLabel_arr = []
    prod_qualifier_ids = []
    prod_qualifier_labels = []
    i = 0
    #retrieve DISTINCT One hop labels and ids (without hyperRel)
    for cc in qualifier_arr:
        #cc -> [id, label]
        oneHop_id = cc[0].split(' *')
        if(oneHop_id[0] not in oneHopId_arr):
            oneHopId_arr.append(oneHop_id[0])
        
        oneHop_label = cc[1].split(' *')
        if(oneHop_label[0] not in oneHopLabel_arr):
            oneHopLabel_arr.append(oneHop_label[0])

    
    hyperRel_id_arr = []
    hyperRel_label_arr = []
    # generate corechain in this form: -> +P0 *P1, *P2
    for oneHopId, oneHopLabel in zip(oneHopId_arr, oneHopLabel_arr): #+-P
        #collet all hyperRel for specific predicate in one array
        for cc in qualifier_arr: #+-P *P
            #cc -> [id, label]
            if(oneHopId in cc[0]):
                hyperRel_id = cc[0].split(oneHopId + ' ')
                hyperRel_label = cc[1].split(oneHopLabel + ' ')

                hyperRel_id_arr.append(hyperRel_id[1])
                hyperRel_label_arr.append(hyperRel_label[1])

        #product of hyperRel_id_arr with itslef {ids}  and  hyperRel_label_arr with itslef {labels} and add onehop predicate for both
        #ex +P0 *P1, *P2
        i = 0
        hyperRel_arr = list(zip(hyperRel_id_arr, hyperRel_label_arr))
        for cc in hyperRel_arr:
            ccId = oneHopId + " " + cc[0] + ", " + cc[0]
            ccLabel = oneHopLabel + " " + cc[1] + ", " + cc[1]
            prod_qualifier_ids.append(ccId)
            prod_qualifier_labels.append(ccLabel) 
            cc_line = ', '.join(entityIds_arr).replace('wd:', '') + "\t" + ccSign + "\t" + ccLabel + "\t" + ccId
            write_to_file(F_corechains_cache, cc_line) 
            for j in range(i+1, len(hyperRel_arr)):
                ccId = oneHopId + " " + cc[0] + ", " + hyperRel_arr[j][0]
                ccLabel = oneHopLabel + " " + cc[1] + ", " + hyperRel_arr[j][1]
                prod_qualifier_ids.append(ccId)
                prod_qualifier_labels.append(ccLabel) 

                cc_line = ', '.join(entityIds_arr).replace('wd:', '') + "\t" + ccSign + "\t" + ccLabel + "\t" + ccId
                write_to_file(F_corechains_cache, cc_line)   
            i += 1

    corechains = list(zip(prod_qualifier_ids, prod_qualifier_labels))
    write_to_file(F_cache_terminal, ccSign + ' ' + str(len(corechains)))
    print(ccSign, len(corechains))
    return corechains

#! all functions needed to scan all paths in order to create corchain
def lcquad_corechain_func(entityIds_arr, oneTE_templates):

    if len(entityIds_arr) == 1 :   #if len(entityIds_arr) == 1: # if entityIds_arr[0] == 'wd:Q12204':#
        #* generate ONE hop corechain Right and Left
        #if entityIds_arr in get_lcquad_entities(oneTE_templates['oneHop']):
        #----TE P1 OBJ
        r_oneHop = corechains_oneHop_cache(entityIds_arr, "right")
        l_oneHop = corechains_oneHop_cache(entityIds_arr, "left")

        #* generate hyperRel corechain Right and Left
        #----TE1 P1 OBJ1 PQ OBJ2
        if entityIds_arr in get_lcquad_entities(oneTE_templates['hyperRel']):
            r_hyper = quilifiers_corechains_cache(entityIds_arr, "right")
            l_hyper = quilifiers_corechains_cache(entityIds_arr, "left")
        
        #* generate TWO hops corechain Right and Left
        #----TE1 P1 OBJ1 P2 OBJ2
        if entityIds_arr in get_lcquad_entities(oneTE_templates['oneTE_twoHops_RR']):
            rr_twoHops_oneEnt = corechains_twoHops_cache(entityIds_arr, 'RR')
            # rl_twoHops_oneEnt = generate_twoHops_corechains(entityIds_arr, r_oneHop, 'RL')
        
        #----special case: TE1 (P31) OBJ1 P2 OBJ2 --> {-P31 +Pxx}
        if entityIds_arr in get_lcquad_entities(oneTE_templates['oneTE_twoHops_LR']):
            lr_twoHops_oneEnt = corechains_twoHops_cache(entityIds_arr, 'LR')
            # ll_twoHops_oneEnt = generate_twoHops_corechains(entityIds_arr, l_oneHop, 'LL')

        #* generate product of ONE hop corechain for template 24 ONLY
        #----TE1 P1 OBJ1, TE1 P2 OBJ2
        if entityIds_arr in get_lcquad_entities(oneTE_templates['product_oneHop']):
            r_prod_oneHop = corechain_product_list_itself(entityIds_arr, r_oneHop)
            # l_prod_oneHop = corechain_product_list_itself(entityIds_arr, l_oneHop)


    if len(entityIds_arr) == 2:  # if i == -314:  # 
        #----TE P1 OBJ
        #r_twoTE_oneHop_labels, r_twoTE_oneHop_ids
        r_twoTE_oneHop = corechains_oneHop_cache(entityIds_arr, "right",  specialUse=None)
        l_twoTE_oneHop = corechains_oneHop_cache(entityIds_arr, "left",  specialUse=None)

        #--- QUILIFIERS
        #{NoQualifier} TE1 P TE2 PQ Qual -> [+-P *PQ]
        #r_twoTE_NoQualifier_labels, r_twoTE_NoQualifier_ids 
        r_twoTE_NoQualifier = quilifiers_corechains_cache(entityIds_arr, 'right', specialUse = 'NoQualifier')
        l_twoTE_NoQualifier = quilifiers_corechains_cache(entityIds_arr, 'left', specialUse = 'NoQualifier')

        #{TE2Qualifier} TE1 P OBJ PQ TE2 -> [+-P, *PQ] {comma means the answer between} 
        r_twoTE_TE2Qualifier = quilifiers_corechains_cache(entityIds_arr, 'right', specialUse = 'TE2Qualifier')
        l_twoTE_TE2Qualifier = quilifiers_corechains_cache(entityIds_arr, 'left', specialUse = 'TE2Qualifier')

        # #{TE1Qualifier} TE2 P OBJ PQ TE1 ->  [*PQ +-P]  [not in the LCQuAd2 Tem]                    {NOT EXIST IN TEM}
        # r_twoTE_TE1Qualifier = quilifiers_corechains_cache(entityIds_arr, 'right', specialUse = 'TE1Qualifier')
        # l_twoTE_TE1Qualifier = quilifiers_corechains_cache(entityIds_arr, 'left', specialUse = 'TE1Qualifier')
        
        # #{TwoQualifier} val1 P val2 PQ1 TE1, PQ2 TE2 ->  [*PQ1 *PQ2 +-P]  [not in the LCQuAd2 Tem]  {NOT EXIST IN TEM}
        # r_twoTE_TwoQualifier = quilifiers_corechains_cache(entityIds_arr, 'right', specialUse = 'TwoQualifier')
        # l_twoTE_TwoQualifier = quilifiers_corechains_cache(entityIds_arr, 'left', specialUse = 'TwoQualifier')

        #{ProdNoQualifier}  TE1 P TE2 PQ1 Qual1, PQ2 Qul2 -> [+-P *PQ1, *PQ2] {this is product of the first one above {NoQualifier} }
        r_twoTE_prod_NoQualifier = generate_prod_quilifiers_corechain(entityIds_arr, r_twoTE_NoQualifier, 'right')
        l_twoTE_prod_NoQualifier = generate_prod_quilifiers_corechain(entityIds_arr, l_twoTE_NoQualifier, 'left')

        #======== TE1 P1 OBJ P2 TE2 ========

        #----special case: TE1 P1 OBJ1 (P31) TE2 --> {+Pxx +P31}
        # OR:
        #----special case: TE1 P1 OBJ1 P TE2 --> {+P +P}
        if entityIds_arr in get_lcquad_entities(oneTE_templates['twoTE_twoHops_RR']):
            rr_twoTE_twoHops = corechains_twoHops_cache(entityIds_arr, 'RR') 

        #----special case: TE1 (P31) OBJ1 P2 TE2 --> {-P31 +Pxx}
        # OR:
        #----special case: TE1 P1 OBJ1 (P31) TE2 --> {-Pxx +P31}
        if entityIds_arr in get_lcquad_entities(oneTE_templates['twoTE_twoHops_LR']):
            lr_twoTE_twoHops = corechains_twoHops_cache(entityIds_arr, 'LR')     

        # #* generate product of two hops corechain for template 13,14 ONLY
        # #----TE1 P1 OBJ1 P2 OBJ2, OBJ1 P3 TE2
        if entityIds_arr in get_lcquad_entities(oneTE_templates['product_twoTE_twoHops']):
            lr_prod_twoTE_twoHops = generate_twoHops_corechains_product(entityIds_arr, 'LR', 'right')
            #lr_prod_twoTE_twoHops = generate_prod_twoTE_corechain(entityIds_arr, 'LR')

    if len(entityIds_arr) == 3:
         #* generate ONE hop corechain Right and Left
        #if entityIds_arr in get_lcquad_entities(oneTE_templates['oneHop']):
        #----TE P1 OBJ
        new_entityIds_arr =[]
        new_entityIds_arr.append(entityIds_arr[0])
        r_oneHop_extra = corechains_oneHop_cache(new_entityIds_arr, "right", 'specialUse')
        l_oneHop_extra = corechains_oneHop_cache(new_entityIds_arr, "left", 'specialUse')
        #* generate product of ONE hop corechain for template 24 ONLY
        #----TE1 P1 TE2, TE1 P2 TE3   -> +-P1, +-P2
        if entityIds_arr in get_lcquad_entities(oneTE_templates['product_threeTE_oneHop']):
            r_prod_oneHop = corechain_product_list_itself(entityIds_arr, r_oneHop_extra)
            # l_prod_oneHop = corechain_product_list_itself(entityIds_arr, l_oneHop_extra)


def try_example():
    tem24ex = [['wd:Q1896989'],
    ['wd:Q4830453','wd:Q13677'],
    ['wd:Q15632617'],
    ['wd:Q134808','wd:Q155857'],
    ['wd:Q193020','wd:Q15229207'],
    ['wd:Q187247','wd:Q159'],
    ['wd:Q26299'],
    ['wd:Q270'],
    ['wd:Q120533','wd:Q99'],
    ['wd:Q184697','wd:Q738258'],
    ['wd:Q567'],
    ['wd:Q1017','wd:Q4022'],
    ['wd:Q555578'],
    ['wd:Q112099'],
    ['wd:Q4830453','wd:Q151139'],
    ['wd:Q7725634','wd:Q34660'],
    ['wd:Q2'],
    ['wd:Q9554','wd:Q6729875'],
    ['wd:Q4916','wd:Q33','wd:Q211'],
    ['wd:Q7284118'],
    ['wd:Q320588'],
    ['wd:Q9035'],
    ['wd:Q3376'],
    ['wd:Q557801','wd:Q34820'],
    ['wd:Q161210'],
    ['wd:Q14091'],
    ['wd:Q131324']]

    top_ent = ['wd:Q641', 'wd:Q5']#tem24ex[8]
    print(top_ent)
    start_time = time.time()
    lcquad_corechain_func(top_ent, oneTE_templates)
    elapsed_time = time.time() - start_time
    print(elapsed_time)

def lcquad_cache_error_correction():
    start_time = time.time()
    # entityIds_arr = ['wd:Q11424', 'wd:Q22006653']
    # lr_twoTE_twoHops = generate_twoHops_corechains_product(entityIds_arr , 'LR','right')

    entityIds_arr = ['wd:Q11173']
    lr_twoTE_twoHops = corechains_twoHops_cache(entityIds_arr, 'LR') 

    elapsed_time = time.time() - start_time
    print(elapsed_time)
    # F_most_used_entities = open("data/lcquad_cache_corechain_error_sv1.txt", "r")
    # out = F_most_used_entities.readlines()  # will append in the list out
    # i = 0
    # start_time = time.time()
    # entityIds_arr = []
    # for line in out:
    #     arguments = line.split("\t")
    #     entityIds_arr.append(arguments[0])
   
    # entityIds_arr = list(set(entityIds_arr)) 
    # i=0
    # for ent in entityIds_arr:
    #     i+=1
    #     entityIds = []
    #     ent_ids = ent.split(', ')
    #     if len(ent_ids) == 1:
    #         entityIds = [ent_ids[0]]
    #     if len(ent_ids) == 2:
    #         entityIds = [ent_ids[0],ent_ids[1]]
    #     print(i, entityIds)
    #     # lcquad_corechain_func(entityIds, oneTE_templates)
    #      #* generate TWO hops corechain Right and Left
    #     #----TE1 P1 OBJ1 P2 OBJ2
    #     if entityIds in get_lcquad_entities(oneTE_templates['oneTE_twoHops_RR']):
    #         rr_twoHops_oneEnt = corechains_twoHops_cache(entityIds, 'RR')
    #     #----special case: TE1 (P31) OBJ1 P2 OBJ2 --> {-P31 +Pxx}
    #     if entityIds in get_lcquad_entities(oneTE_templates['oneTE_twoHops_LR']):
    #         lr_twoHops_oneEnt = corechains_twoHops_cache(entityIds, 'LR')

    #     if entityIds in get_lcquad_entities(oneTE_templates['twoTE_twoHops_RR']):
    #         rr_twoTE_twoHops = corechains_twoHops_cache(entityIds, 'RR') 
    #     if entityIds in get_lcquad_entities(oneTE_templates['twoTE_twoHops_LR']):
    #         lr_twoTE_twoHops = corechains_twoHops_cache(entityIds, 'LR')

def lcquad_cache_creation():
    #F_most_used_entities = open("data/most_used/most_used_entities_lcquad.txt", "r")
    F_most_used_entities = open("data/most_used_entities_lcquad_test.txt", "r")
    out = F_most_used_entities.readlines()  # will append in the list out
    i = 0
    start_time = time.time()
    corechains = []
    time_loop_arr = []
    for line in out:
        start_time_loop = time.time()
        i += 1
        if i > 0: 
            entityIds_arr = []
            arguments = line.split("\t")
            # arguments[0]:Entity id   arguments[1]: Entity id2 OR occurrence of entity
            entityIds_arr.append("wd:" + arguments[0])
            
            if arguments[1].startswith('Q'):
                entityIds_arr.append("wd:" + arguments[1])
                
            write_to_file(F_cache_terminal, str(i) + '\t' + ', '.join(entityIds_arr).replace("wd:", ''))
            print(i, ', '.join(entityIds_arr).replace("wd:", ''))
            lcquad_corechain_func(entityIds_arr, oneTE_templates)
        
            elapsed_time_loop = time.time() - start_time_loop
            time_loop_arr.append(elapsed_time_loop)
            write_to_file(F_cache_time, ', '.join(entityIds_arr).replace("wd:", '') + "\t" + str(elapsed_time_loop))

    elapsed_time = time.time() - start_time
    print("Max time", max(time_loop_arr))
    print("Min time", min(time_loop_arr))
    print("Avg time", sum(time_loop_arr) / len(time_loop_arr))
    
    print("All Time", elapsed_time)

lcquad_cache_creation()
# try_example()
# lcquad_cache_error_correction()