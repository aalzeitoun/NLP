import time
import json
import re
import sys
from SPARQLWrapper import SPARQLWrapper, JSON
import itertools
from collections import OrderedDict
from sparqlQueries import dict_sparqlQueries, cache_lcquad_entities, mu_prop_lcquad,get_topicEntity_val, lcquad_templates, ask_triple, dict_lcquad_predicates, get_query_results, write_to_file

F_corechains = open("data/sq/sq_train_corechain.txt", "w")
F_error = open("data/sq/sq_train_corechain_error.txt", "w")
F_terminal = open("data/sq/sq_train_terminal.txt", "w")
F_time = open("data/sq/sq_train_time.txt", "w")
F_has_no_ans = open("data/sq/sq_train_has_no_ans.txt", "w")

F_error_lcquad_query = open("data/sq/sq_train_runningquery_error_not_imp.txt", "w")

lcquad2_temp = lcquad_templates()
lcquad_MUE = cache_lcquad_entities()

#--------- put cache corechain in a list (written to save time)
F_corechains_from_cache = open("data/sq/sq_cache/sq_cache_corechain.txt", "r")
cacheCorechain = F_corechains_from_cache.readlines()


#! -------- One Hope corechain Cashe
def corechains_oneHop(entityIds_arr, prop_dir, answerCC, qUID, tempID, lcquadQues, dataType, answerFound, specialUse=None):
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
    
    error_msg = str(qUID) + ' ' + ', '.join(entityIds_arr) + "\t" + prop_sign
    write_queryMsg = [F_error, error_msg]

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

                score = 0
                if(cc_id == answerCC):
                    score = 1
                    answerFound = True
                rawInfo = str(qUID) + "\t" + str(tempID) + "\t" + dataType + "\t" + ', '.join(entityIds_arr).replace('wd:', '') + "\t" + prop_sign
                cc_line = rawInfo + "\t" + str(score) + "\t" + lcquadQues + "\t" + cc_label + "\t" + cc_id

                #------ create corechains
                corechains_labels.append(cc_label)
                corechains_ids.append(cc_id)
                if not specialUse:
                    write_to_file(F_corechains, cc_line)
    # -------- if timeout
    if len(results) == 1:
        write_to_file(F_terminal,"fixig error using filters / One Hop " + prop_sign)
        print("fixig error using filters / One Hop " + prop_sign)
        corechains_labels, corechains_ids, answerFound = corechains_oneHop_fix(entityIds_arr, prop_dir, answerCC, qUID, tempID, lcquadQues, dataType, answerFound, specialUse)
    if not specialUse:
        write_to_file(F_terminal, prop_sign + ' ' + str(len(corechains_labels)))
        print(prop_sign, len(corechains_labels))
    corechains = list(zip(corechains_ids,corechains_labels))
    return corechains, answerFound
#! -------- One Hope corechain Cashe fixing using filters 
def corechains_oneHop_fix(entityIds_arr, prop_dir, answerCC, qUID, tempID, lcquadQues, dataType, answerFound, specialUse=None):
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
        
        error_msg = str(qUID) + " " + ', '.join(entityIds_arr) + "\t" + prop_sign + "\t" + "filter:" + str(j)
        write_queryMsg = [F_error, error_msg]
        i = 0
        results = get_query_results(query, write_queryMsg)
        if results:
            for result in results["results"]["bindings"]:
                i += 1
                prop_oneHop = result['property']['value'].replace(
                    'http://www.wikidata.org/entity/', '')

                cc_label = prop_sign + result['propertyLabel']['value']
                cc_id = prop_sign + prop_oneHop
                score = 0
                if(cc_id == answerCC):
                    score = 1
                    answerFound = True
                rawInfo = str(qUID) + "\t" + str(tempID) + "\t" + dataType + "\t" + ', '.join(entityIds_arr).replace('wd:', '') + "\t" + prop_sign
                cc_line = rawInfo + "\t" + str(score) + "\t" + lcquadQues + "\t" + cc_label + "\t" + cc_id

                #------ create corechains
                corechains_labels.append(cc_label)
                corechains_ids.append(cc_id)
                if not specialUse:
                    write_to_file(F_corechains, cc_line)
   
    if len(entityIds_arr) == 1:
        if(prop_dir == "left"):
            #*get corechains of excluded predicates
            for prop in lcquad_props["exclude_props"]:
                #prop[0]: predicate is , prop[1]: predicate label
                if(ask_triple(entityIds_arr[0].replace('wd:', ''), prop[0], prop_dir)):

                    cc_label = prop_sign + prop[1]
                    cc_id = prop_sign + prop[0]
                    
                    score = 0
                    if(cc_id == answerCC):
                        score = 1
                    rawInfo = str(qUID) + "\t" + str(tempID) + "\t" + dataType + "\t" + ', '.join(entityIds_arr).replace('wd:', '') + "\t" + prop_sign
                    cc_line = rawInfo + "\t" + str(score) + "\t" + lcquadQues + "\t" + cc_label + "\t" + cc_id

                    #------ create corechains
                    corechains_labels.append(cc_label)
                    corechains_ids.append(cc_id)
                    write_to_file(F_corechains, cc_line)

    return corechains_labels, corechains_ids, answerFound

#! -------- Quilifiers corechain Cashe
def quilifiers_corechains(entityIds_arr, prop_dir, answerCC, qUID, tempID, lcquadQues, dataType, answerFound, specialUse=None):
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

    error_msg = str(qUID) + ' ' +', '.join(entityIds_arr) + "\t" + ccSign
    write_queryMsg = [F_error, error_msg]
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
                cc_line = ""

                cc_label = prop_sign + result['propertyLabel']['value']
                cc_id = prop_sign + prop_oneHop
                cc_hyper_id = hyper_sign + result['hyperq']['value'].replace('http://www.wikidata.org/entity/', '')
                cc_hyper_label = hyper_sign + result['hyperqLabel']['value']
                
                score = 0
                retrievedCC = cc_id + ' ' + cc_hyper_id
                givenCC = answerCC.replace(',','')
                if( retrievedCC == givenCC):
                    score = 1
                    answerFound = True
                
                rawInfo = str(qUID) + "\t" + str(tempID) + "\t" + dataType + "\t" + ', '.join(entityIds_arr).replace('wd:', '') + "\t" + ccSign

                # TE1 P TE2 PQ Qual -> [+-P *PQ]
                if len(entityIds_arr) == 2 and specialUse == 'NoQualifier':  
                    cc_line = rawInfo + "\t" + str(score) + "\t" + lcquadQues + "\t" + cc_label + ' ' + cc_hyper_label + "\t" + cc_id + ' ' + cc_hyper_id
                    #------ create corechains
                    corechains_labels.append(cc_label + ' ' + cc_hyper_label)
                    corechains_ids.append(cc_id + ' ' + cc_hyper_id)

                # TE1 P OBJ PQ TE2 -> [+-P, *PQ]  {comma means the answer between}
                elif len(entityIds_arr) == 2 and specialUse == 'TE2Qualifier':
                    cc_line = rawInfo + "\t" + str(score) + "\t" + lcquadQues + "\t" + cc_label + ', ' + cc_hyper_label + "\t" + cc_id + ', ' + cc_hyper_id
                    #------ create corechains
                    corechains_labels.append(cc_label + ", " + cc_hyper_label)
                    corechains_ids.append(cc_id + ", " + cc_hyper_id)

                # TE2 P OBJ PQ TE1 ->  [*PQ +-P]  [not in the LCQuAd2 Tem]
                elif len(entityIds_arr) == 2 and specialUse == 'TE1Qualifier':
                    cc_line = rawInfo + "\t" + str(score) + "\t" + lcquadQues + "\t" + cc_hyper_label + ' ' + cc_label + "\t" + cc_hyper_id + ' ' + cc_id
                    #------ create corechains
                    corechains_labels.append(cc_hyper_label + ' ' + cc_label)
                    corechains_ids.append(cc_hyper_id + ' ' + cc_id)

                # val1 P val2 PQ1 TE1, PQ2 TE2 ->  [*PQ1 *PQ2 +-P]  [not in the LCQuAd2 Tem]
                elif len(entityIds_arr) == 2 and specialUse == 'TwoQualifier':
                    cc_hyper_id2 = hyper_sign + result['hyperq2']['value'].replace('http://www.wikidata.org/entity/', '')
                    cc_hyper_label2 = hyper_sign + result['hyperq2Label']['value']
                    cc_line = rawInfo + "\t" + str(score) + "\t" + lcquadQues + "\t" + cc_hyper_label + ' ' + cc_hyper_label2 + ' ' + cc_label + "\t" + cc_hyper_id + ' ' + cc_hyper_id2 + ' ' + cc_id
                    #------ create corechains
                    corechains_labels.append(cc_hyper_label + ' ' + cc_hyper_label2 + ' ' + cc_label)
                    corechains_ids.append(cc_hyper_id + ' ' + cc_hyper_id2 + ' ' + cc_id)
              
                # TE P OBJ PQ Qual -> [+-P *PQ]:
                elif len(entityIds_arr) == 1:  
                    cc_line = rawInfo + "\t" + str(score) + "\t" + lcquadQues + "\t" + cc_label + ' ' + cc_hyper_label + "\t" + cc_id + ' ' + cc_hyper_id
                    #------ create corechains
                    corechains_labels.append(cc_label + ' ' + cc_hyper_label)
                    corechains_ids.append(cc_id + ' ' + cc_hyper_id)

                write_to_file(F_corechains, cc_line)
    # -------- if timeout
    if len(results) == 1:
        write_to_file(F_terminal, "fixig error using filters - hyperFunc: " + ccSign)
        print("fixig error using filters - hyperFunc: " + ccSign)
        corechains_labels, corechains_ids, answerFound = quilifiers_corechains_fix(entityIds_arr, prop_dir, answerCC, qUID, tempID, lcquadQues, dataType, answerFound, specialUse)

    write_to_file(F_terminal, ccSign + ' ' + str(len(corechains_labels)))
    print(ccSign , len(corechains_labels))
    corechains = list(zip(corechains_ids,corechains_labels))
    return corechains, answerFound

#! -------- Quilifiers corechain Cashe fixing using filters
def quilifiers_corechains_fix(entityIds_arr, prop_dir, answerCC, qUID, tempID, lcquadQues, dataType, answerFound, specialUse=None):
    prop_sign = "+"
    if(prop_dir == "left"):
        prop_sign = "-"
    corechains_labels = []
    corechains_ids = []
    hyper_sign = "*"
    ccSign = prop_sign + hyper_sign
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
            ccSign = prop_sign + ',' + hyper_sign

        # TE2 P OBJ PQ TE1 ->  [*PQ +-P]  [not in the LCQuAd2 Tem]
        if len(entityIds_arr) == 2 and specialUse == 'TE1Qualifier':
            query = dict_sparqlQueries["query_only_hyperRel_" + prop_dir] % {
                'target_resource': entityIds_arr[1], 'target_prop': "?p", "target_resource2": '?obj', "target_qualifier": entityIds_arr[0], 'filter_in': filter}
            error_msg = ', '.join(entityIds_arr) + "\t" + \
                hyper_sign + prop_sign + "\t" + "filter:" + str(j)
            ccSign = hyper_sign + prop_sign

        # val1 P val2 PQ1 TE1, PQ2 TE2 ->  [*PQ1 *PQ2 +-P]  [not in the LCQuAd2 Tem]
        if len(entityIds_arr) == 2 and specialUse == 'TwoQualifier':
            query = dict_sparqlQueries["query_twoTE_as_qualifiers"] % {
                'target_resource': entityIds_arr[0], "target_resource2": entityIds_arr[1], 'filter_in': filter}
            error_msg = ', '.join(entityIds_arr) + "\t" + \
                hyper_sign + hyper_sign + prop_sign + "\t" + "filter:" + str(j)
            ccSign = hyper_sign + hyper_sign + prop_sign

        write_queryMsg = [F_error, error_msg]
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
                
                score = 0
                retrievedCC = cc_id + ' ' + cc_hyper_id
                givenCC = answerCC.replace(',','')
                if( retrievedCC == givenCC):
                    score = 1
                    answerFound = True

                rawInfo = str(qUID) + "\t" + str(tempID) + "\t" + dataType + "\t" + ', '.join(entityIds_arr).replace('wd:', '') + "\t" + ccSign

                # TE1 P OBJ PQ TE2 -> [+-P, *PQ]  {comma means the answer between}
                if len(entityIds_arr) == 2 and specialUse == 'TE2Qualifier':
                    cc_line = rawInfo + "\t" + str(score) + "\t" + lcquadQues + "\t" + cc_label + ', ' + cc_hyper_label + "\t" + cc_id + ', ' + cc_hyper_id
                    #------ create corechains
                    corechains_labels.append(cc_label + ", " + cc_hyper_label)
                    corechains_ids.append(cc_id + ", " + cc_hyper_id)

                # TE2 P OBJ PQ TE1 ->  [*PQ +-P]  [not in the LCQuAd2 Tem]
                elif len(entityIds_arr) == 2 and specialUse == 'TE1Qualifier':
                    cc_line = rawInfo + "\t" + str(score) + "\t" + lcquadQues + "\t" + cc_hyper_label + ' ' + cc_label + "\t" + cc_hyper_id + ' ' + cc_id
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
                    cc_line = rawInfo + "\t" + str(score) + "\t" + lcquadQues + "\t" + cc_hyper_label + ' ' + cc_hyper_label2 + ' ' + cc_label + "\t" + cc_hyper_id + ' ' + cc_hyper_id2 + ' ' + cc_id
                    #------ create corechains
                    corechains_labels.append(
                        cc_hyper_label + ' ' + cc_hyper_label2 + ' ' + cc_label)
                    corechains_ids.append(
                        cc_hyper_id + ' ' + cc_hyper_id2 + ' ' + cc_id)

                # TE P OBJ PQ Qual -> [+-P *PQ]:
                elif len(entityIds_arr) == 1:
                    cc_line = rawInfo + "\t" + str(score) + "\t" + lcquadQues + "\t" + cc_label + ' ' + cc_hyper_label + "\t" + cc_id + ' ' + cc_hyper_id
                    #------ create corechains
                    corechains_labels.append(cc_label + ' ' + cc_hyper_label)
                    corechains_ids.append(cc_id + ' ' + cc_hyper_id)

                write_to_file(F_corechains, cc_line)

    return corechains_labels, corechains_ids, answerFound

#! -------- Two Hops corechain Cashe
def corechains_twoHops(entityIds_arr, prop_dir, answerCC, qUID, tempID, lcquadQues, dataType, answerFound):
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
    write_queryMsg = [F_error, error_msg]    
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

                #------ create corechains
                corechains_labels.append(cc_label)
                corechains_ids.append(cc_id)

    corechains = list(zip(corechains_ids, corechains_labels))
    #remove duplicates
    corechains = list(OrderedDict.fromkeys(corechains))
    rawInfo = str(qUID) + "\t" + str(tempID) + "\t" + dataType + "\t" + ', '.join(entityIds_arr).replace('wd:', '') + "\t" + ccSign
    for cc in corechains:
        score = 0
        if(cc[0] == answerCC):
            score = 1
            answerFound = True
        cc_line =  rawInfo + "\t" + str(score) + "\t" + lcquadQues + "\t" + cc[1] + "\t" + cc[0]
        #------ create corechains
        write_to_file(F_corechains, cc_line)

    # -------- if timeout
    if len(results) == 1:
        write_to_file(F_terminal, "fixing error by changing in the query " + ccSign)       
        print("fixig error by changing the query" + ccSign)       
        corechains, answerFound = corechains_twoHops_fix(entityIds_arr, prop_dir, answerCC, qUID, tempID, lcquadQues, dataType, answerFound)
    
    write_to_file(F_terminal, ccSign + ' ' + str(len(corechains))) 
    print(ccSign, len(corechains)) 
     
    return corechains, answerFound

#! -------- Two Hops corechain Cashe fixing by changing in the query
def corechains_twoHops_fix(entityIds_arr, prop_dir, answerCC, qUID, tempID, lcquadQues, dataType, answerFound):
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
    write_queryMsg = [F_error, error_msg]
    
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
        #remove duplicates
        corechains = list(OrderedDict.fromkeys(corechains))
        rawInfo = str(qUID) + "\t" + str(tempID) + "\t" + dataType + "\t" + ', '.join(entityIds_arr).replace('wd:', '') + "\t" + ccSign
        for cc in corechains:
            score = 0
            if(cc[0] == answerCC):
                score = 1
                answerFound = True
            cc_line =  rawInfo + "\t" + str(score) + "\t" + lcquadQues + "\t" + cc[1] + "\t" + cc[0]
            #------ create corechains
            write_to_file(F_corechains, cc_line)
    # if len(results) == 1:
    #     specialUse_txt = 'withoutProd'
    #     terminal_txt = 'fixing the error by using prod func'
        
    #     write_to_file(F_terminal, terminal_txt + ccSign)    
    #     print(terminal_txt + ccSign)
    #     corechains = generate_prod_twoTE_corechain(entityIds_arr, prop_dir, specialUse_txt)
    
    return corechains, answerFound

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
    write_queryMsg = [F_error, error_msg]
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

#! generate corecahins for Tem 17 and 24 -->{P1, P2}
def corechain_product_list_itself(entityIds_arr, oneHop_arr, answerCC, qUID, tempID, lcquadQues, dataType, answerFound):
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
        score = 0
        answerCC_arr = answerCC.split(', ')
        answerCC2 = answerCC_arr[1] + ', ' + answerCC_arr[0]
        if(prod_lists_ids[i] == answerCC or prod_lists_ids[i] == answerCC2):
            score = 1
            answerFound = True

        rawInfo = str(qUID) + "\t" + str(tempID) + "\t" + dataType + "\t" + ', '.join(entityIds_arr).replace('wd:', '') + "\t" + ccSign
        line = rawInfo + "\t" + str(score) + "\t" + lcquadQues + "\t" + cc_lbl + "\t" + prod_lists_ids[i]
        write_to_file(F_corechains, line)
        i += 1
    
    corechains = list(zip(prod_lists_ids, prod_lists_labels))
    write_to_file(F_terminal, ccSign + ' ' + str(len(corechains)))
    print(ccSign, len(corechains))
    return corechains, answerFound

#! generate corecahins for two hops  with three predicates
# #----TE1 P1 OBJ1 P2 OBJ2, OBJ1 P3 TE2 ---> P1 P2, P3
def generate_twoHops_corechains_product(entityIds_arr, prop_dir, prod_dir, answerCC, qUID, tempID, lcquadQues, dataType, answerFound):
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
    write_queryMsg = [F_error, error_msg]
    
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
            score = 0
            answerCC_arr = answerCC.split(' ')
            answerCC2 = answerCC_arr[0] + ' ' + answerCC_arr[2] + ', ' + answerCC_arr[1].replace(',','')
            if(cc[0] == answerCC or cc[0] == answerCC2):
                score = 1
                answerFound = True

            rawInfo = str(qUID) + "\t" + str(tempID) + "\t" + dataType + "\t" + ', '.join(entityIds_arr).replace('wd:', '') + "\t" + ccSign
            cc_line = rawInfo + "\t" + str(score) + "\t" + lcquadQues + "\t" +  cc[1] + "\t" + cc[0]
            #------ create corechains
            write_to_file(F_corechains, cc_line)
    # if len(results) == 1:
    #     terminal_txt = 'fixing the error by using prod func'
    #     write_to_file(F_terminal, terminal_txt + ccSign)    
    #     print(terminal_txt + ccSign)
    #     corechains = generate_prod_twoTE_corechain(entityIds_arr, prop_dir)

    write_to_file(F_terminal, ccSign + ' ' + str(len(corechains))) 
    print(ccSign, len(corechains))
    return corechains, answerFound

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
        r_oneHop_cc = corechains_oneHop([entityIds_arr[0], ent], dirOneHop, 'specialUse') #specialUse means to not write to the txt file
        if len(entityIds_arr) == 2:
            r_twoHops_cc = corechains_oneHop([ent, entityIds_arr[1]], dirTwoHop, 'specialUse')#specialUse means to not write to the txt file
        
        #generate oneHop for ent
        r_ent = []
        if not specialUse:
            r_ent = corechains_oneHop([ent], 'right', 'specialUse')#specialUse means to not write to the txt file      
        
        if specialUse == 'withoutProd' and len(entityIds_arr) == 1:
            r_ent = corechains_oneHop([ent], dirTwoHop, 'specialUse')#specialUse means to not write to the txt file      
        

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
        write_to_file(F_corechains, cc_line)
        i += 1
    # new_ccSign = prop_sign1 + '(+/-)' + ',' + prop_sign2

    corechains = list(zip(all_twoTE_twoHops_ids, all_twoTE_twoHops_labels))
    if not specialUse:
        write_to_file(F_terminal, ccSign + ' ' + str(len(corechains)))
        print(ccSign, len(corechains))
        if(len(corechains) == 0):
            write_to_file(F_has_no_ans, ', '.join(entityIds_arr).replace('wd:', '') + "\t" + ccSign )

    return corechains

#! genrate {ProdNoQualifier} corechains: TE1 P TE2 PQ1 Qual1, PQ2 Qul2 -> [+-P *PQ1, *PQ2] 
def generate_prod_quilifiers_corechain(entityIds_arr, qualifier_arr, prop_dir, answerCC, qUID, tempID, lcquadQues, dataType, answerFound):
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

            #! CC creation No 1
            score = 0
            if answerCC == ccId:
                score = 1
                answerFound = True
            rawInfo = str(qUID) + "\t" + str(tempID) + "\t" + dataType + "\t" + ', '.join(entityIds_arr).replace('wd:', '') + "\t" + ccSign
            cc_line = rawInfo + "\t" + str(score) + "\t" + lcquadQues + "\t" + ccLabel + "\t" + ccId
            write_to_file(F_corechains, cc_line) 
            prod_qualifier_ids.append(ccId)
            prod_qualifier_labels.append(ccLabel)

            for j in range(i+1, len(hyperRel_arr)):
                ccId = oneHopId + " " + cc[0] + ", " + hyperRel_arr[j][0]
                ccLabel = oneHopLabel + " " + cc[1] + ", " + hyperRel_arr[j][1]

                #! CC creation No 2
                score = 0
                ccId_form2 = oneHopId + " " + hyperRel_arr[j][0] + ", " + cc[0]
                if answerCC == ccId or answerCC == ccId_form2:
                    score = 1
                    answerFound = True
                rawInfo = str(qUID) + "\t" + str(tempID) + "\t" + dataType + "\t" + ', '.join(entityIds_arr).replace('wd:', '') + "\t" + ccSign
                cc_line = rawInfo + "\t" + str(score) + "\t" + lcquadQues + "\t" + ccLabel + "\t" + ccId
                write_to_file(F_corechains, cc_line) 
                prod_qualifier_ids.append(ccId)
                prod_qualifier_labels.append(ccLabel)   
            i += 1

    corechains = list(zip(prod_qualifier_ids, prod_qualifier_labels))
    write_to_file(F_terminal, ccSign + ' ' + str(len(corechains)))
    print(ccSign, len(corechains))
    return corechains, answerFound

#! ==== ===== ===== ====== Check result and report if there is issue and fix it ====== ======= ======
def fix_and_report_answerNotFound(entityIds_arr, answerCC, qUID, tempID, lcquadQues, dataType, lcquadQues_Query):
    systemIssue = False
    msg = "Answer is not found!! Checking original LCQuAD2 query..."
    print(msg)
    write_to_file(F_terminal, msg)
    query = lcquadQues_Query
    error_msg = str(qUID) + ' ' + ', '.join(entityIds_arr) + "\t" + lcquadQues
    write_queryMsg = [F_error_lcquad_query, error_msg]
    results = get_query_results(query, write_queryMsg)
    
    if len(results) == 1 : systemIssue = False
    elif len(results["results"]["bindings"]) == 0: systemIssue = False 
    else: systemIssue = True

    if not systemIssue:
        msg = " -> It is LCQuAD2 Query issue :("
        print(msg)
        write_to_file(F_terminal, msg)
        line = str(qUID) + "\t" + str(tempID) + "\t" + 'LCQuAD2 Query issue' + "\t" + lcquadQues + "\t" + lcquadQues_Query
        write_to_file(F_has_no_ans, line)
    else:
        msgtxt = 'System issue: '
        prop_is_valid = True # check if the predicates in the answer are exist in Most Used predicates
        cc_ids = []
        cc_lbls = []
        ccSign = ''
        prop_notIn_MUP = ''
        
        answerCC_arr = answerCC.split(' ')
        for prop in answerCC_arr:
            comma = ','
            sign = prop[0]
            prop_noSign = prop.replace(sign, '')
            if comma in prop_noSign:
                prop_noSign = prop_noSign.replace(comma, '')
            else:
                comma = ''
            ccSign += sign + comma
            proplbl = ''
            if not mu_prop_lcquad(prop_noSign, 'id'):
                prop_notIn_MUP = prop_noSign
                prop_is_valid = False
                propInfo = get_topicEntity_val(prop_noSign) #it has [id, label]
                proplbl = propInfo[1]
            else:
                proplbl = mu_prop_lcquad(prop_noSign, 'id')

            proplbl = sign + proplbl + comma
            cc_ids.append(prop)
            cc_lbls.append(proplbl)

        if not prop_is_valid: #if not in MU prop
            msgtxt += prop_notIn_MUP + ' is not in MUP '  
        else:
            msgtxt += 'over limit ' 

        score = 1
        rawInfo = str(qUID) + "\t" + str(tempID) + "\t" + dataType + "\t" + ', '.join(entityIds_arr).replace('wd:', '') + "\t" + ccSign
        cc_line = rawInfo + "\t" + str(score) + "\t" + lcquadQues + "\t" + ' '.join(cc_lbls) + "\t" + ' '.join(cc_ids)
        write_to_file(F_corechains, cc_line) 

        msgtxt += '(solved)'
        print(msgtxt)
        write_to_file(F_terminal, msgtxt)
        line = str(qUID) + "\t" + str(tempID) + "\t" + msgtxt + "\t" + lcquadQues + "\t" + lcquadQues_Query
        write_to_file(F_has_no_ans, line)

#! all functions needed to scan all paths in order to create corchain
def lcquad_corechain_func(entityIds_arr, answerCC, qUID, tempID, lcquadQues, dataType, lcquadQues_Query):
    answerFound = False
    if len(entityIds_arr) == 1 :   #if len(entityIds_arr) == 1: # if entityIds_arr[0] == 'wd:Q12204':#
        #* generate ONE hop corechain Right and Left
        #----TE P1 OBJ
        r_oneHop, r_oneHop_AnsF = corechains_oneHop(entityIds_arr, "right", answerCC, qUID, tempID, lcquadQues, dataType, answerFound)
        l_oneHop, l_oneHop_AnsF = corechains_oneHop(entityIds_arr, "left", answerCC, qUID, tempID, lcquadQues, dataType, answerFound)
        if(r_oneHop_AnsF or l_oneHop_AnsF):
            answerFound = True

        #* generate hyperRel corechain Right and Left
        #----TE1 P1 OBJ1 PQ OBJ2
        if tempID in lcquad2_temp['hyperRel']:
            r_hyper, r_hyper_AnsF = quilifiers_corechains(entityIds_arr, "right", answerCC, qUID, tempID, lcquadQues, dataType, answerFound)
            l_hyper, l_hyper_AnsF = quilifiers_corechains(entityIds_arr, "left", answerCC, qUID, tempID, lcquadQues, dataType, answerFound)
            if(r_hyper_AnsF or l_hyper_AnsF):
                answerFound = True

        #* generate TWO hops corechain Right and Left
        #----TE1 P1 OBJ1 P2 OBJ2
        if tempID in lcquad2_temp['oneTE_twoHops_RR']:
            rr_twoHops_oneEnt, rr_twoHops_oneEnt_AnsF = corechains_twoHops(entityIds_arr, 'RR', answerCC, qUID, tempID, lcquadQues, dataType, answerFound)
            if(rr_twoHops_oneEnt_AnsF):
                answerFound = True
        
        #----special case: TE1 (P31) OBJ1 P2 OBJ2 --> {-P31 +Pxx}
        if tempID in lcquad2_temp['oneTE_twoHops_LR']:
            lr_twoHops_oneEnt, lr_twoHops_oneEnt_AnsF = corechains_twoHops(entityIds_arr, 'LR', answerCC, qUID, tempID, lcquadQues, dataType, answerFound)
            # ll_twoHops_oneEnt = generate_twoHops_corechains(entityIds_arr, l_oneHop, 'LL')
            if(lr_twoHops_oneEnt_AnsF):
                answerFound = True

        #* generate product of ONE hop corechain for template 24 ONLY
        #----TE1 P1 OBJ1, TE1 P2 OBJ2
        if tempID in lcquad2_temp['product_oneHop']:
            r_prod_oneHop, r_prod_oneHop_AnsF = corechain_product_list_itself(entityIds_arr, r_oneHop, answerCC, qUID, tempID, lcquadQues, dataType, answerFound)
            if(r_prod_oneHop_AnsF):
                answerFound = True

    if len(entityIds_arr) == 2:
        #----TE P1 OBJ
        #r_twoTE_oneHop_labels, r_twoTE_oneHop_ids
        r_twoTE_oneHop, r_twoTE_oneHop_AnsF = corechains_oneHop(entityIds_arr, "right", answerCC, qUID, tempID, lcquadQues, dataType, answerFound)
        l_twoTE_oneHop, l_twoTE_oneHop_AnsF = corechains_oneHop(entityIds_arr, "left", answerCC, qUID, tempID, lcquadQues, dataType, answerFound)
        if(r_twoTE_oneHop_AnsF or l_twoTE_oneHop_AnsF):
            answerFound = True

        #--- QUILIFIERS
        #{NoQualifier} TE1 P TE2 PQ Qual -> [+-P *PQ]
        #r_twoTE_NoQualifier_labels, r_twoTE_NoQualifier_ids 
        r_twoTE_NoQualifier, r_twoTE_NoQualifier_AnsF = quilifiers_corechains(entityIds_arr, 'right', answerCC, qUID, tempID, lcquadQues, dataType, answerFound, specialUse = 'NoQualifier')
        l_twoTE_NoQualifier, l_twoTE_NoQualifier_AnsF = quilifiers_corechains(entityIds_arr, 'left', answerCC, qUID, tempID, lcquadQues, dataType, answerFound, specialUse = 'NoQualifier')

        #{TE2Qualifier} TE1 P OBJ PQ TE2 -> [+-P, *PQ] {comma means the answer between} 
        r_twoTE_TE2Qualifier, r_twoTE_TE2Qualifier_AnsF = quilifiers_corechains(entityIds_arr, 'right', answerCC, qUID, tempID, lcquadQues, dataType, answerFound, specialUse = 'TE2Qualifier')
        l_twoTE_TE2Qualifier, l_twoTE_TE2Qualifier_AnsF = quilifiers_corechains(entityIds_arr, 'left', answerCC, qUID, tempID, lcquadQues, dataType, answerFound, specialUse = 'TE2Qualifier')

        #{ProdNoQualifier}  TE1 P TE2 PQ1 Qual1, PQ2 Qul2 -> [+-P *PQ1, *PQ2] {this is product of the first one above {NoQualifier} }
        r_twoTE_prod_NoQualifier, r_twoTE_prod_NoQualifier_AnsF = generate_prod_quilifiers_corechain(entityIds_arr, r_twoTE_NoQualifier, 'right', answerCC, qUID, tempID, lcquadQues, dataType, answerFound)
        l_twoTE_prod_NoQualifier, l_twoTE_prod_NoQualifier_AnsF = generate_prod_quilifiers_corechain(entityIds_arr, l_twoTE_NoQualifier, 'left', answerCC, qUID, tempID, lcquadQues, dataType, answerFound)

        if(r_twoTE_NoQualifier_AnsF or l_twoTE_NoQualifier_AnsF or r_twoTE_TE2Qualifier_AnsF or l_twoTE_TE2Qualifier_AnsF or r_twoTE_prod_NoQualifier_AnsF or l_twoTE_prod_NoQualifier_AnsF):
            answerFound = True

        #======== TE1 P1 OBJ P2 TE2 ========

        #----special case: TE1 P1 OBJ1 P2 TE2 --> {+P1 +P2}
        if tempID in lcquad2_temp['twoTE_twoHops_RR']:
            rr_twoTE_twoHops, rr_twoTE_twoHops_AnsF = corechains_twoHops(entityIds_arr, 'RR', answerCC, qUID, tempID, lcquadQues, dataType, answerFound)
            if(rr_twoTE_twoHops_AnsF):
                answerFound = True  

        #----special case: TE1 (P31) OBJ1 P2 TE2 --> {-P1 +P2}
        if tempID in lcquad2_temp['twoTE_twoHops_LR']:
            lr_twoTE_twoHops, lr_twoTE_twoHops_AnsF = corechains_twoHops(entityIds_arr, 'LR', answerCC, qUID, tempID, lcquadQues, dataType, answerFound)
            if(lr_twoTE_twoHops_AnsF):
                answerFound = True    

        # #* generate product of two hops corechain for template 13,14 ONLY
        # #----TE1 P1 OBJ1 P2 OBJ2, OBJ1 P3 TE2
        if tempID in lcquad2_temp['product_twoTE_twoHops']:
            lr_prod_twoTE_twoHops, lr_prod_twoTE_twoHops_AnsF = generate_twoHops_corechains_product(entityIds_arr, 'LR', 'right', answerCC, qUID, tempID, lcquadQues, dataType, answerFound)
            if(lr_prod_twoTE_twoHops_AnsF):
                answerFound = True  

    if len(entityIds_arr) == 3:
         #* generate ONE hop corechain Right and Left
        #----TE P1 OBJ
        new_entityIds_arr =[]
        new_entityIds_arr.append(entityIds_arr[0])
        r_oneHop_extra, noNeed = corechains_oneHop(new_entityIds_arr, "right", answerCC, qUID, tempID, lcquadQues, dataType, answerFound, 'specialUse')
        #* generate product of ONE hop corechain for template 24 ONLY
        #----TE1 P1 TE2, TE1 P2 TE3   -> +-P1, +-P2
        if tempID in lcquad2_temp['product_threeTE_oneHop']:
            r_prod_oneHop, r_prod_oneHop_AnsF = corechain_product_list_itself(entityIds_arr, r_oneHop_extra, answerCC, qUID, tempID, lcquadQues, dataType, answerFound)
            if(r_prod_oneHop_AnsF):
                answerFound = True 
    
    #* ==== ===== ===== ====== Check result and report if there is issue ====== ======= ======
    sel_queryMsg = " "
    if 'ASK' in lcquadQues_Query or 'ask' in lcquadQues_Query:
        answerFound = True
        sel_queryMsg = "-it's ASK query"

    if 'COUNT' in lcquadQues_Query or 'count' in lcquadQues_Query:
        answerFound = True
        sel_queryMsg = "-it's COUNT query"
    
    if answerFound:
        msg = 'The answer is FOUND :)' + sel_queryMsg
        print(msg)
        write_to_file(F_terminal, msg)
    else:
        fix_and_report_answerNotFound(entityIds_arr, answerCC, qUID, tempID, lcquadQues, dataType, lcquadQues_Query)


#! =========== retrieve corechain from cache
def corechain_from_cache(entityIds_arr, answerCC, qUID, tempID, lcquadQues, dataType, lcquadQues_Query):
    answerFound = False
    counter_r = 0 # +
    counter_l = 0 # -
    counter_rh = 0 # +*
    counter_lh = 0 # -*
    counter_rh_comma = 0 # +,* 
    counter_lh_comma = 0 # -,* 
    counter_rhh_comma = 0 # +*,*
    counter_lhh_comma = 0 # -*,*
    counter_rr = 0 # ++
    counter_lr = 0 # -+
    counter_lrr = 0 # -+,+ 
    counter_rr_comma = 0 # +,+
    
    corechains_labels = []  
    corechains_ids = []
    i = 0
    for ccLine in cacheCorechain:
        i += 1
        arguments = ccLine.split("\t")
        ccCache_entIds = arguments[0]
        ccCache_sign = arguments[1]
        ccCache_lbl = arguments[2]
        ccCache_id = arguments[3].replace("\n","")

        entityIds = ', '.join(entityIds_arr).replace("wd:", '')
        
        if entityIds == ccCache_entIds:
            score = 0
            if answerCC == ccCache_id :
                score = 1
                answerFound = True
            rawInfo = str(qUID) + "\t" + str(tempID) + "\t" + dataType + "\t" + ', '.join(entityIds_arr).replace('wd:', '') + "\t" + ccCache_sign
            cc_line = rawInfo + "\t" + str(score) + "\t" + lcquadQues + "\t" + ccCache_lbl + "\t" + ccCache_id
            write_to_file(F_corechains, cc_line)

            corechains_labels.append(ccCache_lbl)
            corechains_ids.append(ccCache_id)
            #===== count cc types
            if ccCache_sign == '+':
                counter_r += 1
            if ccCache_sign == '-':
                counter_l += 1
            if ccCache_sign == '+*':
                counter_rh += 1
            if ccCache_sign == '-*':
                counter_lh += 1
            if ccCache_sign == '+,*':
                counter_rh_comma += 1
            if ccCache_sign == '-,*':
                counter_lh_comma += 1
            if ccCache_sign == '+,**':
                counter_rhh_comma += 1 
            if ccCache_sign == '-,**':
                counter_lhh_comma += 1 
            if ccCache_sign == '++':
                counter_rr += 1 
            if ccCache_sign == '-+':
                counter_lr += 1
            if ccCache_sign == '-+,+':
                counter_lrr += 1
            if ccCache_sign == '+,+':
                counter_rr_comma += 1
    
    write_to_file(F_terminal, '+' + ' ' + str(counter_r))
    write_to_file(F_terminal, '-' + ' ' + str(counter_l))
    write_to_file(F_terminal, '+*' + ' ' + str(counter_rh))
    write_to_file(F_terminal, '-*' + ' ' + str(counter_lh))
    write_to_file(F_terminal, '+,*' + ' ' + str(counter_rh_comma))
    write_to_file(F_terminal, '-,*' + ' ' + str(counter_lh_comma))
    write_to_file(F_terminal, '+*,*' + ' ' + str(counter_rhh_comma))
    write_to_file(F_terminal, '-*,*' + ' ' + str(counter_lhh_comma))
    write_to_file(F_terminal, '++' + ' ' + str(counter_rr))
    write_to_file(F_terminal, '-+' + ' ' + str(counter_lr))
    write_to_file(F_terminal, '-+,+' + ' ' + str(counter_lrr))
    write_to_file(F_terminal, '+,+' + ' ' + str(counter_rr_comma))
    #print ==
    print('+', str(counter_r))
    print('-', str(counter_l))
    print('+*', str(counter_rh))
    print('-*', str(counter_lh))
    print('+,*', str(counter_rh_comma))
    print('-,*', str(counter_lh_comma))
    print('+*,*', str(counter_rhh_comma))
    print('-*,*', str(counter_lhh_comma))
    print('++', str(counter_rr))
    print('-+', str(counter_lr))
    print('-+,+', str(counter_lrr))
    print('+,+', str(counter_rr_comma))

    corechains = list(zip(corechains_ids,corechains_labels))

    #* ==== ===== ===== ====== Check result and report if there is issue ====== ======= ======
    sel_queryMsg = " "
    if 'ASK' in lcquadQues_Query or 'ask' in lcquadQues_Query:
        answerFound = True
        sel_queryMsg = "-it's ASK query"

    if 'COUNT' in lcquadQues_Query or 'count' in lcquadQues_Query:
        answerFound = True
        sel_queryMsg = "-it's COUNT query"
    
    if answerFound:
        msg = 'The answer is FOUND :)' + sel_queryMsg
        print(msg)
        write_to_file(F_terminal, msg)
    else:
        fix_and_report_answerNotFound(entityIds_arr, answerCC, qUID, tempID, lcquadQues, dataType, lcquadQues_Query)


def lcquad_train_corechain():
    F_most_used_entities = open("data/lcquad2_dataset/lcquad_train_answer.txt", "r")
    out = F_most_used_entities.readlines()  # will append in the list out
    i = 0
    start_time = time.time()
    corechains = []
    time_loop_arr = []
    for line in out:
        start_time_loop = time.time()
        i += 1
        arguments = line.split("\t")
        # arguments[0]:uid   arguments[1]:TempId    arguments[2]: TEs 
        # arguments[3]:answerCC   arguments[4]: Question
        qUID = int(arguments[0])
        tempID = int(arguments[1])
        answerCC = arguments[3]
        lcquadQues = arguments[4].replace('\n', '')
        lcquadQues_Query = arguments[5].replace('\n', '')
        entityIds_arr = []
        entityIds = arguments[2].split(', ')
        for entID in entityIds:
            entityIds_arr.append('wd:' + entID)

        found_in_cache = False
        if arguments[2] in lcquad_MUE:
            found_in_cache = True
   
        dataType = 'train'
        if not found_in_cache:
            if i < 1 : #  if entityIds_arr == ['wd:Q183257', 'wd:Q2275640']: # 28113
                write_to_file(F_terminal, str(i)+':' + '\t' + str(qUID) + "\t" + str(tempID) + "\t" + ', '.join(entityIds_arr).replace("wd:", '') + "\t" + lcquadQues)
                print(str(i)+':', qUID, tempID, ', '.join(entityIds_arr).replace("wd:", ''), lcquadQues)
                lcquad_corechain_func(entityIds_arr, answerCC, qUID, tempID, lcquadQues, dataType, lcquadQues_Query)
        else:
            if i > 0 :
                print(str(i)+':', qUID, tempID, ', '.join(entityIds_arr).replace("wd:", ''), lcquadQues, answerCC)
                write_to_file(F_terminal, str(i)+':' + '\t' + str(qUID) + "\t" + str(tempID) + "\t" + ', '.join(entityIds_arr).replace("wd:", '') + "\t" + lcquadQues + "\t" + answerCC)
                corechain_from_cache(entityIds_arr, answerCC, qUID, tempID, lcquadQues, dataType, lcquadQues_Query)

            elapsed_time_loop = time.time() - start_time_loop
            time_loop_arr.append(elapsed_time_loop)
            write_to_file(F_time, str(qUID) + "\t" + str(qUID) + "\t" + lcquadQues + "\t" + ', '.join(entityIds_arr).replace("wd:", '') + "\t" + str(elapsed_time_loop))

    elapsed_time = time.time() - start_time
    print("Max time", max(time_loop_arr))
    print("Min time", min(time_loop_arr))
    print("Avg time", sum(time_loop_arr) / len(time_loop_arr))
    
    print("All Time", elapsed_time)

lcquad_train_corechain()


#* Result for all train (cache is excluded)
# Max time 0.000213623046875
# Min time 2.86102294921875e-06
# Avg time 4.56864054675199e-05
# All Time 55559.70000219345



#
#


#scp /Users/University/Development/DevThesis/ThesisProject/data/lcquad_train/lcquad_train_corechain_all.txt ssh ahmad.alzeitoun@sda-srv04.iai.uni-bonn.de:/data/ahmad.alzeitoun/dev/ThesisProject/data/lcquad_train
#scp ssh ahmad.alzeitoun@sda-srv04.iai.uni-bonn.de:/data/ahmad.alzeitoun/dev/ThesisProject/sentence-transformers/corechains_dataset/lcquad_train_corechain_iPad_new.txt /Users/University/Desktop

#scp /Users/University/Desktop/lcquad_train_corechain_iPad_new.txt ssh ahmad.alzeitoun@sda-srv05.iai.uni-bonn.de:/data/home/sda-srv05/ahmad.alzeitoun/dev/ThesisProject/sentence-transformers/corechains_dataset/