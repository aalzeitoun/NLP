import time
from sparqlQueries import dict_sparqlQueries, ask_triple, dict_lcquad_predicates, get_query_results, write_to_file

# F_corechains_cache = '' #open("data/corechains_cache.txt", "w")
# F_cache_error = ''  # open("data/cache_error.txt", "w")

F_corechains_cache = open("data/question_cc_test.txt", "w")
F_cache_error = open("data/question_cc_test_error.txt", "w")

#! -------- One Hope corechain Cashe
def corechains_cache(entity_id, prop_dir):
    prop_sign = "+"
    if(prop_dir == "left"):
        prop_sign = "-"

    corechains_lines = []
    lcquad_props = dict_lcquad_predicates(prop_dir)
    error_msg = entity_id + "\t" + prop_sign
    write_queryMsg = [F_cache_error, error_msg]
    query = dict_sparqlQueries["query_" + prop_dir + "_oneHope"] % {
        'target_resource': entity_id, 'filter_in': ''}
    i=0
    results = get_query_results(query, write_queryMsg)
    if len(results) > 1:
        for result in results["results"]["bindings"]:
            propId = result['property']['value'].replace(
                'http://www.wikidata.org/entity/', '')
            if(propId and propId in lcquad_props["lcquad_props"]):
                i += 1
                cc_line = entity_id.replace('wd:', '') + "\t" + prop_sign + "\t" + prop_sign + \
                    result['propertyLabel']['value'] + "\t" + prop_sign + result['property']['value'].replace(
                    'http://www.wikidata.org/entity/', '')
                corechains_lines.append(cc_line)
                write_to_file(F_corechains_cache, cc_line)
    # -------- if timeout
    if len(results) == 1:
        print("fixig error using filters")
        corechains_cache_fix(entity_id, prop_dir)

    print(prop_sign, len(corechains_lines))

#! -------- One Hope corechain Cashe fixing using filters 
def corechains_cache_fix(entity_id, prop_dir):
    prop_sign = "+"
    if(prop_dir == "left"):
        prop_sign = "-"
    
    corechains_lines = []
    lcquad_props = dict_lcquad_predicates(prop_dir)
    j = 0
    for filter in lcquad_props["lcquad_props_filters"]:
        j += 1
        error_msg = entity_id + "\t" + prop_sign + "\t" + "filter:" + str(j)
        write_queryMsg = [F_cache_error, error_msg]
        query = dict_sparqlQueries["query_" + prop_dir + "_oneHope"] % {
            'target_resource': entity_id, 'filter_in': filter}
        i = 0
        results = get_query_results(query, write_queryMsg)
        if results:
            for result in results["results"]["bindings"]:
                i += 1
                cc_line = entity_id.replace('wd:', '') + "\t" + prop_sign + "\t" + prop_sign + \
                    result['propertyLabel']['value'] + "\t" + prop_sign + result['property']['value'].replace(
                    'http://www.wikidata.org/entity/', '')
                corechains_lines.append(cc_line)
                write_to_file(F_corechains_cache, cc_line)

    if(prop_dir == "left"):
        #*get corechains of excluded predicates
        for prop in lcquad_props["exclude_props"]:
            if(ask_triple(entity_id.replace('wd:', ''), prop[0], prop_dir)):
                cc_line = entity_id.replace(
                    'wd:', '') + "\t" + prop_sign + "\t" + prop_sign + prop[1] + "\t" + prop_sign + prop[0]
                corechains_lines.append(cc_line)
                write_to_file(F_corechains_cache, cc_line)

    print(prop_sign, len(corechains_lines))

#! -------- Quilifiers corechain Cashe
def quilifiers_corechains_cache(entity_id, prop_dir):
    prop_sign = "+"
    if(prop_dir == "left"):
        prop_sign = "-"
    corechains_lines = []
    hyper_sign = "*"
    lcquad_props = dict_lcquad_predicates(prop_dir)
    error_msg = entity_id + "\t" + prop_sign + hyper_sign
    write_queryMsg = [F_cache_error, error_msg]
    query = dict_sparqlQueries["query_only_hyperRel_" + prop_dir] % {
        'target_resource': entity_id, 'target_prop': "?p", 'filter_in': ''}
    i = 0
    results = get_query_results(query, write_queryMsg)
    if len(results) > 1:
        for result in results["results"]["bindings"]:
            propId = result['property']['value'].replace(
                'http://www.wikidata.org/entity/', '')
            if(propId and propId in lcquad_props["lcquad_props"]):
                i += 1
                #------ create dataset of right/left corechains
                cc_hyper_rel = ""
                cc_hyper_id = ""
                cc_hyper_id = hyper_sign + \
                    result['hyperq']['value'].replace(
                        'http://www.wikidata.org/entity/', '')
                cc_hyper_rel = " " + hyper_sign + result['hyperqLabel']['value']

                cc_line = entity_id.replace('wd:', '') + "\t" + prop_sign + hyper_sign + "\t" + prop_sign + \
                    result['propertyLabel']['value'] + cc_hyper_rel + "\t" + prop_sign + result['property']['value'].replace(
                    'http://www.wikidata.org/entity/', '') + cc_hyper_id
                corechains_lines.append(cc_line)
                write_to_file(F_corechains_cache, cc_line)
    # -------- if timeout
    if len(results) == 1:
        print("fixig error using filters")
        quilifiers_corechains_cache_fix(entity_id, prop_dir)

    print(prop_sign + hyper_sign, len(corechains_lines))

#! -------- Quilifiers corechain Cashe fixing using filters
def quilifiers_corechains_cache_fix(entity_id, prop_dir):
    prop_sign = "+"
    if(prop_dir == "left"):
        prop_sign = "-"
    corechains_lines = []
    hyper_sign = "*"
    lcquad_props_filters = dict_lcquad_predicates(prop_dir)
    j = 0
    for filter in lcquad_props_filters["lcquad_props_filters"]:
        j += 1
        error_msg = entity_id + "\t" + prop_sign + \
            hyper_sign + "\t" + "filter:" + str(j)
        write_queryMsg = [F_cache_error, error_msg]
        query = dict_sparqlQueries["query_only_hyperRel_" + prop_dir] % {
            'target_resource': entity_id, 'target_prop': "?p", 'filter_in': filter}
        i = 0
        results = get_query_results(query, write_queryMsg)
        if results:
            for result in results["results"]["bindings"]:
                i += 1
                #------ create dataset of right corechains
                cc_hyper_rel = ""
                cc_hyper_id = ""
                cc_hyper_id = hyper_sign + \
                    result['hyperq']['value'].replace(
                        'http://www.wikidata.org/entity/', '')
                cc_hyper_rel = " " + hyper_sign + \
                    result['hyperqLabel']['value']

                cc_line = entity_id.replace('wd:', '') + "\t" + prop_sign + hyper_sign + "\t" + prop_sign + \
                    result['propertyLabel']['value'] + cc_hyper_rel + "\t" + prop_sign + result['property']['value'].replace(
                    'http://www.wikidata.org/entity/', '') + cc_hyper_id
                corechains_lines.append(cc_line)
                write_to_file(F_corechains_cache, cc_line)

    print(prop_sign + hyper_sign, len(corechains_lines))

def cc_most_used_entities():
    F_most_used_entities = open("data/most_used/most_used_entities_sq.txt", "r")
    out = F_most_used_entities.readlines()  # will append in the list out
    i = 0
    start_time = time.time()
    for line in out:
        i += 1
        arguments = line.split("\t")
        # arguments[0]:Entity id   arguments[1]:occurrence of entity
        entity_id = "wd:" + arguments[0]
        if (i == 2117):
            print(i,arguments[0])
            corechains_cache(entity_id, "right")
            #corechains_cache(entity_id, "left")
            quilifiers_corechains_cache(entity_id, "right")
            #quilifiers_corechains_cache(entity_id, "left")

    elapsed_time = time.time() - start_time
    print(elapsed_time)

#cc_most_used_entities()



# #!------try specific entity ID
# def ques_simpleQuestion():
#     entity_id = "wd:" + 'Q379157'
#     start_time = time.time()
#     print(entity_id)
#     corechains_cache(entity_id, "right")
#     corechains_cache(entity_id, "left")
#     quilifiers_corechains_cache(entity_id, "right")
#     quilifiers_corechains_cache(entity_id, "left")
#     elapsed_time = time.time() - start_time
#     print(elapsed_time)


# ques_simpleQuestion()
