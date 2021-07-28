import sys
import time
from SPARQLWrapper import SPARQLWrapper, JSON
from sparqlQueries import dict_sparqlQueries, get_query_results, write_to_file, generate_lcquad_predicates_filters

endpoint_url = "https://query.wikidata.org/sparql"
F_cache_error_not_fixed = open("data/cache_error_not_fixed.txt", "w")
F_cache_error_fixed = open("data/cache_error_fixed.txt", "w")


def right_corechains_cache(entity_id):
    corechains_lines = []
    lcquad_props, lcquad_props_filters = generate_lcquad_predicates_filters()
    for filter in lcquad_props_filters:
        query = dict_sparqlQueries["query_right_and_hyperRel"] % {
            'target_resource': entity_id, 'filter_in': filter}
        error_query = entity_id + "\t" + "+*"
        i = 0
        results = get_query_results(
            endpoint_url, query, error_query, F_cache_error_not_fixed)
        if results:
            for result in results["results"]["bindings"]:
                i += 1
                prop_sign = "+"
                #------ create dataset of right corechains
                cc_hyper_rel = ""
                cc_hyper_id = ""
                hyper_sign = ""
                if (len(result) > 2):
                    hyper_sign = "*"
                    cc_hyper_id = hyper_sign + \
                        result['hyperq']['value'].replace(
                            'http://www.wikidata.org/entity/', '')
                    cc_hyper_rel = " " + hyper_sign + \
                        result['hyperqLabel']['value']

                cc_line = entity_id.replace('wd:', '') + "\t" + prop_sign + hyper_sign + "\t" + prop_sign + \
                    result['propertyLabel']['value'] + cc_hyper_rel + "\t" + prop_sign + result['property']['value'].replace(
                    'http://www.wikidata.org/entity/', '') + cc_hyper_id
                corechains_lines.append(cc_line)
                write_to_file(F_cache_error_fixed, cc_line)

    print("+*", len(corechains_lines))


def left_corechains_cache(entity_id):
    corechains_lines = []
    lcquad_props, lcquad_props_filters = generate_lcquad_predicates_filters()
    j = 0
    for filter in lcquad_props_filters:
        j += 1    
        
        query = dict_sparqlQueries["query_left_and_hyperRel"] % {
            'target_resource': entity_id, 'filter_in': filter}
        error_query = entity_id + "\t" + "filter:"+str(j) + "\t" + "-*"
        i = 0
        results = get_query_results(
            endpoint_url, query, error_query, F_cache_error_not_fixed)
        if results:
            print(entity_id, "filter: " + str(j) + " passed")
            for result in results["results"]["bindings"]:
                i += 1
                prop_sign = "-"
                #------ create dataset of right corechains
                cc_hyper_rel = ""
                cc_hyper_id = ""
                hyper_sign = ""
                if (len(result) > 2):
                    hyper_sign = "*"
                    cc_hyper_id = hyper_sign + \
                        result['hyperq']['value'].replace(
                            'http://www.wikidata.org/entity/', '')
                    cc_hyper_rel = " " + hyper_sign + \
                        result['hyperqLabel']['value']

                cc_line = entity_id.replace('wd:', '') + "\t" + prop_sign + hyper_sign + "\t" + prop_sign + \
                    result['propertyLabel']['value'] + cc_hyper_rel + "\t" + prop_sign + result['property']['value'].replace(
                    'http://www.wikidata.org/entity/', '') + cc_hyper_id
                corechains_lines.append(cc_line)
                write_to_file(F_cache_error_fixed, cc_line)
        else:
            print(entity_id, "filter: " + str(j) + " failed")

    print("-*", len(corechains_lines))


def fix_cache_errors():
    F_cache_error = open("data/cache_error.txt", "r")
    out = F_cache_error.readlines()  # will append in the list out
    i = 0
    start_time = time.time()
    #left_corechains_cache("wd:Q3863")
    for line in out:
        i += 1
        arguments = line.split("\t")
        # arguments[0]:Entity id   arguments[1]:occurrence of entity
        entity_id = arguments[0]
        print(i, entity_id + ' ' + arguments[1].replace("\n",''))
        if (arguments[1].replace("\n", '') == '+*'):
            right_corechains_cache("wd:"+entity_id)
        if (arguments[1].replace("\n", '') == '-*'):
            left_corechains_cache("wd:"+entity_id)
        
    elapsed_time = time.time() - start_time
    print(elapsed_time)


fix_cache_errors()


#1 Q8839 -*
# -* 2
# 2 Q6581097 - *
# -* 0
# 3 Q30 - *
# -* 1
# 4 Q1860 - *
# -* 0
# 5 Q6581072 - *
# -* 0
# 6 Q145 - *
# -* 0
# 7 Q3863 - *


# wd: Q258	2 - *
# wd: Q258	3 - *
# wd: Q258	4 - *
# wd: Q258	5 - *
# wd: Q258	6 - *
# wd: Q258	7 - *
# wd: Q258	8 - *
# wd: Q258	9 - *
# wd: Q258	10 - *
