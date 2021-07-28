
from sentence_transformers import SentenceTransformer, util
from datetime import datetime
import re
import random
import time
from sparqlQueries import ask_triple, ask_triple_full, dict_lcquad_predicates, dict_sq_predicates, get_topicEntity_val, cache_sq_entities, get_query_results, dict_sparqlQueries, write_to_file
F_sts_corechains_sq = open("data/sts_corechains_sq_sparql_error.txt", "w")
F_sts_corechains_sq_failed = open(
    "data/sts_corechains_sq_sbert_failed.txt", "w")

#! --------------- One Hop corechain -----------------------


def corechain_oneHop(sq_Id, topicEntity, prop_dir):
  prop_sign = "+"
  if(prop_dir == "left"):
      prop_sign = "-"

  corechains_labels = []  # only predicates with "+/-"
  corechains_ids = []
  lcquad_props = dict_lcquad_predicates(prop_dir)
  error_msg = sq_Id + "\t" + topicEntity[0] + "\t" + \
      prop_sign
  write_queryMsg = [F_sts_corechains_sq, error_msg]
  query = dict_sparqlQueries["query_" + prop_dir + "_oneHope"] % {
      'target_resource': "wd:"+topicEntity[0], 'filter_in': ''}
  results = get_query_results(query, write_queryMsg)
  if len(results) > 1:
      for result in results["results"]["bindings"]:
        prop_oneHop = result['property']['value'].replace(
            'http://www.wikidata.org/entity/', '')
        if(prop_oneHop and prop_oneHop in lcquad_props["lcquad_props"]):
            #prefix data to be added to every corechain line

            #------ create corechains
            cc_label = prop_sign + result['propertyLabel']['value']
            cc_id = prop_sign + prop_oneHop
            corechains_labels.append(cc_label)
            corechains_ids.append(cc_id)

  # -------- if timeout
  if len(results) == 1:
      print("fixig error using filters")
      corechains_labels, corechains_ids = corechain_oneHop_fix(
          sq_Id, topicEntity, prop_dir)

  return corechains_labels, corechains_ids

#! --------------- One Hop corechain fix ---------------------


def corechain_oneHop_fix(sq_Id, topicEntity, prop_dir):
  prop_sign = "+"
  if(prop_dir == "left"):
      prop_sign = "-"

  corechains_labels = []  # only predicates with "+/-"
  corechains_ids = []
  lcquad_props = dict_lcquad_predicates(prop_dir)
  j = 0
  for filter in lcquad_props["lcquad_props_filters"]:

    error_msg = sq_Id + "\t" + topicEntity[0] + "\t" + \
        prop_sign + "\t" + "filter:" + str(j)
    write_queryMsg = [F_sts_corechains_sq, error_msg]
    query = dict_sparqlQueries["query_" + prop_dir + "_oneHope"] % {
        'target_resource': "wd:"+topicEntity[0], 'filter_in': filter}
    results = get_query_results(query, write_queryMsg)
    if results:
        for result in results["results"]["bindings"]:
          prop_oneHop = result['property']['value'].replace(
              'http://www.wikidata.org/entity/', '')

          #------ create corechains
          cc_label = prop_sign + result['propertyLabel']['value']
          cc_id = prop_sign + prop_oneHop
          corechains_labels.append(cc_label)
          corechains_ids.append(cc_id)

  if(prop_dir == "left"):
    #*get corechains of excluded predicates
    for prop in lcquad_props["exclude_props"]:
        if(ask_triple(topicEntity[0], prop[0], prop_dir)):
            cc_label = prop_sign + prop[1]
            cc_id = prop_sign + prop[0]
            corechains_labels.append(cc_label)
            corechains_ids.append(cc_id)

  return corechains_labels, corechains_ids

#! --------------- Quilifiers corechain -----------{only for prop answer}------------


def corechain_quilifiers(sq_Id, topicEntity, prop_dir):
  prop_sign = "+"
  if(prop_dir == "left"):
      prop_sign = "-"

  hyperSign = "*"
  typeSign = prop_sign + hyperSign
  lcquad_props = dict_lcquad_predicates(prop_dir)
  corechains_labels = []  # only predicates with "+/- *"
  corechains_ids = []

  error_msg = sq_Id + "\t" + topicEntity[0] + "\t" + typeSign
  write_queryMsg = [F_sts_corechains_sq, error_msg]
  query = dict_sparqlQueries["query_only_hyperRel_" + prop_dir] % {
      'target_resource': "wd:"+topicEntity[0], 'target_prop': "?p", 'filter_in': ''}
  results = get_query_results(query, write_queryMsg)

  if len(results) > 1:
      for result in results["results"]["bindings"]:
        prop_oneHope = result['property']['value'].replace(
            'http://www.wikidata.org/entity/', '')
        if(prop_oneHope and prop_oneHope in lcquad_props["lcquad_props"]):
          hyperq = result['hyperq']['value'].replace(
              'http://www.wikidata.org/entity/', '')
          #------ create corechains
          cc_label = prop_sign + \
              result['propertyLabel']['value'] + " " + \
              hyperSign + result['hyperqLabel']['value']
          cc_id = prop_sign + prop_oneHope + hyperSign + hyperq
          corechains_labels.append(cc_label)
          corechains_ids.append(cc_id)

  # -------- if timeout
  if len(results) == 1:
      print("fixig error using filters")
      corechains_labels, corechains_ids = corechain_quilifiers_fix(
          sq_Id, topicEntity, prop_dir)
  return corechains_labels, corechains_ids

#! --------------- Quilifiers corechain fix -----------{only for prop answer}------------


def corechain_quilifiers_fix(sq_Id, topicEntity, prop_dir):
  prop_sign = "+"
  if(prop_dir == "left"):
      prop_sign = "-"

  hyperSign = "*"
  typeSign = prop_sign + hyperSign
  lcquad_props = dict_lcquad_predicates(prop_dir)
  corechains_labels = []  # only predicates with "+/- *"
  corechains_ids = []
  j = 0
  for filter in lcquad_props["lcquad_props_filters"]:
      j += 1
      error_msg = sq_Id + "\t" + topicEntity[0] + "\t" + \
          typeSign + "\t" + "\t" + "filter:" + str(j)
      write_queryMsg = [F_sts_corechains_sq, error_msg]
      query = dict_sparqlQueries["query_only_hyperRel_" + prop_dir] % {
          'target_resource': "wd:"+topicEntity[0], 'target_prop': "?p", 'filter_in': filter}
      results = get_query_results(query, write_queryMsg)
      if results:
          for result in results["results"]["bindings"]:
            prop_oneHope = result['property']['value'].replace(
                'http://www.wikidata.org/entity/', '')

            hyperq = result['hyperq']['value'].replace(
                'http://www.wikidata.org/entity/', '')

            #------ create corechains
            cc_label = prop_sign + \
                result['propertyLabel']['value'] + " " + \
                hyperSign + result['hyperqLabel']['value']
            cc_id = prop_sign + prop_oneHope + hyperSign + hyperq
            corechains_labels.append(cc_label)
            corechains_ids.append(cc_id)

  return corechains_labels, corechains_ids

#! --------------- Two Hop corechain for the answer entity -----------------------
def corechains_twoHop_answer(sq_Id, propSign, topicEntity, prop_answer, q_answer, prop_dir):
    propSign2 = "+"
    if(prop_dir == "left"):
        propSign2 = "-"
    corechains_labels = []  # only predicates with "+/- +/-"
    corechains_ids = []
    lcquad_props = dict_lcquad_predicates(prop_dir)  # LCQuAD filters
    query = dict_sparqlQueries["query_" + prop_dir + "_oneHope"] % {
        'target_resource': "wd:"+q_answer[0], 'filter_in': ''}
    error_msg = sq_Id + "\t" + topicEntity[0] + "\t" + propSign + propSign2
    write_queryMsg = [F_sts_corechains_sq, error_msg]
    results = get_query_results(query, write_queryMsg)
    if len(results) > 1:
        for result in results["results"]["bindings"]:
            prop_twoHop = result['property']['value'].replace(
                'http://www.wikidata.org/entity/', '')
            if(prop_twoHop and prop_twoHop in lcquad_props["lcquad_props"]):
                # to avoid having the same predicate vice versa in the corechain
                if not (prop_answer[0].upper() == prop_twoHop.upper() and propSign != propSign2):
                    #------ create corechains
                    cc_label = propSign + \
                        prop_answer[1] + " " + propSign2 + \
                        result['propertyLabel']['value']
                    cc_id = propSign + prop_answer[0] + propSign2 + prop_twoHop
                    corechains_labels.append(cc_label)
                    corechains_ids.append(cc_id)

    # -------- if timeout
    if len(results) == 1:
        print("fixig error using filters")
        corechains_labels, corechains_ids = corechains_twoHop_answer_fix(
            sq_Id, propSign, topicEntity, prop_answer, q_answer, prop_dir)

    return corechains_labels, corechains_ids

#! ---------------Two Hop corechain for the answer entity (fix) -----------------------
def corechains_twoHop_answer_fix(sq_Id, propSign, topicEntity, prop_answer, q_answer, prop_dir):
    propSign2 = "+"
    if(prop_dir == "left"):
        propSign2 = "-"
    corechains_labels = []  # only predicates with "+/- +/-"
    corechains_ids = []
    lcquad_props = dict_lcquad_predicates(prop_dir)  # LCQuAD filters
    j = 0
    for filter in lcquad_props["lcquad_props_filters"]:
        j += 1
        query = dict_sparqlQueries["query_" + prop_dir + "_oneHope"] % {
            'target_resource': "wd:"+q_answer[0], 'filter_in': filter}
        error_msg = sq_Id + "\t" + topicEntity[0] + "\t" + \
            propSign + propSign2 + "\t" + "filter:" + str(j)
        write_queryMsg = [F_sts_corechains_sq, error_msg]
        results = get_query_results(query, write_queryMsg)

        if results:
            for result in results["results"]["bindings"]:
                prop_twoHop = result['property']['value'].replace(
                    'http://www.wikidata.org/entity/', '')
                # to avoid having the same predicate vice versa in the corechain
                if not (prop_answer[0].upper() == prop_twoHop.upper() and propSign != propSign2):
                    #------ create corechains
                    cc_label = propSign + \
                        prop_answer[1] + " " + propSign2 + \
                        result['propertyLabel']['value']
                    cc_id = propSign + prop_answer[0] + propSign2 + prop_twoHop
                    corechains_labels.append(cc_label)
                    corechains_ids.append(cc_id)

    #--- (ONLY FOR LEFT SIDE)
    if(prop_dir == "left"):
        for item in lcquad_props["exclude_props"]:
          #   print(q_answer[0], item[0], 'left')
            if(ask_triple(q_answer[0], item[0], 'left')):
                #------ create dataset of corechains
                cc_label = propSign + \
                    prop_answer[1] + " " + propSign2 + item[1]
                cc_id = propSign + prop_answer[0] + propSign2 + item[0]
                corechains_labels.append(cc_label)
                corechains_ids.append(cc_id)

    return corechains_labels, corechains_ids

#! --------------- Creation of corechains from wikidata by passing the question and the it's topic entity -----------------------
def create_corechains_wikidata(topicEntity, prop_answer, q_answer, sq_Id):
    corechains_labels = []
    corechains_ids = []
    corechains_twoHops_right = []
    corechains_twoHops_right_ids = []
    corechains_twoHops_left = []
    corechains_twoHops_left_ids = []

    #* Get RIGHT One Hop triples
    right_oneHop, right_oneHop_ids = corechain_oneHop(
        sq_Id, topicEntity, 'right')
    for cc in right_oneHop:
        corechains_labels.append(cc)
    for cc in right_oneHop_ids:
        corechains_ids.append(cc)

    #* Get LEFT One Hop triples
    left_oneHop, left_oneHop_ids = corechain_oneHop(sq_Id, topicEntity, 'left')
    for cc in left_oneHop:
        corechains_labels.append(cc)
    for cc in left_oneHop_ids:
        corechains_ids.append(cc)

    #* Get RIGHT Quilifiers
    right_quilifiers, right_quilifiers_ids = corechain_quilifiers(
        sq_Id, topicEntity, 'right')
    for cc in right_quilifiers:
        corechains_labels.append(cc)
    for cc in right_quilifiers_ids:
        corechains_ids.append(cc)

    #* Get LEFT Quilifiers
    left_quilifiers, left_quilifiers_ids = corechain_quilifiers(
        sq_Id, topicEntity, 'left')
    for cc in left_quilifiers:
        corechains_labels.append(cc)
    for cc in left_quilifiers_ids:
        corechains_ids.append(cc)

    # #* ---------- get HopTwo only for the entity answer -------------
    dirSign = ""
    corechains_twoHops_right = []
    corechains_twoHops_left = []
    if any("+"+prop_answer[1] in s for s in right_oneHop):
        dirSign = "+"
    elif any("-"+prop_answer[1] in s for s in left_oneHop):
        dirSign = "-"

    #this work for sq dataset
    #check if the prop_answer is real in the oneHop otherwise don't retrieve twoHops
    if (dirSign):
        #*get answer corechains from cache/wikidata
        corechains_twoHops_right, corechains_twoHops_right_ids, corechains_twoHops_left, corechains_twoHops_left_ids = get_answer_corechains(sq_Id,
                                                                                                                                             dirSign, topicEntity, q_answer, prop_answer)

    answer_corechains_No = len(
        corechains_twoHops_right) + len(corechains_twoHops_left)

    for cc in corechains_twoHops_right:
        corechains_labels.append(cc)
    for cc in corechains_twoHops_right_ids:
        corechains_ids.append(cc)

    for cc in corechains_twoHops_left:
        corechains_labels.append(cc)
    for cc in corechains_twoHops_left_ids:
        corechains_ids.append(cc)

    corechains = {
        "corechains_labels": corechains_labels,
        "corechains_ids": corechains_ids,
        "right_oneHop": right_oneHop,
        "right_oneHop_ids": right_oneHop_ids,
        "left_oneHop": left_oneHop,
        "left_oneHop_ids": left_oneHop_ids,
        "right_quilifiers": right_quilifiers,
        "right_quilifiers_ids": right_quilifiers_ids,
        "left_quilifiers": left_quilifiers,
        "left_quilifiers_ids": left_quilifiers_ids,
        "corechains_twoHops_right": corechains_twoHops_right,
        "corechains_twoHops_right_ids": corechains_twoHops_right_ids,
        "corechains_twoHops_left": corechains_twoHops_left,
        "corechains_twoHops_left_ids": corechains_twoHops_left_ids
    }
    return corechains


def retrieve_corechains_cache(topicEntity, prop_answer, q_answer, sq_Id):
    corechains_labels = []
    corechains_ids = []
    right_oneHop = []
    right_oneHop_ids = []
    left_oneHop = []
    left_oneHop_ids = []
    right_quilifiers = []
    right_quilifiers_ids = []
    left_quilifiers = []
    left_quilifiers_ids = []
    corechains_twoHops_right = []
    corechains_twoHops_right_ids = []
    corechains_twoHops_left = []
    corechains_twoHops_left_ids = []
    corechainsCounter = 0
    F_corechain_sq = open("data/corechains_cache_sq.txt", 'r')
    coreChains_all = F_corechain_sq.readlines()
    i = 0
    dirSign = ""
    lcquad_props = dict_lcquad_predicates('right')
    for corechain in coreChains_all:
        corechain = corechain.replace("\n", '')
        arguments = corechain.split("\t")
        if (arguments[0] == topicEntity[0]):
            i += 1
            corechainSign = arguments[1]
            corechainsVal = arguments[2]
            corechainsIds = arguments[3]  # ex:+P31*P234

            #get the first hope prop only
            getProp_id = corechainsIds
            getProp_val = corechainsVal.split(' *')
            if("*" in corechainsIds):
                getProp_id = getProp_id.replace("*", '')
                matches = re.findall(r'P(.+?)P', getProp_id)
                getProp_id = "P" + matches[0]
            else:
                getProp_id = corechainsIds.replace("+", '')
                getProp_id = getProp_id.replace("-", '')

            if (getProp_id == prop_answer[0]):
                dirSign = arguments[1].replace("*", '')

             #------ create dataset of corechains
            corechains_labels.append(corechainsVal)
            corechains_ids.append(corechainsIds)

            if(corechainSign == "+"):
                right_oneHop.append(corechainsVal)
                right_oneHop_ids.append(corechainsIds)
            if(corechainSign == "-"):
                left_oneHop.append(corechainsVal)
                left_oneHop_ids.append(corechainsIds)
            if(corechainSign == "+*"):
                right_quilifiers.append(corechainsVal)
                right_quilifiers_ids.append(corechainsIds)
            if(corechainSign == "-*"):
                left_quilifiers.append(corechainsVal)
                left_quilifiers_ids.append(corechainsIds)

    if(dirSign):
        #*get answer corechains from cache/wikidata
        corechains_twoHops_right, corechains_twoHops_right_ids, corechains_twoHops_left, corechains_twoHops_left_ids = get_answer_corechains(sq_Id,
                                                                                                                                             dirSign, topicEntity, q_answer, prop_answer)
        answer_corechains_No = len(
            corechains_twoHops_right) + len(corechains_twoHops_left)
        corechainsCounter += answer_corechains_No
        # print('answer_corechains', answer_corechains_No)
    else:
        print("dirSign was not found for triple:",
              topicEntity[0], q_answer[0], prop_answer[0])

    ccTwo_r = 0
    for cc in corechains_twoHops_right:
        corechains_labels.append(cc)
        corechains_ids.append(corechains_twoHops_right_ids[ccTwo_r])
        ccTwo_r += 1

    ccTwo_l = 0
    for cc in corechains_twoHops_left:
        corechains_labels.append(cc)
        corechains_ids.append(corechains_twoHops_left_ids[ccTwo_l])
        ccTwo_l += 1

    corechains = {
        "corechains_labels": corechains_labels,
        "corechains_ids": corechains_ids,
        "right_oneHop": right_oneHop,
        "right_oneHop_ids": right_oneHop_ids,
        "left_oneHop": left_oneHop,
        "left_oneHop_ids": left_oneHop_ids,
        "right_quilifiers": right_quilifiers,
        "right_quilifiers_ids": right_quilifiers_ids,
        "left_quilifiers": left_quilifiers,
        "left_quilifiers_ids": left_quilifiers_ids,
        "corechains_twoHops_right": corechains_twoHops_right,
        "corechains_twoHops_right_ids": corechains_twoHops_right_ids,
        "corechains_twoHops_left": corechains_twoHops_left,
        "corechains_twoHops_left_ids": corechains_twoHops_left_ids
    }
    return corechains


def retrieve_twoHop_cache_asnwer(propSign, prop_answer, q_answer, prop_dir):
    prop_sign2 = "+"
    if(prop_dir == "left"):
        prop_sign2 = "-"

    corechains_labels = []
    corechains_ids = []
    F_corechain_sq = open("data/corechains_cache_sq.txt", 'r')
    coreChains_all = F_corechain_sq.readlines()
    i = 0
    lcquad_props = dict_lcquad_predicates(prop_dir)
    for corechain in coreChains_all:
        corechain = corechain.replace("\n", '')
        arguments = corechain.split("\t")
        #check if the answer in cache
        if (q_answer[0] == arguments[0]):
            i += 1
            corechainSign = arguments[1]
            corechainsVal = arguments[2]
            corechainsIds = arguments[3]   # ex:+P31*P234
            get_propSign2 = arguments[1].replace("*", '')

            #get the first hope prop_id and prop_val only without hyperRel
            getProp_id = corechainsIds
            #retrieve Prop_Val
            getProp_val = corechainsVal.split(' *')
            if("*" in corechainsIds):
                #retrieve Prop_Id
                getProp_id = getProp_id.replace("*", '')
                matches = re.findall(r'P(.+?)P', getProp_id)
                getProp_id = "P" + matches[0]
            else:
                getProp_id = corechainsIds.replace("+", '')
                getProp_id = getProp_id.replace("-", '')

            #------ create dataset of corechains
            if (prop_sign2 == get_propSign2):  # retrieve only the left or right two hops not both
                cc_label = propSign+prop_answer[1] + " " + getProp_val[0]
                cc_id = propSign+prop_answer[0] + get_propSign2+getProp_id
                corechains_labels.append(cc_label)
                corechains_ids.append(cc_id)

    #to remove duplicates (only for the TwoHops)
    corechains_labels = list(set(corechains_labels))
    corechains_ids = list(set(corechains_ids))

    return corechains_labels, corechains_ids


def get_answer_corechains(sq_Id, dirSign, topicEntity, q_answer, prop_answer):
    # check if the answer entity in the cache
    corechains_twoHops_right = []
    corechains_twoHops_right_ids = []
    corechains_twoHops_left = []
    corechains_twoHops_left_ids = []
    if q_answer[0] in cache_sq_entities():  # cache
        #print(q_answer[0], "cache")
        corechains_twoHops_right, corechains_twoHops_right_ids = retrieve_twoHop_cache_asnwer(
            dirSign, prop_answer, q_answer, 'right')
        corechains_twoHops_left, corechains_twoHops_left_ids = retrieve_twoHop_cache_asnwer(
            dirSign, prop_answer, q_answer, 'left')
    else:
        #retrieve from wikidata
        corechains_twoHops_right, corechains_twoHops_right_ids = corechains_twoHop_answer(sq_Id,
                                                                                          dirSign, topicEntity, prop_answer, q_answer, 'right')
        corechains_twoHops_left, corechains_twoHops_left_ids = corechains_twoHop_answer(sq_Id,
                                                                                        dirSign, topicEntity, prop_answer, q_answer, 'left')

    return corechains_twoHops_right, corechains_twoHops_right_ids, corechains_twoHops_left, corechains_twoHops_left_ids


#!-------------- retrieve corechain for test dataset sq local file
def corechains_test_sq_all(topicEntity):
    corechains_labels = []
    corechains_ids = []
    right_oneHop = []
    right_oneHop_ids = []
    left_oneHop = []
    left_oneHop_ids = []
    right_quilifiers = []
    right_quilifiers_ids = []
    left_quilifiers = []
    left_quilifiers_ids = []
    corechains_twoHops_right = []
    corechains_twoHops_right_ids = []
    corechains_twoHops_left = []
    corechains_twoHops_left_ids = []
    F_corechain_sq = open("data/corechains_test_sq_all.txt", 'r')
    coreChains_all = F_corechain_sq.readlines()
    i = 0
    for corechain in coreChains_all:
        corechain = corechain.replace("\n", '')
        arguments = corechain.split("\t")
        if (arguments[0] == topicEntity[0]):
            i += 1
            corechainSign = arguments[1]
            corechainsVal = arguments[2]
            corechainsIds = arguments[3]  # ex:+P31*P234

            #get the first hope prop only
            getProp_id = corechainsIds
            if("*" in corechainsIds):
                getProp_id = getProp_id.replace("*", '')
                matches = re.findall(r'P(.+?)P', getProp_id)
                getProp_id = "P" + matches[0]
            else:
                getProp_id = corechainsIds.replace("+", '')
                getProp_id = getProp_id.replace("-", '')

             #------ create dataset of corechains
            corechains_labels.append(corechainsVal)
            corechains_ids.append(corechainsIds)

            if(corechainSign == "+"):
                right_oneHop.append(corechainsVal)
                right_oneHop_ids.append(corechainsIds)
            if(corechainSign == "-"):
                left_oneHop.append(corechainsVal)
                left_oneHop_ids.append(corechainsIds)
            if(corechainSign == "+*"):
                right_quilifiers.append(corechainsVal)
                right_quilifiers_ids.append(corechainsIds)
            if(corechainSign == "-*"):
                left_quilifiers.append(corechainsVal)
                left_quilifiers_ids.append(corechainsIds)
            # twoHops of answer:
            # print(corechainSign)
            if(corechainSign == "++" or corechainSign == "-+"):
                corechains_twoHops_right.append(corechainsVal)
                corechains_twoHops_right_ids.append(corechainsIds)

            if(corechainSign == "+-" or corechainSign == "--"):
                corechains_twoHops_left.append(corechainsVal)
                corechains_twoHops_left_ids.append(corechainsIds)

    # ccTwo_r = 0
    # for cc in corechains_twoHops_right:
    #     corechains_labels.append(cc)
    #     corechains_ids.append(corechains_twoHops_right_ids[ccTwo_r])
    #     ccTwo_r += 1

    # ccTwo_l = 0
    # for cc in corechains_twoHops_left:
    #     corechains_labels.append(cc)
    #     corechains_ids.append(corechains_twoHops_left_ids[ccTwo_l])
    #     ccTwo_l += 1

    corechains = {
        "corechains_labels": corechains_labels,
        "corechains_ids": corechains_ids,
        "right_oneHop": right_oneHop,
        "right_oneHop_ids": right_oneHop_ids,
        "left_oneHop": left_oneHop,
        "left_oneHop_ids": left_oneHop_ids,
        "right_quilifiers": right_quilifiers,
        "right_quilifiers_ids": right_quilifiers_ids,
        "left_quilifiers": left_quilifiers,
        "left_quilifiers_ids": left_quilifiers_ids,
        "corechains_twoHops_right": corechains_twoHops_right,
        "corechains_twoHops_right_ids": corechains_twoHops_right_ids,
        "corechains_twoHops_left": corechains_twoHops_left,
        "corechains_twoHops_left_ids": corechains_twoHops_left_ids
    }
    return corechains

#!-------------- using SBERT Model
#epochId = 1,4,8


def sbert_answers(question_val, prop_answer, correct_prop, corechains, epochId):
    model_save_path = ''
    if(epochId == 1):
        model_save_path = 'models/sq_all_distilbert-base-uncased-2021-01-29_17-42-19'  # 1 epoch
    if(epochId == 4):
        model_save_path = 'models/sq_e4_b16_distilbert-base-uncased-2021-01-31_15-14-03'  # 4 epoch
    if(epochId == 8):
        model_save_path = 'models/sq_e8_b64_distilbert-base-uncased-2021-02-03_19-08-22'  # 8 epoch
    model = SentenceTransformer(model_save_path)

    question_sq = []
    top_5 = []
    top_5_ids = []
    corpus = corechains['corechains_labels']
    # print(*corpus, sep='\n')
    #Encode all sentences
    corpus_embeddings = model.encode(corpus)

    question_sq.append(question_val)
    # Encode sentences:
    query_embeddings = model.encode(question_sq)

    # print("\n===========  ===========\n")
    # print(question_sq[0])

   
    start_time = time.time()

    #-------------------------Paraphrase Mining-------------------------------
    # corpus2 = corpus
    # corpus2 = corpus2.append(question_val)
    # paraphrases = util.paraphrase_mining(model, corpus2)

    # for paraphrase in paraphrases[0:10]:
    #     score, i, j = paraphrase
    #     print("{} \t\t {} \t\t Score: {:.4f}".format(
    #         corpus2[i], corpus2[j], score))
    #--------------------------------------------------------------------------


     #-------------- using pytorch
    #Compute cosine similarity between all pairs
    cos_sim = util.pytorch_cos_sim(corpus_embeddings, query_embeddings)

    #Add all pairs to a list with their cosine similarity score
    all_sentence_combinations = []
    for i in range(len(cos_sim)-1):
        all_sentence_combinations.append([cos_sim[i], i])

    #Sort list by the highest cosine similarity score
    all_sentence_combinations = sorted(
        all_sentence_combinations, key=lambda x: x[0], reverse=True)
    # print("\n=========== pytorch cosine ===========\n")
    #print("Top-5 most similar pairs:")
    for score, i in all_sentence_combinations[0:5]:  # len(corpus)
        #print("{} \t {:.4f}".format(corpus[i], cos_sim[i][0]))
        top_5.append(corpus[i])
        print(epochId, corpus[i], cos_sim[i][0])

    #elapsed_time = time.time() - start_time
    #print(elapsed_time)

   
    isFailed = False
    isFailedFirst = ''
    isFailedtopAnswers = ''

    foundFirst = ''
    notFoundFirst = ''
    topFive = ''
    notTopFive = ''

    if(top_5[0] == correct_prop):
        isFailedFirst = 'foundFirst'
        foundFirst = epochId
    else:
        #print('epoch:', epochId, correct_prop, 'Not the first answer')
        isFailed = True
        isFailedFirst = 'NotFoundFirst'
        notFoundFirst = epochId
    if(correct_prop in top_5):
        isFailedtopAnswers = 'topFive'
        topFive = epochId
    else:
        #print('epoch:', epochId, correct_prop, 'Not in the top 5')
        isFailed = True
        isFailedtopAnswers = 'NotTopFive'
        notTopFive = epochId

    # isFailedFirst = 'epoch' + epochId + ' ' + isFailedFirst
    # isFailedtopAnswers = 'epoch' + epochId + ' ' + isFailedtopAnswers

    trainResult = [foundFirst, notFoundFirst, topFive, notTopFive, top_5[0], isFailed]
    return trainResult


def returnSign(singleSign, cc_ids):
    line = ''
    ccSign_counter = [i for i, x in enumerate(cc_ids) if x == singleSign]
    if len(ccSign_counter) == 1:
        line = singleSign
    if len(ccSign_counter) == 2:
        line = singleSign + singleSign
    return line


def write_from_to(file_name, topicEntity_id, corechains):
    cc_labels = corechains["corechains_labels"]
    cc_ids = corechains["corechains_ids"]
    i = 0
    for ccLabel in cc_labels:
        ccSign = ""
        ccSign += returnSign('+', cc_ids[i])
        ccSign += returnSign('-', cc_ids[i])
        ccSign += returnSign('*', cc_ids[i])
        line = topicEntity_id + "\t" + ccSign + \
            "\t" + ccLabel + "\t" + cc_ids[i]
        write_to_file(file_name, str(line).replace('\n', ''))
        i += 1


def sts_corechains_sq():
    f = open("data/simplequestion_dataset/question_answerable.txt", 'r')
    #f = open('data/4epoch_sts_corechains_sq_sbert_failed.txt', 'r')
    out = f.readlines()  # will append in the list out
    i = 0
    cacheCounter = 0
    wdCounter = 0
    start_time = time.time()
    wdCounter_cacheCounter = 0
    corechains = {}
    answer_first_counter = 0
    answer_exist_counter = 0
    # random test question
    # randomUniqueNo = random.sample(range(19481, 25103), 10)
    for line in out:
        i += 1
        #STOPPED at 20249
        if(i == 20000):  # 19481  # 21607 has wrong predicate
            arguments = line.split("\t")
            # # arguments[0]:topic entity   arguments[1]:predicat   arguments[2]:answer   arguments[3]:question
            topicEntity_id = arguments[0]
            prop_id = arguments[1].replace("R", "P")
            answer_id = arguments[2]
            simple_question = arguments[3].replace("\n", '')

            # # For the Failed answers
            # topicEntity_id = arguments[1]
            # prop_id = arguments[2].replace("R", "P")
            # answer_id = arguments[3]
            # simple_question = arguments[4].replace("\n", '')

            #retrieve values of the ids below sparql:   [id,val]
            topicEntity = get_topicEntity_val(topicEntity_id)
            q_answer = get_topicEntity_val(answer_id)
            prop_answer = get_topicEntity_val(prop_id)

            if(not topicEntity[1]):
                print("\n=========== " + str(i) + " ===========")
                print(i, "topicEntity " + topicEntity_id + " is not exist")
                write_to_file(F_sts_corechains_sq, "topicEntity " +
                              topicEntity_id + " is not exist")

            if(topicEntity[1]):
                print("\n=========== " + str(i) + " ===========")
                print(simple_question, topicEntity_id, prop_id, answer_id)

                #---------------------------------------------------------------
                # check if its exist in the cache, otherwise retrieve corechain form wikidata
                if (topicEntity_id in cache_sq_entities()):  # cache
                    cacheCounter += 1
                    wdCounter_cacheCounter += 1
                    corechains = retrieve_corechains_cache(
                        topicEntity, prop_answer, q_answer, str(i))
                else:  # wikidata
                    wdCounter += 1
                    wdCounter_cacheCounter += 1
                    corechains = create_corechains_wikidata(
                        topicEntity, prop_answer, q_answer, str(i))
                #---------------------------------------------------------------

                #print(*corechains['corechains_labels'], sep='\n')

                # write_from_to(F_cache_test_corechains_sq,
                #               topicEntity_id, corechains)

                get_top5 = sbert_answers(simple_question, corechains)

                r_prop = "+"+prop_answer[1]
                l_prop = "-"+prop_answer[1]

                isFailed = False
                isFailedFirst = ''
                isFailedtopAnswers = ''
                q_ans = [r_prop, l_prop]
                if(get_top5[0] in q_ans):
                    answer_first_counter += 1
                    print(prop_answer[1], 'found first')
                else:
                    print(prop_answer[1], 'not the first answer')
                    isFailed = True
                    isFailedFirst = 'isFailedFirst'
                if(r_prop in get_top5 or l_prop in get_top5):
                    answer_exist_counter += 1
                    print(prop_answer[1], 'exist in the top 5')
                else:
                    print(prop_answer[1], 'Not in the top 5')
                    isFailed = True
                    isFailedtopAnswers = 'isFailedtopAnswers'

                if(isFailed):
                    failed_msg = str(i) + "\t" + topicEntity_id + "\t" + prop_id + \
                        "\t" + answer_id + "\t" + simple_question
                    if (not ask_triple(topicEntity_id, prop_id, 'right') and not ask_triple(topicEntity_id, prop_id, 'left')):
                        write_to_file(F_sts_corechains_sq_failed,
                                      failed_msg + "\t" + "Triple is not exist")
                    else:
                        write_to_file(F_sts_corechains_sq_failed,
                                      failed_msg + "\t" + prop_answer[1] + "\t" + get_top5[0] + "\t" + isFailedFirst + "\t" + isFailedtopAnswers)

    print("answer_first_counter", answer_first_counter)
    print("answer_exist_counter", answer_exist_counter)
    elapsed_time = time.time() - start_time
    print(elapsed_time)
    return


# sts_corechains_sq()
# # function optimized to run on gpu
# @jit(target ="cuda")
def sts_corechains_sq_cache():
    f = open("data/simplequestion_dataset/question_answerable.txt", 'r')
    #f = open('data/4epoch_sts_corechains_sq_sbert_failed.txt', 'r')
    out = f.readlines()  # will append in the list out
    i = 0
    cacheCounter = 0
    wdCounter = 0
    start_time = time.time()
    wdCounter_cacheCounter = 0
    corechains = {}
    epoch1_1st_counter = 0
    epoch4_1st_counter = 0
    epoch8_1st_counter = 0

    inCorrect_topicEntity = ['Q2264085', 'Q1831250', 'Q2835689', 'Q16028447', 'Q11791839',
                             'Q4899445', 'Q11396479', 'Q18191773', 'Q6060298', 'Q12332784', 'Q16482238', 'Q15623685']
    # random test question
    randomUniqueNo = random.sample(range(19481, 25103), 10)
    for line in out:
        i += 1
        #STOPPED at 20249
        if(i == 19527):  # 19481  # 21607 has wrong predicate
            arguments = line.split("\t")
            # # arguments[0]:topic entity   arguments[1]:predicat   arguments[2]:answer   arguments[3]:question
            topicEntity_id = arguments[0]
            prop_id = arguments[1].replace("R", "P")
            answer_id = arguments[2]
            simple_question = arguments[3].replace("\n", '')

            # # For the Failed answers
            # topicEntity_id = arguments[1]
            # prop_id = arguments[2].replace("R", "P")
            # answer_id = arguments[3]
            # simple_question = arguments[4].replace("\n", '')

            #retrieve values of the ids below sparql:   [id,val]
            topicEntity = [topicEntity_id, 'topicEntity']
            q_answer = [answer_id, 'q_answer']
            prop_answer = get_topicEntity_val(prop_id)

            # if(not topicEntity[1]):
            #     print("\n=========== " + str(i) + " ===========")
            #     print(i, "topicEntity " + topicEntity_id + " is not exist")
            #     write_to_file(F_sts_corechains_sq, "topicEntity " +
            #                   topicEntity_id + " is not exist")

            if(topicEntity_id not in inCorrect_topicEntity):  # topicEntity[1]
                print("\n=========== " + str(i) + " ===========")
                print(simple_question, topicEntity_id, prop_id, answer_id)

                #---------------------------------------------------------------
                corechains = corechains_test_sq_all(topicEntity)
                #print(*corechains['corechains_labels'], sep='\n')
                signDir = ''
                if(ask_triple_full(topicEntity_id, prop_id, answer_id)):
                    signDir = '+'
                elif(ask_triple_full(answer_id, prop_id, topicEntity_id)):
                    signDir = '-'
                correct_prop = signDir + prop_answer[1]
                print("(" + correct_prop + ")")

                # print(*corechains['corechains_labels'], sep='\n')
                
                # isAnyFailedAnswered = False
                # # return: [foundFirst, NotFoundFirst, topFive, NotTopFive, topOne, isFailed]
                epoch1_result = sbert_answers(
                    simple_question, prop_answer, correct_prop, corechains, 1)
                # epoch4_result = sbert_answers(
                #     simple_question, prop_answer, correct_prop, corechains, 4)
                # epoch8_result = sbert_answers(
                #     simple_question, prop_answer, correct_prop, corechains, 8)
                
                # if(epoch1_result[5]):
                #     epoch1_1st_counter += 1
                # if(epoch4_result[5]):
                #     epoch4_1st_counter += 1
                # if(epoch8_result[5]):
                #     epoch8_1st_counter += 1


                
                # print("-1st:",
                #       epoch1_result[0], epoch4_result[0], epoch8_result[0], "Not1st:", epoch1_result[1], epoch4_result[1], epoch8_result[1], ", -Top5:",
                #       epoch1_result[2], epoch4_result[2], epoch8_result[2], "NotTop5:",
                #       epoch1_result[3], epoch4_result[3], epoch8_result[3])
                # print("-epoch1:", epoch1_1st_counter, "-epoch4:",
                #       epoch4_1st_counter, "-epoch8:", epoch8_1st_counter)
                # if(epoch1_result[5] or epoch4_result[5] or epoch8_result[5]):
                #     failed_msg = str(i) + "\t" + topicEntity_id + "\t" + prop_id + \
                #         "\t" + answer_id + "\t" + simple_question
                #     if (not signDir):
                #         write_to_file(F_sts_corechains_sq_failed,
                #                       failed_msg + "\t" + "Triple is not exist")
                #     else:
                #         write_to_file(F_sts_corechains_sq_failed,
                #                       failed_msg + "\t" + correct_prop + "\t" + epoch1_result[4] + "\t" + str(
                #                           epoch1_result[1]) + ',' + str(epoch1_result[3]) + "\t" + str(epoch4_result[1]) + ',' + str(epoch4_result[3]) + "\t" + str(epoch8_result[1]) + ',' + str(epoch8_result[3]))

                

    print("-epoch1:", epoch1_1st_counter, "-epoch4:",
          epoch4_1st_counter, "-epoch8:", epoch8_1st_counter)
    elapsed_time = time.time() - start_time
    print(elapsed_time)
    return


sts_corechains_sq_cache()

#simlequestions has wrong predicates
#where was amy vanderbilt born Q4749431 P20 Q60


# "corechains_labels": corechains_labels,
# "corechains_ids": corechains_ids,
# "right_oneHop": right_oneHop,
# "right_oneHop_ids": right_oneHop_ids,
# "left_oneHop": left_oneHop,
# "left_oneHop_ids": left_oneHop_ids,
# "right_quilifiers": right_quilifiers,
# "right_quilifiers_ids": right_quilifiers_ids,
# "left_quilifiers": left_quilifiers,
# "left_quilifiers_ids": left_quilifiers_ids,
# "corechains_twoHops_right": corechains_twoHops_right,
# "corechains_twoHops_right_ids": corechains_twoHops_right_ids,
# "corechains_twoHops_left": corechains_twoHops_left,
# "corechains_twoHops_left_ids": corechains_twoHops_left_ids


# scp -r ssh ahmad.alzeitoun@sda-srv04.iai.uni-bonn.de:/data/ahmad.alzeitoun/dev/ThesisProject/sentence-transformers/output /Users/University/Development/DevThesis/ThesisProject

# scp /Users/University/Development/DevThesis/ThesisProject/data/seprated_train/corechains_final_shuffled.txt ssh ahmad.alzeitoun@sda-srv04.iai.uni-bonn.de:/data/ahmad.alzeitoun/dev/ThesisProject/sentence-transformers/corechains_dataset


## Error need to be fixed in ccids which are not compatible with cclabels in SQ corechains_test_sq_all.txt
## Ex: Q605122	+-	-mouth of the watercourse +Commons category	-P403+P910
