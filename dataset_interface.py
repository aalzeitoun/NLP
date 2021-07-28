import sys
import re

import time
import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON
from sparqlQueries import ask_triple, dict_lcquad_predicates, dict_sq_predicates, get_topicEntity_val, cache_sq_entities, get_query_results, dict_sparqlQueries, write_to_file


# F_corechains = open("data/corechains.txt", "w")
# F_corechains_error = open("data/corechains_error.txt", "w")

#! --------------- One Hop corechain -----------------------
def corechain_oneHop(topicEntity, simple_question, prop_answer, splitType, prop_dir):
  prop_sign = "+"
  if(prop_dir == "left"):
      prop_sign = "-"
  
  corechains_lines = []  # only predicates with "+/-"
  lcquad_props = dict_lcquad_predicates(prop_dir)
  error_msg = topicEntity[0] + "\t" + \
      prop_sign + "\t" + simple_question.replace("\n", "")
  write_queryMsg = [F_corechains_error, error_msg]
  query = dict_sparqlQueries["query_" + prop_dir + "_oneHope"] % {
      'target_resource': "wd:"+topicEntity[0], 'filter_in': ''}
  results = get_query_results(query, write_queryMsg)
  if len(results) > 1:
      for result in results["results"]["bindings"]:
        corechain_score = 0
        prop_oneHop = result['property']['value'].replace('http://www.wikidata.org/entity/', '')
        if(prop_oneHop and prop_oneHop in lcquad_props["lcquad_props"]):
          #check if this corechain has the answer
          if (prop_answer[0].upper() == prop_oneHop.upper()):
            corechain_score = 1
          
          #prefix data to be added to every corechain line
          prefix_data = splitType + "\t" + topicEntity[0] + "\t" + prop_sign + "\t" + str(corechain_score) + "\t" + \
              simple_question.replace("\n", "")
          
          #------ create dataset of corechains
          cc_line = prefix_data + "\t" + prop_sign + \
              result['propertyLabel']['value'] + "\t" + prop_sign + prop_oneHop
          corechains_lines.append(cc_line)
          write_to_file(F_corechains, cc_line)
  # -------- if timeout
  if len(results) == 1:
      print("fixig error using filters")
      corechain_oneHop_fix(topicEntity, simple_question, prop_answer, splitType, prop_dir)

  return corechains_lines

#! --------------- One Hop corechain fix ---------------------
def corechain_oneHop_fix(topicEntity, simple_question, prop_answer, splitType, prop_dir):
  prop_sign = "+"
  if(prop_dir == "left"):
      prop_sign = "-"

  corechains_lines = []  # only predicates with "+/-"
  lcquad_props = dict_lcquad_predicates(prop_dir)
  j = 0
  for filter in lcquad_props["lcquad_props_filters"]:

    error_msg = topicEntity[0] + "\t" + \
        prop_sign + "\t" + \
        simple_question.replace("\n", "") + "\t" + "filter:" + str(j)
    write_queryMsg = [F_corechains_error, error_msg]
    query = dict_sparqlQueries["query_" + prop_dir + "_oneHope"] % {
        'target_resource': "wd:"+topicEntity[0], 'filter_in': filter}
    results = get_query_results(query, write_queryMsg)
    if results:
        for result in results["results"]["bindings"]:
          corechain_score = 0
          prop_oneHope = result['property']['value'].replace(
              'http://www.wikidata.org/entity/', '')
          #check if this corechain has the answer
          if (prop_answer[0].upper() == prop_oneHope.upper()):
            corechain_score = 1

          #prefix data to be added to every corechain line
          prefix_data = splitType + "\t" + topicEntity[0] + "\t" + prop_sign + "\t" + str(corechain_score) + "\t" + \
              simple_question.replace("\n", "")

          #------ create dataset of corechains
          cc_line = prefix_data + "\t" + prop_sign + \
              result['propertyLabel']['value'] + \
              "\t" + prop_sign + prop_oneHope
          corechains_lines.append(cc_line)
          write_to_file(F_corechains, cc_line)

  if(prop_dir == "left"):
    #*get corechains of excluded predicates
    for prop in lcquad_props["exclude_props"]:
        if(ask_triple(topicEntity[0], prop[0], prop_dir)):
            cc_line = topicEntity[0] + "\t" + prop_sign + "\t" + \
                prop_sign + prop[1] + "\t" + prop_sign + prop[0]
            corechains_lines.append(cc_line)
            write_to_file(F_corechains, cc_line)

  return corechains_lines

#! --------------- Quilifiers corechain -----------{only for prop answer}------------
def corechain_quilifiers(topicEntity, simple_question, prop_answer, splitType, prop_dir):
  prop_sign = "+"
  if(prop_dir == "left"):
      prop_sign = "-"
 
  hyperSign = "*"
  typeSign = prop_sign + hyperSign
  lcquad_props = dict_lcquad_predicates(prop_dir)
  corechains_lines = []  # only predicates with "+/- *"

  error_msg = topicEntity[0] + "\t" + \
      typeSign + "\t" + simple_question.replace("\n", "")
  write_queryMsg = [F_corechains_error, error_msg]
  query = dict_sparqlQueries["query_only_hyperRel_" + prop_dir] % {
      'target_resource': "wd:"+topicEntity[0], 'target_prop': "p:" + prop_answer[0].upper(), 'filter_in': ''}
  results = get_query_results(query, write_queryMsg)
  
  if len(results) > 1:
      for result in results["results"]["bindings"]:
        corechain_score = 0
        prop_oneHope = result['property']['value'].replace('http://www.wikidata.org/entity/', '')
        if(prop_oneHope and prop_oneHope in lcquad_props["lcquad_props"]):
          #check if this corechain has the answer
          if (prop_answer[0].upper() == prop_oneHope.upper()):
            corechain_score = 0.5
          hyperq = result['hyperq']['value'].replace(
              'http://www.wikidata.org/entity/', '')
          #prefix data to be added to every corechain line
          prefix_data = splitType + "\t" + topicEntity[0] + "\t" + typeSign + "\t" + str(corechain_score) + "\t" + \
              simple_question.replace("\n", "")
          
          #------ create dataset of corechains
          cc_line = prefix_data + "\t" + prop_sign + \
              result['propertyLabel']['value'] + " " + \
              hyperSign + result['hyperqLabel']['value'] + \
              "\t" + prop_sign + prop_oneHope + hyperSign + hyperq
          corechains_lines.append(cc_line)
          # print(cc_line)
          write_to_file(F_corechains, cc_line)
  # -------- if timeout
  if len(results) == 1:
      print("fixig error using filters")
      corechain_quilifiers_fix(
          topicEntity, simple_question, prop_answer, splitType, prop_dir)
  return corechains_lines

#! --------------- Quilifiers corechain fix -----------{only for prop answer}------------
def corechain_quilifiers_fix(topicEntity, simple_question, prop_answer, splitType, prop_dir):
  prop_sign = "+"
  if(prop_dir == "left"):
      prop_sign = "-"

  hyperSign = "*"
  typeSign = prop_sign + hyperSign
  lcquad_props = dict_lcquad_predicates(prop_dir)
  corechains_lines = []  # only predicates with "+/- *"
  j = 0
  for filter in lcquad_props["lcquad_props_filters"]:
      j += 1
      error_msg = topicEntity[0] + "\t" + \
          typeSign + "\t" + \
          simple_question.replace("\n", "") + "\t" + "filter:" + str(j)
      write_queryMsg = [F_corechains_error, error_msg]
      query = dict_sparqlQueries["query_only_hyperRel_" + prop_dir] % {
          'target_resource': "wd:"+topicEntity[0], 'target_prop': "p:" + prop_answer[0].upper(), 'filter_in': filter}
      results = get_query_results(query, write_queryMsg)
      if results:
          for result in results["results"]["bindings"]:
            corechain_score = 0
            prop_oneHope = result['property']['value'].replace(
                'http://www.wikidata.org/entity/', '')
            #check if this corechain has the answer
            if (prop_answer[0].upper() == prop_oneHope.upper()):
              corechain_score = 0.5
            hyperq = result['hyperq']['value'].replace(
                'http://www.wikidata.org/entity/', '')
            #prefix data to be added to every corechain line
            prefix_data = splitType + "\t" + topicEntity[0] + "\t" + typeSign + "\t" + str(corechain_score) + "\t" + \
                simple_question.replace("\n", "")
            #------ create dataset of corechains
            cc_line = prefix_data + "\t" + prop_sign + \
                result['propertyLabel']['value'] + " " + \
                hyperSign + result['hyperqLabel']['value'] + \
                "\t" + prop_sign + prop_oneHope + hyperSign + hyperq
            corechains_lines.append(cc_line)
            # print(cc_line)
            write_to_file(F_corechains, cc_line)

  return corechains_lines

#! --------------- Two Hop corechain for the answer entity -----------------------
def corechains_twoHop_answer(propSign, topicEntity, simple_question, prop_answer, q_answer, splitType, prop_dir):
  propSign2 = "+"
  if(prop_dir == "left"):
      propSign2 = "-"
  corechains_lines = []  # only predicates with "+"
  lcquad_props = dict_lcquad_predicates(prop_dir)  # LCQuAD filters
  corechain_score = 0.5
  query = dict_sparqlQueries["query_" + prop_dir + "_oneHope"] % {
      'target_resource': "wd:"+q_answer[0], 'filter_in': ''}
  error_msg = topicEntity[0] + "\t" + \
      propSign + propSign2 + "\t" + \
      simple_question.replace("\n", "")
  write_queryMsg = [F_corechains_error, error_msg]
  results = get_query_results(query, write_queryMsg)
  if len(results) > 1:
      for result in results["results"]["bindings"]:
        prop_twoHop = result['property']['value'].replace(
            'http://www.wikidata.org/entity/', '')
        if(prop_twoHop and prop_twoHop in lcquad_props["lcquad_props"]):
          # to avoid having the same predicate vice versa in he corechain
          if not (prop_answer[0].upper() == prop_twoHop.upper() and propSign != propSign2):
            #prefix data to be added to every corechain line
            prefix_data = splitType + "\t" + topicEntity[0] + "\t" + propSign + propSign2 + "\t" + str(corechain_score) + "\t" + \
                simple_question.replace("\n", "")

            #------ create dataset of corechains
            cc_line = prefix_data + "\t" + propSign + \
                prop_answer[1] + " " + propSign2 + result['propertyLabel']['value'] + \
                "\t" + propSign + prop_answer[0] + propSign2 + prop_twoHop
            corechains_lines.append(cc_line)
            write_to_file(F_corechains, cc_line)
  # -------- if timeout
  if len(results) == 1:
      print("fixig error using filters")
      corechains_twoHop_answer_fix(
          propSign, topicEntity, simple_question, prop_answer, q_answer, splitType, prop_dir)

  return corechains_lines

#! ---------------Two Hop corechain for the answer entity (fix) -----------------------
def corechains_twoHop_answer_fix(propSign, topicEntity, simple_question, prop_answer, q_answer, splitType, prop_dir):
  propSign2 = "+"
  if(prop_dir == "left"):
      propSign2 = "-"
  corechains_lines = []  # only predicates with "+"
  lcquad_props = dict_lcquad_predicates(prop_dir)  # LCQuAD filters
  corechain_score = 0.5
  j = 0
  for filter in lcquad_props["lcquad_props_filters"]:
    j += 1
    query = dict_sparqlQueries["query_" + prop_dir + "_oneHope"] % {
        'target_resource': "wd:"+q_answer[0], 'filter_in': filter}
    error_msg = topicEntity[0] + "\t" + \
        propSign + propSign2 + "\t" + \
        simple_question.replace("\n", "") + "\t" + "filter:" + str(j)
    write_queryMsg = [F_corechains_error, error_msg]
    results = get_query_results(query, write_queryMsg)
    
    if results:
        for result in results["results"]["bindings"]:
          prop_twoHop = result['property']['value'].replace(
              'http://www.wikidata.org/entity/', '')

          # to avoid having the same predicate vice versa in he corechain
          if not (prop_answer[0].upper() == prop_twoHop.upper() and propSign != propSign2):
            #prefix data to be added to every corechain line
            prefix_data = splitType + "\t" + topicEntity[0] + "\t" + propSign + propSign2  + "\t" + str(corechain_score) + "\t" + \
                simple_question.replace("\n", "")

            #------ create dataset of corechains
            cc_line = prefix_data + "\t" + propSign + \
                prop_answer[1] + " " + propSign2 + result['propertyLabel']['value'] + \
                "\t" + propSign + prop_answer[0] + propSign2 + prop_twoHop
            corechains_lines.append(cc_line)
            write_to_file(F_corechains, cc_line)
  
  #--- (ONLY FOR LEFT SIDE)
  if(prop_dir == "left"):
    for item in lcquad_props["exclude_props"]:
      print(q_answer[0], item[0], 'left')
      if(ask_triple(q_answer[0], item[0], 'left')):
        #prefix data to be added to every corechain line
          prefix_data = splitType + "\t" + topicEntity[0] + "\t" + propSign + propSign2 + "\t" + str(corechain_score) + "\t" + \
              simple_question.replace("\n", "")

          #------ create dataset of corechains
          cc_line = prefix_data + "\t" + propSign + \
              prop_answer[1] + " " + propSign2 + \
              item[1] + "\t" + propSign + prop_answer[0] + propSign2 + item[0]
          corechains_lines.append(cc_line)
          write_to_file(F_corechains, cc_line)

  return corechains_lines

#! --------------- Creation of corechains from wikidata by passing the question and the it's topic entity -----------------------
def create_corechains_wikidata(topicEntity, simple_question, q_answer, prop_answer, splitType):
  corechains = []
  #* Get RIGHT One Hop triples
  right_oneHop = corechain_oneHop(topicEntity, simple_question, prop_answer, splitType, 'right')
  corechains.append(right_oneHop)
    
  #* Get LEFT One Hop triples
  left_oneHop = corechain_oneHop(
      topicEntity, simple_question, prop_answer, splitType, 'left')
  corechains.append(left_oneHop)
  
  #* Get RIGHT Quilifiers 
  right_quilifiers = corechain_quilifiers(topicEntity,  simple_question, prop_answer, splitType, 'right')
  corechains.append(right_quilifiers)

  #* Get LEFT Quilifiers
  left_quilifiers = corechain_quilifiers(topicEntity,  simple_question, prop_answer, splitType, 'left')
  corechains.append(left_quilifiers)

  # #* ---------- get HopTwo only for the entity answer -------------
  dirSign = ""
  corechains_twoHops_right = []
  corechains_twoHops_left = []
  if any("+"+prop_answer[1] in s for s in right_oneHop):
    dirSign = "+"
  elif any("-"+prop_answer[1] in s for s in left_oneHop):
    dirSign = "-"

  #*get answer corechains from cache/wikidata
  answer_corechains_No = get_answer_corechains(
      dirSign, topicEntity, simple_question, q_answer, prop_answer, splitType)
  
  #counters addition:
  print("\n{" + topicEntity[0] + "} " + simple_question.replace("\n", ""))
  print('right_oneHop', len(right_oneHop))
  print('left_oneHop', len(left_oneHop))
  print('right_quilifiers', len(right_quilifiers))
  print('left_quilifiers', len(left_quilifiers))
  print('TwoHop Answer', answer_corechains_No)

  corechains_counter = len(right_oneHop) + len(left_oneHop) + len(right_quilifiers) + \
      len(left_quilifiers) + answer_corechains_No

  return corechains_counter

def retrieve_corechains_cache(topicEntity, simple_question, q_answer, prop_answer, splitType):
  corechains_lines = []
  corechainsCounter = 0
  F_corechain_sq = open("data/corechains_cache_sq.txt", 'r')
  coreChains_all = F_corechain_sq.readlines()
  i = 0
  dirSign = ""
  lcquad_props = dict_lcquad_predicates('right')
  for corechain in coreChains_all:
    corechain_score = 0
    corechain = corechain.replace("\n", '')
    arguments = corechain.split("\t")
    if (arguments[0] == topicEntity[0]):
      i += 1
      corechainSign = arguments[1]
      corechainsVal = arguments[2]
      corechainsIds = arguments[3]  # ex:+P31*P234
     
      #get the first hope prop only
      getProp = corechainsIds
      if("*" in corechainsIds):
          getProp = getProp.replace("*", '')
          matches = re.findall(r'P(.+?)P', getProp)
          getProp = "P" + matches[0]
      else:
          getProp = corechainsIds.replace("+", '')
          getProp = getProp.replace("-", '')
      
      corechain_score = 0
      #check if this prop is in LCQuAD MUP
      #if (getProp in lcquad_props["lcquad_props"]):#this if is not needed anymore as all cashe corechain props are from LCQuAD MUP
      if (getProp == prop_answer[0]):
        corechain_score = 1
        dirSign = arguments[1].replace("*", '')
        if("*" in corechainSign):
          corechain_score = 0.5
      # print(i, getProp, corechain_score)
      prefix_data = splitType + "\t" + topicEntity[0] + "\t" + corechainSign + "\t" + str(corechain_score) + "\t" + \
          simple_question.replace("\n", "")

      #------ create dataset of corechains
      cc_line = prefix_data + "\t" + corechainsVal + \
          "\t" + corechainsIds.replace("\n", '')
      corechains_lines.append(cc_line)
      write_to_file(F_corechains, cc_line)
  
  corechainsCounter += len(corechains_lines)

  print("\n{" + topicEntity[0] + "} " + simple_question.replace("\n", ""))
  print('All_oneHope', len(corechains_lines))

  if(dirSign):
    #*get answer corechains from cache/wikidata
    answer_corechains_No = get_answer_corechains(
        dirSign, topicEntity, simple_question, q_answer, prop_answer, splitType)
    corechainsCounter += answer_corechains_No
    print('answer_corechains', answer_corechains_No)
  else:
    print("dirSign was not found for triple:",
          topicEntity[0], q_answer[0], prop_answer[0], simple_question,)
  return corechainsCounter

def retrieve_twoHop_cache_asnwer(
        propSign, topicEntity, simple_question, prop_answer, q_answer, splitType):
  corechains_lines = []
  F_corechain_sq = open("data/corechains_cache_sq.txt", 'r')
  coreChains_all = F_corechain_sq.readlines()
  i = 0
  lcquad_props = dict_lcquad_predicates('right')
  for corechain in coreChains_all:
    corechain_score = 0.5
    corechain = corechain.replace("\n", '')
    arguments = corechain.split("\t")
    #check if the answer in cache
    if ( q_answer[0] == arguments[0]):
      i += 1
      corechainSign = arguments[1]
      corechainsVal = arguments[2]
      corechainsIds = arguments[3]   # ex:+P31*P234
      propSign2 = arguments[1].replace("*", '')

      #get the first hope prop_id and prop_val only withou hyperRel
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
      #check if this prop is in LCQuAD MUP
      #if (getProp_id in lcquad_props["lcquad_props"]):# this if is not needed anymore as all cashe corechain props are from LCQuAD MUP
      prefix_data = splitType + "\t" + topicEntity[0] + "\t" + propSign+propSign2 + "\t" + str(corechain_score) + "\t" + \
          simple_question.replace("\n", "")
      #------ create dataset of corechains
      cc_line = prefix_data + "\t" + propSign+prop_answer[1] + " " +getProp_val[0] + \
          "\t" + propSign+prop_answer[0] + propSign2+getProp_id
      corechains_lines.append(cc_line)
      #write_to_file(F_corechains, cc_line)
  
  #to remove duplicates (only for the TwoHops)
  corechains_lines = list(set(corechains_lines))
  for line in corechains_lines:
    write_to_file(F_corechains, line)

  return corechains_lines
     
def get_answer_corechains(dirSign, topicEntity, simple_question, q_answer, prop_answer, splitType):
   # check if the answer entity in the cache
  answer_corechains_counter = 0
  if q_answer[0] in cache_sq_entities():  # cache
    #print(q_answer[0], "cache")
    answer_corechains = retrieve_twoHop_cache_asnwer(
        dirSign, topicEntity, simple_question, prop_answer, q_answer, splitType)
    answer_corechains_counter += len(answer_corechains)
  else:
    #retrieve from wikidata
    corechains_twoHops_right = corechains_twoHop_answer(
        dirSign, topicEntity, simple_question, prop_answer, q_answer, splitType, 'right')
    corechains_twoHops_left = corechains_twoHop_answer(
        dirSign, topicEntity, simple_question, prop_answer, q_answer, splitType, 'left')
    answer_corechains_counter += len(corechains_twoHops_right) + \
        len(corechains_twoHops_left)
  
  return answer_corechains_counter


def write_from_train_to_cc(cc_matching):
    for cc in cc_matching:
        write_to_file(F_corechains, cc.replace('\n',''))

#! --------------- Read SimpleQuestions line by line to creat corechain for each -----------------------
def simpleQuestion_ds():
    f = open("data/simplequestion_dataset/question_answerable.txt", 'r')
    f_corechains_train = open("data/corechains_train.txt", 'r')
    out = f.readlines()  # will append in the list out

    cc_train_list = f_corechains_train.readlines()

    i = 0
    j = 0
    x = 0
    x_counter = 0
    counter = 0
    cacheCounter = 0
    wdCounter = 0
    start_time = time.time()  
    splitType = "train"
    wdCounter_cacheCounter = 0
    # random test question
    randomUniqueNo = random.sample(range(19483, 25103), 3000)
    for line in out:
        i += 1
        if(i < 19483):  # (i >= 19483 and i in randomUniqueNo):
            arguments = line.split("\t")
            # arguments[0]:topic entity   arguments[1]:predicat   arguments[2]:answer   arguments[3]:question
            topicEntity_id = arguments[0]
            prop_id = arguments[1].replace("R", "P")
            answer_id = arguments[2]
            simple_question = arguments[3].replace("\n",'')
            if any(simple_question in s for s in cc_train_list):
                x += 1
                cc_matching = [s for s in cc_train_list if simple_question in s]
                x_counter += len(cc_matching)
                write_from_train_to_cc(cc_matching)
                print("\n")
                print(i, x, topicEntity_id, simple_question)
                print("corechains size:", len(cc_matching))

            else:
                j += 1
                if (j > 1000 and j <= 2000):
                    splitType = "dev"
                elif(j > 2500 and j <= 5000):
                    splitType = "test"
                #retrieve values of the ids below sparql:   [id,val]
                topicEntity = get_topicEntity_val(topicEntity_id)
                q_answer = get_topicEntity_val(answer_id)
                prop_answer = get_topicEntity_val(prop_id)

                # check if its exist in the cache, otherwise retrieve corechain form wikidata
                if (topicEntity_id in cache_sq_entities()):  # cache
                    cacheCounter += 1
                    wdCounter_cacheCounter += 1
                    # print(topicEntity_id, "cache")
                    corechains = retrieve_corechains_cache(
                        topicEntity, simple_question, q_answer, prop_answer, splitType)
                    counter += corechains
                else:  # wikidata
                    wdCounter += 1
                    wdCounter_cacheCounter += 1
                    # # print(topicEntity_id, "wkidata")
                    corechains = create_corechains_wikidata(
                        topicEntity, simple_question, q_answer, prop_answer, splitType)
                    counter += corechains
                print(i, j, "cache:"+str(cacheCounter), "wiki:"+str(wdCounter))
    print(j)
    elapsed_time = time.time() - start_time
    print('\nAll CoreChains:', counter + x_counter)
    print('All from train_sqtxt:', x_counter)
    print('All New CoreChains:', counter)
    print("Cache:", cacheCounter)
    print("Wikidata:", wdCounter)
    print(elapsed_time)
    return

simpleQuestion_ds()

#error of the corechain <1000 train:
#Q61881	--	Name a film that bernd eichinger produced	filter:19


#ex of wierdness
#train	Q12152	0	Which Swiss conductor's cause of death is myocardial infarction?	-cause of death


# Skip These:
#  Q82955	R106	Q5644676	what is politician is founder and chairperson of Queens University, Bangladesh

