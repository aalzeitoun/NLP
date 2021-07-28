
import re
import json
import time
import collections
import operator
import random
from sparqlQueries import write_to_file, cache_lcquad_entities, mu_prop_lcquad, dict_lcquad_predicates, get_topicEntity_val
from collections import OrderedDict

F_lcquad_train_correct_corechains = open("data/lcquad_test_correct_corechains.txt", "w")
F_lcquad_train_faults = open("data/lcquad_test_faults.txt", "w")

lcquad_MUE = cache_lcquad_entities()

#* extract correct corechain from a sparql wikidata query 
def get_corechain_from_query(sparqlWikidata, entities):
    query_substring = re.search("{(.*?)\}", sparqlWikidata).group(1) #take only string between {}
    query_substring = query_substring.lower() # lower case estring
    query_filterSplit = query_substring.split("filter", 1) # split based on the 'filter' word

    query_statementSplit = [t for t in query_substring.split(
        ".") if t.strip() and 'rdfs:label' not in t]  # query_filterSplit[0]
    #print('Statments', query_statementSplit)
    mainSubj = ''
    mainObj = ''
    signCC = ''
    stmnt_entities = []
    stmnt_anonyms = []
    extract_cc = []
    topic_entity = entities[0].lower()
    is_TE_in_Statment = False
    i = 0
    queryStatments = []
    for statement in query_statementSplit:
        if (statement.find('wdt:p') != -1 or statement.find('p:p') != -1 or statement.find('pq:p') != -1):
            i += 1
            queryStatments.append(statement)
            #find topic entity only the 1st statement
            stmnt_entities = [t for t in statement.split() if t.startswith('wd:')] 
            
            stmnt_anonyms = [t for t in statement.split() if t.startswith('?')]
            stmnt_predicat = [t for t in statement.split() if t.startswith('wdt:') or t.startswith('p:') or t.startswith('ps:') or t.startswith('pq:')]


            if not stmnt_entities:
                stmnt_entities.append(stmnt_anonyms[1])
            if not stmnt_anonyms:
                stmnt_anonyms.append(stmnt_entities[1])

            pos_TE = statement.find(stmnt_entities[0])
            pos_AE = statement.find(stmnt_anonyms[0])

            if pos_TE < pos_AE :
                if (stmnt_anonyms[0] == mainSubj or stmnt_anonyms[0] == mainObj):
                    signCC = '-'
                if (stmnt_entities[0] == mainSubj or stmnt_entities[0] == mainObj):
                    signCC = '+'
                if i == 1:
                    signCC = '+'
                mainSubj = stmnt_entities[0]
                mainObj = stmnt_anonyms[0]
                
            elif pos_TE > pos_AE:
                if (stmnt_anonyms[0] == mainSubj or stmnt_anonyms[0] == mainObj):
                    signCC = '+'
                if (stmnt_entities[0] == mainSubj or stmnt_entities[0] == mainObj):
                    signCC = '-'
                if i == 1:
                    signCC = '-'            
                mainSubj = stmnt_anonyms[0]
                mainObj = stmnt_entities[0]

            if (topic_entity+' ') in statement and is_TE_in_Statment:
                signCC = ', ' + signCC

            if (topic_entity+' ') in statement:
                is_TE_in_Statment = True

            if (statement.find('wdt:p') != -1):
                build_cc = stmnt_predicat[0].replace('wdt:', '')
                build_cc = signCC + build_cc.upper()
                extract_cc.append(build_cc)
            if (statement.find('p:p') != -1):
                build_cc = stmnt_predicat[0].replace('p:', '')
                build_cc = signCC + build_cc.upper()
                extract_cc.append(build_cc)
            if (statement.find('pq:p') != -1):
                build_cc = stmnt_predicat[0].replace('pq:', '')
                build_cc = '*' + build_cc.upper()
                extract_cc.append(build_cc)

    return extract_cc


 
#* gathering entities and predicates from LCQuAD 2.0 dataset
def generate_lcquad_corechain():
    f = open('data/lcquad2_dataset/lcquad2_test_23Nov.json', 'r')
    lcquad_data = f.read()  # will append in the list out
    i = 0
    all_entities = []
    all_predicates = []

    start_time = time.time()
    # parse file
    obj = json.loads(lcquad_data)
    i = 0
    j = 0
    randomUniqueNo = random.sample(range(1, 24166), 10)
    for q_data in obj:
        i += 1
        
        if (i > 0):
            # if q_data['template_id'] == 24: # not in [13, 14, 17, 24]:
                j += 1
                # put all the entities of the query in an array
                entities = re.findall(r'\bwd:\w+', q_data['sparql_wikidata']) 
                entities = list(OrderedDict.fromkeys(entities)) # remove duplicate and keep the order
                # put all the predicates (wdt:) of the query in an array
                prop_wdt = re.findall(r'\bwdt:\w+', q_data['sparql_wikidata'])
                # put all the predicates (p:) of the query in an array
                prop_p = re.findall(r'\bp:\w+', q_data['sparql_wikidata'])
                # put all the predicates (ps:) of the query in an array
                prop_ps = re.findall(r'\bps:\w+', q_data['sparql_wikidata'])
                # put all the hyperRel (pq:) of the query in an array
                prop_pq = re.findall(r'\bpq:\w+', q_data['sparql_wikidata'])

                ##------------------------------------------------------------------
                #if len(entities) == 1 and entities[0] == 'wd:Q5':
                # print("\n=========== " + str(i) + "--" + str(j) + " ===========")
                # print(q_data['question'])
                # print(q_data['sparql_wikidata'])
                # print('entities', entities)
                # print('prop_wdt', prop_wdt)
                # print('prop_p', prop_p)
                # print('prop_ps', prop_ps)
                # print('prop_pq', prop_pq)

                correct_corechain = get_corechain_from_query(q_data['sparql_wikidata'], entities) 
                template_id = q_data['template_id']


                #* fixing issue in temp 13, 14
                if q_data['template_id'] in [14, 13]:
                    #fix issue with duplicated statments
                    fixed_corechain = []
                    if(len(entities) == 1):
                        # template_id = 12
                        # print(correct_corechain)
                        line = str(q_data['uid']) + "\t" + str(template_id) + "\t" + 'duplicated statements - change Temp ID' + "\t" + q_data['question'].replace('\n','') + "\t" + q_data['sparql_wikidata']
                        write_to_file(F_lcquad_train_faults, line)
                        correct_corechain.pop()
                    
                    #*converting {P1 P2 P3} - > {P1 P2, P3}
                    fixed_corechain = []
                    n = 0
                    if len(correct_corechain) == 3:
                        for cc in correct_corechain:
                            if n == 1:
                                fixed_corechain.append(cc+ ',')
                            else:
                                fixed_corechain.append(cc)
                            n+=1
                        correct_corechain = fixed_corechain
                        
                #* fixing issue in temp 10 convert to 24 temp
                if (q_data['uid'] == 22559):
                    template_id = 24

                #* for Tem 7 that has twoTE converting {P1 PQ} --> {P1, PQ}
                if q_data['template_id'] == 7 and len(entities) == 2:
                    fixed_corechain = []
                    fixed_corechain.append(correct_corechain[0] + ",")
                    fixed_corechain.append(correct_corechain[1])
                    correct_corechain = fixed_corechain

                #* for Tem 8 converting {P1 PQ1 PQ2} --> {P1 PQ1, PQ2}
                if q_data['template_id'] == 8:
                    fixed_corechain = []
                    fixed_corechain.append(correct_corechain[0])
                    fixed_corechain.append(correct_corechain[1] + ",")
                    fixed_corechain.append(correct_corechain[2])
                    correct_corechain = fixed_corechain

                #* for Tem 17, 24 converting {P1 , P2} --> {P1, P2}
                if q_data['template_id'] in [17, 24]:
                    fixed_corechain = []
                    fixed_corechain.append(correct_corechain[0] + ",")
                    fixed_corechain.append(correct_corechain[1].replace(', ',''))
                    correct_corechain = fixed_corechain

                #* writing the result to a txt file
                if len(q_data['question']) < 13:
                    line = str(q_data['uid']) + "\t" + str(template_id) + "\t" + 'Question issue' + "\t" + q_data['question'].replace('\n','') + "\t" + q_data['sparql_wikidata']
                    write_to_file(F_lcquad_train_faults, line)
                else:
                    line = str(q_data['uid']) + "\t" + str(template_id) + "\t" + ', '.join(entities).replace('wd:', '') + "\t" + ' '.join(correct_corechain).replace(' ,',',') + "\t" + q_data['question'].replace('\n','') + "\t" + q_data['sparql_wikidata']
                    write_to_file(F_lcquad_train_correct_corechains, line)




generate_lcquad_corechain()
#get_corechain_from_query('SELECT ?value2 ?value2Label WHERE { ?value1 p:P69 ?s . ?s ps:P69 ?value2 . ?s pq:P512 wd:Q849697 . ?s pq:P812 wd:Q413.}', ['wd:Q849697', 'wd:Q413'])
#get_corechain_from_query('SELECT ?ans_1 ?ans_2 WHERE { wd:Q487338 wdt:P186 ?ans_1 . wd:Q487338 wdt:P790 ?ans_2 }', ['wd:Q487338'])
# get_corechain_from_query('select ?ent where { ?ent wdt:P31 wd:Q4830453 . ?ent wdt:P2226 ?obj . ?ent wdt:P414 wd:Q151139 } ORDER BY DESC(?obj)LIMIT 5 ',['wd:Q4830453','wd:Q151139'])

#* gathering entities and predicates from LCQuAD 2.0 dataset
def get_lcquad_entities_and_prop():
    f = open('data/lcquad2_dataset/lcquad2_test_23Nov.json', 'r')
    lcquad_data = f.read()  # will append in the list out
    i = 0
    all_entities = []
    all_predicates = []

    start_time = time.time()
    # parse file
    obj = json.loads(lcquad_data)
    i = 0
    j = 0
    randomUniqueNo = random.sample(range(1, 24166), 10)
    for q_data in obj:
        i += 1
        
        if (i > 0):
            #if q_data['template_id'] == 8: # not in [13, 14, 17, 24]:
                j += 1
                # put all the entities of the query in an array
                entities = re.findall(r'\bwd:\w+', q_data['sparql_wikidata']) 
                entities = list(OrderedDict.fromkeys(entities)) # remove duplicate and keep the order
                # put all the predicates (wdt:) of the query in an array
                prop_wdt = re.findall(r'\bwdt:\w+', q_data['sparql_wikidata'])
                # put all the predicates (p:) of the query in an array
                prop_p = re.findall(r'\bp:\w+', q_data['sparql_wikidata'])
                # put all the predicates (ps:) of the query in an array
                prop_ps = re.findall(r'\bps:\w+', q_data['sparql_wikidata'])
                # put all the hyperRel (pq:) of the query in an array
                prop_pq = re.findall(r'\bpq:\w+', q_data['sparql_wikidata'])

                ##------------------------------------------------------------------
                #if len(entities) == 1 and entities[0] == 'wd:Q5':
                # print("\n=========== " + str(i) + "--" + str(j) + " ===========")
                # print(q_data['question'])
                # print(q_data['sparql_wikidata'])
                # print('entities', entities)
                # print('prop_wdt', prop_wdt)
                # print('prop_p', prop_p)
                # print('prop_ps', prop_ps)
                # print('prop_pq', prop_pq)

                
                all_entities.append(', '.join(entities))

                # for entity_item in entities:
                #     all_entities.append(entity_item)
                for prop_wdt_item in prop_wdt:
                    all_predicates.append(prop_wdt_item.replace('wdt:',''))
                for prop_p_item in prop_p:
                    all_predicates.append(prop_p_item.replace('p:',''))
                for prop_ps_item in prop_ps:
                    all_predicates.append(prop_ps_item.replace('ps:',''))
                for prop_pq_item in prop_pq:
                    all_predicates.append(prop_pq_item.replace('pq:',''))

                ##-------------------------------check # statements in the query-----------------------------------
                raw_predicate = []
                for prop_wdt_item in prop_wdt:
                    raw_predicate.append(prop_wdt_item.replace('wdt:', ''))
                for prop_p_item in prop_p:
                    raw_predicate.append(prop_p_item.replace('p:', ''))
                for prop_ps_item in prop_ps:
                    raw_predicate.append(prop_ps_item.replace('ps:', ''))
                for prop_pq_item in prop_pq:
                    raw_predicate.append(prop_pq_item.replace('pq:', ''))
                propCounter = len(list(set(raw_predicate))) - len(prop_pq)

                # raw_predicate = list(OrderedDict.fromkeys(raw_predicate))
                # print(i, ', '.join(raw_predicate), ' ======= ' ,q_data['sparql_wikidata'])
            ##------------------------------------------------------------------


    #print("propCounter > 2", j)
    # print(len(all_entities))
    # print(len(set(all_entities)))
    # print(i)
    return all_entities, all_predicates 

# get_lcquad_entities_and_prop() 

def write_entities_lcquad():
    F_most_used_entities = open("data/most_used_entities_lcquad.txt", "w")
    F_most_used_predicates = open("data/most_used_predicates_lcquad.txt", "w")

    all_entities, all_predicates = get_lcquad_entities_and_prop()


    all_entities.sort()
    entity_occurrences = collections.Counter(all_entities)
    print("Size All Entity before:", len(all_entities))
    all_entities = list(set(all_entities))
    print("Size All Entity after:", len(all_entities))

    all_predicates.sort()
    predicate_occurrences = collections.Counter(all_predicates)
    print("Size All predicates before:", len(all_predicates))
    all_predicates = list(set(all_predicates))
    print("Size All predicates after:", len(all_predicates))

    # print("Size of shared entities between both topic and answer:", len(set(allTopicEntity_ids) & set(allAnswerEntity_ids)))
    
    print(len(entity_occurrences))
    print(len(predicate_occurrences))
    #print("entity_occurrences", entity_occurrences)
    #print("predicate_occurrences", predicate_occurrences)

    #!get occurrences_of_occurrences
    entity_occurrences_counter = []

    #sort the list decending based on the occurrences
    sorted_entity_occurrences = dict(
        sorted(entity_occurrences.items(), key=operator.itemgetter(1), reverse=True))
    j = 0
    for entity in sorted_entity_occurrences:
        #entity_occurrences_counter.append(entity_occurrences[entity])

        if entity.replace('wd:','') not in lcquad_MUE: #check if the entity is exist in the (train lcquad MUE)
            if (sorted_entity_occurrences[entity] > 1):
                #write entiity and it's occurrence to a file
                #if there is two TE put \t in between
                ent = entity.split(', ')
                ent = '\t'.join(ent).replace('wd:','')
                F_most_used_entities.write(ent + "\t" + str(sorted_entity_occurrences[entity]) + "\n")

    F_most_used_entities.close()

    #sort the list decending based on the occurrences
    sorted_prop_occurrences = dict(
        sorted(predicate_occurrences.items(), key=operator.itemgetter(1), reverse=True))
    j = 0
    lcquad_props = dict_lcquad_predicates('right')
    for prop in sorted_prop_occurrences:
        j+=1
        if prop not in lcquad_props["lcquad_props"]:
            if (sorted_prop_occurrences[prop] > 0):
                propInfo = get_topicEntity_val(prop)
                # #write entiity and it's occurrence to a file
                print(j, prop)
                F_most_used_predicates.write(propInfo[0] + "\t" + propInfo[1] + "\t" + str(sorted_prop_occurrences[prop]) + "\n")

    F_most_used_predicates.close()

    # occurrences_of_occurrences = collections.Counter(entity_occurrences_counter)
    # print("occurrences_of_occurrences size:", len(occurrences_of_occurrences))
    # print (occurrences_of_occurrences)

# write_entities_lcquad()