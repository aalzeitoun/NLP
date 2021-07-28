import sys
from SPARQLWrapper import SPARQLWrapper, JSON


def get_endpointURL():
    endpoint_url = "https://query.wikidata.org/sparql"
    return endpoint_url


def get_userAgent():
    user_agent = "WDQS-example Python/%s.%s" % (
        sys.version_info[0], sys.version_info[1])
    return user_agent

#*==============================
#*==============================
#*----- Queries of Right path ----- #?object ?objectLabel
query_right_oneHope = '''SELECT DISTINCT ?property ?propertyLabel
  WHERE {
    %(target_resource)s ?p ?statement .
    ?statement ?ps ?object .
    
    ?property wikibase:claim ?p.
    ?property wikibase:statementProperty ?ps.
    %(filter_in)s
    SERVICE wikibase:label { bd:serviceParam wikibase:language "en".
    }
  }ORDER BY DESC(?property)  DESC(?hyperq) 
'''

#*----- Queries of left path ----- #?object ?objectLabel
query_left_oneHope = '''SELECT DISTINCT ?property ?propertyLabel
  WHERE {
    ?subject ?p ?statement .
    ?statement ?ps %(target_resource)s .
    
    ?property wikibase:claim ?p.
    ?property wikibase:statementProperty ?ps.
    %(filter_in)s
    SERVICE wikibase:label { bd:serviceParam wikibase:language "en".
    }
  }ORDER BY DESC(?property)  DESC(?hyperq)
'''
#*==============================
#*==============================
#*----- Queries of hyperRel right path -----
query_only_hyperRel_right = '''SELECT DISTINCT ?property ?propertyLabel ?hyperq ?hyperqLabel 
  WHERE {
    %(target_resource)s %(target_prop)s ?statement .
    ?statement ?ps %(target_resource2)s .
    
    ?property wikibase:claim ?p.
    ?property wikibase:statementProperty ?ps.
    %(filter_in)s
    ?statement ?pq %(target_qualifier)s .
    ?hyperq wikibase:qualifier ?pq .
    SERVICE wikibase:label { bd:serviceParam wikibase:language "en".
    }
  }ORDER BY DESC(?property)  DESC(?hyperq) 
'''

#*----- Queries of hyperRel left path -----
query_only_hyperRel_left = '''SELECT DISTINCT ?property ?propertyLabel ?hyperq ?hyperqLabel 
  WHERE {
    %(target_resource2)s %(target_prop)s ?statement .
    ?statement ?ps %(target_resource)s .
    
    ?property wikibase:claim ?p.
    ?property wikibase:statementProperty ?ps.
    %(filter_in)s
    ?statement ?pq %(target_qualifier)s .
    ?hyperq wikibase:qualifier ?pq .
    SERVICE wikibase:label { bd:serviceParam wikibase:language "en".
    }
  }ORDER BY DESC(?property)  DESC(?hyperq)
'''
#*==============================
#*==============================
#*----- Queries of hyperRel where two TE as qualifiers -----
query_twoTE_as_qualifiers = '''SELECT DISTINCT ?property ?propertyLabel ?hyperq ?hyperqLabel ?hyperq2 ?hyperq2Label 
  WHERE {
    ?val1 ?p ?statement .
    ?statement ?ps ?val2 .
    
    ?property wikibase:claim ?p.
    ?property wikibase:statementProperty ?ps.
    %(filter_in)s
    ?statement ?pq %(target_resource)s .
    ?statement ?pq2 %(target_resource2)s .
    ?hyperq wikibase:qualifier ?pq .
    ?hyperq2 wikibase:qualifier ?pq2 .
    SERVICE wikibase:label { bd:serviceParam wikibase:language "en".
    }
  }ORDER BY DESC(?property)  DESC(?hyperq) 
'''
#==================================NOT USED=====================================
#*----- Queries of Right path ----- #?object ?objectLabel
query_right_and_hyperRel = '''SELECT DISTINCT ?property ?propertyLabel ?hyperq ?hyperqLabel 
  WHERE {
    %(target_resource)s ?p ?statement .
    ?statement ?ps ?object .
    
    ?property wikibase:claim ?p.
    ?property wikibase:statementProperty ?ps.
    %(filter_in)s
    OPTIONAL{
        ?statement ?pq ?qualifier .
        ?hyperq wikibase:qualifier ?pq .
    }
    SERVICE wikibase:label { bd:serviceParam wikibase:language "en".
    }
  }ORDER BY DESC(?property)  DESC(?hyperq) 
'''

#*----- Queries of left path ----- #?object ?objectLabel
query_left_and_hyperRel = '''SELECT DISTINCT ?property ?propertyLabel ?hyperq ?hyperqLabel 
  WHERE {
    ?subject ?p ?statement .
    ?statement ?ps %(target_resource)s .
    
    ?property wikibase:claim ?p.
    ?property wikibase:statementProperty ?ps.
    %(filter_in)s
    OPTIONAL{
        ?statement ?pq ?qualifier .
        ?hyperq wikibase:qualifier ?pq .
    }
    SERVICE wikibase:label { bd:serviceParam wikibase:language "en".
    }
  }ORDER BY DESC(?property)  DESC(?hyperq)
'''
#*==============================
#*==============================
#*----- Queries of Right Right path twoHops ----- TE P1 OBJ1 - OBJ1 P2 OBJ2/TE2
query_RR_twoHops = '''SELECT DISTINCT  %(selectQ)s 
    WHERE {   
    %(target_resource)s  ?p1 ?obj1;
          rdfs:label ?obj1Label.
    ?obj1 ?p2  %(target_resource2)s.
    FILTER(LANG(?obj1Label) = "en").
    #SERVICE wikibase:label { bd:serviceParam wikibase:language "en". } 
    %(directClaim)s

     }LIMIT 10000'''
#*----- Queries of Right Left path twoHops ----- TE P1 OBJ1 - OBJ2/TE2 P2 OBJ1
query_RL_twoHops = '''SELECT DISTINCT  %(selectQ)s  
    WHERE {   
    %(target_resource)s  ?p1 ?obj1;
          rdfs:label ?obj1Label.
    %(target_resource2)s  ?p2 ?obj1.
    FILTER(LANG(?obj1Label) = "en").
    #SERVICE wikibase:label { bd:serviceParam wikibase:language "en". } 
    %(directClaim)s
    }LIMIT 10000'''

#*----- Queries of Left Right path twoHops ----- OBJ1 P1 TE - OBJ1 P2 OBJ2/TE2
query_LR_twoHops = '''SELECT DISTINCT  %(selectQ)s 
    WHERE {   
    ?obj1 ?p1 %(target_resource)s;
          rdfs:label ?obj1Label.
    ?obj1 ?p2 %(target_resource2)s.
    FILTER(LANG(?obj1Label) = "en").
    #SERVICE wikibase:label { bd:serviceParam wikibase:language "en". } 
    %(directClaim)s
    }LIMIT 10000'''
#*----- Queries of Left Left path twoHops ----- OBJ1 P1 TE - OBJ2/TE2 P2 OBJ1
query_LL_twoHops = '''SELECT DISTINCT  %(selectQ)s 
    WHERE {   
    ?obj1 ?p1 %(target_resource)s;
          rdfs:label ?obj1Label.
    %(target_resource2)s  ?p2 ?obj1.
    FILTER(LANG(?obj1Label) = "en").
    #SERVICE wikibase:label { bd:serviceParam wikibase:language "en". } 
    %(directClaim)s
     }LIMIT 10000'''
#*==============================
#*==============================
#* TE P1 OBJ1 - OBJ1 P2 OBJ2 - OBJ1 P3 TE2
#* P1 P2, P3
#*----- Queries of Right Right path twoHops Prod ----- 
query_RR_prod_twoHops = '''SELECT DISTINCT  ?obj1 ?p1 ?p2 ?p3
    WHERE {   
    %(target_resource)s  ?p1 ?obj1;
          rdfs:label ?obj1Label.
    ?obj1 ?p2  %(target_resource2)s.
    %(prod_statement)s
    
    FILTER(LANG(?obj1Label) = "en").
    FILTER(STRSTARTS(str(?p2), 'http://www.wikidata.org/prop/direct/')) .
    FILTER(STRSTARTS(str(?p3), 'http://www.wikidata.org/prop/direct/')) .
    %(emb_filter)s
     }LIMIT 10000'''

query_RL_prod_twoHops = '''SELECT DISTINCT  ?obj1 ?p1 ?p2 ?p3
    WHERE {   
    %(target_resource)s  ?p1 ?obj1;
          rdfs:label ?obj1Label.
    %(target_resource2)s ?p2  ?obj1.
    %(prod_statement)s
    
    FILTER(LANG(?obj1Label) = "en").
    FILTER(STRSTARTS(str(?p2), 'http://www.wikidata.org/prop/direct/')) .
    FILTER(STRSTARTS(str(?p3), 'http://www.wikidata.org/prop/direct/')) .
    %(emb_filter)s
     }LIMIT 10000'''

query_LR_prod_twoHops = '''SELECT DISTINCT  ?obj1 ?p1 ?p2 ?p3
    WHERE {   
   ?obj1 ?p1 %(target_resource)s ;
          rdfs:label ?obj1Label.
    ?obj1 ?p2 %(target_resource2)s.
    %(prod_statement)s
    
    FILTER(LANG(?obj1Label) = "en").
    FILTER(STRSTARTS(str(?p2), 'http://www.wikidata.org/prop/direct/')) .
    FILTER(STRSTARTS(str(?p3), 'http://www.wikidata.org/prop/direct/')) .
    %(emb_filter)s
     }LIMIT 10000'''
query_LL_prod_twoHops = '''SELECT DISTINCT  ?obj1 ?p1 ?p2 ?p3
    WHERE {   
   ?obj1 ?p1 %(target_resource)s ;
          rdfs:label ?obj1Label.
    %(target_resource2)s ?p2  ?obj1.
    %(prod_statement)s
    
    FILTER(LANG(?obj1Label) = "en").
    FILTER(STRSTARTS(str(?p2), 'http://www.wikidata.org/prop/direct/')) .
    FILTER(STRSTARTS(str(?p3), 'http://www.wikidata.org/prop/direct/')) .
    %(emb_filter)s
     }LIMIT 10000'''

#*==============================
#*==============================
#*----- Two TE Queries of Right Right path ----- TE1 P1 OBJ1 - OBJ1 P2 TE2
query_RR_twoTE = '''SELECT DISTINCT  ?obj1
    WHERE {   
    %(target_resource)s %(target_prop)s ?obj1; rdfs:label ?obj1Label.
    ?obj1 ?p2 %(target_resource2)s. 
    FILTER(LANG(?obj1Label) = "en").
    }LIMIT 1000'''

#*----- Two TE Queries of Right Left path ----- TE1 P1 OBJ1 - TE2 P2 OBJ1
query_RL_twoTE = '''SELECT DISTINCT  ?obj1
    WHERE {   
    %(target_resource)s %(target_prop)s ?obj1; rdfs:label ?obj1Label.
    %(target_resource2)s ?p2 ?obj1. 
    FILTER(LANG(?obj1Label) = "en").
    }LIMIT 1000'''
#*----- Two TE Queries of Left Right path ----- OBJ1 P1 TE1 - OBJ1 P2 TE2
query_LR_twoTE = '''SELECT DISTINCT  ?obj1
    WHERE {   
    ?obj1 %(target_prop)s %(target_resource)s; rdfs:label ?obj1Label.
    ?obj1 ?p2 %(target_resource2)s. 
    FILTER(LANG(?obj1Label) = "en").
    }LIMIT 1000'''
#*----- Two TE Queries of Left Left path ----- OBJ1 P1 TE1 - TE2 P2 OBJ1
query_LL_twoTE = '''SELECT DISTINCT  ?obj1
    WHERE {   
    ?obj1 %(target_prop)s %(target_resource)s; rdfs:label ?obj1Label.
    %(target_resource2)s ?p2 ?obj1. 
    FILTER(LANG(?obj1Label) = "en").
    }LIMIT 1000'''
#*==============================
#*==============================
#*----- Two TE Queries of Right path ----- TE1 P1 TE2
query_right_twoTE = '''SELECT DISTINCT  ?property ?propertyLabel
    WHERE {
    %(target_resource)s ?p ?statement .
    ?statement ?ps %(target_resource2)s.
    
    ?property wikibase:claim ?p.
    ?property wikibase:statementProperty ?ps.

    %(filter_in)s
    SERVICE wikibase:label { bd:serviceParam wikibase:language "en".}
    }'''
#*----- Two TE Queries of Right path ----- TE2 P1 TE1
query_left_twoTE = '''SELECT DISTINCT  ?property ?propertyLabel
    WHERE {
    %(target_resource2)s ?p ?statement .
    ?statement ?ps %(target_resource)s.
    
    ?property wikibase:claim ?p.
    ?property wikibase:statementProperty ?ps.

    %(filter_in)s
    SERVICE wikibase:label { bd:serviceParam wikibase:language "en".}
    }'''

query_get_label_topicEntity = '''SELECT DISTINCT ?label  
WHERE { %(target_resource)s rdfs:label ?label .FILTER (langMatches( lang(?label), "EN" ) )} 
LIMIT 1
'''

#ASK where { <http://www.wikidata.org/entity/Q6581097> <http://www.wikidata.org/prop/direct/P21> ?o}
query_ask_right = '''ASK where { <http://www.wikidata.org/entity/%(target_resource)s> <http://www.wikidata.org/prop/direct/%(target_prop)s> ?o}
'''

#ASK where { ?s <http://www.wikidata.org/prop/direct/P21> <http://www.wikidata.org/entity/Q6581097>}
query_ask_left = '''ASK where { ?s <http://www.wikidata.org/prop/direct/%(target_prop)s> <http://www.wikidata.org/entity/%(target_resource)s>}
'''
#ASK where { <http://www.wikidata.org/entity/Q6581097> <http://www.wikidata.org/prop/direct/P21> ?o}
query_ask_triple = '''ASK where { <http://www.wikidata.org/entity/%(target_resource)s> <http://www.wikidata.org/prop/direct/%(target_prop)s> <http://www.wikidata.org/entity/%(target_resource2)s> }
'''

dict_sparqlQueries = {
    "query_right_oneHope": query_right_oneHope,
    "query_left_oneHope": query_left_oneHope,
    "query_RR_twoHops": query_RR_twoHops,
    "query_RL_twoHops": query_RL_twoHops,
    "query_LR_twoHops": query_LR_twoHops,
    "query_LL_twoHops": query_LL_twoHops,
    "query_RR_twoTE": query_RR_twoTE,
    "query_RL_twoTE": query_RL_twoTE,
    "query_LR_twoTE": query_LR_twoTE,
    "query_LL_twoTE": query_LL_twoTE,
    'query_RR_prod_twoHops': query_RR_prod_twoHops,
    'query_RL_prod_twoHops': query_RL_prod_twoHops,
    'query_LR_prod_twoHops': query_LR_prod_twoHops,
    'query_LL_prod_twoHops': query_LL_prod_twoHops,
    "query_right_twoTE": query_right_twoTE,
    "query_left_twoTE": query_left_twoTE,
    "query_only_hyperRel_right": query_only_hyperRel_right,
    "query_only_hyperRel_left": query_only_hyperRel_left,
    "query_twoTE_as_qualifiers": query_twoTE_as_qualifiers,
    "query_right_and_hyperRel": query_right_and_hyperRel,
    "query_left_and_hyperRel": query_left_and_hyperRel,
    "query_get_label_topicEntity": query_get_label_topicEntity,
    "query_ask_right": query_ask_right,
    "query_ask_left": query_ask_left,
    "query_ask_triple": query_ask_triple
}


def get_query_results(query, write_queryMsg=None):
    endpoint_url = get_endpointURL()
    user_agent = "WDQS-example Python/%s.%s" % (
        sys.version_info[0], sys.version_info[1])
    # TODO adjust user agent; see https://w.wiki/CX6
    #user_agent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11"
    sparql = SPARQLWrapper(endpoint_url, agent=user_agent)
    sparqlResult = []
    try:
      sparql.setQuery(query)
      sparql.setReturnFormat(JSON)
      # sparql.setTimeout(0.1)
      sparqlResult = sparql.query().convert()
    except:
       if(write_queryMsg):
          if (write_queryMsg[1].find('filter')):
            get_filter = write_queryMsg[1].split("\t")
            newMsg = get_filter[0] + "\t" + \
                get_filter[1] + "\t" + "timeout to retrieve"
            if len(get_filter) == 3:
              filterNo = get_filter[2].replace("filter:", '')
              newMsg = get_filter[0] + "\t" + \
                  get_filter[1] + "\t" + "filter:" + filterNo

            print(newMsg)
            sparqlResult = ["error"]
          else:
            write_to_file(write_queryMsg[0], write_queryMsg[1])
    return sparqlResult

# #* write function to check all predicates in specific filter with topic entity as a triple
# def check_filterPredicates(file_name, topicEntityId, filterPropIds, dir):
#   for propId in filterPropIds:
#     isTriple = ask_triple(topicEntityId, propId, dir)
#     if(isTriple):

#*To get the Entity/Prop label by passing query has the WD identifier
def get_topicEntity_val(entity_id, write_queryMsg=None):
  query = dict_sparqlQueries["query_get_label_topicEntity"] % {
      'target_resource': "wd:"+entity_id}
  results = get_query_results(query, write_queryMsg)
  topicEntity = []
  topicEntity_val = ""
  if(results):
    for result in results["results"]["bindings"]:
        topicEntity_val = result['label']['value']
    topicEntity = [entity_id, topicEntity_val]
  return topicEntity

#*Ask if the triple exist (right or left)


def ask_triple(entity_id, prop_id, dir):
  query = dict_sparqlQueries["query_ask_"+dir] % {
      'target_resource': entity_id, 'target_prop': prop_id}
  results = get_query_results(query)
  return results['boolean']

  #*Ask if the triple exist (right or left)


def ask_triple_full(subj, prop_id, obj):
  query = dict_sparqlQueries["query_ask_triple"] % {
      'target_resource': subj, 'target_prop': prop_id, 'target_resource2': obj}
  results = get_query_results(query)
  return results['boolean']


def write_to_file(file_name, line):
    file_name.write(line + "\n")


def exclude_properties():
  exclude_props_ids_labels = [['P17', 'country'],
                              ['P21', 'sex or gender'],
                              ['P27', 'country of citizenship'],
                              ['P31', 'instance of'],
                              ['P105', 'taxon rank'],
                              ['P106', 'occupation'],
                              ['P364', 'original language of film or TV show'],
                              ['P407', 'language of work or name'],
                              ['P495', 'country of origin'],
                              ['P641', 'sport'],
                              ['P1001', 'applies to jurisdiction'],
                              ['P1412', 'languages spoken, written or signed']]
  exclude_props_ids = ["P17", "P21", "P27", "P31", "P105",
                       "P106", "P364", "P407", "P495", "P641", "P1001", "P1412"]
  exclude_props = {
      "exclude_props_ids_labels": exclude_props_ids_labels,
      "exclude_props_ids": exclude_props_ids}
  return exclude_props


def dict_lcquad_predicates(dir):
    f = open("data/most_used/most_used_predicates_lcquad.txt", 'r')
    out = f.readlines()  # will append in the list out
    get_exclude_props = exclude_properties()
    exclude_props = get_exclude_props["exclude_props_ids_labels"]
    exclude_props_ids = get_exclude_props["exclude_props_ids"]
    filter_start = "FILTER( str(?property) IN ( str('http://www.wikidata.org/entity/C1') "
    filter_end = "))."
    lcquad_props = []
    lcquad_labeled_props = []
    lcquad_props_filters = []
    lcquad_props_all_groups = []
    lcquad_props_group = []
    filter_in = ""
    j = 67  # no of lcquad predicates = 1340, -->1340/67=20
    i = 0
    z = 0
    for id in out:
      i += 1
      arguments = id.split("\t")
      lcquad_props.append(arguments[0])
      lcquad_labeled_props.append([arguments[0],arguments[1]]) #[id,label,count] ===arguments[2].replace('\n','')
      if (i == 1):
        filter_in += filter_start
      if(i > ((j*z)-j) and i <= (j * (z+1))):
        if (dir == "left"):
          if arguments[0] not in exclude_props_ids:
            filter_in += ",str('http://www.wikidata.org/entity/" + \
                arguments[0] + "') "
            lcquad_props_group.append(arguments[0])
        else:
          filter_in += ",str('http://www.wikidata.org/entity/" + \
              arguments[0] + "') "
          lcquad_props_group.append(arguments[0])
      if not (i % j):
        z += 1
        filter_in += filter_end
        # to fill all filters in one list
        lcquad_props_filters.append(filter_in)
        filter_in = filter_start
        # to fill all groups in one list
        lcquad_props_all_groups.append(lcquad_props_group)
        lcquad_props_group = []
    f.close()

    #* for loop for filter Not in
    filter_not_in = "FILTER( str(?property) NOT IN ( str('http://www.wikidata.org/entity/C1') "
    for propId in exclude_props_ids:
      filter_not_in += ", str('http://www.wikidata.org/entity/" + \
          propId + "') "
    filter_not_in += filter_end

    dict_lcquad_props = {
        "lcquad_props": lcquad_props,  # MU LCQuAD Predicates [id]
        "lcquad_labeled_props":lcquad_labeled_props, # MU LCQuAD Predicates [id,label]
        # LCQuAD predicates divided in 20 sparql filters
        "lcquad_props_filters": lcquad_props_filters,
        "lcquad_props_not_in_filters": filter_not_in,
        # LCQuAD predicates in 20 groups
        "lcquad_props_all_groups": lcquad_props_all_groups,
        "exclude_props": exclude_props, 
        "exclude_props_ids": exclude_props_ids
    }
    # print(lcquad_props_filters[19])
    # print(len(lcquad_props_filters))
    return dict_lcquad_props

def mu_prop_lcquad(propVal, type): #type of  propVal-----type val: id or label
  lcquad_props = dict_lcquad_predicates('right')
  mu_props = lcquad_props["lcquad_labeled_props"]
  if type == 'id':
    i = 0 #index of propLabel
    j = 1
  if type == 'label':
    i = 1 #index of propLabel
    j = 0
  prop_indexes = [index for (index, a_tuple) in enumerate(mu_props) if a_tuple[i]==propVal]
  ret_propVal = []
  if prop_indexes:
    prop_index = prop_indexes[0] #normally it's size is 1, anyway we need the first ele in the list
    #if mu_props[prop_index][2] != str(1):#!addition
    ret_propVal = mu_props[prop_index][j]
  return ret_propVal

def dict_sq_predicates(dir):  # if dir=left will exclude some props from the filter
  f = open("data/most_used/most_used_predicates_sq.txt", 'r')
  exclude_props = exclude_properties()
  out = f.readlines()  # will append in the list out
  i = 0
  filter_in = "FILTER( str(?property) IN ( str('http://www.wikidata.org/entity/C1') "
  for prop_id in out:
    if (dir == "left"):
        if prop_id not in exclude_props:
          filter_in += ",str('http://www.wikidata.org/entity/" + \
              prop_id.replace("\n", "") + "') "
    else:
      filter_in += ",str('http://www.wikidata.org/entity/" + \
          prop_id.replace("\n", "") + "') "

  filter_in += "))"
  f.close()
  return filter_in

# return the most used entities in simple question dataset


def cache_sq_entities():
  F_most_used_entities = open("data/most_used/most_used_entities_sq.txt", "r")
  out = F_most_used_entities.readlines()  # will append in the list out
  i = 0
  sq_entities = []
  for line in out:
      i += 1
      arguments = line.split("\t")
      # arguments[0]:Entity id   arguments[1]:occurrence of entity
      sq_entities.append(arguments[0])
  return sq_entities

def cache_lcquad_entities():
  F_most_used_entities = open("data/most_used/most_used_entities_lcquad.txt", "r")
  out = F_most_used_entities.readlines()  # will append in the list out
  i = 0
  lcquad_entities = []
  for line in out:
      i += 1
      arguments = line.split("\t")
      get_ent = arguments[0]
      if arguments[1].startswith('Q'):
        get_ent += ', ' + arguments[1]
      lcquad_entities.append(get_ent)
  return lcquad_entities

#* split LCQuAD2 templates that has only one TE based on it's corechain graph
def lcquad_templates():
  oneHop = [1, 3, 11, 15, 19, 20, 22]
  hyperRel = [6, 7, 9]
  oneTE_twoHops_LR = [12] # {L = P31}
  oneTE_twoHops_RR = [18, 21, 23]
  twoTE_twoHops_LR = [2, 4, 10]  # 2,4:{L = P31} # 10:{R = P31}
  twoTE_twoHops_RR = [5, 21]
  product_oneHop = [24] # +P22, +P25
  product_twoTE_twoHops = [13, 14] # -P31 +P2664, +P50 {L = P31}
  product_threeTE_oneHop = [17] # +P17, +P17
  oneTE_templates = {
      "oneHop": oneHop,
      "hyperRel": hyperRel,
      "oneTE_twoHops_RR": oneTE_twoHops_RR,
      "oneTE_twoHops_LR": oneTE_twoHops_LR,
      "twoTE_twoHops_LR": twoTE_twoHops_LR,
      "twoTE_twoHops_RR": twoTE_twoHops_RR,
      "product_oneHop": product_oneHop,
      "product_twoTE_twoHops": product_twoTE_twoHops,
      "product_threeTE_oneHop":product_threeTE_oneHop}
  return oneTE_templates

def lcquad_ds(quesID, dsType):
  F_lcquad_ds = open("data/lcquad2_dataset/lcquad_" + dsType + "_answer.txt", "r")
  out = F_lcquad_ds.readlines()  # will append in the list out
  i = 0
  lcquad_line_arr = []
  for line in out:
      i += 1
      arguments = line.split("\t")
      # arguments[0]:uid   arguments[1]:TempId    arguments[2]: TEs 
      # arguments[3]:answerCC   arguments[4]: Question
      qUID = arguments[0]
      if quesID == qUID :
        tempID = arguments[1]
        entityIds = arguments[2]
        answerCC = arguments[3]
        lcquadQues = arguments[4].replace('\n', '')
        lcquadQues_Query = arguments[5].replace('\n', '')
        #add them to a list
        lcquad_line_arr = [qUID, tempID, entityIds, answerCC, lcquadQues, lcquadQues_Query]
      
  return lcquad_line_arr
