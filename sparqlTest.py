import csv
import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON
import json
import sys
from sparqlQueries import generate_lcquad_predicates_filters

#import validators

# F_right_oneHope = open("data/right_oneHope.txt", "w")
# F_left_oneHope = open("data/left_oneHope.txt", "w")
# endpoint_url = "https://query.wikidata.org/sparql"


query_right_oneHope = """SELECT DISTINCT ?property ?propLabel ?object ?objectLabel
WHERE {
  wd:Q937 ?property ?object .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en".
  }
?prop wikibase:directClaim ?property.
}
"""

query = query_right_oneHope

def get_query_results(endpoint_url, query):
    user_agent = "WDQS-example Python/%s.%s" % (sys.version_info[0], sys.version_info[1])
    # TODO adjust user agent; see https://w.wiki/CX6
    sparql = SPARQLWrapper(endpoint_url, agent=user_agent)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    return sparql.query().convert()

# results = get_query_results(endpoint_url, query)
# for result in results["results"]["bindings"]:
#     #print(result['object']['value'], result['objectLabel']['value'])
#     line = result['property']['value'] + " " + result['object']['value'] + "\t" + result['propLabel']['value'] + " " + result['objectLabel']['value']
#     F_right_oneHope.write(line)


lcquad_props, lcquad_props_filters = generate_lcquad_predicates_filters()

print(len(lcquad_props_filters))
i = 0
for filter in lcquad_props_filters:
  print(i, filter)
  i += 1
   




# results_df = pd.json_normalize(results['results']['bindings'])
# results_df[['object.value', 'objectLabel.value']].head()
# print(results_df)


# for i, result in enumerate(results_df):
#     print(i, result)
#     if i==1:
#         print(i, result)

# def has_url(_string):
#     if validators.url(_string):
#         return True
#     return False





# entity_uri = "http://www.wikidata.org/entity/Q9373A"

# if has_url(entity_uri):
#     print(entity_uri, ' its a uri')
# else:
#     print(entity_uri, ' its not a uri')

# myid = "wd:Q937"
# sparqlwd.setQuery("SELECT ?s ?p WHERE {{ {myid} ?p ?o .}}")
# sparqlwd.setReturnFormat(JSON)
# results = sparqlwd.query().convert()
# print(results)
# results_df = pd.io.json.json_normalize(results['results']['bindings'])
# #print(results_df)
# # Serializing json
# json_object = json.dumps(results_df, indent=4)
# # Writing to sample.json
# with open("data.json", "w") as outfile:
#     outfile.write(json_object)


# # From https://www.wikidata.org/wiki/Wikidata:SPARQL_query_service/queries/examples#Cats
# sparqlwd.setQuery("""
# SELECT ?item ?itemLabel
# WHERE
# {
#     ?item wdt:P31 wd:Q146 .
#     SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
# }
# """)
# sparqlwd.setReturnFormat(JSON)
# results = sparqlwd.query().convert()
# results_df = pd.json_normalize(results['results']['bindings'])
# results_df[['item.value', 'itemLabel.value']].head()
# print(results_df)
