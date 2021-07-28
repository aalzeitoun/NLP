# pip install sparqlwrapper
# https://rdflib.github.io/sparqlwrapper/

import re
import random
import time
from SPARQLWrapper import SPARQLWrapper, JSON
from sparqlQueries import ask_triple, write_to_file, get_query_results, get_topicEntity_val, dict_lcquad_predicates, dict_sparqlQueries

F_cache_error_not_fixed = ""#open("data/cache_error_not_fixed.txt", "w")
F_cache_error_fixed = ""#open("data/cache_error_fixed.txt", "w")

#! usable more then once


def shuffle_sq():
    with open('data/seprated_train/corechain_final.txt', 'r') as source:
        data = [(random.random(), line) for line in source]
    data.sort()
    with open('data/seprated_train/corechains_final_shuffled.txt', 'w') as target:
        for _, line in data:
            target.write(line)

#! usable more then once


def remove_occurencesLines_in_file():
    # #file has ocurencess
    f = open("data/corechains_cache_sq.txt", 'r')
    # #new file
    f_new = open("data/corechains_cache_sq_new.txt", 'w+')

    out = f.readlines()
    print(len(out))
    print(len(set(out)))
    new_out = set(out)
    new_final = []
    for line in new_out:
        new_final.append(line.replace('\n', ''))
    new_final.sort()
    for line in new_final:
        write_to_file(f_new, line.replace('\n', ''))
  

remove_occurencesLines_in_file()

#correct_corechains_cache()


# F_train_q = open("data/seprated_train/train_q.txt", "w")
# F_test_q = open("data/seprated_train/test_q.txt", "w")
# F_dev_q = open("data/seprated_train/dev_q.txt", "w")

def write_from_to(file_name,cc_matching):
    for cc in cc_matching:
        # print(cc.replace('\n', ''))
        write_to_file(file_name, str(cc).replace('\n', ''))

def reduce_testData_ds():
    f = open("data/corechains_train_all.txt", 'r')
    out = f.readlines()  # will append in the list out
    # if any(split_tab in s for s in out):
    cc_matching_train = [s for s in out if 'train	Q' in s]
    write_from_to(F_train_q, cc_matching_train)

    cc_matching_test = [s for s in out if 'test	Q' in s]
    write_from_to(F_test_q, cc_matching_test)

    cc_matching_dev = [s for s in out if 'dev\tQ' in s]
    write_from_to(F_dev_q, cc_matching_dev)
# reduce_testData_ds()


# F_test_q0 = open("data/seprated_train/test_q_0.txt", "w")
# F_test_q1 = open("data/seprated_train/test_q_1.txt", "w")
# F_dev_q0 = open("data/seprated_train/dev_q_0.txt", "w")
# F_dev_q1 = open("data/seprated_train/dev_q_1.txt", "w")

#! replace some string in line in text file based in split line
def change_devData_ds():
    f = open("data/seprated_train/dev_q.txt", 'r')
    out = f.readlines()  # will append in the list out
    print(len(out))

    #split line
    split_index1 = out.index(
        "dev	Q2115340	+	0	What is darko jevtić's gender?	+given name	+P735\n")
    out2 = out[:split_index1] #everythin after
    print(len(out2))
    
    split_index2 = out.index(
        "dev	Q2115340	+	0	What is darko jevtić's gender?	+given name	+P735\n")
    out3 = out[split_index2:]#everythin before
    print(len(out3))
    out3 = [w.replace('dev	', 'train	') for w in out3]

    write_from_to(F_dev_q0, out2)
    write_from_to(F_dev_q1, out3)


def change_testData_ds():
    f = open("data/seprated_train/test_q.txt", 'r')
    out = f.readlines()  # will append in the list out
    print(len(out))
    split_index1 = out.index(
        "test	Q381110	+	0	What is the birth place of raoul bova?	+official website	+P856\n")
    out2 = out[:split_index1]
    print(len(out2))

    split_index2 = out.index(
        "test	Q381110	+	0	What is the birth place of raoul bova?	+official website	+P856\n")
    out3 = out[split_index2:]
    print(out3[0])
    print(len(out3))
    out3 = [w.replace('test	', 'train	') for w in out3]

    write_from_to(F_test_q0, out2)
    write_from_to(F_test_q1, out3)


# F_corechain_final = open("data/seprated_train/corechain_final.txt", 'w')
def combine_all_together():
    f_dev_q0 = open("data/seprated_train/dev_q_0.txt", 'r')
    f_dev_q1 = open("data/seprated_train/dev_q_1.txt", 'r')
    f_test_q0 = open("data/seprated_train/test_q_0.txt", "r")
    f_test_q1 = open("data/seprated_train/test_q_1.txt", "r")
    f_train = open("data/seprated_train/train_q.txt", "r")

    out_dev_q0 = f_dev_q0.readlines()
    out_dev_q1 = f_dev_q1.readlines()
    out_test_q0 = f_test_q0.readlines()
    out_test_q1 = f_test_q1.readlines()
    out_train  = f_train.readlines()
    
    #train omly
    write_from_to(F_corechain_final, out_train)
    write_from_to(F_corechain_final, out_dev_q1)
    write_from_to(F_corechain_final, out_test_q1)
    # dev and test
    write_from_to(F_corechain_final, out_dev_q0)
    write_from_to(F_corechain_final, out_test_q0)

#correct the score for the question that has reverse predicate
def correct_score_train_data():
    print("hi")
