#import sys
import time
#import pandas as pd

#generate the most used predicate of simple question data set and write them to most_used_predicates_sq.txt
# def get_predicates_simpleQuestion():
#     F_most_used_predicates = open("data/most_used_predicates_sq.txt", "w")
#     f = open("data/question_answerable.txt", 'r')
#     out = f.readlines()  # will append in the list out
#     i = 0
#     answerble_predicate = []

#     start_time = time.time()
#     for line in out:
#         # arguments[0]:topic entity   arguments[1]:predicat   arguments[2]:answer   arguments[3]:question
#         i += 1
#         arguments = line.split("\t")
#         predicat_id = arguments[1].replace("R", "P")
#         answerble_predicate.append(predicat_id)

#     answerble_predicate.sort()
#     answerble_predicate = list(set(answerble_predicate))
#     print("Size after:", len(answerble_predicate))
#     #print(answerble_predicate)

#     #write predicates in a file
#     for predicate in answerble_predicate:
#       F_most_used_predicates.write(predicate + "\n")  # 

#     elapsed_time = time.time() - start_time
#     F_most_used_predicates.close()
#     print(elapsed_time)

# #get_predicates_simpleQuestion()


# set_filter = generate_sparqle_predicates_filter()
# print(set_filter)


# def combine_lcquad_sq_predicates():
#     f_sq_predicates = open("data/most_used_predicates_sq.txt", 'r')
#     f_lcquad_predicates = open("data/most_used_predicates_lcquad.txt", 'r')
#     all_lcquad_sq_props = []
#     all_sq_props = []
#     all_lcquad_props = []
#     sq_props = f_sq_predicates.readlines()  # will append in the list out
#     for sqProp in sq_props:
#       all_sq_props.append(sqProp.replace("\n", ""))

#     f_sq_predicates.close()

#     lcquad_props = f_lcquad_predicates.readlines()  # will append in the list out
#     for lcquadProp in lcquad_props:
#       arguments = lcquadProp.split("\t")
#       all_lcquad_props.append(arguments[0].replace("\n", ""))

#     f_lcquad_predicates.close()
#     # print('all_lcquad_props', all_lcquad_props)
#     # print('all_sq_props', all_sq_props)
#     print (len( set(all_lcquad_props) & set(all_sq_props)))
#     diff = set(all_sq_props) - set(all_lcquad_props)
#     #intersect_diff = set(all_lcquad_props) - set(all_sq_props)
#     #remainder_diff = set()
#     print(diff)
