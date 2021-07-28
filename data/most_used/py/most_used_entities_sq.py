#import sys
import time
#import pandas as pd
import collections
import operator


def get_entities_simpleQuestion():
    f = open("data/question_answerable.txt", 'r')
    F_most_used_entities = open("data/most_used_entities_sq.txt", "w")

    out = f.readlines()  # will append in the list out
    i = 0
    allEntity = []
    allTopicEntity_ids = []
    allAnswerEntity_ids = []

    start_time = time.time()
    for line in out:
        # arguments[0]:topic entity   arguments[1]:predicat   arguments[2]:answer   arguments[3]:question
        i += 1
        arguments = line.split("\t")
        topicEntity_id = arguments[0].upper()
        allTopicEntity_ids.append(topicEntity_id)
        answerEntity_id = arguments[2].upper()
        allAnswerEntity_ids.append(answerEntity_id)
        allEntity.append(topicEntity_id)
        allEntity.append(answerEntity_id)

    f.close()
    allEntity.sort()
    entity_occurrences = collections.Counter(allEntity)
    print ("Size All Entity before:", len(allEntity))
    allEntity = list(set(allEntity))
    print("Size All Entity after:", len(allEntity))
    
    allTopicEntity_ids.sort()
    occurrences_allTopicEntity = collections.Counter(allTopicEntity_ids)
    print("Size All Topic Entity before:", len(allTopicEntity_ids))
    allTopicEntity_ids = list(set(allTopicEntity_ids))
    print("Size All Topic Entity after:", len(allTopicEntity_ids))
    
    allAnswerEntity_ids.sort()
    occurrences_allAnswerEntity = collections.Counter(allAnswerEntity_ids)
    print("Size All Answer Entity before:", len(allAnswerEntity_ids))
    allAnswerEntity_ids = list(set(allAnswerEntity_ids))
    print("Size All Answer Entity after:", len(allAnswerEntity_ids))

    print("Size of shared entities between both topic and answer:",
          len(set(allTopicEntity_ids) & set(allAnswerEntity_ids)))
    print("\n")

    print(len(entity_occurrences))
    #print(entity_occurrences)
    #print("occurrences_allTopicEntity", occurrences_allTopicEntity)
    #print("occurrences_allAnswerEntity", occurrences_allAnswerEntity)
    
    #!get occurrences_of_occurrences
    entity_occurrences_counter = []

    #sort the list decending based on the occurrences
    sorted_entity_occurrences = dict(
        sorted(entity_occurrences.items(), key=operator.itemgetter(1), reverse=True))
    j=0
    for entity in sorted_entity_occurrences:
        #entity_occurrences_counter.append(entity_occurrences[entity])
        if (sorted_entity_occurrences[entity] > 1):
            #write entiity and it's occurrence to a file
            F_most_used_entities.write(
                entity + "\t" + str(sorted_entity_occurrences[entity]) + "\n")
        
    F_most_used_entities.close()


    # occurrences_of_occurrences = collections.Counter(entity_occurrences_counter)
    # print("occurrences_of_occurrences size:", len(occurrences_of_occurrences))
    # print (occurrences_of_occurrences)


    
#get_entities_simpleQuestion()

#* Size All Entity before: 50206
# Size All Entity after: 31747
#* Size All Topic Entity before: 25103
# Size All Topic Entity after: 22005
#* Size All Answer Entity before: 25103
# Size All Answer Entity after: 11196
#* Size of commen entities between both topic and answer: 1454
# 85 Counter({1: 28954, 2: 1697, 3: 360, 4: 164, 5: 111, 6: 65, 7: 57, 9: 31,
#         10: 31, 8: 26, 12: 21, 11: 20, 21: 16, 13: 14, 14: 12, 18: 11, 
#         17: 9, 15: 8, 19: 8, 20: 7, 23: 7, 28: 6, 49: 5, 31: 5, 16: 5, 
#         41: 4, 67: 4, 24: 4, 25: 4, 22: 3, 103: 3, 27: 3, 34: 3, 36: 3, 
#         33: 3, 58: 2, 43: 2, 26: 2, 276: 2, 53: 2, 57: 2, 39: 2, 30: 2, 
#         119: 2, 29: 2, 40: 2, 46: 2, 32: 2, 144: 1, 132: 1, 366: 1, 153: 1, 
#         133: 1, 110: 1, 35: 1, 164: 1, 574: 1, 50: 1, 37: 1, 324: 1, 84: 1, 
#         125: 1, 271: 1, 77: 1, 237: 1, 1393: 1, 141: 1, 60: 1, 111: 1, 116: 1, 
#         242: 1, 85: 1, 128: 1, 66: 1, 62: 1, 106: 1, 390: 1, 1616: 1, 81: 1, 
#         138: 1, 122: 1, 44: 1, 105: 1, 64: 1, 76: 1})




array1 = ["Q123", "Q345", "Q6789", "Q987", "Q654",
          "Q321", "Q0510", "Q123", "Q345", "Q6789", "Q123"]
array2 = ["Q123", "Q30045", "Q6789", "Q91187", "Q654", "Q35521"]

#array3 = set(array2) & set(array1)
array3 = collections.Counter(array1)

# print(array3)
# print (array3[list(array3.keys())[0]])
