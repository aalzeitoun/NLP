from sentence_transformers import SentenceTransformer, util
from datetime import datetime
import re
import random
import time
from sparqlQueries import write_to_file, mu_prop_lcquad

from scipy.spatial.distance import cosine

# modelType = 'e1_b16'
# model_save_path = 'models/lcquad_e1_b16_distilbert-base-uncased-2021-03-21_14-20-47'

# modelType = 'e1_b64'
# model_save_path = 'models/lcquad_e1_b64_distilbert-base-uncased-2021-03-20_21-28-06'

# modelType = 'e2_b64'
# model_save_path = 'models/lcquad_e2_b64_distilbert-base-uncased-2021-03-23_08-06-11'

# modelType = 'e4_b16'
# model_save_path = 'models/lcquad_e4_b16_distilbert-base-uncased-2021-03-22_12-36-35'

# modelType = 'e5_b16'
# model_save_path = 'models/lcquad_e5_b16_distilbert-base-uncased-2021-03-25_23-57-27'


# modelType = 'e5_b64'
# model_save_path = 'models/lcquad_e5_b64_distilbert-base-uncased-2021-03-24_22-14-29'

modelType = 'e6_b64'
model_save_path = 'models/lcquad_e6_b64_distilbert-base-uncased-2021-07-14_09-38-36'


# modelType = 'e7_b64'
# model_save_path = 'models/lcquad_e7_b64_distilbert-base-uncased-2021-07-14_13-03-51'

# modelType = 'e8_b32'
# model_save_path = 'models/lcquad_e8_b32_distilbert-base-uncased-2021-07-15_17-19-08'

# modelType = 'e10_b64'
# model_save_path = 'models/lcquad_e10_b64_distilbert-base-uncased-2021-03-23_16-54-19'

#-----------
# modelType = 'e3_b64_stsb-mpnet'
# model_save_path = 'models/lcquad_e3_b64_sentence-transformers-stsb-mpnet-base-v2-2021-07-23_19-37-11'

# modelType = 'e5_b64_paraphrase-mpnet'
# model_save_path = 'models/lcquad_e5_b64_paraphrase-mpnet-base-v2-2021-07-22_22-34-04'



print(modelType)

F_sbert = open("data/lcquad_sbert_" + modelType + ".txt", "w")
F_sbert_terminal = open("data/lcquad_sbert_terminal_" + modelType + ".txt", "w")

def reform_givenAnswer(correctAns):
    div_parts_noSign = correctAns.replace(',','')
    div_parts = div_parts_noSign.split(' ')
    new_correctAns_arr = []
    
    # ex: [+P1, +P2]
    if len(div_parts) == 2: 
        sign1= div_parts[0][0]
        sign2= div_parts[1][0]
        if sign2 != '*':
            prop1_id = div_parts[0]
            prop2_id = div_parts[1]
            
            prop1_lbl = sign1 + mu_prop_lcquad(prop1_id.replace(sign1,''), 'id')
            prop2_lbl = sign2 + mu_prop_lcquad(prop2_id.replace(sign2,''), 'id')

            cc_ids = prop2_id + ", " + prop1_id
            cc_lbl = prop2_lbl + ", " + prop1_lbl 
            new_correctAns_arr = [cc_ids, cc_lbl]          
    
    # ex: [-P1 +P2, +P3]    or   [+P1 *P2, *P3]
    if len(div_parts) == 3: 
        sign1= div_parts[0][0]
        sign2= div_parts[1][0]
        sign3= div_parts[2][0]

        prop1_id = div_parts[0]
        prop2_id = div_parts[1]
        prop3_id = div_parts[2]

        prop1_lbl = sign1 + mu_prop_lcquad(prop1_id.replace(sign1,''), 'id')
        prop2_lbl = sign2 + mu_prop_lcquad(prop2_id.replace(sign2,''), 'id')
        prop3_lbl = sign3 + mu_prop_lcquad(prop3_id.replace(sign3,''), 'id')

        cc_ids = prop1_id + ' ' + prop3_id + ", " + prop2_id
        cc_lbl = prop1_lbl + ' ' + prop3_lbl + ", " + prop2_lbl
        new_correctAns_arr = [cc_ids, cc_lbl]

    return new_correctAns_arr

def sbert_answers(question_val, correctAns, corechains):
    # correctAns = correctAns.replace(', ', ' ') 
    # print('correctAns', correctAns)
    #corechains = [w.replace(', ', ' ') for w in corechains]
    
    # model = SentenceTransformer(model_save_path)
    model = SentenceTransformer('stsb-roberta-large')
    
    id_CC = [item[0]for item in corechains] #added replace   .replace(', ', ' ') 
    # print (id_CC)
    lbl_CC = [item[1] for item in corechains] #added replace
    # print (lbl_CC)


    question = []
    top_5 = []
    corpus = lbl_CC
    #Encode all sentences
    corpus_embeddings = model.encode(corpus)

    question.append(question_val)
    # Encode sentences:
    query_embeddings = model.encode(question)

    #-------------------------Paraphrase Mining-------------------------------
    # corpus2 = corpus
    # corpus2 = corpus2.append(question_val)
    # paraphrases = util.paraphrase_mining(model, corpus2)

    # for paraphrase in paraphrases[0:10]:
    #     score, i, j = paraphrase
    #     print("{} \t\t {} \t\t Score: {:.4f}".format(
    #         corpus2[i], corpus2[j], score))
    #--------------------------------------------------------------------------

    # for sent in corpus:
    #     sim = cosine(query_embeddings, model.encode([sent])[0])
    #     print("Sentence = ", sent, "; similarity = ", sim)

     #-------------- using pytorch
    #Compute cosine similarity between all pairs
    cos_sim = util.pytorch_cos_sim(corpus_embeddings, query_embeddings)

    #Add all pairs to a list with their cosine similarity score
    all_sentence_combinations = []
    for i in range(len(cos_sim)):
        all_sentence_combinations.append([cos_sim[i], i])

    #Sort list by the highest cosine similarity score
    all_sentence_combinations = sorted(all_sentence_combinations, key=lambda x: x[0], reverse=True)
    # print("\n=========== pytorch cosine ===========\n")
    #print("Top-5 most similar pairs:")
    for score, i in all_sentence_combinations[0:5]:  # len(corpus)
        # print("{} \t {:.4f}".format(corpus[i], cos_sim[i][0]))
        top_5.append(corpus[i])
        # print(corpus[i], cos_sim[i][0])  

    print('top answer:', top_5[0])
    write_to_file(F_sbert_terminal, 'top answer: ' + top_5[0])

    foundFirst = 0
    topFive = 0
    notTopFive = 0
    
    
    # top1 id and label
    index_ans = lbl_CC.index(top_5[0])
    top1_ccId = corechains[index_ans][0]
    top1_cclbl = top_5[0]
    
    #given answer Id and label
    if correctAns in id_CC:
        index_given_ans = id_CC.index(correctAns)

    given_ccId = correctAns
    given_cclbl = corechains[index_given_ans][1]

    reform_Ans_arr = [given_ccId, given_cclbl]


    #if the given answer is the top answer
    if(top1_ccId == given_ccId):
        foundFirst = 1

    # if the given answer not the top answer but in the top 5
    if(given_cclbl in top_5) and (top1_ccId != given_ccId):
        topFive = 1
    
    # if the given answer not the top answer but in the top 5
    if(given_cclbl not in top_5):
        notTopFive = 1

    gAnswer_topAnswer = [given_cclbl, top1_cclbl]

    testResult = [foundFirst, topFive, notTopFive]
    return testResult, gAnswer_topAnswer

#============================================================================
#========  put lbl, ids of corechains for specific TE in one list
#============================================================================
def lcquad_test_corechain(qUID, entityIds, question_val, correctAns, test_corechain):
    corechains_labels = []  
    corechains_ids = []
    isFound = False
    i = 0
    indexAns = 0

    reform_Ans_arr = reform_givenAnswer(correctAns) # return [id, label]
    isReform = False
    for ccLine in test_corechain:
        i += 1
        arguments = ccLine.split("\t")
        cc_entIds = arguments[0]
        cc_sign = arguments[1]
        cc_lbl = arguments[2]
        cc_id = arguments[3].replace("\n","")

        
        if correctAns == cc_id:
            isFound = True
            indexAns = i - 1

        if reform_Ans_arr and reform_Ans_arr[0] == cc_id:
            isFound = True
            indexAns = i - 1
            isReform = True

        corechains_labels.append(cc_lbl)
        corechains_ids.append(cc_id)
     
    corechains = list(zip(corechains_ids,corechains_labels))

    final_testResult = []
    gAnswer_topAnswer = []
    if isFound :
        print(qUID, entityIds, correctAns, len(test_corechain), question_val)
        write_to_file(F_sbert_terminal, str(qUID) + '\t' + entityIds + '\t' + correctAns + '\t' + str(len(test_corechain)) + '\t' + question_val)
        
        print(corechains[indexAns], '\n')
        write_to_file(F_sbert_terminal, ', '.join(corechains[indexAns]))

        if isReform : correctAns = reform_Ans_arr[0] # set the reform of the given answer to  be the given answer..
        testResult, gAnswer_topAnswer = sbert_answers(question_val, correctAns, corechains)
        final_testResult = [testResult[0], testResult[1], testResult[2], 1]
    else:
        final_testResult = [0, 0, 0, 0]

    return final_testResult, gAnswer_topAnswer

#============================================================================
#========  read question (Quest, answer, TE) and extract corechains
#============================================================================
def lcquad_test_questions():
    F_test_corechains = open("data/lcquad_test/lcquad_test_corechain.txt", "r")
    all_test_corechains = F_test_corechains.readlines()

    # F_sbert_fault = open("data/lcquad_test_sbert_faults.txt", "w")

    F_test_questions = open("data/lcquad2_dataset/lcquad_test_answer.txt", "r")
    out = F_test_questions.readlines()  # will append in the list out
    i = 0
    start_time = time.time()

    time_loop_arr = []

    foundFirst = 0
    topFive = 0
    notTopFive = 0
    j = 0
    for line in out:
        start_time_loop = time.time()
        i += 1
        if i > 0 :
            start_time_loop = time.time()
            arguments = line.split("\t")
            # arguments[0]:uid   arguments[1]:TempId    arguments[2]: TEs 
            # arguments[3]:answerCC   arguments[4]: Question
            qUID = int(arguments[0])
            tempID = int(arguments[1])
            entityIds = arguments[2]
            answerCC = arguments[3]
            lcquadQues = arguments[4].replace('\n', '')
            lcquadQues_Query = arguments[5].replace('\n', '')

            tabbed_entIDs = entityIds + '\t'

            ent_corechains = [x for x in all_test_corechains if tabbed_entIDs in x]
            print('========= ' + str(i) + ' =========')
            write_to_file(F_sbert_terminal, '========= ' + str(i) + ' =========')
            
            if ent_corechains: 
                testResult, gAnswer_topAnswer = lcquad_test_corechain(qUID, entityIds, lcquadQues, answerCC, ent_corechains)
                if testResult[3] == 1:# if the given answer is exist in the given corechains              
                    j += 1
                    resultNote = ''
                    if (testResult[0] == 1):
                        resultNote = 'top one'
                        print(resultNote)
                        write_to_file(F_sbert_terminal, resultNote)

                    if (testResult[1] == 1):
                        resultNote = 'Top 5'
                        print(resultNote)
                        write_to_file(F_sbert_terminal, resultNote)
                        #*===================
                        cc_line = str(qUID) + '\t' + str(tempID) + '\t' + entityIds + '\t' + lcquadQues + '\t' + gAnswer_topAnswer[0] + '\t' + gAnswer_topAnswer[1] + '\t' + resultNote
                        write_to_file(F_sbert, cc_line)
                    
                    if (testResult[2] == 1):
                        resultNote = 'Not top 5'
                        print(resultNote)
                        write_to_file(F_sbert_terminal, resultNote)
                        #*===================
                        cc_line = str(qUID) + '\t' + str(tempID) + '\t' + entityIds + '\t' + lcquadQues + '\t' + gAnswer_topAnswer[0] + '\t' + gAnswer_topAnswer[1] + '\t' + resultNote
                        write_to_file(F_sbert, cc_line)

                    foundFirst += testResult[0]
                    topFive += testResult[1]
                    notTopFive += testResult[2]

                    print(j, foundFirst, topFive, notTopFive)
                    write_to_file(F_sbert_terminal, str(j) + '\t' + str(foundFirst) + '\t' + str(topFive) + '\t' + str(notTopFive))                    

                else:
                    print(qUID, 'given Answer is not exist in the corechain')
                    write_to_file(F_sbert_terminal, str(qUID) + '\t' + 'given Answer is not exist in the corechain')
                    # #!
                    # cc_line = str(qUID) + '\t' + str(tempID) + '\t' + 'given Answer is not exist in the corechain' + '\t' + lcquadQues + '\t' + lcquadQues_Query
                    # write_to_file(F_sbert_fault, cc_line)
            else:
                print(qUID, 'has no corechain')
                write_to_file(F_sbert_terminal, str(qUID) + '\t' + 'has no corechain')
                # #!
                # cc_line = str(qUID) + '\t' + str(tempID) + '\t' + 'has no corechain' + '\t' + lcquadQues + '\t' + lcquadQues_Query
                # write_to_file(F_sbert_fault, cc_line)
            
            elapsed_time_loop = time.time() - start_time_loop
            print('Time:', elapsed_time_loop)
            write_to_file(F_sbert_terminal, 'Time: ' + str(elapsed_time_loop))

            time_loop_arr.append(elapsed_time_loop)
            print('')

       
    print('\n')
    print(j, foundFirst, topFive, notTopFive)
    write_to_file(F_sbert_terminal, str(j) + '\t' + str(foundFirst) + '\t' + str(topFive) + '\t' + str(notTopFive))

    print('all question #: ' + str(i))
    write_to_file(F_sbert_terminal, 'all question #: ' + '\t' + str(i))

    print('correct question #: ' + str(j))
    write_to_file(F_sbert_terminal, 'correct question #: ' + '\t' + str(j))

    print('1st #: ' + str(foundFirst))
    write_to_file(F_sbert_terminal, '1st #: ' + '\t' + str(foundFirst))

    print('in top5 but not 1st #: ' + str(topFive))
    write_to_file(F_sbert_terminal, 'in top5 but not 1st #: ' + '\t' + str(topFive))

    print('not in top5 #: ' + str(notTopFive))
    write_to_file(F_sbert_terminal, 'not in top5 #: ' + '\t' + str(notTopFive))

    elapsed_time = time.time() - start_time
    print(elapsed_time) 
    write_to_file(F_sbert_terminal, 'elapsed_time' + '\t' + str(elapsed_time))

    print("Max time", max(time_loop_arr))
    write_to_file(F_sbert_terminal, 'Max time' + '\t' + str(max(time_loop_arr)))
    print("Min time", min(time_loop_arr))
    write_to_file(F_sbert_terminal, 'Min time' + '\t' + str(min(time_loop_arr)))
    print("Avg time", sum(time_loop_arr) / len(time_loop_arr))
    write_to_file(F_sbert_terminal, 'Avg time' + '\t' + str(sum(time_loop_arr) / len(time_loop_arr)))
    


#* test with single question
def lcquad_single_q(line):
    F_test_corechains = open("data/lcquad_test/lcquad_test_corechain.txt", "r")
    all_test_corechains = F_test_corechains.readlines()

    i = 0
    foundFirst = 0
    topFive = 0
    notTopFive = 0
    j = 0


    start_time_loop = time.time()
    arguments = line.split("\t")
    # arguments[0]:uid   arguments[1]:TempId    arguments[2]: TEs 
    # arguments[3]:answerCC   arguments[4]: Question
    qUID = int(arguments[0])
    tempID = int(arguments[1])
    entityIds = arguments[2]
    answerCC = arguments[3]
    lcquadQues = arguments[4].replace('\n', '')
    lcquadQues_Query = arguments[5].replace('\n', '')

    tabbed_entIDs = entityIds + '\t'

    ent_corechains = [x for x in all_test_corechains if tabbed_entIDs in x]
    print('========= ' + str(i) + ' =========')
    # write_to_file(F_sbert_terminal, '========= ' + str(i) + ' =========')
    
    if ent_corechains: 
        testResult, gAnswer_topAnswer = lcquad_test_corechain(qUID, entityIds, lcquadQues, answerCC, ent_corechains)
        if testResult[3] == 1:# if the given answer is exist in the given corechains              
            j += 1
            resultNote = ''
            if (testResult[0] == 1):
                resultNote = 'top one'
                print(resultNote)
                # write_to_file(F_sbert_terminal, resultNote)

            if (testResult[1] == 1):
                resultNote = 'Top 5'
                print(resultNote)
                # write_to_file(F_sbert_terminal, resultNote)
                #*===================
                cc_line = str(qUID) + '\t' + str(tempID) + '\t' + entityIds + '\t' + lcquadQues + '\t' + gAnswer_topAnswer[0] + '\t' + gAnswer_topAnswer[1] + '\t' + resultNote
                # write_to_file(F_sbert, cc_line)
            
            if (testResult[2] == 1):
                resultNote = 'Not top 5'
                print(resultNote)
                # write_to_file(F_sbert_terminal, resultNote)
                #*===================
                cc_line = str(qUID) + '\t' + str(tempID) + '\t' + entityIds + '\t' + lcquadQues + '\t' + gAnswer_topAnswer[0] + '\t' + gAnswer_topAnswer[1] + '\t' + resultNote
                # write_to_file(F_sbert, cc_line)

            foundFirst += testResult[0]
            topFive += testResult[1]
            notTopFive += testResult[2]

            print(j, foundFirst, topFive, notTopFive)
            # write_to_file(F_sbert_terminal, str(j) + '\t' + str(foundFirst) + '\t' + str(topFive) + '\t' + str(notTopFive))                    

        else:
            print(qUID, 'given Answer is not exist in the corechain')
            write_to_file(F_sbert_terminal, str(qUID) + '\t' + 'given Answer is not exist in the corechain')
    else:
        print(qUID, 'has no corechain')
        # write_to_file(F_sbert_terminal, str(qUID) + '\t' + 'has no corechain')
 
    
    elapsed_time_loop = time.time() - start_time_loop
    print('Time:', elapsed_time_loop)
    # write_to_file(F_sbert_terminal, 'Time: ' + str(elapsed_time_loop))

    print(elapsed_time_loop)
    print('')


# ================= test with single question
# line = '9952	24	Q265538	+P664, +P641	Who is the organizer for the sport of World Series?	SELECT ?ans_1 ?ans_2 WHERE { wd:Q265538 wdt:P664 ?ans_1 . wd:Q265538 wdt:P641 ?ans_2 }'

# lcquad_single_q(line)

lcquad_test_questions()