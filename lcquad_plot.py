
import sys
import time
import random
from collections import OrderedDict
from SPARQLWrapper import SPARQLWrapper, JSON
from sparqlQueries import get_topicEntity_val, write_to_file, mu_prop_lcquad, lcquad_ds,get_query_results
# from lcquad_test_corechain import lcquad_corechain_func
import matplotlib
import matplotlib.pyplot as plt
import numpy as np


def autolabel(rects, sbert_model):
    """Attach a text label above each bar in *rects*, displaying its height."""
    for rect in rects:
        height = rect.get_height()
        label = '{:.1f}'.format(height) + "%"

        if sbert_model != 'e5_b64' :
            label = ''
        plt.annotate(label,
                    xy=(rect.get_x() + rect.get_width() / 2, height),
                    xytext=(0, 2),  # # distance from text to points (x,y)
                    textcoords="offset points", # how to position the text
                    ha= 'center',  #'center' # horizontal alignment can be left, right or center
                    va='bottom')

def percentage_per_temp(sbert_model):
    F_test_faults = open("data/lcquad2_dataset/lcquad_test_faults.txt", "r")
    test_faults_arr = F_test_faults.readlines()

    F_test_answer = open("data/lcquad2_dataset/lcquad_test_answer.txt", "r")
    test_answer_arr = F_test_answer.readlines()

    F_sbert_result = open("data/lcquad_sbert/lcquad_sbert_" + sbert_model + "_5485.txt", "r")
    sbert_result_arr = F_sbert_result.readlines()

    tmpIDs_arr = [] #x
    tmpIDs_ocurrence = [] #y
    valid_to_answer_counter = 0
    correctAnswerperModel = 0
    for i in range(1, 25):
        if i > 0:
            #shape tempID
            tabbed_tempID = '\t' + str(i) + '\t'

            #collect all tempID in 'sbert_eX_bY'
            sbertModel_arr = [x for x in sbert_result_arr if tabbed_tempID in x ]
            

            #collect all tempID in 'test_faults' put in list
            test_fault_temp_arr = [x for x in test_faults_arr if tabbed_tempID in x and 'LCQuAD2 Query' in x]
            fault_qUID_arr = []
            for faultLine in test_fault_temp_arr:
                argts = faultLine.split('\t')
                fault_qUID = argts[0]
                fault_qUID_arr.append(fault_qUID)


            #collect all tempID in 'test_answer', then exlude the ones in 'test_faults'
            test_answer_temp_arr = [x for x in test_answer_arr if tabbed_tempID in x ]
            answered_qUID_arr = []
            for line in test_answer_temp_arr:
                arguments = line.split('\t')
                qUID = arguments[0]
                if qUID not in fault_qUID_arr:
                    answered_qUID_arr.append(qUID)
            
            valid_to_answer_counter += len(answered_qUID_arr)

            #* result:
            correctAnswerPerTemp = len(answered_qUID_arr) - len(sbertModel_arr)
            temp_percentage = float( (correctAnswerPerTemp*100) / len(answered_qUID_arr) )

            tmpIDs_arr.append(i)
            tmpIDs_ocurrence.append(temp_percentage)

            # print(i, temp_percentage, correctAnswerPerTemp, len(answered_qUID_arr))

            correctAnswerperModel += correctAnswerPerTemp

    # print('valid_to_answer_counter', valid_to_answer_counter)
    total_percentage = float( (correctAnswerperModel*100) / valid_to_answer_counter)
    print(sbert_model, 'correctAnswerperModel_percentage', total_percentage)
    return tmpIDs_ocurrence, total_percentage #
    #! plot
    # plt.title("Answered Question based on the Temp-Id (Model:" + sbert_model + ")")
    # plt.xlabel("Template number")
    # plt.ylabel("Temp recurrence")
    # plt.xticks(tmpIDs_arr)
    # X = np.arange(1,25)
    
    # autolabel(tmpIDs_arr, tmpIDs_ocurrence, (X + 0.00) , 'b')
    # autolabel(tmpIDs_arr, tmpIDs_ocurrence2, (X + 0.25) , 'g')
    # plt.show()


def plot_all_sbertResult():


    sbert_model1 = 'e5_b64'

    sbert_model2 = 'Paraphrase-mpnet_e5_b64' #--Para
    sbert_model3 = 'STSb-mpnet_e3_b64' #--STSb
    sbert_model4 = 'Finetune_e5_b64'  #--Fine
    sbert_model5 = 'Crossencoder_e4_b16'  #--Cros
    

    #paraphrase-mpnet
    #e3_b64_stsb-mpnet
    #e5_b64_finetune
    #crossencoder_e4_b16
    
    
    

    scut1 = sbert_model1[0].upper() + sbert_model1[1] 
    scut2 = sbert_model2[0].upper() + sbert_model2[1]
    scut3 = sbert_model3[0].upper() + sbert_model3[1] + sbert_model3[2]
    scut4 = sbert_model4[0].upper() + sbert_model4[1] 
    scut5 = sbert_model5[0].upper() + sbert_model5[1] 
    
    tmpIDs_ocurrence1, total_percentage1 = percentage_per_temp(sbert_model1)
    tmpIDs_ocurrence2, total_percentage2 = percentage_per_temp(sbert_model2)
    tmpIDs_ocurrence3, total_percentage3 = percentage_per_temp(sbert_model3)
    tmpIDs_ocurrence4, total_percentage4 = percentage_per_temp(sbert_model4)
    tmpIDs_ocurrence5, total_percentage5 = percentage_per_temp(sbert_model5)


    barInfo1 = scut1 + ' ' + '{:.1f}'.format(total_percentage1) + "%"
    barInfo2 = scut2 + ' ' + '{:.1f}'.format(total_percentage2) + "%"
    barInfo3 = scut3 + ' ' + '{:.1f}'.format(total_percentage3) + "%"
    barInfo4 = scut4 + ' ' + '{:.1f}'.format(total_percentage4) + "%"
    barInfo5 = scut5 + ' ' + '{:.1f}'.format(total_percentage5) + "%"


    ####################### Plot total for each model ===================
    xAxis = [scut1, scut2, scut4 , scut5, scut3]
    yAxis = [total_percentage1, total_percentage2, total_percentage4, total_percentage5, total_percentage3]
    #* should be disabled to plot total per template
    # plot_total(xAxis, yAxis) 
    #==================================#############################


    labels =  list(range(1, 25))
    x = np.arange(len(labels))  # the label locations
    width = 0.15 # the width of the bars

    fig, ax = plt.subplots()
    rects1 = ax.bar(x - width/2, tmpIDs_ocurrence1, width, label= barInfo1)
    rects2 = ax.bar(x + width/2, tmpIDs_ocurrence2, width, label= barInfo2)
    rects3 = ax.bar(x + 3*(width/2), tmpIDs_ocurrence3, width, label= barInfo3)
    rects4 = ax.bar(x + 5*(width/2), tmpIDs_ocurrence4, width, label= barInfo4)
    rects5 = ax.bar(x + 7*(width/2), tmpIDs_ocurrence5, width, label= barInfo5)


    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_ylabel('Answered question per template')
    ax.set_xlabel("Template ID")
    ax.set_title('Answered Question based on the Temp-Id')
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.legend()


    autolabel(rects1, sbert_model1)
    autolabel(rects2, sbert_model2)
    autolabel(rects3, sbert_model3)
    autolabel(rects4, sbert_model4)
    autolabel(rects5, sbert_model5)


   

    fig.tight_layout()

    plt.show()
   
def singleBar(rects):
    """Attach a text label above each bar in *rects*, displaying its height."""
    for rect in rects:
        height = rect.get_height()
        label = '{:.1f}'.format(height) + '%'
        plt.annotate(label,
                    xy=(rect.get_x() + rect.get_width() / 2, height),
                    xytext=(0, 2),  # 3 points vertical offset
                    textcoords="offset points",
                        ha='center', va='bottom')

def plot_total(xAxis, yAxis):
    x = np.arange(len(xAxis))  # the label locations
    width = 0.25  # the width of the bars

    fig, ax = plt.subplots()
    rects = ax.bar(x - width/15, yAxis, width)

    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_ylabel('Answered question per Model')
    ax.set_xlabel("Epoch Number")
    ax.set_title('Training Model: Different Epochs')
    ax.set_xticks(x)
    ax.set_xticklabels(xAxis)
    ax.legend()

    singleBar(rects)

    fig.tight_layout()

    plt.show()

plot_all_sbertResult()


