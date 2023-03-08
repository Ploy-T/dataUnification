from jellyfish import jaro_winkler
import json

def name_similarity(left_record, right_record):
    return jaro_winkler(left_record['customerName'].lower() or '', right_record['customerName'].lower() or '')

def match(customerName,masterData):
    temp = []
    for i in range(len(masterData)):
        t = name_similarity(masterData[str(i)],customerName)
        # print(masterData[str(i)])
        # print(t)
        temp.append(t)

    X = max(temp)
    if X > 0.8:
        I = temp.index(max(temp))
        output = masterData[str(I)]['customerName']
    else:
        output = 'ATLANTIC AVIATION'
    return output


        

