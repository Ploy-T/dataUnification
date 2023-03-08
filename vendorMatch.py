from jellyfish import jaro_winkler
import json

def name_similarity(left_record, right_record):
    return jaro_winkler(left_record['vendorName'].lower() or '', right_record['vendorName'].lower() or '')

def match(vendorName,masterData):
    temp = []
    for i in range(len(masterData)):
        t = name_similarity(masterData[str(i)],vendorName)
        # print(masterData[str(i)])
        # print(t)
        temp.append(t)

    X = max(temp)
    if X > 0.8:
        I = temp.index(max(temp))
        output = masterData[str(I)]['vendorName']
    else:
        output = ''
    return output


        

