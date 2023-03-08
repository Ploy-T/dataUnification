import csv
import json
import pandas as pd

# df = pd.read_csv ('vendorMaster.csv')
# df.to_json ('jsonMasterList_ori.json')

def csv_to_json(csvFilePath, jsonFilePath):
    data_dict = {}
      
    #read csv file
    with open(csvFilePath, encoding='utf-8-sig') as csvf: 
        #load csv file data using csv library's dictionary reader
        csvReader = csv.DictReader(csvf) 

        #convert each csv row into python dict
        for rows in csvReader: 
            #add this python dict to json array
            key = rows['customerNumber']
            data_dict[key] = rows
  
    #convert python jsonArray to JSON String and write to file
    with open(jsonFilePath, 'w', encoding='utf-8') as jsonf: 
        jsonString = json.dumps(data_dict, indent=4)
        jsonf.write(jsonString)

csvFilePath = 'customerMasterKKR.csv'
jsonFilePath = 'customerNameMasterListKKR.json'
csv_to_json(csvFilePath, jsonFilePath)