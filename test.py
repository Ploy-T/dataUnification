from jellyfish import jaro_winkler
import json

def name_similarity(left_record, right_record):
    return jaro_winkler(left_record['vendorName'] or '', right_record['vendorName'] or '')

def match(vendorName,masterList)
    # df_master = pd.read_excel('invoice-vend0r-fileName-1.xlsx', sheet_name = "master", engine = 'openpyxl')
    # df_master['vendorName']=df_master['vendorName'].str.lower()

    # masterVendorName = dict()
    # for index, row in df_master.iterrows():
    #     masterVendorName.update({index: {'id': row['id'],
    #                             'vendorName': row['vendorName']}})

    # right = {'vendorName': 'bally cold refrigerated boxes inc'}
    # temp = []
    # for i in range(len(masterVendorName)):
    #     t = name_similarity(masterVendorName[i],right)
    #     temp.append(t)

    # masterList = 'jsonMasterList.json'
    # Opening JSON file
    f = open(masterList)
    
    # returns JSON object as a dictionary
    data = json.load(f)   
    f.close()

    temp = []
    for i in range(len(data)):
        t = name_similarity(data[str(i)],verdorName)
        temp.append(t)

    X = max(temp)
    I = temp.index(max(temp))

    match = data[str(I)]['vendorName']
    return match


        

