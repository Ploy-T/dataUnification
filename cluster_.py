import pandas as pd
import numpy as np
import re
from fuzzywuzzy import fuzz
from sklearn.cluster import AffinityPropagation
import difflib
import collections
import csv
from openpyxl import Workbook
import json


def clean(df):
    LIST = []
    for i in range(len(df)):
        try:
            a = df['vendorName'][i].lower()
            for k in a.split("\n"):
                kk = re.sub(r"[^a-zA-Z0-9]+", ' ', k).strip()
                LIST.append(kk)
        except:
            LIST.append(a)
    df['cleaned_vendorName'] = LIST

    # remove nan and ''
    dff = df[df['cleaned_vendorName'].notna()].reset_index() #127127
    dff = dff[dff['cleaned_vendorName'] != ''].reset_index() #127123

    # remove dups 
    cleaned =  dff.drop_duplicates(subset = ['cleaned_vendorName']) #1191
    return cleaned

def fuzz_similarity(name):
    similarity_array = np.ones((len(name),(len(name))))*100
    for i in range(1,len(name)):
        for j in range(i):
            s1 = fuzz.token_set_ratio(name[i],name[j]) + 0.0000000001
            s2 = fuzz.partial_ratio(name[i],name[j]) + 0.0000000001
            similarity_array[i][j] = 2*s1*s2/(s1+s2)
    for i in range(len(name)):
        for j in range(i+1,len(name)):
            similarity_array[i][j] = similarity_array[j][i]
    np.fill_diagonal(similarity_array,100)
    return similarity_array

def standard_name(df_eval):
    d_standard_name = {}
    for cluster in df_eval.cluster.unique():
        names = df_eval[df_eval['cluster'] == cluster].cleaned_vendorName.to_list()
        l_common_substring = []
        if len(names) > 1:
            for i in range(0,len(names)):
                for j in range(i+1,len(names)):
                    seqMatch = difflib.SequenceMatcher(None,names[i],names[j])
                    match = seqMatch.find_longest_match(0,len(names[i]),0,len(names[j]))
                    if (match.size!=0):
                        l_common_substring.append(names[i][match.a: match.a + match.size].strip())
            n = len(l_common_substring)
            counts = collections.Counter(l_common_substring)
            get_mode = dict(counts)
            mode = [k for k,v in get_mode.items() if v == max(list(counts.values()))]
            d_standard_name[cluster] = ';'.join(mode)
        else:
            d_standard_name[cluster] = names[0]
    
    df_standard_names = pd.DataFrame(list(d_standard_name.items()),columns = ['cluster','standardName'])
    df_eval = df_eval.merge(df_standard_names, on='cluster',how='left')
    df_eval['Score_with_standard'] = df_eval.apply(lambda x: fuzz.token_set_ratio(x['standardName'],x['cleaned_vendorName']),axis=1)
    return df_eval

def Cluster(dataFile,masterFile):
    # df = pd.read_excel(data, sheet_name = "data", engine = 'openpyxl')
    # df_master = pd.read_excel(master, sheet_name = "master", engine = 'openpyxl')
    cleaned =  clean(dataFile)
    cleaned_master = clean(masterFile)

    sim = fuzz_similarity(cleaned['cleaned_vendorName'].tolist())
    clus = AffinityPropagation(affinity='precomputed').fit_predict(sim)
    cleaned['cluster'] = clus
    cust_ids = cleaned['index'].to_list()
    df_clus = pd.DataFrame(list(zip(cust_ids,clus)),columns=['index','cluster'])
    df_eval = df_clus.merge(cleaned, on = 'index',how = 'left')
    df_eval.rename(columns={"cluster_x": "cluster"},inplace=True)

    df_cluster = standard_name(df_eval)

    matched_vendors = []

    for row in df_cluster.index:
        vendor_name = df_cluster._get_value(row,"cleaned_vendorName")
        for columns in cleaned_master.index:
            master_vendor_name = cleaned_master._get_value(columns,"cleaned_vendorName")
            matched_token = fuzz.partial_ratio(vendor_name,master_vendor_name)
            if matched_token> 80:
                matched_vendors.append([vendor_name,master_vendor_name,matched_token])
    m = pd.DataFrame(matched_vendors)
    m.columns =['cleaned_vendorName', 'masterVendorName', 'Score_with_master']

    df_cluster_ = df_cluster.merge(m, on='cleaned_vendorName',how='left')
    df_cluster_.rename(columns={"cleaned_vendorName": "cleanedVendorName"},inplace=True)

    df_final = df_cluster_[['index','vendorName','cleanedVendorName','masterVendorName','Score_with_master','cluster','standardName','Score_with_standard']]
    json_f = df_final.to_json(orient = 'split')
    json_final = json.loads(json_f)
    return df_final, json_final
