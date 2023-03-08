import pandas as pd
import numpy as np
import re
from fuzzywuzzy import fuzz
from sklearn.cluster import AffinityPropagation
import difflib
import collections

def clean(df):
    LIST = []
    for i in range(len(df)):
        a = df['vendorName'][i]
        try:
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

def cluster(df,df_master):
    cleaned =  clean(df)
    cleaned_master = clean(df_master)

    sim = fuzz_similarity(cleaned['cleaned_vendorName'].tolist())
    clus = AffinityPropagation(affinity='precomputed').fit_predict(sim)
    cleaned['cluster'] = clus
    cust_ids = cleaned['fileName'].to_list()
    df_clus = pd.DataFrame(list(zip(cust_ids,clus)),columns=['fileName','cluster'])
    df_eval = df_clus.merge(cleaned, on = 'fileName',how = 'left')
    df_eval.rename(columns={"cluster_x": "cluster"},inplace=True)

    df_cluster = standard_name(df_eval)

    matched_vendors = []

    for row in df_cluster.index:
        vendor_name = df_cluster.get_value(row,"cleaned_vendorName")
        for columns in cleaned_master.index:
            master_vendor_name = cleaned_master.get_value(columns,"cleaned_vendorName")
            matched_token = fuzz.partial_ratio(vendor_name,master_vendor_name)
            if matched_token> 80:
                matched_vendors.append([vendor_name,master_vendor_name,matched_token])
    m = pd.DataFrame(matched_vendors)
    m.columns =['cleaned_vendorName', 'masterVendorName', 'Score_with_master']

    df_cluster_ = df_cluster.merge(m, on='cleaned_vendorName',how='left')
    df_cluster_.rename(columns={"cleaned_vendorName": "cleanedVendorName"},inplace=True)

    df_final = df_cluster_[['fileName','vendorName','cleanedVendorName','masterVendorName','Score_with_master','cluster','standardName','Score_with_standard']]
    return df_final

df = pd.read_csv('invoice-vend0r-fileName-1.csv')
df_master = pd.read_csv('vendorMaster.csv')




#########################
res = []
for i in LIST:
    if i not in res:
        res.append(i)

data = res
treshold     = 75
minGroupSize = 1

from itertools import combinations

paired = { c:{c} for c in data }
for a,b in combinations(data,2):
    if getRatio(a,b) < treshold: continue
    paired[a].add(b)
    paired[b].add(a)

groups    = list()
ungrouped = set(data)
while ungrouped:
    bestGroup = {}
    for city in ungrouped:
        g = paired[city] & ungrouped
        for c in g.copy():
            g &= paired[c] 
        if len(g) > len(bestGroup):
            bestGroup = g
    if len(bestGroup) < minGroupSize : break  # to terminate grouping early change minGroupSize to 3
    ungrouped -= bestGroup
    groups.append(bestGroup)

G = pd.DataFrame(groups)
G.to_csv('grouped_vendornames-127k.csv')
#########################

def clean(txt):
    seps = [" ","-",": :",":"]
    default_sep = seps[0]
    for sep in seps[1:]:
        txt = txt.replace(sep,default_sep)
    re.sub(' +',' ',txt)
    temp_list = [i.strip() for i in txt.split(default_sep)]
    temp_list = [i for i in temp_list if i]
    return " ".join(temp_list)


def data_cleaning(data,nameCol = 'vendorName',dropForeign = True):
    data.dropna(subset=[nameCol],inplace=True)
    data = data.rename_axis('vendorID').reset_index()




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

def vendor_clusters(x):
    sim = fuzz_similarity(res)
    clus = AffinityPropagation(affinity='precomputed').fit_predict(sim)
    return clus



from collections import Counter
stripJunk = str.maketrans("","","- ")
def getRatio(a,b):
    a = a.lower().translate(stripJunk)
    b = b.lower().translate(stripJunk)
    total  = len(a)+len(b)
    counts = (Counter(a)-Counter(b))+(Counter(b)-Counter(a))
    return 100 - 100 * sum(counts.values()) / total