#!/bin/env
# ------------------------- imports -------------------------
import json
import sys
import os
import io
import time
import numpy as np
from tqdm import trange
#from neuclease.dvid import *
import pandas as pd
#from requests import HTTPError
from neuclease.util import compute_parallel, read_csv_col
from neuclease.dvid import find_master, fetch_sizes, fetch_keys

dvid_server = sys.argv[1]
dvid_uuid = sys.argv[2]
keyvalue = sys.argv[3]
stale_body_file = sys.argv[4]

node = (dvid_server, dvid_uuid)
bodyList = fetch_keys(*node, keyvalue)
#seg_annot_count = len(all_keys)

master_seg = (dvid_server, dvid_uuid, 'segmentation')
body_groups = []
group_list = []
body_count = 0

# chunk body list into groups of 1000
for bodyID in bodyList:
    if bodyID[0].isdigit():    
        group_list.append(int(bodyID))
        body_count += 1
    if body_count == 1000:
        body_groups.append(group_list)
        group_list = []
        body_count = 0

if len(group_list) > 0:
    body_groups.append(group_list)

PROCESSES = 10
def get_sizes(label_ids):
    try:
        sizes_pd = fetch_sizes(*master_seg, label_ids, supervoxels=False)
    except HTTPError:
        s_empty_pd = pd.Series(index=label_ids, data=-1, dtype=int)
        s_empty_pd.name = 'size'
        s_empty_pd.index.name = 'body'
        return(s_empty_pd)
    else:
        return(sizes_pd)

body_sizes_df_list = compute_parallel(get_sizes, body_groups, chunksize=100, processes=PROCESSES, ordered=False)

#This is actually a Series
body_sizes_df = pd.concat(body_sizes_df_list)

stale_bodies_df = pd.DataFrame(columns=['body','size'])

for bodyId, size in body_sizes_df.iteritems():
    #print(bodyId, size)
    if size == 0:
        stale_bodies_df = stale_bodies_df.append({'body':bodyId, 'size':size}, ignore_index=True)

#print(stale_bodies_df)
stale_bodies_df.to_csv(stale_body_file, index=False)

#for index, data in body_sizes_df.iterrows():
#    print(index)
#stale_df = pd.DataFrame(columns=['body','size'])
#for index, row in body_sizes_df.iterrows():
#    bodyId = row['body']
#    size = row['size']
#    if size == 0:
        #stale_df = stale_df.append(row, ignore_index=True)
#        print(str(bodyId) + "," + str(size))
#stale_dif.to_csv(stale_body_file, index=True)
