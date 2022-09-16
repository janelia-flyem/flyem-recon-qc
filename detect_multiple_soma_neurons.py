#!/bin/env

import os
import sys
import logging
import json
import requests
from tqdm import tqdm_notebook
tqdm = tqdm_notebook

import numpy as np
import pandas as pd
from neuclease.dvid import find_master, fetch_elements, fetch_labels_batched

#hemibrain v1.0
dvid_server = sys.argv[1]
uuid_master = sys.argv[2]

dvid_uuid = find_master(dvid_server,uuid_master)

print("usig dvid uuid:", dvid_uuid)

annotation = "nuclei-centroids"

master = (dvid_server, dvid_uuid)

#box_zyx = [(0,0,0), (64000,76800,96000)]
i = 0
z_max = 64000

df_elements = pd.DataFrame(columns = [ 'z','y','x','kind','tags','size'])

while i < z_max:
    z_a = i + 1
    i += 2000
    z_b = i
    box_zyx = [(z_a,0,0), (z_b,76800,96000)]
    df = fetch_elements(*master, annotation, box_zyx, format='pandas')
    #print(df)
    #df_elements.append(df, ignore_index = True)
    df_elements = pd.concat([df_elements,df], ignore_index = True)
    #print(df_elements)
    

#print(df_elements)
master_seg = (dvid_server, dvid_uuid, 'segmentation')
labels = fetch_labels_batched(*master_seg, df_elements[['z', 'y', 'x']].values, threads=8)
df_elements['body'] = labels

count = 1
body_count = {}
for index, row in df_elements.iterrows():
    bodyId = row['body']
    if bodyId in body_count:
        body_count[bodyId] += 1
    else:
        body_count[bodyId] = 1
    #print(row['body'], count, row['x'], row['y'], row['z'])
    #print(str(row['body']) + "," + str(count) + " 0 " + str(row['x']) + " " +  str(row['y']) + " " + str(row['z']) + " " + str(row['size'])  + " -1") 
    #count += 1

print("bodyId,soma_count")
for bodyId in body_count:
    soma_count = body_count[bodyId]
    if soma_count > 1:
        print(str(bodyId) + "," + str(soma_count))

