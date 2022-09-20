#!/bin/env

import os
import sys
import logging
import pandas as pd
from neuclease.dvid import find_master, fetch_synapses_in_batches, check_synapse_consistency
from neuclease.util import decode_coords_from_uint64


server = sys.argv[1]

uuid = find_master(server)
#uuid = uuid[:4]
print(f"Using uuid: {uuid}")
annotation = sys.argv[2]
proj = sys.argv[3]

master = (server, uuid)


this_pid = os.getpid()
logfile = proj + "_synapse_check_" + str(this_pid) + ".log"
logging.basicConfig(filename=logfile, level=logging.INFO)

# get as pandas dataframe
synapses_df, pre_partner_df, post_partner_df = fetch_synapses_in_batches(*master, annotation, format='pandas', return_both_partner_tables=True)

orphan_tbars, orphan_psds, pre_dupes, post_dupes, only_in_tbar, only_in_psd, bad_tbar_refs, bad_psd_refs, oversubscribed_post, oversubscribed_pre = check_synapse_consistency(synapses_df, pre_partner_df, post_partner_df) 

#synapses_df.to_csv(f'synapse_info.csv', header=True, index=False)
#pre_coords_zyx = decode_coords_from_uint64(only_in_psd['pre_id'].values)
#post_coords_zyx = decode_coords_from_uint64(only_in_psd['post_id'].values)
#pre_coords_xyz = pre_coords_zyx[:, ::-1]
#post_coords_xyz = post_coords_zyx[:, ::-1]
#print(pre_coords_xyz)
#print(post_coords_xyz)

orphan_tbars.to_csv(f'orphan_tbars.csv', header=True, index=False)
orphan_psds.to_csv(f'orphan_psds.csv', header=True, index=False)
pre_dupes.to_csv(f'pre_dupes.csv', header=True, index=False)
post_dupes.to_csv(f'post_dupes.csv', header=True, index=False)
only_in_tbar.to_csv(f'only_in_tbar.csv', header=True, index=False)
only_in_psd.to_csv(f'only_in_psd.csv', header=True, index=False)
bad_tbar_refs.to_csv(f'bad_tbar_refs.csv', header=True, index=False)
bad_psd_refs.to_csv(f'bad_psd_refs.csv', header=True, index=False)
oversubscribed_post.to_csv(f'oversubscribed_post.csv', header=True, index=False)
oversubscribed_pre.to_csv(f'oversubscribed_pre.csv', header=True, index=False)

#only_in_psd.to_csv(f'only_in_psd.csv', header=True, index=False)
