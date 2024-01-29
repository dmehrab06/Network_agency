from bayes_opt import BayesianOptimization
from bayes_opt.logger import JSONLogger
from bayes_opt.event import Events
from bayes_opt.util import load_logs
from file_paths_and_consts import *

import pandas as pd
import os
import numpy as np
import random
import warnings
warnings.filterwarnings('ignore')
from bayes_opt import UtilityFunction
import json

SEED_VALUE = 590
random.seed(SEED_VALUE)

#OUTPUT_DIR

def get_memory(hh):
    if hh<=100000:
        return 8000
    if hh<=300000:
        return 16000
    return 32000

def get_core(hh):
    if hh<=100000:
        return 1
    if hh<=300000:
        return 2
    return 4

def get_results_for_2(hyper_comb, who='refugee',region_name=["all"],prefix='fresh_calib_batch_simulation',look_until=100,ROLL=7):
    
    search_in_ids = all_ids
    if region_name!=["all"]:
        search_in_ids = region_name
    
    all_dfs = []
    found = 0
    for cur_id in search_in_ids:
        f_name = prefix+'_completed_'+str(cur_id)+'_'+str(hyper_comb).zfill(5)+'.csv'
        f2_name = prefix+'_'+str(cur_id)+'_'+str(hyper_comb).zfill(5)+'.csv'
        if os.path.isfile(OUTPUT_DIR+f_name):
            true_f_name = f_name
        elif os.path.isfile(OUTPUT_DIR+f2_name):
            true_f_name = f2_name
        else:
            continue
            
        cur_df = pd.read_csv(OUTPUT_DIR+true_f_name)
        #print(cur_df.shape[0],end=' ')
        cur_df['time'] = pd.to_datetime(cur_df['time'])
        
        #cur_df = cur_df.sort_values(by=['time',who],ascending=[True,False])
        #cur_df = cur_df.drop_duplicates(keep='first')
        
        all_dfs.append(cur_df)
        found = found + 1
    
    ovr_df = pd.concat(all_dfs)
    ovr_df = ovr_df.groupby('time')[who].sum().reset_index()
    ovr_df[who] = ovr_df[who].rolling(ROLL).mean()
    #print(ovr_df.columns.tolist())
    ovr_df = ovr_df.dropna(subset=[who])
    print(found,'raions found')
    return ovr_df

ROLLING = 7
refugee_df = pd.read_csv(GROUND_TRUTH_DIR+'ukraine_refugee_data_2.csv')
refugee_df['time'] = pd.to_datetime(refugee_df['time'])
refugee_df['refugee'] = refugee_df['refugee'].rolling(ROLLING).mean()
refugee_df = refugee_df.dropna(subset=['refugee'])

cur_household_data = pd.read_csv(HOUSEHOLD_DIR+'ukraine_household_data.csv')
id_to_name = cur_household_data[['matching_place_id','matching_place_name']].drop_duplicates()
all_ids = []#cur_household_data.matching_place_id.unique().tolist()
#id_to_name['matching_place_name'].value_counts()
dict_id_to_name = dict(zip(id_to_name.matching_place_id, id_to_name.matching_place_name))

neighbor_adm2 = pd.read_csv(UNCLEANED_DATA_DIR+'neighbor_raions.csv')
all_ids = all_ids + neighbor_adm2.ADM2_EN_x.unique().tolist()

abm_pbounds = {
    'D':(1.0,5.0),
    'A':(30.0,70.0),
    'T':(0.5,3.0),
    'S':(0.01,1.0),
    #'t_l':(5,21),
    't_r':(1,14),
    'ps':(0.0,1.0),
    'ews':(0.0,1.0),
    #'pactive':(0,60),
    'p_hi':(0.2,1.0),
    'p_lo':(0,0.1),
    'lambda':(0,1),
    #'phase_shift':(0,60),
    'b_prob':(0.2,0.4),
    #'m_lo':(0.7,1.0),
    #'m_hi':(1.5,2.0),
    'peer_it':(1,5),
}

network_struct = 13
dynamic_network = 1
use_neighbor = 5#random.sample([2,5,8,10],1)[0]#random.unifrom(0.0,1.0)

optimizer = BayesianOptimization(
    f=None,
    pbounds=abm_pbounds,
    verbose=2,
    random_state=3,
)
utility = UtilityFunction(kind="ucb", kappa=2.5, xi=0.0)
load_logs(optimizer, logs=["./logs/bayes_examples_all_log_revised.json"])
print("ABM Optimizer is now aware of {} points.".format(len(optimizer.space)))
logger = JSONLogger(path="./logs/bayes_examples_current_log.json")
optimizer.subscribe(Events.OPTIMIZATION_STEP, logger)
next_point_to_probe = optimizer.suggest(utility)
print("Next point to probe is:", next_point_to_probe)

import subprocess
subprocess_output = subprocess.run(["squeue","-u","zm8bh","--name=bayes"],capture_output=True)

print(len(str(subprocess_output)))

if len(str(subprocess_output))==222:
    
    if len(optimizer.space)>=0:

        print('deleting previous runs')
        all_files = [f for f in os.listdir(".") if f.endswith('.out')]
        all_files.sort()
        for f in all_files[0:-3]:
            os.remove(f)
        hyper_comb = len(optimizer.space)+10000
        print('job finished for',hyper_comb)
        res = get_results_for_2(hyper_comb,prefix='mim_result')
        comp_df = res.merge(refugee_df,on='time',how='inner')

        comp_df['diff'] = (comp_df['refugee_y']-comp_df['refugee_x'])**2
        #rmse = ((comp_df[0:10]['diff'].sum()+comp_df[45:55]['diff'].sum())/20)**0.5 #10 data points
        rmse = ((comp_df[0:10]['diff'].sum()+comp_df[40:60]['diff'].sum())/30)**0.5 #10 data points
        #rmse = comp_df['diff'].mean()**0.5
        target = -rmse
        print("Found the target value to be:", target)

        optimizer.register(
            params=next_point_to_probe,
            target=target,
        )

        with open('./logs/bayes_examples_current_log.json') as json_file:
            data = json.load(json_file)
        print(data)
        with open('./logs/bayes_examples_all_log_revised.json', "a") as f:
            f.write(json.dumps(data) + "\n")

        optimizer = BayesianOptimization(
            f=None,
            pbounds=abm_pbounds,
            verbose=2,
            random_state=3,
        )
        utility = UtilityFunction(kind="ucb", kappa=2.5, xi=0.0)
        load_logs(optimizer, logs=["./logs/bayes_examples_all_log_revised.json"])
        print("ABM Optimizer is now aware of {} points.".format(len(optimizer.space)))
        logger = JSONLogger(path="./logs/bayes_examples_current_log.json")
        optimizer.subscribe(Events.OPTIMIZATION_STEP, logger)
        next_point_to_probe = optimizer.suggest(utility)
        print("Next point to probe is:", next_point_to_probe)

        #raion_df = pd.read_csv('/home/zm8bh/radiation_model/migration_shock/scripts/analysis_notebooks/memory_req_raion.csv')

    X = next_point_to_probe
    hyper_comb = len(optimizer.space)+10000

    f = open("bayesian_calibration_"+str(hyper_comb)+".sh", "w")

    D = X['D']
    A = X['A']
    S = X['S']*100.0
    T = X['T']
    t_r = int(X['t_r'])
    t_l = t_r
    ps = X['ps']
    ews = X['ews']
    pactive = 1000
    peer_thresh_lo = X['p_lo']
    peer_thresh_hi = X['p_hi']
    LAMBDA = X['lambda']
    border_cross_prob = X['b_prob']
    phase_shift = 1000
    multiply_lo = 1.0
    multiply_hi = 1.8
    max_peer_it = int(X['peer_it'])
    
    comment = 'AAMAS_'+str(hyper_comb)

    necessary_mem = pd.read_csv('/home/zm8bh/radiation_model/migration_shock/scripts/analysis_notebooks/memory_req_raion.csv')
    raion_df = pd.read_csv('hh_cnts.csv')

    raion_df = raion_df.sort_values(by='hh',ascending=False)
    raion_df['memory'] = raion_df['hh'].apply(lambda x: get_memory(x))
    raion_df['core'] = raion_df['hh'].apply(lambda x: get_core(x))
    raion_df = raion_df.merge(necessary_mem[['Raion','mem_need']],left_on='raion',right_on='Raion',how='inner')
    #raion_df['memory'] = raion_df[["memory", "mem_need"]].max(axis=1)
    raion_df = raion_df.sort_values(by='hh',ascending=False)

    raion = raion_df['raion'].tolist()
    mem = raion_df['memory'].tolist()
    cc = raion_df['core'].tolist()

    partition = 1

    set1_raion = raion[0:partition]
    set1_mem = mem[0:partition]
    set1_cc = cc[0:partition]

    set2_raion = raion[partition:]
    set2_mem = mem[partition:]
    set2_cc = cc[partition:]
    set2_raion.reverse()
    set2_mem.reverse()
    set2_cc.reverse()

    for i in range(len(set1_raion)):
        name = set1_raion[i]
        mem_req = set1_mem[i]
        core_use = set1_cc[i]
        print('sbatch --mem='+str(mem_req)+' --cpus-per-task='+str(core_use)+' abm.sbatch',end=' ',file=f)
        if name.startswith('Chornobyl'):
            print('"'+name+'"',hyper_comb,D,A,T,S,t_l,t_r,ps,ews,pactive,peer_thresh_lo,peer_thresh_hi,
                  LAMBDA,dynamic_network,use_neighbor,border_cross_prob,phase_shift,multiply_lo,multiply_hi,core_use,max_peer_it,file=f)
        else:
            print(name,hyper_comb,D,A,T,S,t_l,t_r,ps,ews,pactive,peer_thresh_lo,peer_thresh_hi,
                  LAMBDA,dynamic_network,use_neighbor,border_cross_prob,phase_shift,multiply_lo,multiply_hi,core_use,max_peer_it,file=f)


    for i in range(len(set2_raion)):
        name = set2_raion[i]
        mem_req = set2_mem[i]
        core_use = set2_cc[i]
        print('sbatch --mem='+str(mem_req)+' --cpus-per-task='+str(core_use)+' abm.sbatch',end=' ',file=f)
        if name.startswith('Chornobyl'):
            print('"'+name+'"',hyper_comb,D,A,T,S,t_l,t_r,ps,ews,pactive,peer_thresh_lo,peer_thresh_hi,
                  LAMBDA,dynamic_network,use_neighbor,border_cross_prob,phase_shift,multiply_lo,multiply_hi,core_use,max_peer_it,file=f)
        else:
            print(name,hyper_comb,D,A,T,S,t_l,t_r,ps,ews,pactive,peer_thresh_lo,peer_thresh_hi,
                  LAMBDA,dynamic_network,use_neighbor,border_cross_prob,phase_shift,multiply_lo,multiply_hi,core_use,max_peer_it,file=f)

    f.close()
    
#     df = pd.DataFrame(columns=['hyper_comb','DISTANCE_DECAY','SIGMOID_SCALAR','SIGMOID_EXPONENT','MEMORY_DECAY',
#                                'TIME_LEFT','TIME_RIGHT','BIAS_SCALE','EVENT_WEIGHT_SCALE','PEER_AFFECT_ACTIVE_DAY',
#                                'THRESH_HI','USE_NEIGHBOR','BORDER_CROSS_PROB','THRESH_LO','LAMBDA','COMMENT',
#                                'FIRST_PHASE_BORDER_CROSS_SCALE','SECOND_PHASE_BORDER_CROSS_SCALE'])

#     new_row = {'hyper_comb':hyper_comb,'DISTANCE_DECAY':D,'SIGMOID_SCALAR':A,'SIGMOID_EXPONENT':T,'MEMORY_DECAY':S,'TIME_LEFT':t_l,
#                'TIME_RIGHT':t_r,'BIAS_SCALE':ps,'EVENT_WEIGHT_SCALE':ews,'PEER_AFFECT_ACTIVE_DAY':pactive,
#                'THRESH_HI':peer_thresh_hi,'THRESH_LO':peer_thresh_lo,'LAMBDA':LAMBDA,
#                'USE_NEIGHBOR':use_neighbor,'BORDER_CROSS_PROB':border_cross_prob,'FIRST_PHASE_BORDER_CROSS_SCALE':multiply_lo,
#                'SECOND_PHASE_BORDER_CROSS_SCALE':multiply_hi,'COMMENT':comment}

#     df = df.append(new_row,ignore_index=True)

#     df.to_csv('../runtime_log/bayes_explore.csv', mode='a', index=False, header=False)
    
    print("job started for",hyper_comb)
    submit_output = subprocess.run(["bash","bayesian_calibration_"+str(hyper_comb)+".sh"],capture_output=True)
    print(submit_output)
else:
    print('exit program')
