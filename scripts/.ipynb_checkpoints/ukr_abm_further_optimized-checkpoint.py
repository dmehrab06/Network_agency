import pandas as pd
import numpy as np
import sys
import random
import time
import warnings
from file_paths_and_consts import *
import math
import s2sphere
import resource
import datetime
import multiprocessing as mp
import gc
##################################3

def haversine(lon1, lat1, lon2, lat2):
    KM = 6372.8 #Radius of earth in km instead of miles
    lat1, lon1, lat2, lon2 = map(np.deg2rad, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(a))
    total_km = KM * c
    return total_km

## rule about how each agent is affected by each impact
def prob_conflict(impact,dis,t_diff=0,DIS_EXPONENT=2.212):
    return ((impact)/(dis**DIS_EXPONENT))*(1.0/(1+t_diff))

## rule about how an agent is affected overall by all impact   (P(violence))
def aggregated_prob_conflict(x,A=55,T=0.8):  
    return 1 / (1 + A*math.exp(-T*x))

#https://www.nature.com/articles/s41599-018-0094-8
def memory_decay(x,S=0.9867):
    return x*S

## rule about how different types of demographic group decides to move
def get_move_prob(DEMO_NUMS): ##this function can be played with
    tot_size = 0
    for v in DEMO_NUMS:
        tot_size = tot_size+v
    if tot_size>1:
        move_prob = 0.0
        for i in range(0,len(DEMO_NUMS)):
            move_prob = move_prob + DEMO_NUMS[i]*FAMILY_PROB[i]
        return move_prob/tot_size
    else:
        move_prob = 0.0
        for i in range(0,len(DEMO_NUMS)):
            move_prob = move_prob + DEMO_NUMS[i]*MOVE_PROB[i]
        return move_prob/tot_size

def bernoulli(val,p):
    if (val<=p):
        return 1
    else:
        return 0
    
def bernoulli_border(val,moves,current_phase,multiply_lo=0.8,multiply_hi=1.5):
    if current_phase == 0:
        multiply = multiply_lo
    elif current_phase==1:
        multiply = multiply_hi
    else:
        multiply = multiply_hi
    if moves==0:
        return 0
    else:
        if val<=BORDER_CROSS_PROB*multiply:
            return 2
        else:
            return 1
    
def find_fixed_point(k,n,thresh_lo=0.0,thresh_hi=10.0):
    if n==0:
        n = 1
    if ((k-1))>=thresh_hi:
        return 1
    if (k)<thresh_lo:
        return 0
    return -1
    
def take_final_decision(intention,fixed_point):
    if fixed_point==0 or fixed_point==1:
        return fixed_point
    return intention

def refine_through_peer_effect(temp_household,current_phase=0):
    
    new_temp_households = temp_household.sort_values(by='hid')
    
    hh_movers = new_temp_households.groupby('s2_cell')['moves'].sum().reset_index()
    hh_neighbors = new_temp_households.groupby('s2_cell')['moves'].count().reset_index()
    
    hh_neighbors = hh_neighbors.rename(columns={'moves':'neighbors'})
    
    hh_neigh_vs_move = hh_neighbors.merge(hh_movers,on='s2_cell',how='inner')
    hh_neigh_vs_move['fixed_point'] = hh_neigh_vs_move.apply(lambda x: find_fixed_point(x['moves'],x['neighbors'],THRESH_LO,THRESH_HI),axis=1)
    
    new_temp_households = new_temp_households.merge(hh_neigh_vs_move[['s2_cell','fixed_point']],on='s2_cell',how='inner')
    new_temp_households['moves'] = new_temp_households.apply(lambda x: take_final_decision(x['moves'],x['fixed_point']),axis=1)
    new_temp_households = new_temp_households.drop(columns=['fixed_point'])
    #new_temp_households['moves'] = h_network_peer_move_count['peer_affected_move']
    return new_temp_households

def peer_effect_parallel(args):
    #temp_household,neighbor_chunk,THRESH_LO,THRESH_HI = args
    temp_household,THRESH_LO,THRESH_HI = args
    new_temp_households = temp_household.sort_values(by='hid')
    
    hh_movers = new_temp_households.groupby('s2_cell')['moves'].sum().reset_index()
    hh_neighbors = new_temp_households.groupby('s2_cell')['moves'].count().reset_index()
    
    hh_neighbors = hh_neighbors.rename(columns={'moves':'neighbors'})
    
    hh_neigh_vs_move = hh_neighbors.merge(hh_movers,on='s2_cell',how='inner')
    hh_neigh_vs_move['fixed_point'] = hh_neigh_vs_move.apply(lambda x: find_fixed_point(x['moves'],x['neighbors'],THRESH_LO,THRESH_HI),axis=1)
    
    new_temp_households = new_temp_households.merge(hh_neigh_vs_move[['s2_cell','fixed_point']],on='s2_cell',how='inner')
    new_temp_households['moves'] = new_temp_households.apply(lambda x: take_final_decision(x['moves'],x['fixed_point']),axis=1)
    new_temp_households = new_temp_households.drop(columns=['fixed_point'])
    #new_temp_households['moves'] = h_network_peer_move_count['peer_affected_move']
    return new_temp_households

# who is initiating 
# ethnicity data
def get_event_weight(event_type,sub_event_type):
    if sub_event_type==ablation_conflict_type:
        return 0
    if event_type=="Battles":
        return 3
    if event_type.startswith('Civilian'):
        return 8
    if event_type.startswith('Explosions'):
        return 5
    if event_type.startswith('Violence'):
        return 3
    if event_type.startswith('Protests') or event_type.startswith('Riots'):
        return 0
    return 0

def calc_attitude(cur_impact_data,cur_household_data,min_date):
    cur_impact_data = cur_impact_data.rename(columns={'latitude':'impact_lat','longitude':'impact_lng'})
    cur_impact_data['cur_time'] = min_date
    cur_impact_data['time_diff_to_event'] = (cur_impact_data['cur_time'] - cur_impact_data['time']) / np.timedelta64(1,'D')
    cur_impact_data['impact_intensity'] = cur_impact_data['event_weight']*cur_impact_data['event_intensity']*EVENT_WEIGHT_SCALAR
    cur_impact_data['impact_intensity'].replace(to_replace = 0, value = EPS, inplace=True)
    
    impact_in_homes = cur_impact_data.merge(cur_household_data,on='matching_place_id',how='inner')
    impact_in_homes['dis_conflict_home'] = haversine(impact_in_homes['h_lng'],impact_in_homes['h_lat'],impact_in_homes['impact_lng'],impact_in_homes['impact_lat'])
    impact_in_homes['prob_conflict'] = impact_in_homes['prob_conflict'].apply(lambda x: memory_decay(x,S))
    impact_in_homes['prob_conflict'] = impact_in_homes['prob_conflict'] + impact_in_homes.apply(lambda x: prob_conflict(x['impact_intensity'],x['dis_conflict_home'],x['time_diff_to_event'],DIS_EXPONENT),axis=1)
    cur_household_data = cur_household_data.drop(columns='prob_conflict')
    home_conflict_df = impact_in_homes.groupby(['hid'])['prob_conflict'].sum().reset_index()
    home_conflict_df['P(violence)'] = home_conflict_df['prob_conflict'].apply(lambda x: aggregated_prob_conflict(x,A,T))
    home_conflict_df = home_conflict_df.merge(cur_household_data,on='hid',how='inner')
    return home_conflict_df

def calc_attitude_parallel(args):
    cur_household_data,cur_impact_data,min_date,EVENT_WEIGHT_SCALAR,EPS,DIS_EXPONENT,A,T,S = args
    #cur_impact_data = impact_data[(impact_data.time>=lookahead_date_1) & (impact_data.time<=lookahead_date_2)]
    cur_impact_data = cur_impact_data.rename(columns={'latitude':'impact_lat','longitude':'impact_lng'})
    cur_impact_data['cur_time'] = min_date
    cur_impact_data['time_diff_to_event'] = (cur_impact_data['cur_time'] - cur_impact_data['time']) / np.timedelta64(1,'D')
    cur_impact_data['impact_intensity'] = cur_impact_data['event_weight']*cur_impact_data['event_intensity']*EVENT_WEIGHT_SCALAR
    cur_impact_data['impact_intensity'].replace(to_replace = 0, value = EPS, inplace=True)
    
    impact_in_homes = cur_impact_data.merge(cur_household_data,on='matching_place_id',how='inner')
    impact_in_homes['dis_conflict_home'] = haversine(impact_in_homes['h_lng'],impact_in_homes['h_lat'],impact_in_homes['impact_lng'],impact_in_homes['impact_lat'])
    impact_in_homes['prob_conflict'] = impact_in_homes['prob_conflict'].apply(lambda x: memory_decay(x,S))
    impact_in_homes['prob_conflict'] = impact_in_homes['prob_conflict'] + impact_in_homes.apply(lambda x: prob_conflict(x['impact_intensity'],x['dis_conflict_home'],x['time_diff_to_event'],DIS_EXPONENT),axis=1)
    cur_household_data = cur_household_data.drop(columns='prob_conflict')
    home_conflict_df = impact_in_homes.groupby(['hid'])['prob_conflict'].sum().reset_index()
    home_conflict_df['P(violence)'] = home_conflict_df['prob_conflict'].apply(lambda x: aggregated_prob_conflict(x,A,T))
    home_conflict_df = home_conflict_df.merge(cur_household_data,on='hid',how='inner')
    return home_conflict_df

def multiproc_attitude(cur_household_data, impact_data, lookahead_date_1, lookahead_date_2, min_date):
    cpus = USE_CORE#mp.cpu_count()
    #st_time = time.time()
    hh_splits = np.array_split(cur_household_data, cpus) #--this a list with multiple dataframe.. each dataframe is used by one core
    cur_impact_data = impact_data[(impact_data.time>=lookahead_date_1) & (impact_data.time<=lookahead_date_2)]
    pool_args = [(h_chunk,cur_impact_data,min_date,EVENT_WEIGHT_SCALAR,EPS,DIS_EXPONENT,A,T,S) for h_idx,h_chunk in enumerate(hh_splits)]
    #print('total time taken to split',time.time()-st_time)
    pool = mp.Pool(processes = cpus)
    results = pool.map(calc_attitude_parallel, pool_args)
    pool.close()
    pool.join()
    return results

def multiproc_peer_effect(temp_household):
    gb = temp_household.groupby('core_id')
    hh_chunks = [gb.get_group(x) for x in gb.groups]
    grps = [x for x in gb.groups]
    
    chunk_sizes = [bleh.shape[0] for bleh in hh_chunks]
    print('hh chunk sizes before sending to peer effect workers',flush=True)
    print(chunk_sizes,flush=True)
    
    cpus = min(USE_CORE,len(grps))
    #pool_args = [(h_chunk,neighbor_chunks[grps[h_idx]],THRESH_LO,THRESH_HI) for h_idx,h_chunk in enumerate(hh_chunks)]
    pool_args = [(h_chunk,THRESH_LO,THRESH_HI) for h_idx,h_chunk in enumerate(hh_chunks)]
    pool = mp.Pool(processes = cpus)
    results = pool.map(peer_effect_parallel, pool_args)
    pool.close()
    pool.join()
    return results

def getl13(lat,lng,req_level=13):
    p = s2sphere.LatLng.from_degrees(lat, lng) 
    cell = s2sphere.Cell.from_lat_lng(p)
    cellid = cell.id()
    for i in range(1,30):
        #print(cellid)
        if cellid.level()==req_level:
            return cellid
        cellid = cellid.parent()

def get_core_id(s2cellid):
    return (int(s2cellid.to_token(),16)//16)%USE_CORE

    
if __name__ == "__main__":
    print('starting',flush=True)
    mp.set_start_method('forkserver')
    warnings.filterwarnings('ignore')

    #random.seed(time.time())
    random.seed(420)
    
    start_time = time.time()
    print(datetime.datetime.now(),flush=True)

    #############################2
    APPLY_PEER = 1
    EPS = 0.0001 #0.0001 ## calibrated
    CONFLICT_DATA_PREFIX = 'ukraine_conflict_data_ADM2_HDX_buffer_'
    HOUSEHOLD_DATA_PREFIX = 'ukraine_household_data_ADM2_HDX.csv'
    NEIGHBOR_DATA_PREFIX = 'ukraine_neighbor_'
    START_DATE = '2022-02-24'
    END_DATE = '2022-10-15'

    USE_CORE = int(sys.argv[21])

    PLACE_NAME = sys.argv[1]
    print(PLACE_NAME,flush=True)
    hyper_comb = int(sys.argv[2])
    DIS_EXPONENT = float(sys.argv[3])
    A = float(sys.argv[4]) 
    T = float(sys.argv[5]) 
    S = float(sys.argv[6])##probably don't use it anymore 
    lookbefore_days_left = int(sys.argv[7])
    lookbefore_days_right = int(sys.argv[8])
    PROB_SCALAR = float(sys.argv[9]) 
    EVENT_WEIGHT_SCALAR = float(sys.argv[10])
    USE_PEER_EFFECT = int(sys.argv[11]) ##probably always use it
    USE_CIVIL_DATA = 0
    THRESH_LO = float(sys.argv[12])
    THRESH_HI = float(sys.argv[13])
    STRUCT = int(sys.argv[14]) ##probably always 13
    CHANGE_NETWORK = int(sys.argv[15]) ##don't change it
    USE_NEIGHBOR = int(sys.argv[16]) ##always 5
    BORDER_CROSS_PROB = float(sys.argv[17])
    PHASE_SHIFT = int(sys.argv[18])
    ablation_conflict_type = 'None'
    multiply_lo = float(sys.argv[19])
    multiply_hi = float(sys.argv[20])
    multiply_very_hi = random.uniform(2.5,2.8)

    MOVE_PROB = [0.25,0.7,0.02,0.7]
    FAMILY_PROB = [0.25,0.85,0.1,0.85]


    for i in range(len(MOVE_PROB)):
        MOVE_PROB[i] = MOVE_PROB[i]*PROB_SCALAR
        FAMILY_PROB[i] = FAMILY_PROB[i]*PROB_SCALAR



    total_impact_data = pd.read_csv(IMPACT_DIR+CONFLICT_DATA_PREFIX+str(USE_NEIGHBOR)+'_km.csv')
    total_household_data = pd.read_csv(HOUSEHOLD_DIR+HOUSEHOLD_DATA_PREFIX)

    impact_data = total_impact_data[total_impact_data.matching_place_id==PLACE_NAME]
    impact_data['time'] = pd.to_datetime(impact_data['time'])
    impact_data['event_weight'] = impact_data.apply(lambda x: get_event_weight(x['event_type'],x['sub_event_type']),axis=1)

    cur_household_data = total_household_data[total_household_data.matching_place_id==PLACE_NAME]
    cur_household_data['s2_cell'] = cur_household_data.apply(lambda x: getl13(x['latitude'],x['longitude'],STRUCT),axis=1)

    if USE_CORE>1:
        cur_household_data['core_id'] = cur_household_data['s2_cell'].apply(lambda x: get_core_id(x))
        
    print('data loaded until garbage collector',flush=True)
    cur_household_data['hh_size'] = cur_household_data[DEMO_TYPES].sum(axis = 1, skipna = True)
    cur_household_data['P(move|violence)'] = cur_household_data.apply(lambda x: get_move_prob([x['OLD_PERSON'],x['CHILD'],x['ADULT_MALE'],x['ADULT_FEMALE']]),axis=1)
    cur_household_data['prob_conflict'] = 0
    cur_household_data['moves'] = 0
    cur_household_data['move_type'] = 0 # 0 means did not move, 1 means IDP, 2 means outside

    if 'h_lat' not in cur_household_data.columns.tolist():
        cur_household_data = cur_household_data.rename(columns={'latitude':'h_lat','longitude':'h_lng'})
    temp_prefix = ''
    if ablation_conflict_type!='None':
        OUTPUT_DIR = ABLATION_DIR
        temp_prefix = 'ablation_'

    f = 0
    start = time.time()
    cur_checkpoint = 1000
    #print('combination_no',hyper_comb)

    prev_temp_checkpoint = 0
    last_saved_checkpoint = -1

    peer_used = 0

    DEL_COLUMNS = ['P(violence)','P(move)','random']
    min_date = pd.to_datetime(START_DATE)
    end_date = pd.to_datetime(END_DATE)
    simulated_refugee_df = pd.DataFrame(columns=['id','time','refugee','old_people','child','male','female'])
    simulated_leaving_df = pd.DataFrame(columns=['id','time','leaving','old_people','child','male','female'])
    timing_log_df = pd.DataFrame(columns=['step','remaining_household_agent','remaining_person_agent','conflict_events_now','network_nodes',
                                          'network_edges','attitude_time','pcb_time','subjective_norm_time','pre_time','post_time'])
    who_went_where = []
    hid_displacement_df = []
    print('simulation_starting')
    ATT_FLAG = 1
    PBC_FLAG = 1
    SN_FLAG = 1 #0-0 1-1 2-0 3-1
    #########################################5
    for i in range(0,100):

        print(min_date,flush=True)
        preprocess_start = time.time()
        prev_temp_checkpoint = prev_temp_checkpoint + 1

        max_date = min_date + pd.DateOffset(days=1)
        lookahead_date_1 = min_date - pd.DateOffset(days=lookbefore_days_left)
        lookahead_date_2 = min_date - pd.DateOffset(days=lookbefore_days_right)

        if(f==1 and min_date > end_date):
            break

        if(f!=0):
            cur_household_data = pd.read_csv(TEMPORARY_DIR+'last_saved_household_data_'+str(temp_prefix)+str(PLACE_NAME)+'_'+str(hyper_comb)+'.csv')
            cur_household_data = cur_household_data[cur_household_data.moves==0]

        if(cur_household_data.shape[0]==0):
            new_row = {'id':PLACE_NAME,'time':min_date,'refugee':0,'old_people':0,'child':0,'male':0,'female':0}
            new_row_2 = {'id':PLACE_NAME,'time':min_date,'leaving':0,'old_people':0,'child':0,'male':0,'female':0}
            min_date = max_date
            simulated_refugee_df = simulated_refugee_df.append(new_row,ignore_index=True)
            simulated_leaving_df = simulated_leaving_df.append(new_row_2,ignore_index=True)
            continue

        cur_impact_data = impact_data[(impact_data.time>=lookahead_date_1) & (impact_data.time<=lookahead_date_2)]

        if(cur_impact_data.shape[0]==0):
            new_row = {'id':PLACE_NAME,'time':min_date,'refugee':0,'old_people':0,'child':0,'male':0,'female':0}
            new_row_2 = {'id':PLACE_NAME,'time':min_date,'leaving':0,'old_people':0,'child':0,'male':0,'female':0}
            min_date = max_date
            simulated_refugee_df = simulated_refugee_df.append(new_row,ignore_index=True)
            simulated_leaving_df = simulated_leaving_df.append(new_row_2,ignore_index=True)
            continue
        preprocess_end = time.time()
        ############# Social theory and main agent decision start ###########
        ### attitude
        print(preprocess_end-preprocess_start,'time to preprocess',flush=True)
        
        attitude_start = time.time()
        if USE_CORE==1:
            home_conflict_df = calc_attitude(cur_impact_data,cur_household_data,min_date)
        else:
            home_conflict_df = multiproc_attitude(cur_household_data,impact_data,lookahead_date_1,lookahead_date_2,min_date)
            home_conflict_df = pd.concat(home_conflict_df)
        attitude_end = time.time()
        print(attitude_end-attitude_start,'time to attitue',flush=True)
        ## attitude

        ##pcb
        pcb_start = time.time()
        if PBC_FLAG==1:
            home_conflict_df['P(move)'] = home_conflict_df['P(violence)']*home_conflict_df['P(move|violence)']
        else:
            home_conflict_df['P(move)'] = home_conflict_df['P(violence)']
        home_conflict_df['random'] = np.random.random(home_conflict_df.shape[0])
        home_conflict_df['moves'] = home_conflict_df.apply(lambda x: bernoulli(x['random'],x['P(move)']),axis=1)
        home_conflict_df = home_conflict_df.drop(columns=DEL_COLUMNS)
        pcb_end = time.time()
        print(pcb_end-pcb_start,'time to pbc',flush=True)
        ##pcb

        subjective_norm_start = time.time()
        temp_households = home_conflict_df
        nodes = temp_households.shape[0]
        phase = 0 if (peer_used < PHASE_SHIFT) else 1

        if APPLY_PEER==1 and peer_used<USE_PEER_EFFECT and SN_FLAG==1:
            if USE_CORE==1:
                temp_households = refine_through_peer_effect(temp_households,phase)
            else:
                #temp_households = refine_through_peer_effect(temp_households,phase)
                print('peer effect household size',temp_households.shape[0],flush=True)
                temp_households = multiproc_peer_effect(temp_households)
                temp_households = pd.concat(temp_households)
            peer_used = peer_used + 1

        temp_households['move_type_random'] = np.random.random(temp_households.shape[0])
        temp_households['move_type'] = temp_households.apply(lambda x: bernoulli_border(x['move_type_random'],x['moves'],phase,multiply_lo,multiply_hi),axis=1)
        temp_households = temp_households.drop(columns=['move_type_random'])
        subjective_norm_end = time.time()
        print(subjective_norm_end-subjective_norm_start,'time to peer effect',flush=True)
        ############# Social theory and main agent decision end ########### 
        post_process_and_save_start = time.time()

        new_row = {'id':PLACE_NAME,'time':min_date,'refugee':temp_households[temp_households.move_type==2]['hh_size'].sum(),
                   'old_people':temp_households[temp_households.move_type==2]['OLD_PERSON'].sum(),
                   'child':temp_households[temp_households.move_type==2]['CHILD'].sum(),
                   'male':temp_households[temp_households.move_type==2]['ADULT_MALE'].sum(),
                   'female':temp_households[temp_households.move_type==2]['ADULT_FEMALE'].sum()}
        simulated_refugee_df = simulated_refugee_df.append(new_row,ignore_index=True)
        new_row_2 = {'id':PLACE_NAME,'time':min_date,'leaving':temp_households[temp_households.moves==1]['hh_size'].sum(),
                   'old_people':temp_households[temp_households.moves==1]['OLD_PERSON'].sum(),
                     'child':temp_households[temp_households.moves==1]['CHILD'].sum(),
                    'male':temp_households[temp_households.moves==1]['ADULT_MALE'].sum(),
                     'female':temp_households[temp_households.moves==1]['ADULT_FEMALE'].sum()}
        simulated_leaving_df = simulated_leaving_df.append(new_row_2,ignore_index=True)

        temp_households['move_date'] = str(min_date)
        hid_displacement_df.append(temp_households[temp_households.move_type!=0])
        temp_households = temp_households.drop(columns=['move_date'])

        curtime = time.time()
        if((curtime-start)>=1):
            #print('checkpoint for ',str(PLACE_NAME))
            simulated_refugee_df.to_csv(OUTPUT_DIR+'mim_result_'+str(PLACE_NAME)+'_'+str(hyper_comb).zfill(5)+'.csv',index=False)
            simulated_leaving_df.to_csv(OUTPUT_DIR+'mim_result_leaving_'+str(PLACE_NAME)+'_'+str(hyper_comb).zfill(5)+'.csv',index=False)
            start = curtime

        temp_households.to_csv(TEMPORARY_DIR+'last_saved_household_data_'+str(temp_prefix)+str(PLACE_NAME)+'_'+str(hyper_comb)+'.csv',index=False)
        
#         moving_households_df = temp_households[temp_households.move_type==2].reset_index()

#         dest_prob_distribution_df = pd.read_csv(OUTPUT_DIR+'destination_probability_distribution.csv')
#         if(str(min_date) not in dest_prob_distribution_df.columns.tolist()):
#             cur_dest_prob_df = dest_prob_distribution_df[['ISO','2022-03-01 00:00:00']]
#             weight_col = '2022-03-01 00:00:00'
#         else:
#             cur_dest_prob_df = dest_prob_distribution_df[['ISO',str(min_date)]]
#             weight_col = str(min_date)
#         dest_sampled = cur_dest_prob_df.sample(moving_households_df.shape[0],replace=True,weights=cur_dest_prob_df[weight_col]).reset_index()
#         moving_households_df['destination'] = dest_sampled['ISO']
#         moving_households_df['moved_date'] = str(min_date)
#         who_went_where.append(moving_households_df)

        last_saved_checkpoint = prev_temp_checkpoint
        min_date = max_date
        f = 1

        post_process_and_save_end = time.time()
        print(post_process_and_save_end-post_process_and_save_start,'time to post process',flush=True)

        timing_row = {'step':i,'remaining_household_agent':cur_household_data.shape[0],'remaining_person_agent':cur_household_data['hh_size'].sum(),
                      'conflict_events_now':cur_impact_data.shape[0],'network_nodes':nodes,'network_edges':nodes*nodes,
                      'attitude_time':attitude_end-attitude_start,'pcb_time':pcb_end-pcb_start,'subjective_norm_time':subjective_norm_end-subjective_norm_start,
                      'pre_time':preprocess_end-preprocess_start,'post_time':post_process_and_save_end-post_process_and_save_start}
        timing_log_df = timing_log_df.append(timing_row,ignore_index=True)


    ##################################6
    hid_all_displacement_df = pd.concat(hid_displacement_df)
    hid_all_displacement_df.to_csv(OUTPUT_DIR+'mim_hid_completed_'+str(PLACE_NAME)+'_'+str(hyper_comb).zfill(5)+'.csv',index=False)

    #simulated_refugee_with_dest_df = pd.concat(who_went_where)
    #simulated_refugee_with_dest_df.to_csv(OUTPUT_DIR+'mdm_result_completed_'+str(PLACE_NAME)+"_"+str(hyper_comb).zfill(5)+'.csv',index=False)
    
    simulated_refugee_df.to_csv(OUTPUT_DIR+'mim_result_completed_'+str(PLACE_NAME)+'_'+str(hyper_comb).zfill(5)+'.csv',index=False)
    simulated_leaving_df.to_csv(OUTPUT_DIR+'mim_result_leaving_completed_'+str(PLACE_NAME)+'_'+str(hyper_comb).zfill(5)+'.csv',index=False)

    end = time.time()
    timing_log_df.to_csv(OUTPUT_DIR+'timing_log_'+str(PLACE_NAME)+'_'+str(hyper_comb).zfill(5)+'.csv',index=False)

    data = {'raion': [PLACE_NAME],'runtime':[end-start_time],'hyper_comb':[hyper_comb],
            'start_time':start_time,'end_time':end,'memory_consumed':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}
    run_df = pd.DataFrame(data)

    # append data frame to CSV file
    run_df.to_csv('../runtime_log/runtime_raion_for_paper.csv', mode='a', index=False, header=False)

    #TODO:
    #a) add timing module
    #b) save info about remaining households and agents after each costly module, at least the number of households for now
    print(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,flush=True)
    print(datetime.datetime.now(),flush=True)
