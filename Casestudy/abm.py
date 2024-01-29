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
import os
from datetime import timedelta
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
def memory_decay(x,S=98.67):
    return x*(S/100.00)

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
## thresh_type==0
## if my current decision is to migrate (1), if less than thresh_lo of my neighbors are migrating, i will change my decision to (0)

## thresh_type==1
## if my current decision is not to migrate(0), if at least thresh_hi of my neigbhors are migrating, i will change my decision to (1)

def apply_threshold_function(cur_decision,cur_neighbor_migrating,thresh_lo=0,thresh_hi=10):
        if cur_decision==1:
            if cur_neighbor_migrating<thresh_lo:
                return 0
        if cur_decision==0:
            if cur_neighbor_migrating>=thresh_hi:
                return 1
        return cur_decision

def apply_voter_model(cur_decision,p_fraction,m_fraction,LAMBDA=0.99,thresh_lo=0.01,thresh_hi=0.4):
        tot_fraction = p_fraction*LAMBDA + m_fraction*(1.0-LAMBDA)
        if tot_fraction>=thresh_hi:
            return 1
        if tot_fraction<=thresh_lo:
            return 0
        return cur_decision
    
def refine_through_peer_effect(temp_household,current_phase=0):
    
    new_temp_households = temp_household.sort_values(by='hid')
    
    network_creation_start = time.time()
    cur_neighbor_household = neighbor_household_data[neighbor_household_data.hid_x.isin(new_temp_households.hid.tolist())]
    cur_neighbor_household = cur_neighbor_household[cur_neighbor_household.hid_y.isin(new_temp_households.hid.tolist())]#hidx,hidy
    cur_neighbor_household = cur_neighbor_household.merge(new_temp_households[['hid','moves']],left_on='hid_x',right_on='hid',how='inner')#hidx--hid--moves,hidy
    cur_neighbor_household = cur_neighbor_household.rename(columns={'moves':'moves_x'})
    cur_neighbor_household = cur_neighbor_household.drop(columns=['hid'])#hidx-movex-hidy
    h_network = cur_neighbor_household.merge(new_temp_households[['hid','moves']],left_on='hid_y',right_on='hid',how='inner')
    h_network = h_network.rename(columns={'moves':'moves_y'})
    h_network = h_network.drop(columns=['hid'])#hidx--moves_x--hidy--moves_y
    #print('network creation through node merging by s2 takes',time.time()-network_creation_start,'seconds')
    
    neighbor_count_start = time.time()
    h_network_peer_move_count = (h_network.groupby('hid_x')['moves_y'].sum().reset_index()).merge(h_network[['hid_x','moves_x']].drop_duplicates(),on='hid_x',how='inner')
    h_network_peer_move_count['moves_y'] = h_network_peer_move_count['moves_y'] - h_network_peer_move_count['moves_x'] #hid_x,moves_y
    
    h_network_peer_rem_count = h_network.groupby('hid_x')['moves_y'].count().reset_index()
    h_network_peer_rem_count = h_network_peer_rem_count.merge(new_temp_households[['hid','N_size']],left_on='hid_x',right_on='hid',how='inner')
    h_network_peer_rem_count['N_gone'] =  h_network_peer_rem_count['N_size'] - h_network_peer_rem_count['moves_y']
    h_network_peer_rem_count = h_network_peer_rem_count.drop(columns=['hid','moves_y']) #hid_x,N_size,'N_gone'
    
    h_state = h_network_peer_move_count[['hid_x','moves_y','moves_x']].merge(h_network_peer_rem_count,on='hid_x',how='inner')
    #print(h_network_peer_move_count.shape,h_network_peer_rem_count.shape,h_state.shape)
    h_state['p_fraction'] = h_state['moves_y']/h_state['N_size']
    h_state['m_fraction'] = h_state['N_gone']/h_state['N_size']
    
    
    threshold_func_start = time.time()
    h_state['peer_affected_move'] = h_state.apply(lambda x: apply_voter_model(x['moves_x'],x['p_fraction'],x['m_fraction'],LAMBDA,THRESH_LO,THRESH_HI),axis=1)
    #print('applying threshold function takes',time.time()-threshold_func_start,'seconds')
    h_state = h_state.sort_values(by='hid_x')

    #print(h_network_peer_move_count[h_network_peer_move_count.moves_x==1].shape,h_network_peer_move_count[h_network_peer_move_count.peer_affected_move==1].shape)
    #print(h_network_peer_move_count.shape,new_temp_households.shape)
    new_temp_households['moves'] = h_state['peer_affected_move']
    return new_temp_households

def peer_effect_parallel(args):
    temp_household,neighbor_chunk,THRESH_LO,THRESH_HI,LAMBDA = args
    
    new_temp_households = temp_household.sort_values(by='hid')
    
    network_creation_start = time.time()
    cur_neighbor_household = neighbor_chunk[neighbor_chunk.hid_x.isin(new_temp_households.hid.tolist())]
    cur_neighbor_household = cur_neighbor_household[cur_neighbor_household.hid_y.isin(new_temp_households.hid.tolist())]#hidx,hidy
    cur_neighbor_household = cur_neighbor_household.merge(new_temp_households[['hid','moves']],left_on='hid_x',right_on='hid',how='inner')#hidx--hid--moves,hidy
    cur_neighbor_household = cur_neighbor_household.rename(columns={'moves':'moves_x'})
    cur_neighbor_household = cur_neighbor_household.drop(columns=['hid'])#hidx-movex-hidy
    h_network = cur_neighbor_household.merge(new_temp_households[['hid','moves']],left_on='hid_y',right_on='hid',how='inner')
    h_network = h_network.rename(columns={'moves':'moves_y'})
    h_network = h_network.drop(columns=['hid'])#hidx--moves_x--hidy--moves_y
    #print('network creation through node merging by s2 takes',time.time()-network_creation_start,'seconds')
    
    neighbor_count_start = time.time()
    h_network_peer_move_count = (h_network.groupby('hid_x')['moves_y'].sum().reset_index()).merge(h_network[['hid_x','moves_x']].drop_duplicates(),on='hid_x',how='inner')
    h_network_peer_move_count['moves_y'] = h_network_peer_move_count['moves_y'] - h_network_peer_move_count['moves_x'] #hid_x,moves_y
    
    h_network_peer_rem_count = h_network.groupby('hid_x')['moves_y'].count().reset_index()
    h_network_peer_rem_count = h_network_peer_rem_count.merge(new_temp_households[['hid','N_size']],left_on='hid_x',right_on='hid',how='inner')
    h_network_peer_rem_count['N_gone'] =  h_network_peer_rem_count['N_size'] - h_network_peer_rem_count['moves_y']
    h_network_peer_rem_count = h_network_peer_rem_count.drop(columns=['hid','moves_y']) #hid_x,N_size,'N_gone'
    
    h_state = h_network_peer_move_count[['hid_x','moves_y','moves_x']].merge(h_network_peer_rem_count,on='hid_x',how='inner')
    h_state['p_fraction'] = h_state['moves_y']/h_state['N_size']
    h_state['m_fraction'] = h_state['N_gone']/h_state['N_size']
    
    
    threshold_func_start = time.time()
    h_state['peer_affected_move'] = h_state.apply(lambda x: apply_voter_model(x['moves_x'],x['p_fraction'],x['m_fraction'],LAMBDA,THRESH_LO,THRESH_HI),axis=1)
    #print('applying threshold function takes',time.time()-threshold_func_start,'seconds')
    h_state = h_state.sort_values(by='hid_x')

    #print(h_network_peer_move_count[h_network_peer_move_count.moves_x==1].shape,h_network_peer_move_count[h_network_peer_move_count.peer_affected_move==1].shape)
    #print(h_network_peer_move_count.shape,new_temp_households.shape)
    new_temp_households['moves'] = h_state['peer_affected_move']
    
    return new_temp_households


def get_event_weight(event_type,sub_event_type,INTENSITY_SCALE):
    if sub_event_type==ablation_conflict_type:
        return 0
    if event_type=="Battles":
        return 3*INTENSITY_SCALE
    if event_type.startswith('Civilian'):
        return 8*INTENSITY_SCALE
    if event_type.startswith('Explosions'):
        return 5*INTENSITY_SCALE
    if event_type.startswith('Violence'):
        return 3*INTENSITY_SCALE
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
    impact_in_homes['cur_prob_conflict'] = impact_in_homes.apply(lambda x: prob_conflict(x['impact_intensity'],x['dis_conflict_home'],x['time_diff_to_event'],DIS_EXPONENT),axis=1)
    #print(impact_in_homes.shape[0],flush=True)
    impact_in_homes['prob_conflict'] = impact_in_homes['prob_conflict'] + impact_in_homes['cur_prob_conflict']
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
    impact_in_homes['cur_prob_conflict'] = impact_in_homes.apply(lambda x: prob_conflict(x['impact_intensity'],x['dis_conflict_home'],x['time_diff_to_event'],DIS_EXPONENT),axis=1)
    #print(impact_in_homes.shape[0],flush=True)
    impact_in_homes['prob_conflict'] = impact_in_homes['prob_conflict'] + impact_in_homes['cur_prob_conflict']
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
    #print('# of chunks',len(hh_chunks))
    chunk_sizes = [bleh.shape[0] for bleh in hh_chunks]
    #print('hh chunk sizes before sending to peer effect workers',flush=True)
    #print(chunk_sizes,flush=True)
    
    cpus = min(USE_CORE,len(grps))
    pool_args = [(h_chunk,neighbor_chunks[grps[h_idx]],THRESH_LO,THRESH_HI, LAMBDA) for h_idx,h_chunk in enumerate(hh_chunks)]
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

def trim_neighborhood(hh_series,USE_CORE,graph):
    if USE_CORE==1:
        #graph is just a single dataframe
        graph = graph[graph.hid_x.isin(hh_series.tolist())]
    else:
        for i in range(len(graph)):
            graph[i] = graph[i][graph[i].hid_x.isin(hh_series.tolist())]

    
if __name__ == "__main__":
    print('starting',flush=True)
    mp.set_start_method('forkserver')
    warnings.filterwarnings('ignore')

    
    start_time = time.time()
    print(datetime.datetime.now(),flush=True)

    #############################2
    APPLY_PEER = 1
    EPS = 0.0001 #0.0001 ## calibrated
    CONFLICT_DATA_PREFIX = 'ukraine_conflict_data_ADM2_HDX_buffer_'
    HOUSEHOLD_DATA_PREFIX = 'ukraine_household_data_ADM2_HDX.csv'
    NEIGHBOR_DATA_PREFIX = 'ukraine_neighbor_'
    START_DATE = '2022-02-24'
    END_DATE = '2022-05-31'
    
    MAX_PEER_IT = 4
    USE_CORE = int(sys.argv[3])
    INTENSITY_SCALE = float(sys.argv[4])
    TIME_SHIFT = int(sys.argv[5])
    PLACE_NAME = sys.argv[1]
    hyper_comb = int(sys.argv[2])
    SEED = int(sys.argv[6])
    #random.seed(time.time())
    random.seed(SEED)
    np.random.seed(SEED)
    
    print(PLACE_NAME,hyper_comb,flush=True)
    DIS_EXPONENT = 2.18663941021782
    A = 37.060756808501864 
    T = 1.0348449500885029 
    S = 57.91356700688765
    lookbefore_days_left = int(6.0)
    lookbefore_days_right = int(6.0)
    PROB_SCALAR = 0.1781240295913104 
    EVENT_WEIGHT_SCALAR = 0.8762308137940877
    USE_PEER_EFFECT = 1000 ##probably always use it
    USE_CIVIL_DATA = 0
    THRESH_LO = 0.09264433888912414
    THRESH_HI = 0.6674105660199474
    STRUCT = 13 ##probably always 13
    LAMBDA = 0.4980495151203558
    CHANGE_NETWORK = 1 ##don't change it
    USE_NEIGHBOR = 5 ##always 5
    BORDER_CROSS_PROB = 0.32547145503549796
    PHASE_SHIFT = 1000
    ablation_conflict_type = 'None'
    multiply_lo = 1.0
    multiply_hi = 1.8
    multiply_very_hi = random.uniform(2.5,2.8)
    NETWORK_TYPE = '_R_0.01_P_0.04_Q_8_al_2.3.csv'
    
    MOVE_PROB = [0.25,0.7,0.02,0.7]
    FAMILY_PROB = [0.25,0.85,0.1,0.85]


    for i in range(len(MOVE_PROB)):
        MOVE_PROB[i] = MOVE_PROB[i]*PROB_SCALAR
        FAMILY_PROB[i] = FAMILY_PROB[i]*PROB_SCALAR

    if os.path.isfile(OUTPUT_DIR+'mim_result_completed_'+str(PLACE_NAME)+'_'+str(hyper_comb).zfill(5)+'.csv'):
        print('ache')
        exit(0)

    total_impact_data = pd.read_csv(IMPACT_DIR+CONFLICT_DATA_PREFIX+str(USE_NEIGHBOR)+'_km.csv')
    total_household_data = pd.read_csv(HOUSEHOLD_DIR+HOUSEHOLD_DATA_PREFIX)
    if os.path.isfile(HOUSEHOLD_DIR+'KSW_HH_BALL_AAMAS_'+PLACE_NAME+NETWORK_TYPE):
        neighbor_household_data = pd.read_csv(HOUSEHOLD_DIR+'KSW_HH_BALL_AAMAS_'+PLACE_NAME+NETWORK_TYPE)
    else:
        neighbor_household_data = pd.read_csv(HOUSEHOLD_DIR+NEIGHBOR_DATA_PREFIX+PLACE_NAME+'_'+str(STRUCT)+'_s2.csv',usecols=['hid_x','hid_y'])

    neighbor_cnts = neighbor_household_data['hid_x'].value_counts().reset_index().rename(columns={'index':'hid','hid_x':'N_size'})

    impact_data = total_impact_data[total_impact_data.matching_place_id==PLACE_NAME]
    impact_data['time'] = pd.to_datetime(impact_data['time'])
    impact_data['time'] = impact_data['time'] + timedelta(days=TIME_SHIFT)
    impact_data['event_weight'] = impact_data.apply(lambda x: get_event_weight(x['event_type'],x['sub_event_type'],INTENSITY_SCALE),axis=1)

    cur_household_data = total_household_data[total_household_data.matching_place_id==PLACE_NAME]
    cur_household_data['s2_cell'] = cur_household_data.apply(lambda x: getl13(x['latitude'],x['longitude'],STRUCT),axis=1)
    cur_household_data = cur_household_data.merge(neighbor_cnts,on='hid',how='inner') 

    if USE_CORE>1:
        neighbor_household_data['core_id'] = neighbor_household_data['s2_id']%USE_CORE
        neighbor_household_data = neighbor_household_data.drop(columns=['s2_id'])
        gb = neighbor_household_data.groupby('core_id')
        neighbor_chunks = [gb.get_group(x) for x in gb.groups]
        cur_household_data['core_id'] = cur_household_data['s2_cell'].apply(lambda x: get_core_id(x))
        del neighbor_household_data
        gc.collect()
        
    #print('data loaded until garbage collector',flush=True)
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
    SN_FLAG = 1
    #########################################5
    for i in range(0,300):

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
            if USE_CORE==1:
                trim_neighborhood(cur_household_data['hid'],USE_CORE,neighbor_household_data)
            else:
                trim_neighborhood(cur_household_data['hid'],USE_CORE,neighbor_chunks)

        if(cur_household_data.shape[0]<2):
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
                for peer_it in range(0,MAX_PEER_IT):
                    temp_households = refine_through_peer_effect(temp_households,phase)
            else:
                #temp_households = refine_through_peer_effect(temp_households,phase)
                #print('peer effect household size',temp_households.shape[0],flush=True)
                for peer_it in range(0,MAX_PEER_IT):
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
        if((curtime-start)>=cur_checkpoint):
            #print('checkpoint for ',str(PLACE_NAME))
            simulated_refugee_df.to_csv(OUTPUT_DIR+'mim_result_'+str(PLACE_NAME)+'_'+str(hyper_comb).zfill(5)+'.csv',index=False)
            simulated_leaving_df.to_csv(OUTPUT_DIR+'mim_result_leaving_'+str(PLACE_NAME)+'_'+str(hyper_comb).zfill(5)+'.csv',index=False)
            start = curtime

        temp_households.to_csv(TEMPORARY_DIR+'last_saved_household_data_'+str(temp_prefix)+str(PLACE_NAME)+'_'+str(hyper_comb)+'.csv',index=False)
        

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
print('done')
