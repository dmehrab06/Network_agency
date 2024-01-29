import pandas as pd
import random
import time
import sys

random.seed(time.time())

param = {'A': 70.0, 'D': 5.0, 'S': 1.0, 'T': 3.0, 'b_prob': 0.4, 'ews': 1.0, 'lambda': 1.0, 'p_hi': 1.0, 'p_lo': 0.1, 'peer_it': 5.0, 'ps': 1.0, 't_r': 14.0}

D = param['D']
A = param['A']
T = param['T']
S = param['S']*100.0

t_l = int(param['t_r'])#random.sample([10,12,14,18,21],1)[0]
t_r = t_l

ps = param['ps']#0.21#random.uniform(0.4,1.0) ##make this a parameter
ews = param['ews']
pactive = 1000#random.randint(15,30)
peer_thresh_hi = param['p_hi']#0.8
peer_thresh_lo = param['p_lo']
LAMBDA = param['lambda']
network_struct = 13
dynamic_network = 1
phase_shift = 1000
use_neighbor = 5#random.sample([2,5,8,10],1)[0]#random.unifrom(0.0,1.0)
border_cross_prob = param['b_prob']
multiply_lo = 1.0
multiply_hi = 1.8

max_peer_it = int(param['peer_it'])
hyper_comb = int(sys.argv[1])


#first: order the raions
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
    print('sbatch --mem='+str(mem_req)+' --cpus-per-task='+str(core_use)+' abm.sbatch',end=' ')
    if name.startswith('Chornobyl'):
        print('"'+name+'"',hyper_comb,D,A,T,S,t_l,t_r,ps,ews,pactive,peer_thresh_lo,peer_thresh_hi,
              LAMBDA,dynamic_network,use_neighbor,border_cross_prob,phase_shift,multiply_lo,multiply_hi,core_use,max_peer_it)
    else:
        print(name,hyper_comb,D,A,T,S,t_l,t_r,ps,ews,pactive,peer_thresh_lo,peer_thresh_hi,
              LAMBDA,dynamic_network,use_neighbor,border_cross_prob,phase_shift,multiply_lo,multiply_hi,core_use,max_peer_it)


for i in range(len(set2_raion)):
    name = set2_raion[i]
    mem_req = set2_mem[i]
    core_use = set2_cc[i]
    print('sbatch --mem='+str(mem_req)+' --cpus-per-task='+str(core_use)+' abm.sbatch',end=' ')
    if name.startswith('Chornobyl'):
        print('"'+name+'"',hyper_comb,D,A,T,S,t_l,t_r,ps,ews,pactive,peer_thresh_lo,peer_thresh_hi,
              LAMBDA,dynamic_network,use_neighbor,border_cross_prob,phase_shift,multiply_lo,multiply_hi,core_use,max_peer_it)
    else:
        print(name,hyper_comb,D,A,T,S,t_l,t_r,ps,ews,pactive,peer_thresh_lo,peer_thresh_hi,
              LAMBDA,dynamic_network,use_neighbor,border_cross_prob,phase_shift,multiply_lo,multiply_hi,core_use,max_peer_it)

