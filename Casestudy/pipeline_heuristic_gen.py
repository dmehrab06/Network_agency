import pandas as pd
import random
import time
import sys

random.seed(time.time())

#90000 intensity
#80000 time shift

hyper_comb = int(sys.argv[1])
comment = str(sys.argv[2])
incomplete = []
intensity = float(sys.argv[3])
time_shift = int(sys.argv[4])
random = int(sys.argv[5])
#first: order the raions
def get_memory(hh):
    if hh<=100000:
        return 16000
    if hh<=300000:
        return 64000
    return 256000

def get_core(hh):
    if hh<=100000:
        return 1
    if hh<=300000:
        return 4
    return 8

necessary_mem = pd.read_csv('/home/zm8bh/radiation_model/migration_shock/scripts/analysis_notebooks/memory_req_raion.csv')
raion_df = pd.read_csv('hh_cnts.csv')

raion_df = raion_df.sort_values(by='hh',ascending=False)
raion_df['memory'] = raion_df['hh'].apply(lambda x: get_memory(x))
raion_df['core'] = raion_df['hh'].apply(lambda x: get_core(x))
raion_df = raion_df.merge(necessary_mem[['Raion','mem_need']],left_on='raion',right_on='Raion',how='inner')
raion_df['memory'] = raion_df[["memory", "mem_need"]].max(axis=1)
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
        print('"'+name+'"',hyper_comb,core_use,intensity,time_shift,random)
    else:
        print(name,hyper_comb,core_use,intensity,time_shift,random)


for i in range(len(set2_raion)):
    name = set2_raion[i]
    mem_req = set2_mem[i]
    core_use = set2_cc[i]
    print('sbatch --mem='+str(mem_req)+' --cpus-per-task='+str(core_use)+' abm.sbatch',end=' ')
    if name.startswith('Chornobyl'):
        print('"'+name+'"',hyper_comb,core_use,intensity,time_shift,random)
    else:
        print(name,hyper_comb,core_use,intensity,time_shift,random)

