#!/bin/bash
#SBATCH -N 1
#SBATCH -n 1
#SBATCH --cpus-per-task=1
#SBATCH -t 05:00:00
#SBATCH -p bii
#SBATCH -A nssac_covid19
#SBATCH --job-name=bayes_lhs

source activate abm_env

python abm.py "$1" $2 $3 $4 $5 $6


