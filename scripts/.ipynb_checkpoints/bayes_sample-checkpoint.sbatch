#!/bin/bash
#SBATCH -N 1
#SBATCH -n 1
#SBATCH --job-name=calibration
#SBATCH --cpus-per-task=1
#SBATCH -t 00:30:00
#SBATCH -p bii
#SBATCH -A nssac_covid19

source activate biocomplexity

python bayes_optimization.py

sleep 30

bash bayes_run.sh
