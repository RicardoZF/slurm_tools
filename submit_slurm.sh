#!/bin/bash

pwd
USER=$(whoami)

if [ $# -lt 9 ]; then
  echo "usage: submit_slurm.sh CMD NCPUS SLURM_HOME nodes mem_per_cpu queue ngpus name nodelist"
  exit 0;
fi

cmd=${1}
ncpus=${2}
SLURM_HOME=${3}
nodes=$4
ntasks=$5
mem_per_cpu=${6}
queue=$7
ngpus=$8
name=$9

if [[ -n $10 ]]; then
 nodelist=$10
fi

rndstr="re_"$(head /dev/urandom | tr -dc A-Za-z0-9 | head -c10)
mkdir -p ${SLURM_HOME}/${rndstr}
chmod 777 -R ${SLURM_HOME}/${rndstr}

SLURM_SH=${SLURM_HOME}/${rndstr}/run_${rndstr}_rs.sh

touch ${SLURM_SH}
chmod 777 ${SLURM_SH}

cat <<EOT >> ${SLURM_SH}
#!/bin/bash
#SBATCH -J ${name}
#SBATCH -p ${queue}
#SBATCH --nodes=${nodes}
#SBATCH --ntasks=${ntasks}
#SBATCH --cpus-per-task=${ncpus}
#SBATCH --mem-per-cpu=${mem_per_cpu}
#SBATCH --gres=gpu:${ngpus}
#SBATCH --get-user-env
#SBATCH -x comput11,GPU16,GPU18
#SBATCH -e ${SLURM_HOME}/job-%j.err
#SBATCH -o ${SLURM_HOME}/job-%j.out

#export PATH=$PATH
echo "\$SLURM_JOB_NODELIST"

date
${cmd}

date

echo "COMPLETE ..."
EOT


echo "[INFO] `date`: Running job: sbatch ${SLURM_SH}"
SLURM_JOBID=$(sbatch --parsable ${SLURM_SH})
echo "job_id=${SLURM_JOBID}"
echo "user=${USER}"
echo "slurm_sh=${SLURM_SH}"
echo "[INFO] `date`: slurm submit cmd (${cmd}) completed ..."
#rm -rfv run_${rndstr}.sh

