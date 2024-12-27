#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Wrapper cmd and Submit cmd to Slurm.

Author: Zhou Fan
Contact: 1370174361@qq.com
Date: Jun 09. 2022.

"""
import argparse
import os
import pickle
import re
import shutil
import sys
import subprocess as sp
import getpass
import time
import datetime
import json


# base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
base_path = os.path.dirname(os.path.abspath(__file__))
ini_config_file = os.path.join(base_path, 'config.json')


ini_sh = os.path.join(base_path, "submit_slurm.sh")
ini_home = os.path.join(base_path, "example")


class SlurmScheduler(object):

    def __init__(self, cmd_list:list, cmd_cnt:int=20, slurm_home:str=ini_home,
                 config:str=ini_config_file, submit_wait=1, cmd_out=None):
        """
        Parameters
        ----------
        cmd_list:list, The list of cmd for submit
        cmd_cnt:int, Number of cmd submissions per script for slurm
        slurm_home:str, A directory path to save slurm log
        config:str, A config file for storing slurm parameters
        submit_wait:int, time(s) step in each submit
        cmd_out:str, cmd output, used to check

        """
        self.sh = ini_sh
        self.cmd_list = cmd_list
        self.slurm_home = slurm_home
        self.cmd_cnt = cmd_cnt
        self.config = config
        # self.config_dict = self.get_config(self.config)
        self.submit_wait = submit_wait
        self.cmd_out = cmd_out

    def submit_before(self):
        """
        the method at submit cmd before
        """
        pass

    def check_config(self):
        pass

    def check_out(self, cmd_run):
        """
        used to check cmd success or not
        """
        if self.cmd_out:
            cmd_list = []
            for cmds in cmd_run:
                cmd_list.extend(cmds.get('cmds'))
            failed_cmds = set()
            for cmd in cmd_list:
                if not os.path.exists(self.cmd_out):
                    failed_cmds.add(cmd)
                else:
                    if not os.path.getsize(self.cmd_out):
                        failed_cmds.add(cmd)
            return failed_cmds

    # @staticmethod
    def get_config(self):
        """

        :param config: json config file or dict
        :return: config dict
        """
        config = self.config
        if not os.path.exists(config):
            print(f"[ERROR]: submit failed, config file is not exist!!!")
            sys.exit()
        elif os.path.exists(config):
            with open(config, 'r') as f:
                config_dict = json.load(f)
        else:
            config_dict = json.loads(config)
        return config_dict

    def prepare_cmd(self, cmd_list=None):
        """
        group cmd by self.cmd_cnt
        """
        max_tasks = self.get_config().get('max_tasks')

        try:
            max_tasks = int(max_tasks)
        except:
            max_tasks = 15

        if not cmd_list:
            cmd_list = self.cmd_list
        else:
            cmd_list = list(cmd_list)
        cmd_l = len(cmd_list)
        cmd_res = []
        if cmd_l < max_tasks * self.cmd_cnt:
            cnt = cmd_l // max_tasks if cmd_l % max_tasks == 0 else cmd_l // max_tasks + 1

            for i in range(max_tasks):
                cmds = cmd_list[i * cnt:(i + 1) * cnt]
                if cmds:
                    cmd_res.append(dict(cmds=cmds))

        else:
            cnt = self.cmd_cnt
            for i in range(0, cmd_l, cnt):
                cmds = cmd_list[i: i+cnt]
                if cmds:
                    cmd_res.append(dict(cmds=cmds))

        return cmd_res
    
    
    def prepare_cmd2(self, cmd_list=None):
        """
        Number of cmd submissions per script is 1.
        """
        
        max_tasks = self.get_config().get('max_tasks')
        try:
            max_tasks = int(max_tasks)
        except:
            max_tasks = 15

        if not cmd_list:
            cmd_list = self.cmd_list
        else:
            cmd_list = list(cmd_list)
        
        cmd_res = [{"cmds":[cmd] } for cmd in cmd_list]
        # for i in range(max_tasks):
        #     cmds = cmd_list[i:i+1]
        #     if cmds:
        #         cmd_res.append(dict(cmds=cmds)) 

        return cmd_res
        
        
    def submit(self, cmd, slurm_sh=''):
        """
        submit cmd to slurm
        """
        print(f"[SLURM]:  {self.get_config()}")
        nodes = self.get_config().get('nodes') or 1
        ntasks = self.get_config().get('ntasks') or 1
        cpus_per_task = self.get_config().get('cpus-per-task') or 4
        mem_per_cpu = self.get_config().get('mem-per-cpu') or '4GB'
        queue = self.get_config().get('queue') or 'all'
        ngpus = self.get_config().get('ngpus') or 0
        name = self.get_config().get('name') or 'zf_slurm'

        command = f"{self.sh} \"{cmd}\" {cpus_per_task} {self.slurm_home} {nodes} {ntasks} " \
                  f"{mem_per_cpu} {queue} {slurm_sh} {ngpus} {name}"
        # print(f'[ZF]==== cmd === {command}')
        job = sp.Popen(command, shell=True, stdin=sp.PIPE, stdout=sp.PIPE, stderr=sp.PIPE, encoding='utf-8')
        out, err = job.communicate()
        #print(f'[ZF]==== out === {out}')
        if self.submit_wait:
            time.sleep(self.submit_wait)

        if err:
            print(f"[ERROR]: cmd can't run cmd: \"{command}\" in Slurm job, please check !!!")
            print(f"[ERROR INFO]: {err.strip()}")
            #sys.exit(0)
        return out

    @staticmethod
    def out_to_dict(out):
        """

        Parameters
        ----------
        out: cmd out

        Returns
        -------
        slurm_res: dict, {job_id:str, user:str, slurm_sh:str, restart_times:int}
        """

        slurm_res = {}
        for i in out.splitlines():

            if "=" in i:
                k, v = i.split("=")
                slurm_res[k] = v

        return slurm_res

    @staticmethod
    def get_slurm_tasks(job_ids: list):
        """

        :param job_ids: list， the list of slurm job_id
        :return: int, the number of running tasks
        """

        job_ids = ",".join(job_ids)
        # cmd = f"squeue -u  {user} -j {job_ids} -h | wc -l"
        cmd = f"squeue -j {job_ids} -h | wc -l"
        try:
            ntasks = int(sp.check_output(cmd, shell=True, encoding='utf-8'))
        except:
            ntasks = 0

        return ntasks

    @staticmethod
    def get_job_ids(job_ids: list, cmd_type: str = "run"):
        """

        :param job_ids: list, job_ids
        :param cmd_type: str, option:run/all
        :param user: str, user name
        :return: list, job_ids
        """
        job_ids = ",".join(job_ids)
        if cmd_type == "run":
            # cmd = f"squeue -u {user} | grep vsdock | awk '{{print $1}}'"
            cmd = f"squeue  -j {job_ids} -h | awk '{{print $1}}'"
        else:
            cmd = f"sacct -n -j {job_ids} | grep -v '^[0-9]*\.' | awk '{{print $1}}'"
        sp_res = sp.check_output(cmd, shell=True, encoding='utf-8').splitlines()

        return sp_res

    def wait_jobs(self, job_ids, cmd_run, max_tasks=0, sleep_time=20):
        failed_cmds = []
        while True:
            tasks = self.get_slurm_tasks(job_ids)
            run_jobs = self.get_job_ids(job_ids, cmd_type="run")
            print(f"[{datetime.datetime.now().strftime('%H:%M:%S')} SLURM]: run_jobs= {','.join(run_jobs)}")
            sys.stdout.flush()

            if max_tasks:
                if tasks < max_tasks:
                    break
                else:
                    time.sleep(sleep_time)
            else:
                if tasks == 0:
                    failed_cmds = self.check_out(cmd_run)
                    time.sleep(3)
                    break
                else:
                    time.sleep(sleep_time)
        return failed_cmds

    @staticmethod
    def get_failed_cmds(cmd_run):
        failed_cmds = []
        for cmd_dict in cmd_run:
            job_id = cmd_dict['job_id']
            cmds = cmd_dict['cmds']

            sacct_cmd = f"sacct -b -n -j {job_id} | grep -v '^[0-9]*\.' | awk '{{print $2}}'"
            try:
                state = sp.check_output(sacct_cmd, shell=True, encoding='utf-8').strip()
            except:
                print(f"[ERROR INFO]: job_id {job_id} is not find !!!")
                failed_cmds.extend(cmds)
                continue

            if state in ["FAILED", "TIMEOUT"]:
                print(f"[INFO]: job_id {job_id} is {state}, try restart now...")
                failed_cmds.extend(cmds)

        return failed_cmds

    def run(self, cmd_list):
        cmd_res = self.prepare_cmd2(cmd_list)

        # failed_cmds = set()
        # scores = dict()

        job_ids = []
        cmd_run = []
        for task, cmd_dict in enumerate(cmd_res):

            # submit cmd
            out = self.submit(cmd=" ; ".join(cmd_dict["cmds"]))
            slurm_res = self.out_to_dict(out)
            job_id = slurm_res.get('job_id')
            if not slurm_res:
                continue

            cmd_dict["job_id"] = job_id
            print(f"[{datetime.datetime.now().strftime('%H:%M:%S')} SLURM]: job_id is {job_id} ")
            print(f"[{datetime.datetime.now().strftime('%H:%M:%S')} SLURM]: cmd_dict is {cmd_dict} ")

            job_ids.append(job_id)
            cmd_run.append(cmd_dict)
            max_tasks=self.get_config().get('max_tasks')
            if task+1 < max_tasks: 
                continue
            self.wait_jobs(job_ids=job_ids, cmd_run=cmd_run, max_tasks=max_tasks)

        failed_cmds = self.wait_jobs(job_ids=job_ids, cmd_run=cmd_run, max_tasks=0)

        return failed_cmds

    def main(self):
        # cmd_list = self.check_cmd()

        if not self.cmd_list:
            print(f"[INFO]: SLURM compete, cmd list is None!!!")
            return []

        self.submit_before()

        failed_cmds = self.run(self.cmd_list)
        if failed_cmds:
            print(f"[SLURM]: failed_cmds is {failed_cmds}, submit again now ...")
            failed_cmds = self.run(cmd_list=failed_cmds)
        # print(f"[INFO]: SLURM compete, len(scores) is {len(scores)}")
        print(f"[INFO]: SLURM compete, failed_cmds is {failed_cmds}")
        return failed_cmds


def arguments():
    d = """
    Wrapper cmd and Submit cmd to Slurm.

    Input cmd list, cmd number to per script, and config json file.

    Examples:

    python slurm.py -i cmd1,cmd2,cmd3 -n 20 -o slurm_log/ -f config.json
    """

    parser = argparse.ArgumentParser(description=d)

    parser.add_argument("-i", type=str, help="Input. The list of cmd for submit, split by ','. \n"
                                             "eg. cmd1,cmd2,cmd3")
    parser.add_argument("-fi", type=str, help="Input. The file of cmd for submit, split by '\\n'.")
    parser.add_argument("-n", type=int, default=20,
                        help="Input. Default is 20. "
                             "Number of cmd submissions per script for slurm.")
    parser.add_argument("-o", type=str, default="slurm_log",
                        help="Output. Optional. Default is slurm_log \n"
                             "A directory path to save slurm log")
    parser.add_argument("-c", type=str, default=ini_config_file,
                        help="Input. Optional. A configuration file that defines the slurm parameters.")

    args = parser.parse_args()


    return args


if __name__ == '__main__':

    args = arguments()
    if args.fi:
        with open(args.fi, 'r') as f:
            cmd = f.read().splitlines()
    else:
        cmd = args.i.split(',')

    if cmd:
        slurm_scheduler = SlurmScheduler(cmd_list=cmd, cmd_cnt=args.n, slurm_home=args.o, config=args.c)
        slurm_scheduler.main()
