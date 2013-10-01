#!/usr/bin/python2

import sys

if sys.version_info.major != 2:
    print "i3sched requires python2"
    print "tested for python 2.7.x"
    sys.exit(1)

import os
import signal

from time import sleep
from time import time

import subprocess
from subprocess import check_output as cmd

from operator import itemgetter
from itertools import groupby

try:
    from logging import Log
except ImportError:
    print "logging.py not found"
    sys.exit(1)

try:
    from config import Config, ConfigError
except ImportError:
    print "config.py not found"
    sys.exit(1)

try:
    from client import OneRPC, OneRPCError, OneError, OneVM, OneHost
except ImportError:
    print "client.py not found"
    sys.exit(1)

try:
    from sge import SGE, SGEError, SGEJob
except ImportError:
    print "sge.py not found"
    sys.exit(1)

try:
    from slurm import Slurm, SlurmError, SlurmJob
except ImportError:
    print "slurm.py not found"
    sys.exit(1)

class Scheduler(object):

    def __init__(self):
        
        # Indicates if scheduler is done (=> shutdown)
        self.done = False
        
        # Log Object
        self.log = Log()

        # Config Object
        self.cfg = Config()
        
        # reading config
        try:
            status, info, msg = self.cfg.readConfig()
        except ConfigError, err:
            if err.error == "noConfig":
                self.log.writeSchedLog("error", err.msg)
                self.log.writeSchedLog("error", "You can create one with Config.createConfig(). See the configspec")
                sys.exit(1)
            if err.error == "readError":
                self.log.writeSchedLog("error", err.msg)
                sys.exit(1)
        if status == False:
            self.log.writeSchedLog("error", "Could not validate config")
            self.log.writeSchedLog("error", "You can create a valid config with Config.createConfig(). See the configspec")
            sys.exit(1)

        if status == True and info == "missing":
            self.log.writeSchedLog("info", "Config options missing (defaults will be used): %s" % msg)            
            
        # time in seconds between two schedule cycles
        self.sleepTime = self.cfg.sleepTime

        self.shutdownTimeout = self.cfg.shutdownTimeout
         
        # RPC object
        try:
            self.rpc = OneRPC(hostname=self.cfg.oneRPC["hostname"], port=self.cfg.oneRPC["port"])
        except OneError, error:
            self.log.writeSchedLog("error", "%s" % error)
            self.log.writeOnedLog("error", "Scheduler stopped: %s" % error)
            sys.exit(1)
        except OneRPCError, error:
            self.log.writeSchedLog("error", "%s" % error)
            self.log.writeOnedLog("error", "Scheduler stopped: %s" % error)
            sys.exit(1)
    
        
        # SGE object
        try:
            #self.sge = SGE(qsub=self.cfg.sge["qsub"], qstat=self.cfg.sge["qstat"], qdel=self.cfg.sge["qdel"], qhost=self.cfg.sge["qhost"],
            #        idleCmd=self.cfg.sge["idleCmd"], standardParallelEnvironment=self.cfg.sge["parallelEnvironment"], 
            #        standardQueue=self.cfg.sge["queue"], standardHostSuffix=self.cfg.sge["hostSuffix"])
	    self.scheduler = Slurm(qsub=self.cfg.sge["qsub"], squeue="squeue", sacct="sacct", qdel=self.cfg.sge["qdel"], qhost=self.cfg.sge["qhost"],
	                     idleCmd=self.cfg.sge["idleCmd"], standardParallelEnvironment=self.cfg.sge["parallelEnvironment"],
			     standardQueue=self.cfg.sge["queue"], standardHostSuffix=self.cfg.sge["hostSuffix"])
        except SGEError, error:
            self.log.writeSchedLog("error", "%s" % error)
            self.log.writeOnedLog("error", "Scheduler stopped: %s" % error)
            sys.exit(1)


        # VM lists
        self.vms = []
        self.runningVMs = []
        self.finishedVMs = []
        self.failedVMs = []
        self.pendingVMs = []
        self.unknownVMs = []
        self.shutdownVMs = []

        self.shutdownVMTime = {}
        
        # start ID for RPC vmpoolinfo
        self.startVMid = self.cfg.oneRPC["startVM"]
        
        # Host lists
        self.hosts = []
        self.hostIDs = {}
        # only monitored hosts will be used for deployment
        self.monitoredHosts = [] 

        # Job lists
        #self.jobs = []
        self.runningJobs = []
        self.pendingJobs = []
        self.finishedJobs = []
        
    def signalHandler(self, signal, frame):
        """signal handler for controlled shutdown"""
        self.done = True
    
    def loadHosts(self):
        """loadHosts loads the OneHosts through OneRPC and sets a hostIDs dict up.
        Catches no exceptions!
        """
        
        self.hosts = []
        self.hostIDs = {}
        self.monitoredHosts = []
            
        self.hosts = self.rpc.hostpoolInfo()

        for host in self.hosts:
            fullHostname = host.name + self.cfg.sge["hostSuffix"]
            self.hostIDs[fullHostname] = host.id
            if (host.state == "monitored" or host.state == "monitoring_monitored") and fullHostname in self.scheduler.getQueueHosts():
                self.monitoredHosts.append(fullHostname)
    
    def hostNameToOneHostID(self, hostName):
        """hostNameToOneHostID() searches for hostName in self.hostIDs
        raises KeyError if hostName not in List
        """

        return self.hostIDs[hostName]   
        
    
    def loadVMs(self):
        """loadVMs empties all VM lists and refills them through the OneRPC interface"""
        self.finishedVMs = []
        self.pendingVMs = []
        self.failedVMs = []        
        self.runningVMs = []
        self.unknownVMs = []
        self.shutdownVMs = []
        self.vms = self.rpc.vmpoolInfo(startRange=self.startVMid)
        for vm in self.vms:
            if vm.state == "done":
                self.finishedVMs.append(vm)
            elif vm.state == "pending":
                self.pendingVMs.append(vm)
            elif vm.state == "failed":
                self.failedVMs.append(vm)
            elif vm.state == "active" or vm.state == "poweroff":
                self.runningVMs.append(vm)           
 
            if vm.lcm_state == "unknown":
                self.unknownVMs.append(vm)
            elif vm.lcm_state == "shutdown":
                self.shutdownVMs.append(vm)

   
    def loadJobs(self):
        """loadJobs empties all SGE Job lists and refills them with the SGE interface"""
        self.runningJobs = []
        self.pendingJobs = []
        self.finishedJobs = []
        self.sge.reloadJobs()
        self.runningJobs = self.sge.getRunningJobs()
        self.pendingJobs = self.sge.getPendingJobs()
        self.finishedJobs = self.sge.getFinishedJobs()

    def shutdownRunningZombies(self):
        """Check for running VMs without SGE Job. 
        If such a vm is found, let OpenNebula initiate VM shutdown with rpc.vmShutdown.
        """
        for vm in self.runningVMs:
            found = False
            for job in self.runningJobs:
                if 'one-'+str(vm.id) == job.name:
                    found = True
                    break
            if not found:
                try:
                    self.rpc.vmShutdown(vm.id)
                    self.log.writeSchedLog("debug", "Zombie VM '%s' shutdown initiated" % (vm.name))
                except OneError, error:
                    self.log.writeSchedLog("error", "Zombie VM '%s' shutdown not possible: %s" % (vm.name, error))
                    if (error.error == 2048) or (error.error == "2048"):
                        self.log.writeSchedLog("error", "Zombie VM '%s' lcm sate: %s" % (vm.name, vm.lcm_state))
                except OneRPCError, error:
                    self.log.writeSchedLog("error", "Zombie VM '%s' shutdown not possible: %s" % (vm.name, error))

    def checkShutdownTimeout(self):
        """Check for running VMs with lcm_state shutdown.
        If such a vm is found and it's longer in this state than self.shutdownTimeout seconds,
        ssh destroy it and delete it
        """
        for vm in self.shutdownVMs:
            now = int(time())
            if not vm.name in self.shutdownVMTime.keys():
                 self.shutdownVMTime[vm.name] = now
             
            # TODO add shutdown timeout
            if now - self.shutdownVMTime[vm.name] >= self.shutdownTimeout:
                if vm.hostname is None:
                    # VM was created with opennebula < 3.4; Don't know the hostname
                    self.log.writeSchedLog("info", "VM '%s' was created with opennebula < 3.4, can't destroy VM (no hostname)" % vm.name)
                    continue
                
                if not vm.hostname is None:
                    # Try to destory VM
                    status, msg = self.sshDestroyVM(vm.name, vm.hostname, vm.history)
                    if status == False:
                        self.log.writeSchedLog("error", "VM '%s' was not destroyed (shutdown timeout): %s" % (vm.name, msg))
                    else:
                        self.log.writeSchedLog("debug", "VM '%s' was destroyed: shutdown timeout" % vm.name)

                # delete from OpenNebula 
                # FIXME Only if destruction was successful?
                if status == True:
                    try:
                        self.rpc.vmDelete(vm.id)
                        self.log.writeSchedLog("debug", "VM '%s' was deleted" % vm.name)
                    except OneError, error:
                        self.log.writeSchedLog("error", "VM '%s' was not deleted: %s" % (vm.name, error))
                    except OneRPCError, error:
                        self.log.writeSchedLog("error", "VM '%s' was not deleted: %s" % (vm.name, error))
                    try:
                        self.runningVMs.remove(vm)
                    except:
                        pass
    
    def sshDestroyVM(self, name, hostname, history=None):
        """sshDestroyVM destroys a xen vm through ssh"""
        try:
            output = cmd("ssh %s sudo xm destroy %s" % (hostname, name) , shell=True, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError, error:
            output = error.output
        
        if output.startswith("Error:"):
            return (False, output.strip())
        else:
            return (True, "")

    def cancelUnknownVMs(self):
        """Check for VMs with lcm_state unknown.
        If such a vm is found, delete the VM through SSH.
        """
        for vm in self.unknownVMs:
            if vm.hostname is None:
                # VM was created with opennebula < 3.4; Don't know the hostname
                self.log.writeSchedLog("info", "VM '%s' was created with opennebula < 3.4, can't destroy VM (no hostname)" % vm.name)
                continue

            
            # Try to destory VM
            if not vm.hostname is None:
                status, msg = self.sshDestroyVM(vm.name, vm.hostname, vm.history)
                if status == False:
                    self.log.writeSchedLog("error", "VM '%s' was not destroyed (lcm state unknown): %s" % (vm.name, msg))
                else:
                    self.log.writeSchedLog("debug", "VM '%s' was destroyed: lcm state unknown" % vm.name)
            
            # delete from OpenNebula 
            # FIXME Only if destruction was successful?
            if status == True:
                try:
                    self.rpc.vmDelete(vm.id)
                    self.log.writeSchedLog("debug", "VM '%s' was deleted" % vm.name)
                except OneError, error:
                    self.log.writeSchedLog("error", "VM '%s' was not deleted: %s" % (vm.name, error))
                except OneRPCError, error:
                    self.log.writeSchedLog("error", "VM '%s' was not deleted: %s" % (vm.name, error))

                try:
                    self.runningVMs.remove(vm)
                except:
                    pass

    def deleteFinishedVMs(self):
        """Checks for SGE Jobs which have no longer a VM (running or pending).
        If such a job is found, it will be deleted.
        """
        for job in self.runningJobs:
            found = False
            for vm in self.runningVMs:
                if job.name == 'one-'+str(vm.id):
                    found = True
                    break
            
            if not found:
                for vm in self.pendingVMs:
                    if job.name == 'one-'+str(vm.id):
                        found = True
                        break

            if not found:
                try:
                    self.sge.deleteJob(job.name)
                    self.log.writeSchedLog("debug", "SGE job '%s' deleted, there was no VM for this job" % job.name)
                except SGEError, error:
                    self.log.writeSchedLog("error", "SGE job %s not deleted: %s, there is no VM for this job" % (job.name, error))


    def deployNewRunningJobs(self):
        """Check for pending VMs with newly running SGE Jobs.
        If such a VM is found, deploy it.
        """
        for vm in self.pendingVMs[:]:
            for job in self.runningJobs:
                if 'one-'+str(vm.id) == job.name:
                    try:
                        hostid = self.hostNameToOneHostID(job.hostname)
                    except KeyError:
                        self.log.writeSchedLog("error", "VM '%s' NOT deployed on Host %s: could not resolve host id" % (vm.name, job.hostname))
                        break

                    if not job.hostname in self.monitoredHosts:
                        try:
                            self.sge.deleteJob(job.name)
                        except SGEError:
                            self.log.writeSchedLog("error", "SGE job % deletion failed: %s" % (job.name, error))
                        if "CPU" in vm.template:
                           requestedCPU = vm.template["CPU"]
                        else:
                           requestedCPU = None
                        requestedMemory = vm.template["MEMORY"]
                        self.log.writeSchedLog("info", "Host %s not monitored => resubmit SGE job '%s' for VM '%s'" % (job.hostname, job.name, vm.name))
                        try:
                            self.sge.submitJob(name='one-'+str(vm.id), hosts=self.monitoredHosts, cpu=requestedCPU, memory=requestedMemory)
                        except SGEError, error:
                            self.log.writeSchedLog("error", "SGE Job for VM '%s' submittion failed: %s" % (vm.name, error))
                        break

                    try:
                        self.rpc.vmDeploy(vm.id, hostid)
                        self.log.writeSchedLog("debug", "VM '%s' deployed on Host %s" % (vm.name, job.hostname))
                        self.pendingVMs.remove(vm)
                        self.runningVMs.append(vm)
                    except OneError, error:
                        self.log.writeSchedLog("error", "VM '%s' NOT deployed on Host %s: %s" % (vm.name, job.hostname, error))
                    except OneRPCError, error:
                        self.log.writeSchedLog("error", "VM '%s' NOT deployed on Host %s: %s" % (vm.name, job.hostname, error))
                    break

    def submitNewVMs(self):
        """Check for pending VMs without SGE Job (pending or running).
        If such a vm is found, submit a SGE Job with the vm's requirements.
        """
        for vm in self.pendingVMs[:]:
            found = False
            for job in self.pendingJobs:
                if 'one-'+str(vm.id) == job.name:
                    found = True
                    break
            
            if not found: 
                for job in self.runningJobs:
                    if 'one-'+str(vm.id) == job.name:
                        found = True
                        break

            if not found:
                try:
                    if "CPU" in vm.template:
                      requestedCPU = vm.template["CPU"]
                    else:
                      requestedCPU = None
                    requestedMemory = vm.template["MEMORY"]
                    # get a list with all the hostnames
                    status, msg = self.sge.submitJob(name='one-'+str(vm.id), hosts=self.monitoredHosts, cpu=requestedCPU, memory=requestedMemory)
                    if status:
                        self.log.writeSchedLog("debug", "SGE job (id: %s, name: %s) for VM '%s' submitted" % (msg, vm.name, vm.name))
                    else:
                        self.log.writeSchedLog("error", "SGE job submission for VM %s failed: %s" % (vm.name, msg))
                except SGEError, error:
                    self.log.writeSchedLog("error", "SGE job submission for VM %s failed: %s" % (vm.name, error))
                    


    
    def newStartVM(self):
        """newStartVM determines new startVMid"""
        # id list with all finished vm
        idList = [vm.id for vm in self.finishedVMs]
        # see http://stackoverflow.com/questions/3149440/python-splitting-list-based-on-missing-numbers-in-a-sequence
        try:
            self.startVMid = max([map(itemgetter(1), g) for k, g in groupby(enumerate(idList), lambda (i,x):i-x)][0])
        except:
            self.startVMid = 1
        if self.startVMid < 1:
            self.startVMid = 1
    
    def schedule(self):
        """Manages running, pending & finished Opennebula VMs and SGE Jobs.
        See each method for more information.
        """
        
        # if loadHosts, loadVMs, or loadJobs fails, 
        # the current scheduling run will be stopped,
        # but the scheduler will continue to run

        # load hosts
        try:
            self.loadHosts()
        except OneRPCError, error:
            self.log.writeSchedLog("error", "Could not load hosts: %s" % error)
            return
        except OneRPCError, error:
            self.log.writeSchedLog("error", "Could not load hosts: %s" % error)
            return            
        
        # load vms
        try:
            self.loadVMs()
        except OneRPCError, error:
            self.log.writeSchedLog("error", "Could not load vms: %s" % error)
            return
        except OneRPCError, error:
            self.log.writeSchedLog("error", "Could not load vms: %s" % error)
            return      
        
        # load jobs
        try:
            self.loadJobs()
        except SGEError, error:
            self.log.writeSchedLog("error", "Could not load SGE jobs: %s" % error)
            return

        
        # Check for running VMs without SGE Job => Shutdown VMs
        self.shutdownRunningZombies()
        
        # Check for VMs with lcm_state unknown => ssh xm destroy (Tell the hypervisor to kill these VMs)
        self.cancelUnknownVMs()
 
        # Check for VMs wuth lcm_state shutdown, if a vm is longer in this state than shutdownTimeout seconds => ssh xm destroy (Tell the hypervisor to kill this VM)
        self.checkShutdownTimeout()
        
        # Check if a pending VM SGE Job is now running => Deploy VM
        self.deployNewRunningJobs()
        
        # Check for finished VMs with running SGE Job => Delete SGE Job
        self.deleteFinishedVMs()
        
        # Check for new pending VMs without SGE Job => Submit SGE Job
        self.submitNewVMs()

    
    def run(self):
        """Starts the scheduler and enters the main working loop
        Loop interval time see sleepTime
        """
        # Capture SIGINT (^c) & SIGTERM and run self.signalHandler
        try:
            signal.signal(signal.SIGINT, self.signalHandler)
            signal.signal(signal.SIGTERM, self.signalHandler)
        except ValueError:
            pass
        
        self.log.writeSchedLog("info", "-------------------")
        self.log.writeSchedLog("info", "i3sched was started")
        self.log.writeSchedLog("info", "sleep time: %s sec" % (self.sleepTime))
        self.log.writeSchedLog("info", "shutdown timeout: %s sec" % (self.shutdownTimeout))
        self.log.writeSchedLog("info", "-------------------")

        while not self.done:

            # Schedule
            self.schedule()

            # Determine VM ID for rpc vmpoolinfo start range
            self.newStartVM()

            # Sleep
            sleep(self.sleepTime)

        # TODO Clean up & save
        
        self.log.writeSchedLog("info", "-------------------")
        self.log.writeSchedLog("info", "i3sched was stopped")
        self.log.writeSchedLog("info", "-------------------")
    
    @classmethod
    def createConfig(cls):
        cfg = Config()
        cfg.createConfig()
        return cfg.configFile

