import os
from subprocess import check_output as cmd
from subprocess import CalledProcessError
from subprocess import STDOUT

import xml.etree.ElementTree as ET

class SlurmError(Exception):
    def __init__(self, msg):
        self.msg = msg
    def __str__(self):
        return repr("%s" % (self.msg))


class Slurm(object):
    qsub = "qsub"
    squeue = "squeue"
    sacct = "sacct"
    qdel = "qdel"
    qhost = "qhost"
    
    idleCmd = "echo '/bin/sleep 3144960000'"
    
    standardParallelEnvironment = "shm"
    standardQueue = "on.q"
    standardHostSuffix = ".informatik.uni-erlangen.de"

    def __init__(self, qsub="qsub", squeue="squeue", sacct="sacct",
            qdel="qdel", qhost="qhost", idleCmd="echo '/bin/sleep 3144960000'",
            standardParallelEnvironment = "shm",
            standardQueue = "on.q", standardHostSuffix=".informatik.uni-erlangen.de"):
        
        self.qsub = qsub
        self.squeue = squeue
        self.qdel = qdel
        self.qhost = qhost
        self.sacct = sacct

        self.idleCmd = idleCmd
        
        self.standardParallelEnvironment = standardParallelEnvironment
        self.standardQueue = standardQueue
        self.standardHostSuffix = standardHostSuffix
                
        self.jobs = []
        self.runningJobs = []
        self.pendingJobs = []
        self.deletedJobs = []
        self.finishedJobs = []       
        self._receiveJobs()
    
    def reloadJobs(self):
        self.jobs = []
        self.runningJobs = []
        self.pendingJobs = []
        self.deletedJobs = []
        self.finishedJobs = []
        self._receiveJobs()
    
    def _receiveJobs(self):
        try:
            running = cmd("%s -o '%%i %%P %%j %%u %%t %%M %%D %%R' -h -u `whoami` --states=R,CG" % self.squeue, shell=True, stderr=STDOUT)
            #finished = cmd("%s -o '%%i %%P %%j %%u %%t %%M %%D %%R' -h -u `whoami` --states=CD,S,CA,F,TO,NF" % self.squeue, shell=True, stderr=STDOUT)
            finished = cmd("%s -n -o 'jobid,jobname,account,partition,alloccpus,elapsed,state,exitcode'" % self.sacct, shell=True, stderr=STDOUT) 
            pending = cmd("%s -o '%%i %%P %%j %%u %%t %%M %%D %%R' -h -u `whoami` --states=PD" % self.squeue, shell=True, stderr=STDOUT)
        except OSError:
            raise SlurmError("squeue command \"%s\" not found" % self.qstat)
        except CalledProcessError, error:
            raise SlurmError("squeue failed: could not receive SGE jobs: %s" % error.output.strip())

        #xmlRunningPending = ET.fromstring(runningPending)
        #xmlFinished = ET.fromstring(finished)

        # running jobs
        #running = xmlRunningPending.find("queue_info").findall("job_list")
        # pending jobs
        #pending = xmlRunningPending.find("job_info").findall("job_list")
        # finished jobs
        #finished = xmlFinished.find("job_info").findall("job_list")


        for jobString in running.split('\n'):
	    if not jobString=='': 
                job = SlurmJob(jobString)
                self.jobs.append(job)
                if job.state == "running":
                    self.runningJobs.append(job)
            #elif job.state == "deleted":
            #    self.deletedJobs.append(job)

        for jobString in pending.split('\n'):
	    if not jobString=='':
                job = SlurmJob(jobString)
                self.jobs.append(job)
                self.pendingJobs.append(job)

        for jobString in finished.split('\n'):
	    if not jobString=='':
                job = SlurmJob(jobString)
                self.jobs.append(job)
                self.finishedJobs.append(job)


    def _receiveJobsString(self):
        try:
            runningPending = cmd(self.qstat, shell=True, stderr=STDOUT)
            finished = cmd("%s -s z" % self.qstat, shell=True, stderr=STDOUT)
        except OSError:
            raise SGEError("qstat command \"%s\" not found" % self.qstat)
        except CalledProcessError, error:
            raise SGEError("qstat failed: could not receive SGE jobs: %s", error.output.strip())
       
        runningPendingLines = runningPending.strip().split("\n")
        del runningPendingLines[0:2]
        
        finishedLines = finished.strip().split("\n")
        del finishedLines[0:2]
        
        for line in runningPendingLines:
            data = line.strip().split(" ")
            data = filter(None, data)
            job = Job(data)
            self.jobs.append(job)
            if job.state == "running":
                self.runningJobs.append(job)
            elif job.state == "pending":
                self.pendingJobs.append(job)
            elif job.state == "deleted":
                self.deletedJobs.append(job)

        for line in finishedLines:
            data = line.strip().split(" ")
            data = filter(None, data)
            job = Job(data)
            job.state = "finished"
            self.jobs.append(job)
            self.finishedJobs.append(job)

    def getQueueHosts(self, requiredQueue=None):
        if requiredQueue is None:
             requiredQueue = self.standardQueue
        
        try:
            output = cmd("%s -q -xml" % (self.qhost), shell=True, stderr=STDOUT)
        except OSError:
            raise SlurmError("qhost command \"%s\" not found" % self.qhost)
        except CalledProcessError, error:
            raise SlurmError("qhost failed: %s" % error.output.strip())
        
        agreedHosts = []

        xml = ET.fromstring(output)
        hosts = xml.findall("host")
        for host in hosts:
            queues = host.findall("queue")
            for queue in queues:
                if queue.get("name") == requiredQueue:
                    agreedHosts.append(host.get("name"))
                    break

        return(agreedHosts)



    def getAllJobs(self):
        return self.jobs
    
    def getPendingJobs(self):
        return self.pendingJobs
    
    def getRunningJobs(self):
        return self.runningJobs

    def getFinishedJobs(self):
        return self.finishedJobs

    def submitJob(self, name, hosts=[], queue=None, hostSuffix=None, memory=None, cpu=None, parallelEnvironment=None, stdout="/dev/null", stderr="/dev/null"):
        if queue is None:
            queue = self.standardQueue
        if hostSuffix is None:
            hostStuffix = self.standardHostSuffix
        if parallelEnvironment is None:
            parallelEnvironment = self.standardParallelEnvironment

        if len(hosts) == 0:
            assembledQueue = queue
        else:
            #assembledQueue = ",".join([queue+"@"+host+hostSuffix for host in hosts])
            # host have to be complete
            assembledQueue = ",".join([queue+"@"+host for host in hosts])
        if memory is None:
            memory = ""
        else:
            #memory = "-l mem_total=%sM" % memory
            memory = "-l mem_free=%sM" % memory
        
        if cpu is None:
            cpu = ""
        else:
            cpu = "-pe %s %s" % (parallelEnvironment,cpu)

        qsubArgs = "-N \"%s\" -q %s %s %s -o %s -e %s" % (name, assembledQueue, memory, cpu, stdout, stderr)
        
        try:
            output = cmd("%s | %s %s" % (self.idleCmd, self.qsub, qsubArgs), shell=True, stderr=STDOUT)
        except OSError:
            raise SGEError("qsub command \"%s\" not found" % self.qsub)
        except CalledProcessError, error:
            raise SGEError("qsub failed: %s" % error.output.strip())

        output = output.strip()
        if output.endswith("has been submitted"):
            jobId = output.split(" ")[2]
            return (True, jobId)
        else:
            return (False, output)


    def deleteJob(self, name="", id=0):
        if id == 0 and name == "":
            return (False, "No ID or name given")
        
        if id != 0:
            delete = str(id)
        else:
            delete = name
            
        try:
            output = cmd("%s \"%s\"" % (self.qdel, delete), shell=True, stderr=STDOUT)
        except OSError:
            raise SGEError("qdel command \"%s\" not found" % self.qdel)
        except CalledProcessError, error:
            raise SGEError("qdel failed: %s" % error.output.strip())

        output = output.strip()
        if output.endswith("for deletion"):
            jobId = output.split(" ")[5]
            return (True, jobId)
        else:
            return (False, output)

class JobString(object):
    def __init__(self, jobData):        
        self.id = jobData[0]
        self.priority = jobData[1]
        self.name = jobData[2]
        self.username = jobData[3]
        #self.state = "running" if jobData[4] == "r" else "pending"
        if jobData[4] == "r":
            self.state = "running"
        elif jobData[4] == "qw":
            self.state = "pending"
        elif jobData[4] == "dr":
            self.state = "deleted"
        self.submitTime = "%s %s" % (jobData[5], jobData[6])
        
        if len(jobData) == 9:
            self.qname, self.hostname = jobData[7].split("@") 
        else:    
            self.qname = ""
            self.hostname = ""
        
        self.slots = jobData[-1]

class SlurmJob(object):

    def __init__(self, jobString):
        print "jobString: "+jobString
        fields = jobString.split()

        #state = fields[4]
        
        self.id = fields[0]
        self.name = fields[2]
        #self.username = fields[3]
	#finishedStates = ["CD","S","CA","F","TO","NF"]
	#runningStates = ["R","CG"]
	#pendingStates = ["PD"]
        #if state in runningStates:
	#    self.state = "running"            
        #elif state in pendingStates:
        #    self.state = "pending"
        #elif state in finishedStates:
        #    self.state = "finished"
        
        #try:
        #    self.submitTime = jobXML.find("JB_submission_time").text.replace("T", " ")
        #except:
        #    self.submitTime = ""
        #try:
        #    self.startTime = jobXML.find("JAT_start_time").text.replace("T", " ")
        #except:
        #    self.startTime = ""

        #self.runTime = fields[5]
        #self.partition = fields[2]
        #if not fields[7].startswith("("):
        #  self.hostname = fields[7]
	#else:
	#  self.hostname = ""
        #if jobXML.find("queue_name").text is None:
        #    self.qname = ""
        #    self.hostname = ""
        #else:    
        #    self.qname, self.hostname = jobXML.find("queue_name").text.split("@")
        
        #self.slots = jobXML.find("slots").text
