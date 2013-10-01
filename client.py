# Python RPC interface for OpenNebula 3.4

import os

import xmlrpclib

import xml.etree.ElementTree as ET

from operator import itemgetter

class OneError(Exception):
    def __init__(self, msg, error):
        self.msg = msg
        self.error = error
    def __str__(self):
        return repr("%s (Error code: %s)" % (self.msg, self.error))

class OneRPCError(Exception):
    def __init__(self, msg):
        self.msg = msg
    def __str__(self):
        return repr("%s" % (self.msg))
    

class OneRPC(object):
    defaultAuthFile = "~/.one/one_auth"

    def __init__(self, hostname="localhost", port=2633, auth=None):
        """
        Constructor
        """

        requiredMethods = set([
                "one.hostpool.info",
                "one.host.info",
                "one.vm.deploy"
                ])
        
        if auth is None:
            auth = OneRPC.getAuth()

        #self.auth = "%s:%s" % (user, password)
        self.auth = auth

        uri = "http://%s:%i" % (hostname, port)
        self.rpc = xmlrpclib.ServerProxy(uri)

        try:
            methods = self.rpc.system.listMethods()
        except Exception, err:
            raise OneRPCError("Cannot connect to ONE XML RPC server at %s" % uri)

        if not requiredMethods.issubset(set(methods)):
            raise OneRPCError("XML RPC server does not support required methods.")

    def hostpoolInfo(self):
        """ get all hosts """
        try:
            status, msg, error = self.rpc.one.hostpool.info(self.auth)
        except xmlrpclib.Fault, err:
            raise OneRPCError("XMLRPC fault: %s" % err.faultString)
        except Exception, err:
            raise OneRPCError("XMLRPC fault: %s" % err)

        if status == True:
            hosts = OneHost.xmlToHostpool(msg.encode('utf-8'))
            return hosts
        else:
            raise OneError(msg, error)

    def hostInfo(self, id):
        """ get host info"""
        try:
            status, msg, error = self.rpc.one.host.info(self.auth, id)
        except xmlrpclib.Fault, err:
            raise OneRPCError("XMLRPC fault: %s" % err.faultString)
        except Exception, err:
            raise OneRPCError("XMLRPC fault: %s" % err)

        if status == True:
            host = OneHost.xmlToHost(msg.encode('utf-8'))
            return host
        else:
            raise OneError(msg, error)

    def vmpoolInfo(self, stateFilter="any", startRange=-1, endRange=-1):
        """ returns a list with all VMs """
        states = {
                "any": -2,
                "anyButDone": -1,
                "init": 0,
                "pending": 1,
                "hold": 2,
                "active": 3,
                "stopped": 4,
                "suspended": 5,
                "done": 6,
                "failed": 7,
                }
        
        try:
            stateFilter = states[stateFilter]
        except KeyError:
            raise ValueError("%s is not a valid vm state" % stateFilter)

        try:
            # -2: all vms; -1: start range; -1: end range; -2: vm states;
            status, msg, error = self.rpc.one.vmpool.info(self.auth, -2, startRange, endRange, stateFilter)
        except xmlrpclib.Fault, err:
            raise OneRPCError("XMLRPC fault: %s" % err.faultString)
        except Exception, err:
            raise OneRPCError("XMLRPC fault: %s" % err)
        
        if status == True:
            vms = OneVM.xmlToVMPool(msg.encode('utf-8'))
            return vms
        else:
            raise OneError(msg, error)
    
    def vmInfo(self, id):
        """ returns VM with specified id """
        try:
            # -2: all vms; -1: start range; -1: end range; -2: vm states;
            status, msg, error = self.rpc.one.vm.info(self.auth, id)
        except xmlrpclib.Fault, err:
            raise OneRPCError("XMLRPC fault: %s" % err.faultString)
        except Exception, err:
            raise OneRPCError("XMLRPC fault: %s" % err)
        
        if status == True:
            vm = OneVM.xmlToVM(msg.encode('utf-8'))
            return vm
        else:
            raise OneError(msg, error)

    
    def vmAction(self, action, vid):
        """run a known VM action
        known actions:  finialze, restart, shutdown, suspend, hold,
                        resubmit, stop, resume, release, cancel, reboot
        """
        knownActions = [
                "finalize", "restart", "shutdown", "suspend", "hold",
                "resubmit", "stop", "resume", "release", "cancel", "reboot"
                ]
        if not action in knownActions:
            raise OneError("%s is not a valid action" % action, 1919)
        
        try:
            status, msg, error = self.rpc.one.vm.action(self.auth, action, vid)
        except xmlrpclib.Fault, err:
            raise OneRPCError("XMLRPC fault: %s" % err.faultString)
        except Exception, err:
            raise OneRPCError("XMLRPC fault: %s" % err)
        
        if status == False:
            raise OneError(msg, error)

    def vmDelete(self, vid):
        self.vmAction("finalize", vid)

    def vmRestart(self, vid):
        self.vmAction("restart", vid)
    
    def vmShutdown(self, vid):
        self.vmAction("shutdown", vid)
    
    def vmSuspend(self, vid):
        self.vmAction("suspend", vid)

    def vmHold(self, vid):
        self.vmAction("hold", vid)

    def vmResubmit(self, vid):
        self.vmAction("resubmit", vid)
    
    def vmStop(self, vid):
        self.vmAction("stop", vid)

    def vmResume(self, vid):
        self.vmAction("resume", vid)

    def vmRelease(self, vid):
        self.vmAction("release", vid)

    def vmCancel(self, vid):
        """ vmCancel(vid) cancels VM with vid """
        self.vmAction("cancel", vid)

    def vmDeploy(self, vid, hid):
        """ vmDeploy(vid, hid) deploys VM with vid on HOST with hid """
        try:
            status, msg, error = self.rpc.one.vm.deploy(self.auth, vid, hid)
        except xmlrpclib.Fault, err:
            raise OneRPCError("XMLRPC fault: %s" % err.faultString)
        except Exception, err:
            raise OneRPCError("XMLRPC fault: %s" % err)
        if status == False:
            raise OneError(msg, error)
    
    @staticmethod
    def getAuth():
        if not os.environ.has_key("ONE_AUTH"):
            authFile = OneRPC.defaultAuthFile
        else:
            authFile = os.environ["ONE_AUTH"]

        authFile = os.path.expanduser(authFile)

        if not os.path.exists(authFile):
            raise OneError("Auth File not Found", 1919)
        
        try:
            with open(authFile) as f:
                auth = f.readline().strip()
                return auth
        except:
            raise OneError("Auth file not readable", 4242)

            




def parse_template(template_element):
    template = {}
    if template_element != None:
        for subelement in template_element:
            name = subelement.tag
            if len(subelement) == 0:
                template[name] = subelement.text
            else:
                template[name] = {}
                for subsubelement in subelement:
                    template[name][subsubelement.tag] = subsubelement.text
    return template

class OneHost(object):
    
    states = {
            0:"init",
            1:"monitoring_monitored",
            2:"monitored",
            3:"error",
            4:"disabled",
            5:"monitoring_error"
            }
    
    def __init__(self, xmlHost):
        self.id = int(xmlHost.find("ID").text)
        self.name = xmlHost.find("NAME").text
        self.state = self.states[int(xmlHost.find("STATE").text)]

        self.im_mad = xmlHost.find("IM_MAD").text
        self.vm_mad = xmlHost.find("VM_MAD").text
        self.vn_mad = xmlHost.find("VN_MAD").text
        self.last_mon_time = int(xmlHost.find("LAST_MON_TIME").text)
        
        host_share_element = xmlHost.find("HOST_SHARE")

        self.disk_usage = int(host_share_element.find("DISK_USAGE").text)
        self.mem_usage = int(host_share_element.find("MEM_USAGE").text)
        self.cpu_usage = int(host_share_element.find("CPU_USAGE").text)
        self.max_disk = int(host_share_element.find("MAX_DISK").text)
        self.max_mem = int(host_share_element.find("MAX_MEM").text)
        self.max_cpu = int(host_share_element.find("MAX_CPU").text)
        self.free_disk = int(host_share_element.find("FREE_DISK").text)
        self.free_mem = int(host_share_element.find("FREE_MEM").text)
        self.free_cpu = int(host_share_element.find("FREE_CPU").text)
        self.used_disk = int(host_share_element.find("USED_DISK").text)
        self.used_mem = int(host_share_element.find("USED_MEM").text)
        self.used_cpu = int(host_share_element.find("USED_CPU").text)
        self.running_vms = int(host_share_element.find("RUNNING_VMS").text)

        self.template = parse_template(xmlHost.find("TEMPLATE"))

    @classmethod
    def xmlToHost(cls, xmlStr):
        xmlHost = ET.fromstring(xmlStr)
        return cls(xmlHost)

    @classmethod
    def xmlToHostpool(cls, xmlStr):
        xmlHostpool = ET.fromstring(xmlStr)
        hosts = xmlHostpool.findall("HOST")
        return [cls(host) for host in hosts]

class OneVM(object):
    """OneVM represents a opennebula VM with all its properties.
    See the constructor for class variables.
    There are no getter and setter methods.
    """
    states = {
            0: "init",
            1: "pending",
            2: "hold",
            3: "active",
            4: "stopped",
            5: "suspended",
            6: "done",
            7: "failed",
            8: "poweroff",
            9: "undeployed"
            }

    lcmStates = {
            0: "init",
            1: "prolog",
            2: "boot",
            3: "running",
            4: "migrate",
            5: "save_stop",
            6: "save_suspend",
            7: "save_migrate",
            8: "prolog_migrate",
            9: "prolog_resume",
            10: "epilog_stop",
            11: "epilog",
            12: "shutdown",
            13: "cancel",
            14: "failure",
            15: "cleanup_resubmit",
            16: "unknown",
            17: "hotplug", 
            18: "shutdown_poweroff",
            19: "boot_unknown",
            20: "boot_poweroff",
            21: "boot_suspended",
            22: "boot_stopped",
            23: "cleanup_delete",
            24: "hotplug_snapshot",
            25: "hotplug_nic",
            26: "hotplug_saveas",
            27: "hotplug_saveas_poweroff",
            28: "hotplug_saveas_suspended",
            29: "shutdown_undeploy",
            30: "epilog_undeploy",
            31: "prolog_undeploy",
            32: "boot_undeploy"
            }
    def __init__(self, xmlVM):
        self.id = int(xmlVM.find("ID").text)
        self.uid = int(xmlVM.find("UID").text)
        self.gid = int(xmlVM.find("GID").text)
        self.uname = xmlVM.find("UNAME").text
        self.gname = xmlVM.find("GNAME").text
        self.name = xmlVM.find("NAME").text
        permissions = xmlVM.find("PERMISSIONS")
        if permissions is not None:
            self.owner_u = int(permissions.find("OWNER_U").text)
            self.owner_m = int(permissions.find("OWNER_M").text)
            self.owner_a = int(permissions.find("OWNER_A").text)
            self.group_u = int(permissions.find("GROUP_U").text)
            self.group_m = int(permissions.find("GROUP_M").text)
            self.group_a = int(permissions.find("GROUP_A").text)
            self.other_u = int(permissions.find("OTHER_U").text)
            self.other_m = int(permissions.find("OTHER_M").text)
            self.other_a = int(permissions.find("OTHER_A").text)
        self.last_poll = int(xmlVM.find("LAST_POLL").text)
        self.state = self.states[int(xmlVM.find("STATE").text)]       
        self.lcm_state = self.lcmStates[int(xmlVM.find("LCM_STATE").text)]
        self.stime = int(xmlVM.find("STIME").text)
        self.etime = int(xmlVM.find("ETIME").text)
        self.deploy_id = xmlVM.find("DEPLOY_ID").text
        self.memory =  int(xmlVM.find("MEMORY").text)
        self.cpu = int(xmlVM.find("CPU").text)
        self.net_tx = int(xmlVM.find("NET_TX").text)
        self.net_rx = int(xmlVM.find("NET_RX").text)

        self.template = parse_template(xmlVM.find("TEMPLATE"))
        
        # get all history elements 
        records = xmlVM.find("HISTORY_RECORDS")
        
        self.history = []
        self.hostname = ""

        if records is not None:
            self.history = map(parse_template, records.findall("HISTORY"))
            self.history = sorted(self.history, key=itemgetter('SEQ'))
            # set current hostname
            try:
                self.hostname = self.history[-1]["HOSTNAME"]
            except IndexError:
                # This only happens if VM was created with an opennebula version < 3.4
                self.hostname = None

            
    @classmethod
    def xmlToVM(cls, xmlStr):
        xmlVM = ET.fromstring(xmlStr)
        return cls(xmlVM)
    
    @classmethod
    def xmlToVMPool(cls, xmlStr):
        xmlVMPool = ET.fromstring(xmlStr)
        vms = xmlVMPool.findall("VM")
        return [cls(vm) for vm in vms]
