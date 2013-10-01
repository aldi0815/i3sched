import sys
import os

from datetime import datetime

class Log(object):
    logTypes = {
            "error" : "E",
            "warning" : "W",
            "info" : "I",
            "debug" : "D"
            }
    
    def __init__(self):
        oneLocation = os.getenv("ONE_LOCATION")
        if oneLocation is None:
            self.onedFile = "/var/log/one/oned.log"
            self.schedFile = "/var/log/one/sched.log"
        else:
            self.onedFile = os.path.join(oneLocation, "var/oned.log")
            self.schedFile = os.path.join(oneLocation, "var/sched.log")

    def writeOnedLog(self, logType, *msg):
        if logType not in self.logTypes.keys():
            raise KeyError("unknown log type")
        self.write(self.onedFile, logType, msg)

    def writeSchedLog(self, logType, *msg):
        if logType not in self.logTypes.keys():
            raise KeyError("unknown log type")        
        self.write(self.schedFile, logType, msg)

    def write(self, filename, logType, msgList):
        try:
            f = file(filename, "a+")
            for msg in msgList:
                f.write("%s [SCHED][%s]: %s\n" % (datetime.now().ctime(), self.logTypes[logType], msg))
            f.close()
        except IOError, err:
            sys.stderr.write("Logging failed: %s\n" % err)
            for msg in msgList:
                sys.stderr.write("i3sched error: %s\n" % msg)

