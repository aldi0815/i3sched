# This file provides a config class.
# The config class reads, writes and validates i3sched's configuration file. 
# If you want to add options, they have to be added to configspec and to readConfig/writeConfig

import sys
import os

try:
    from configobj import ConfigObj
    from validate import Validator
except ImportError:
    print "configobj and validate have to be available!"
    print "get them at http://www.voidspace.org.uk/python/configobj.html#downloading"
    sys.exit(1)

class ConfigError(Exception):
    def __init__(self, error, msg):
        self.error = error
        self.msg = msg
    def __str__(self):
        return repr("%s" % (self.msg))

class Config(object):
    """Reads and writes i3sched's config file"""
    
    # config specification file is used to validate the config
    configSpecFile = "configspec"

    def __init__(self):
        """constructor determines configFile location"""
        oneLocation = os.getenv("ONE_LOCATION")
        if oneLocation is None:
            self.configFile = "/etc/one/i3sched.conf"
        else:
            self.configFile = os.path.join(oneLocation, "etc/i3sched.conf")

        #self.configFile = "i3sched.conf"
        
        
        self.sleepTime = 0
        self.shutdownTimeout = 0

        self.oneRPC = {}
        self.sge = {}


    def createConfig(self):
        """creates a config file with default values, uses the config specification"""
        vdt = Validator()
        config = ConfigObj(configspec=self.configSpecFile)
        config.filename = self.configFile
        config.validate(vdt, copy=True)
        try:
            config.write()
        except IOError:
            raise ConfigError("writeError", "Could not write config file to %s" % self.configFile)

    def readConfig(self):
        """reads the config file"""
        
        # checking for read access
        if not os.access(self.configFile, os.R_OK):
            # no read access, but file exits
            if os.path.exists(self.configFile):
                error = "readError"
                msg = "Could not read config file %s" % self.configFile
            # no read access, because file does not exist
            else:
                error = "noConfig"
                msg = "Config file %s does not exist" % self.configFile
            raise ConfigError(error, msg)

        # read config file
        try:
            config = ConfigObj(self.configFile, configspec=self.configSpecFile, file_error=True)
        except IOError:
            # this should never happen, because of test above
            raise ConfigError("readError", "Could not read config file %s, but it should be possible..." % self.configFile)

        # config validation
        val = Validator()
        validation = config.validate(val)

        if not validation:
            # This should not happen because of the default values
            return (False, "error", "Validation failed")
    
        # set config values

        self.sleepTime = config["sleepTime"]
        self.shutdownTimeout = config["shutdownTimeout"]

        self.oneRPC["hostname"] = config["OneRPC"]["hostname"]
        self.oneRPC["port"] = config["OneRPC"]["port"]
        self.oneRPC["startVM"] = config["OneRPC"]["startVM"]

        self.sge["qsub"] =  config["SGE"]["qsub"]
        self.sge["qstat"] =  config["SGE"]["qstat"]
        self.sge["qdel"] =  config["SGE"]["qdel"]
        self.sge["qhost"] = config["SGE"]["qhost"]
        self.sge["idleCmd"] =  config["SGE"]["idleCmd"]
        self.sge["parallelEnvironment"] = config["SGE"]["parallelEnvironment"]
        self.sge["queue"] = config["SGE"]["queue"]
        self.sge["hostSuffix"] = config["SGE"]["hostSuffix"]

        
        # values missing, report which default values were used
        if len(config.defaults) > 0 or len(config["OneRPC"].defaults) > 0 or len(config["SGE"].defaults) > 0:
            missing = "Root: "
            if len(config.defaults) > 0:
                for option in config.defaults:
                    missing += "%s," % option
            else:
                missing += "is complete"
            
            missing += "; Section OneRPC: "
            if len(config["OneRPC"].defaults) > 0:
                for option in config["OneRPC"].defaults:
                    missing += "%s," % option
            else:
                missing += "is complete"

            missing += "; Section SGE: "
            if len(config["SGE"].defaults) > 0:
                for option in config["SGE"].defaults:
                    missing += "%s," % option
            else:
                missing += "is complete"
            
            return (True, "missing", missing)

        # everything worked fine, nothing to report
        return (True, "", "")

