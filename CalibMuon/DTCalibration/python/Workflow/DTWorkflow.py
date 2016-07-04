import os,sys
import glob
import logging
import argparse
import subprocess
import time, datetime
import urllib2
import json
from importlib import import_module

import tools
import FWCore.ParameterSet.Config as cms
log = logging.getLogger(__name__)

class DTWorkflow(object):
    """ This is the base class for all DTWorkflows and contains some
        common tasks """
    def __init__(self, options):
        # perform imports only when creating instance. This allows to use the classmethods e.g.for
        # CLI construction before crab is sourced.
        self.crabFunctions =  import_module('CalibMuon.DTCalibration.Workflow.Crabtools.crabFunctions')
        self.options = options
        self.digilabel = "muonDTDigis"

        # These variables are determined in the derived classes
        self.pset_name = ""
        self.outpath_command_tag = ""
        self.outpath_workflow_mode_tag = ""
        self.output_files = []
        self.input_files = []
        # cached member variables
        self._crab = None
        self.run_all_command = False
        self.files_reveived = False
        self._user = ""
        # change to working directory
        os.chdir(self.options.working_dir)


    def run(self):
        """ Generalized function to run workflow command"""
        msg = "Preparing %s workflow" % self.options.workflow
        if hasattr(self.options, "command"):
            msg += " for command %s" % self.options.command
        log.info(msg)
        self.prepare_workflow()
        # create output folder if they do not exist yet
        if not os.path.exists( self.local_path ):
            os.makedirs(self.local_path)
        # dump used options
        self.dump_options()

        try:
            run_function = getattr(self, self.options.command)
        except AttributeError:
            errmsg = "Class `{}` does not implement `{}` for workflow %s" % self.options.workflow
            if hasattr(self.options, "workflow_mode"):
                errmsg += "and workflow mode %s" % self.options.workflow_mode
            raise NotImplementedError( errmsg.format(self.__class__.__name__,
                                                     self.options.command))
        log.debug("Running command %s" % self.options.command)
        # call chosen function
        run_function()

    def prepare_workflow(self):
        """ Abstract implementation of prepare workflow function"""
        errmsg = "Class `{}` does not implement `{}`"
        raise NotImplementedError( errmsg.format(self.__class__.__name__,
                                                     "prepare_workflow"))

    def all(self):
        """ generalized function to perform several workflow mode commands in chain.
            All commands mus be specified in self.all_commands list in workflow mode specific
            prepare function in child workflow objects.
        """
        self.run_all_command = True
        for command in self.all_commands:
            self.options.command = command
            self.run()

    def submit(self):
        # create a crab config
        log.info("Creating crab config")
        self.create_crab_config()
        #write crab config
        full_crab_config_filename = self.write_crabConfig()
        if self.options.no_exec:
            log.info("Runing with option no-exec exiting")
            return True
        #submit crab job
        log.info("Submitting crab job")
        self.crab.submit(full_crab_config_filename)
        log.info("crab job submitted. Waiting 30 seconds before first status call")
        time.sleep( 30 )
        task = self.crabFunctions.CrabTask(crab_config = full_crab_config_filename)
        task.update()
        success_states = ( 'QUEUED', 'SUBMITTED', "COMPLETED", "FINISHED")
        if task.state in success_states:
            log.info("Job in state %s" % task.state )
            return True
        else:
            log.error("Job submission not successful, crab state:%s" % task.state)
            return False

    def check(self):
        """ Function to check status of submitted crab tasks """
        print self.crab_config_filepath
        task = self.crabFunctions.CrabTask(crab_config = self.crab_config_filepath,
                                            initUpdate = False)
        for n_check in range(self.options.max_checks):
            task.update()
            if task.state in ( "COMPLETED"):
                print "Crab task complete. Getting output locally"
                output_path = os.path.join( self.local_path, "unmerged_results" )
                self.get_output_files(task, output_path)
                return True
            if task.state in ("SUBMITFAILED", "FAILED"):
                print "Crab task failed"
                return False
            possible_job_states =  ["nUnsubmitted",
                                    "nIdle",
                                    "nRunning",
                                    "nTransferring",
                                    "nCooloff",
                                    "nFailed",
                                    "nFinished",
                                    "nComplete" ]

            jobinfos = ""
            for jobstate in possible_job_states:
                njobs_in_state = getattr(task, jobstate)
                if njobs_in_state > 0:
                    jobinfos+="%s: %d " % (jobstate[1:], njobs_in_state)

            #clear line for reuse
            sys.stdout.write("\r")
            sys.stdout.write("".join([" " for i in range(tools.getTerminalSize()[0] ) ] ) )
            sys.stdout.write("\r")
            prompt_text = "Check (%d/%d). Task state: %s (%s). Press q and enter to stop checks: " % (n_check,
                self.options.max_checks, task.state, jobinfos)
            user_input = tools.stdinWait(prompt_text, "", self.options.check_interval)
            if user_input in ("q","Q"):
                return False
        print "Task not completed after %d checks (%d minutes)" % ( self.options.max_checks,
            int( self.options.check_interval / 60. ))
        return False

    def write(self):
        returncode = self.runCMSSWtask()
        if returncode != 0:
            raise RuntimeError("Failed to use cmsRun for pset %s" % self.pset_name)

    def add_preselection(self):
        """ Add preselection to the process object stored in workflow_object"""
        if not hasattr(self, "process"):
            raise NameError("Process is not initalized in workflow object")
        pathsequence = self.options.preselection.split(':')[0]
        seqname = self.options.preselection.split(':')[1]
        self.process.load(pathsequence)
        tools.prependPaths(self.process, seqname)

    def add_raw_option(self):
        if self.options.datasettype == "MC" and self.options.run_on_RAW:
            getattr(self.process, self.digilabel).inputLabel = 'rawDataCollector'
            tools.prependPaths(self.process,self.digilabel)

    def add_local_t0_db(self):
        connect_path ='sqlite_file:%s' % os.path.basename(self.options.inputT0DB)
        self.addPoolDBESSource( process = self.process,
                                moduleName = 't0DB',
                                record = 'DTT0Rcd',
                                tag = 't0',
                                connect = connect_path)
        self.input_files.append(os.path.abspath(self.options.inputT0DB))

    def add_local_vdrift_db(self):
        connect_path = 'sqlite_file:%s' % os.path.basename(self.config.inputVDriftDB)
        self.addPoolDBESSource( process = self.process,
                                moduleName = 'vDriftDB',
                                record = 'DTMtimeRcd',
                                tag = 'vDrift',
                                connect = connect_path)
        self.input_files.append( os.path.abspath(self.options.inputVDriftDB) )

    def prepare_common_submit(self):
        """ Common operations used in most prepare_[workflow_mode]_prepare functions"""
        if not self.options.run:
            raise ValueError("Option run is required for submission!")
        if hasattr(self.options, "inputT0DB") and self.options.inputT0DB:
            self.add_local_t0_db()

        if hasattr(self.options, "inputVDriftDB") and self.options.inputVDriftDB:
            self.add_local_vdrift_db()

        if self.options.run_on_RAW:
            self.add_raw_option()
        if self.options.preselection:
            self.add_preselection()

    def addPoolDBESSource( self,
                           process,
                           moduleName,
                           record,
                           tag,
                           connect='sqlite_file:',
                           label='',):

        from CondCore.CondDB.CondDB_cfi import CondDB

        calibDB = cms.ESSource("PoolDBESSource",
                               CondDB,
                               timetype = cms.string('runnumber'),
                               toGet = cms.VPSet(cms.PSet(
                                   record = cms.string(record),
                                   tag = cms.string(tag),
                                   label = cms.untracked.string(label)
                                    )),
                               )
        calibDB.connect = cms.string( connect )
        #if authPath: calibDB.DBParameters.authenticationPath = authPath
        if 'oracle:' in connect:
            calibDB.DBParameters.authenticationPath = '/afs/cern.ch/cms/DB/conddb'
        setattr(process,moduleName,calibDB)
        setattr(process,"es_prefer_" + moduleName,cms.ESPrefer('PoolDBESSource',
                                                                moduleName)
                                                                )

    @property
    def crab(self):
        """ Retuns a CrabController instance from cache or creates new
           on on first call """
        if self._crab is None:
            if not self.voms_proxy_time_left() > 0:
                errmsg = "No valid proxy, please create a proxy before"
                errmsg += "or crab might use wrong voGroup and role"
                raise ValueError(errmsg)
            self.cert_info = self.crabFunctions.CertInfo()
            if self.cert_info.voGroup:
                self._crab = self.crabFunctions.CrabController(voGroup = self.cert_info.voGroup)
            else:
                self._crab = self.crabFunctions.CrabController()
        return self._crab

    def voms_proxy_time_left(self):
        process = subprocess.Popen(['voms-proxy-info', '-timeleft'], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        stdout = process.communicate()[0]
        if process.returncode != 0:
            return 0
        else:
            return int(stdout)

    def create_crab_config(self):
        """ Create a crab config object dependent on the chosen command option"""
        from CalibMuon.DTCalibration.Workflow.Crabtools.crabConfigParser import CrabConfigParser
        self.crab_config = CrabConfigParser()
        """ Fill common options in crab config """
        ### General section
        self.crab_config.add_section('General')
        if "/" in self.crab_taskname:
            raise ValueError( 'Sample contains "/" which is not allowed' )
        self.crab_config.set( 'General', 'requestName', self.crab_taskname )
        self.crab_config.set( 'General', 'workArea', self.local_path)
        if self.options.no_log:
            self.crab_config.set( 'General', 'transferLogs', 'False' )
        else:
            self.crab_config.set( 'General', 'transferLogs', 'True' )
        ### JobType section
        self.crab_config.add_section('JobType')
        self.crab_config.set( 'JobType', 'pluginName', 'Analysis' )
        self.crab_config.set( 'JobType', 'psetName', self.pset_path )
        self.crab_config.set( 'JobType', 'outputFiles', self.output_files)
        if self.input_files:
            self.crab_config.set( 'JobType', 'inputFiles', self.input_files)
        ### Data section
        self.crab_config.add_section('Data')
        self.crab_config.set('Data', 'inputDataset', self.options.datasetpath)
        # set job splitting options
        if self.options.datasettype =="MC":
            self.crab_config.set('Data', 'splitting', 'FileBased')
            self.crab_config.set('Data', 'unitsPerJob', str(self.options.filesPerJob) )
        else:
            self.crab_config.set('Data', 'splitting', 'LumiBased')
            self.crab_config.set('Data', 'unitsPerJob', str(self.options.lumisPerJob) )
            if self.options.runselection:
                self.crab_config.set( "Data",
                                      "runRange",
                                      ",".join( self.options.runselection )
                                    )
        # set output path in compliance with crab3 structure
        self.crab_config.set('Data', 'publication', False)
        self.crab_config.set('Data', 'outputDatasetTag', self.remote_out_path["outputDatasetTag"])
        self.crab_config.set('Data', 'outLFNDirBase', self.remote_out_path["outLFNDirBase"] )

        # set site section options
        self.crab_config.add_section('Site')
        self.crab_config.set('Site', 'storageSite', self.options.output_site)
        self.crab_config.set('Site', 'whitelist', self.options.ce_white_list)
        self.crab_config.set('Site', 'blacklist', self.options.ce_black_list)

        #set user section options if necessary
        if self.cert_info.voGroup or self.cert_info.role:
            self.crab_config.add_section('User')
            if self.cert_info.voGroup:
                self.crab_config.set('User', "voGroup", self.cert_info.voGroup)
            if self.cert_info.role:
                self.crab_config.set('User', "voGroup", self.cert_info.role)
        log.debug("Created crab config: %s " % self.crab_config_filename)

    def write_crabConfig(self):
        """ Write crab config file in working dir with label option as name """
        base_path = os.path.join( self.options.working_dir,self.local_path)
        filename = os.path.join( base_path, self.crab_config_filename)
        if not os.path.exists(base_path):
            os.makedirs(base_path)
        if os.path.exists(filename):
            raise IOError("file %s alrady exits"%(filename))
        self.crab_config.writeCrabConfig(filename)
        log.info( 'created crab config file %s'%filename )
        return filename

    def list_remote_files(self, crabtask, extension=".root"):
        query_url = "https://cmsweb.cern.ch/phedex/datasvc/json/prod/lfn2pfn?"
        query_url += "protocol=srmv2"
        query_url += "&node=%s" % crabtask.crabConfig.Site.storageSite
        query_url += "&lfn=/"
        raw_json = urllib2.urlopen(query_url).read()
        pfn_base_path = json.loads(raw_json)["phedex"]["mapping"][0]["pfn"]
        pfn_path = pfn_base_path + crabtask.crabConfig.Data.outLFNDirBase[1:]
        log.info("Getting list of files on local storage element")
        log.info(pfn_path)
        log.info("lcg-lsR.sh %s" % pfn_path)
        process = subprocess.Popen( "lcg-lsR.sh %s" % pfn_path,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT,
                                    shell=True)
        stdout, stderr = process.communicate()
        lfn_files = []
        for line in stdout.split("\n"):
            if not extension in line:
                continue
            filename = "store/" + line.split()[-1].split("/store/")[-1]
            lfn_files.append(filename)
        log.info(lfn_files)
        return lfn_files, pfn_path, pfn_base_path

    def get_output_files(self, crabtask, output_path, extension=".root"):
        lfn_files, pfn_path, pfn_base_path = self.list_remote_files(crabtask, extension)
        if not os.path.exists(output_path):
            os.makedirs(output_path)
        abs_resultpath = os.path.abspath(os.path.join(output_path) )
        if not os.path.exists( abs_resultpath ):
            os.makedirs(abs_resultpath)
        existing_files = glob.glob( abs_resultpath + "/*"+ extension)
        log.info("Copying output files to local disk")
        if not lfn_files:
            log.error("Found no files in remote storage element, exiting")
            sys.exit(1)
        for lfn_path in lfn_files:
            if not crabtask.crabConfig.Data.outputDatasetTag in lfn_path:
                continue
            for existing_file in existing_files:
                if os.path.basename(lfn_path) in existing_file:
                    continue
            local_file_path = os.path.join( abs_resultpath, os.path.basename(lfn_path) )
            remote_file_path = pfn_base_path + lfn_path
            tries = 0
            while tries < 3:
                command = [ "lcg-cp",
                            remote_file_path,
                            "file:///" + local_file_path ]
                log.info(" ".join(command))
                process = subprocess.Popen(command, stdout=subprocess.PIPE)
                process.communicate()
                if process.returncode==0:
                    log.info(lfn_path)
                    break
                tries +=1
            if tries == 3:
                raise RuntimeError("Unable to copy file to local disk: %s" % filename)
        self.files_reveived = True

    def runCMSSWtask(self):
        process = subprocess.Popen( "cmsRun %s" % self.pset_path,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT,
                            shell = True)
        stdout = process.communicate()[0]
        print stdout
        return process.returncode

    @property
    def crab_config_filename(self):
        if hasattr(self.options, "crab_config_path"):
            return self.options.crab_config_path
        return 'crab_%s_cfg.py' % self.crab_taskname

    @property
    def crab_config_filepath(self):
        base_path = os.path.join( self.options.working_dir,self.local_path)
        return os.path.join( base_path, self.crab_config_filename)

    @property
    def crab_taskname(self):
        taskname = self.options.label + "_" + self.options.workflow + "_"
        if hasattr( self.options, "workflow_mode"):
            taskname+= self.options.workflow_mode + "_"
        taskname += "run_" + str(self.options.run) + "_v" + str(self.options.trial)
        return taskname

    @property
    def remote_out_path(self):
        """ Output path on remote excluding user base path
        Returns a dict if crab is used due to crab path setting policy"""
        if self.options.command =="submit":
            return {
                "outLFNDirBase" : os.path.join( "/store",
                                                "user",
                                                self.user,
                                                'DTCalibration/',
                                                self.outpath_command_tag,
                                                self.outpath_workflow_mode_tag),
                "outputDatasetTag" :   'Run' + str(self.options.run) + '_v' + str(self.options.trial)
                    }
        else:
            return os.path.join( 'DTCalibration/',
                                 datasetstr,
                                 'Run' + str(self.options.run),
                                 self.outpath_command_tag,
                                 self.outpath_workflow_mode_tag,
                                 'v' + str(self.options.trial),
                                )
    @property
    def user(self):
        if self._user:
            return self._user
        if hasattr(self.options, "user") and self.options.user:
            self._user = self.options.user
        else:
            self._user = self.crab.checkusername()
        return self._user

    def fill_options_from_crab_config(self):
        crabtask = CrabTask( crab_config = self.crab_config_filename )
        splitinfo = crabtask.crabConfig.Data.outputDatasetTag.split("_")
        run, trial = splitinfo[0].split("Run")[-1], splitinfo[1].split("v")[-1]
        if not self.options.run:
            self.options.run = int(run)
        self.options.trail = int(trial)
        if not hasattr(self.options, "datasetpath"):
            self.options.datasetpath = crabtask.crabConfig.Data.inputDataset
        if not hasattr(self.options, "label"):
            self.options.label = crabtask.crabConfig.General.requestName.split("_")[0]
    @property
    def local_path(self):
        """ Output path on local machine """
        prefix = "Run%d-%s_v%d" % ( self.options.run,
                                    self.options.label,
                                    self.options.trial)
        if self.outpath_workflow_mode_tag:
            return os.path.join( self.options.working_dir,
                                    prefix,
                                 self.outpath_workflow_mode_tag,
                                  )
        else:
            return os.path.join( self.options.working_dir,
                                 prefix,
                                 self.outpath_command_tag )
    @property
    def pset_template_base_bath(self):
        """ Base path to folder containing pset files for cmsRun"""
        return os.path.expandvars(os.path.join("$CMSSW_BASE",
                                               "src",
                                               "CalibMuon",
                                               "test",
                                               )
                                 )

    @property
    def pset_path(self):
        """ full path to the pset file """
        basepath = os.path.join( self.local_path, "psets")
        if not os.path.exists( basepath ):
            os.makedirs( basepath )
        return os.path.join( basepath, self.pset_name )

    def write_pset_file(self):
        if not hasattr(self, "process"):
            raise NameError("Process is not initalized in workflow object")
        if not os.path.exists( self.local_path):
            os.makedirs( self.local_path )
        print "pset_path", self.pset_path
        with open( self.pset_path,'w') as pfile:
            pfile.write(self.process.dumpPython())

    def get_config_name(self, command= ""):
        """ Create the name for the output json file which will be dumped"""
        if not command:
            command = self.options.command
        return "config_" + command + ".json"

    def dump_options(self):
        with open(os.path.join(self.local_path, self.get_config_name()),"w") as out_file:
            json.dump(vars(self.options), out_file)

    def load_options(self, config_file_path):
        if not os.path.exists(config_file_path):
            raise IOError("File %s not found" % config_file_path)
        with open(config_file_path, "r") as input_file:
            config_json = json.load(input_file)
            for key, val in config_json.items():
                if not hasattr(self.options, key) or not getattr(self.options, key):
                    setattr(self.options, key, val)

    def load_options_command(self, command ):
        """Load options for previous command in workflow """
        #~ if not self.run_all_command:s
        if not self.options.config_path:
            self.options.config_path = os.path.join(self.local_path,
                                                    self.get_config_name(command))
        self.load_options( self.options.config_path )

    @classmethod
    def add_parser_options(cls, parser):
        # Subparsers are used to choose a calibration workflow
        workflow_subparsers = parser.add_subparsers( help="workflow option help", dest="workflow" )
        return workflow_subparsers

    @classmethod
    def get_common_options_parser(cls):
        """ Return a parser with common options for each workflow"""
        common_opts_parser = argparse.ArgumentParser(add_help=False)
        common_opts_group = common_opts_parser.add_argument_group(
            description ="General options")
        common_opts_group.add_argument("-r","--run", type=int,
            help="set reference run number (typically first or last run in list)")
        common_opts_group.add_argument("--trial", type=int, default = 1,
            help="trial number used in the naming of output directories")
        common_opts_group.add_argument("--label", default="dtCalibration",
            help="label used in the naming of workflow output default:%(default)s")
        common_opts_group.add_argument("--datasettype", default = "Data",
            choices=["Data", "Cosmics", "MC"], help="Type of input dataset default: %(default)s")
        common_opts_group.add_argument("--config-path",
            help="Path to alternative workflow config json file, e.g. used to submit the job")
        common_opts_group.add_argument("--user", default="",
            help="User used e.g. for submission. Defaults to user HN name")
        common_opts_group.add_argument("--working-dir",
            default=os.getcwd(), help="connect string default:%(default)s")
        common_opts_group.add_argument("--no-exec",
            action="store_true", help="Do not execute or submit any workflow")
        return common_opts_parser

    @classmethod
    def get_input_db_options_parser(cls):
        """ Return a parser object with options relevant for input databases"""
        db_opts_parser = argparse.ArgumentParser(add_help=False)
        dp_opts_group = db_opts_parser.add_argument_group(
            description ="Options for Input databases")
        db_opts_parser.add_argument("--inputDBRcd",
            help="Record used for PoolDBESSource")
        db_opts_parser.add_argument("--inputDBTag",
            help="Tag used for PoolDBESSource")
        return db_opts_parser

    @classmethod
    def get_local_input_db_options_parser(cls):
        """ Return a parser object with options relevant for input databases"""
        db_opts_parser = argparse.ArgumentParser(add_help=False)
        dp_opts_group = db_opts_parser.add_argument_group(
            description ="Options for local input databases")
        db_opts_parser.add_argument("--inputVDriftDB",
            help="Local alternative VDrift database")
        db_opts_parser.add_argument("--inputTtrigDB",
            help="Local alternative Ttrig database")
        db_opts_parser.add_argument("--inputT0DB",
            help="Local alternative T0 database")
        db_opts_parser.add_argument("--inputCalibDB",
            help="Local alternative calib database")
        return db_opts_parser

    @classmethod
    def get_submission_options_parser(cls):
        """ Return a parser object with options relevant to remote submission"""
        submission_opts_parser = argparse.ArgumentParser(add_help=False)
        submission_opts_group = submission_opts_parser.add_argument_group(
            description ="Options for Job submission")
        submission_opts_group.add_argument("--datasetpath", required=True,
            help="dataset name to process")
        submission_opts_group.add_argument("--run-on-RAW", action = "store_true",
            help="Flag if run on RAW dataset")
        submission_opts_group.add_argument("--globaltag", required = True,
        help="global tag identifier (with the '::All' string, if necessary)")
        submission_opts_group.add_argument("--runselection", default = [], nargs="+",
            help="run list or range")
        submission_opts_group.add_argument("--filesPerJob", default = 5,
            help="Number of files to process for MC grid jobs")
        submission_opts_group.add_argument("--lumisPerJob", default = 10000,
            help="Number of lumi sections to process for RAW / Comsics grid jobs")
        submission_opts_group.add_argument("--preselection", dest="preselection",
            help="configuration fragment and sequence name, separated by a ':', defining a pre-selection filter")
        submission_opts_group.add_argument("--connect", dest="connectStrDBTag",
            default='frontier://FrontierProd/CMS_COND_31X_DT', help="connect string default:%(default)s")
        submission_opts_group.add_argument("--output-site", default = "T2_DE_RWTH",
            help="Site used for stage out of results")
        submission_opts_group.add_argument("--ce-black-list", default = [], nargs="+",
            help="add sites to black list when run on Grid")
        submission_opts_group.add_argument("--ce-white-list", default = [], nargs="+",
            help="add sites to white list when run on Grid")
        submission_opts_group.add_argument("--no-log",
            action="store_true", help="Do not transfer crab logs:%(default)s")
        return submission_opts_parser

    @classmethod
    def get_check_options_parser(cls):
        """ Return a parser object with options relevant to check the status of remote submission"""
        check_opts_parser = argparse.ArgumentParser(add_help=False)
        check_opts_group = check_opts_parser.add_argument_group(
            description ="Options for Job submission")
        check_opts_group.add_argument("--check-interval", default = 600,type=int,
            help="Time in seconds between check operations default: %(default)s")
        check_opts_group.add_argument("--max-checks", default =1000, type=int,
            help="Maximum number of checks before check is considered failed default: %(default)s")
        return check_opts_parser

    @classmethod
    def get_write_options_parser(cls):
        """ Return a parser object with options relevant to write results to dbs"""
        check_opts_parser = argparse.ArgumentParser(add_help=False)
        check_opts_group = check_opts_parser.add_argument_group(
            description ="Options for write jobs")
        check_opts_group.add_argument("--skip-stageout", action="store_true",
            help="Skip stageout to local disk and merging")
        return check_opts_parser
