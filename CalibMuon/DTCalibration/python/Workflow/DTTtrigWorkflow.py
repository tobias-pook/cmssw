import os
import logging

import tools
import FWCore.ParameterSet.Config as cms
from DTWorkflow import DTWorkflow

log = logging.getLogger(__name__)

class DTttrigWorkflow( DTWorkflow ):
    """ This class creates and performce / submits ttrig workflow jobs"""
    def __init__(self, options):
        # call parent constructor
        super( DTttrigWorkflow, self ).__init__( options )

        self.outpath_command_tag = "TTrigCalibration"
        self.outpath_workflow_mode_tag = ""
        output_file_dict ={ "timeboxes" : "DTTimeBoxes.root",
                            "residuals" : 'residuals.root',
                            "validation" : "validation.root"
                            }
        self.output_file = output_file_dict[self.options.workflow_mode]
        self.output_files = [self.output_file]

    def prepare_workflow(self):
        """ Generalized function to prepare workflow dependent on workflow mode"""
        function_name = "prepare_" + self.options.workflow_mode + "_" + self.options.command

        try:
            fill_function = getattr(self, function_name)
        except AttributeError:
            errmsg = "Class `{}` does not implement `{}`"
            raise NotImplementedError( errmsg.format(my_cls.__class__.__name__,
                                                     method_name))
        log.debug("Preparing workflow with function %s" % function_name)
        # call chosen function
        fill_function()
        # dump used options

    def prepare_timeboxes_submit(self):
        self.pset_name = 'dtTTrigCalibration_cfg.py'
        self.pset_template = 'CalibMuon.DTCalibration.dtTTrigCalibration_cfg'
        if self.options.datasettype == "Cosmics":
            self.pset_template = 'CalibMuon.DTCalibration.dtTTrigCalibration_cosmics_cfg'
        log.debug('Using pset template ' + self.pset_template)
        self.process = tools.loadCmsProcess(self.pset_template)
        self.process.GlobalTag.globaltag = self.options.globaltag
        self.process.dtTTrigCalibration.rootFileName = self.output_file
        self.process.dtTTrigCalibration.digiLabel = self.digilabel
        self.outpath_workflow_mode_tag = "TimeBoxes"

        if self.options.inputDBTag:
            moduleName = 'customDB%s' % self.options.inputDBRcd
            self.addPoolDBESSource( self.process,
                                    moduleName,
                                    self.options.inputDBRcd,
                                    self.options.inputDBTag)

        self.prepare_common_submit()

        self.write_pset_file()

    def prepare_timeboxes_check(self):
        self.outpath_workflow_mode_tag = "TimeBoxes"
        self.load_options_command("submit")

    def prepare_timeboxes_write(self):
        self.outpath_workflow_mode_tag = "TimeBoxes"
        self.load_options_command("submit")
        crabtask = self.crabFunctions.CrabTask(crab_config = self.crab_config_filepath)
        output_path = os.path.join( self.local_path, "unmerged_results" )
        result_path = os.path.abspath(os.path.join(self.local_path,"results"))
        merged_file = os.path.join(result_path, self.output_file)
        if not self.options.skip_stageout or self.files_reveived:
            self.get_output_files(crabtask, output_path)
            log.info("Received files from storage element")
            log.info("Using hadd to merge output files")
            if not os.path.exists(result_path):
                os.makedirs(result_path)
        returncode = tools.haddLocal(output_path, merged_file)
        if returncode != 0:
            raise RuntimeError("Failed to merge files with hadd")
        ttrig_uncorrected = "trig_uncorrected_"+ crabtask.crabConfig.Data.outputDatasetTag + ".db"
        ttrig_uncorrected = os.path.join(result_path, ttrig_uncorrected)
        self.pset_name = 'dtTTrigWriter_cfg.py'
        self.pset_template = "CalibMuon.DTCalibration.dtTTrigWriter_cfg"
        self.process = tools.loadCmsProcess(self.pset_template)
        self.process.dtTTrigWriter.rootFileName = "file:///" + merged_file
        self.process.PoolDBOutputService.connect = 'sqlite_file:%s' % ttrig_uncorrected
        self.process.GlobalTag.globaltag = cms.string(str(self.options.globaltag))
        self.write_pset_file()

    def prepare_timeboxes_all(self):
        # individual prepare functions for all tasks will be called in
        # main implementation of all
        self.all_commands=["submit", "check","write"]

    def prepare_residuals_submit(self):
        self.outpath_workflow_mode_tag = "Residuals"
        self.pset_name = 'dtResidualCalibration_cfg.py'
        self.pset_template = 'CalibMuon.DTCalibration.dtResidualCalibration_cfg'
        if self.options.datasettype == "Cosmics":
            self.pset_template = 'CalibMuon.DTCalibration.dtResidualCalibration_cosmics_cfg'
        self.process = tools.loadCmsProcess(self.pset_template)
        #~ self.process.GlobalTag.globaltag = cms.string(self.options.globaltag)
        self.process.GlobalTag.globaltag = self.options.globaltag
        self.process.dtResidualCalibration.rootFileName = self.output_file

        self.create_crab_config()
        if self.options.inputDBTag:
            moduleName = 'customDB%s' % self.options.inputDBRcd
            self.addPoolDBESSource( process = self.process,
                                    moduleName = moduleName,
                                    record = self.options.inputDBRcd,
                                    tag = self.options.inputDBTag,
                                    connect = self.options.connectStrDBTag)

        if self.options.inputCalibDB:
            self.addPoolDBESSource( process = self.process,
                                    moduleName = 'calibDB',
                                    record = 'DTTtrigRcd',
                                    tag = 'ttrig',
                                    connect = 'sqlite_file:%s' % os.path.basename(self.options.inputCalibDB))
            self.input_files.append(os.path.abspath( self.options.inputCalibDB ))

        self.prepare_common_submit()

        self.write_pset_file()

    def prepare_residuals_check(self):
        self.outpath_workflow_mode_tag = "Residuals"
        self.load_options_command("submit")

    def correction(self):
        returncode = self.runCMSSWtask()
        if returncode != 0:
            raise RuntimeError("Failed to use cmsRun for pset %s" % self.pset_name)

    def prepare_residuals_correction(self):
        self.outpath_workflow_mode_tag = "Residuals"
        self.pset_name = "dtTTrigResidualCorrection_cfg.py"
        self.pset_template = 'CalibMuon.DTCalibration.dtTTrigResidualCorrection_cfg'
        self.process = tools.loadCmsProcess(self.pset_template)
        self.load_options_command("submit")
        self.process.source.firstRun = self.options.run
        self.process.GlobalTag.globaltag = cms.string(str(self.options.globaltag))

        result_path = os.path.abspath(os.path.join(self.local_path,"results"))
        if not os.path.exists(result_path):
            os.makedirs(result_path)
        output_path = os.path.join( self.local_path, "unmerged_results" )
        print output_path
        merged_file = os.path.join(result_path, self.output_file)
        if not self.options.skip_stageout or self.files_reveived:
            self.get_output_files(crabtask, output_path)
            log.info("Received files from storage element")
            log.info("Using hadd to merge output files")
            if not os.path.exists(result_path):
                os.makedirs(result_path)
        returncode = tools.haddLocal(output_path, merged_file)
        if returncode != 0:
            raise RuntimeError("Failed to merge files with hadd")
        if self.options.inputT0DB:
            log.warning("Option inputT0DB not supported for residual corrections")

        if self.options.inputVDriftDB:
            self.addPoolDBESSource( process = self.process,
                                    moduleName = 'vDriftDB',
                                    record = 'DTMtimeRcd',
                                    tag = 'vDrift',
                                    connect = 'sqlite_file:%s' % os.path.abspath(self.config.inputVDriftDB))
        if self.options.inputCalibDB:
            self.addPoolDBESSource( process = self.process,
                                    moduleName = 'calibDB',
                                    record = 'DTTtrigRcd',
                                    tag = 'ttrig',
                                    #~ connect = cms.string('sqlite_file:%s' % os.path.abspath(self.options.inputCalibDB))
                                    connect = str("sqlite_file:%s" % os.path.abspath(self.options.inputCalibDB))
                                    )
        # Change DB label if running on Cosmics
        if self.options.datasettype == "Cosmics":
            self.process.dtTTrigResidualCorrection.dbLabel = 'cosmics'
            self.process.dtTTrigResidualCorrection.correctionAlgoConfig.dbLabel = 'cosmics'
        ttrig_ResidCorr_db = os.path.abspath( os.path.join(result_path,
                                              "ttrig_residuals_" + str(self.options.run) + ".db"))
        self.process.PoolDBOutputService.connect = 'sqlite_file:%s' % ttrig_ResidCorr_db
        rootfile_path = os.path.abspath( os.path.join(result_path, self.output_file))
        self.process.dtTTrigResidualCorrection.correctionAlgoConfig.residualsRootFile = merged_file

        self.write_pset_file()

    @classmethod
    def add_parser_options(cls, subparser_container):
        ttrig_parser = subparser_container.add_parser( "ttrig",
        #parents=[mutual_parent_parser, common_parent_parser],
        help = "" ) # What does ttrig


        ################################################################
        #                Sub parser options for workflow modes         #
        ################################################################
        ttrig_subparsers = ttrig_parser.add_subparsers( dest="workflow_mode",
            help="Possible workflow modes",)
        ## Add all workflow modes for ttrig
        ttrig_timeboxes_subparser = ttrig_subparsers.add_parser( "timeboxes",
            #parents=[mutual_parent_parser, common_parent_parser],
            help = "" )
        ttrig_residuals_subparser = ttrig_subparsers.add_parser( "residuals",
            #parents=[mutual_parent_parser, common_parent_parser],
            help = "" )
        ttrig_validation_subparser = ttrig_subparsers.add_parser( "validation",
            #parents=[mutual_parent_parser, common_parent_parser],
            help = "" )

        ################################################################
        #        Sub parser options for workflow mode timeboxes        #
        ################################################################
        ttrig_timeboxes_subparsers = ttrig_timeboxes_subparser.add_subparsers( dest="command",
            help="Possible commands for timeboxes")
        ttrig_timeboxes_submit_parser = ttrig_timeboxes_subparsers.add_parser(
            "submit",
            parents=[super(DTttrigWorkflow,cls).get_common_options_parser(),
                    super(DTttrigWorkflow,cls).get_submission_options_parser(),
                    super(DTttrigWorkflow,cls).get_input_db_options_parser()],
            help = "Submit job to the GRID via crab3")

        ttrig_timeboxes_check_parser = ttrig_timeboxes_subparsers.add_parser(
            "check",
            parents=[super(DTttrigWorkflow,cls).get_common_options_parser(),
                    super(DTttrigWorkflow,cls).get_check_options_parser(),],
            help = "Check status of submitted jobs")

        ttrig_timeboxes_write_parser = ttrig_timeboxes_subparsers.add_parser(
            "write",
            parents=[super(DTttrigWorkflow,cls).get_common_options_parser(),
                     super(DTttrigWorkflow,cls).get_write_options_parser()
                    ],
            help = "Write result from root output to text file")

        ttrig_timeboxes_write_parser = ttrig_timeboxes_subparsers.add_parser(
            "all",
            parents=[super(DTttrigWorkflow,cls).get_common_options_parser(),
                     super(DTttrigWorkflow,cls).get_submission_options_parser(),
                     super(DTttrigWorkflow,cls).get_check_options_parser(),
                     super(DTttrigWorkflow,cls).get_input_db_options_parser(),
                     super(DTttrigWorkflow,cls).get_write_options_parser()
                    ],
            help = "Perform all steps: submit, check, write in this order")

        ################################################################
        #        Sub parser options for workflow mode residuals        #
        ################################################################
        ttrig_residuals_subparsers = ttrig_residuals_subparser.add_subparsers( dest="command",
            help="Possible commands for residuals")
        ttrig_residuals_submit_parser = ttrig_residuals_subparsers.add_parser(
            "submit",
            parents=[super(DTttrigWorkflow,cls).get_common_options_parser(),
                    super(DTttrigWorkflow,cls).get_submission_options_parser(),
                    super(DTttrigWorkflow,cls).get_check_options_parser(),
                    super(DTttrigWorkflow,cls).get_local_input_db_options_parser(),
                    super(DTttrigWorkflow,cls).get_input_db_options_parser()],
            help = "Submit job to the GRID via crab3")

        ttrig_residuals_check_parser = ttrig_residuals_subparsers.add_parser(
            "check",
            parents=[super(DTttrigWorkflow,cls).get_common_options_parser(),
                    super(DTttrigWorkflow,cls).get_check_options_parser(),],
            help = "Check status of submitted jobs")

        ttrig_residuals_correct_parser = ttrig_residuals_subparsers.add_parser(
            "correction",
            parents=[super(DTttrigWorkflow,cls).get_common_options_parser(),
                    super(DTttrigWorkflow,cls).get_write_options_parser(),
                    super(DTttrigWorkflow,cls).get_local_input_db_options_parser(),
                    super(DTttrigWorkflow,cls).get_input_db_options_parser()],
            help = "Perform residual corrections")
        ttrig_residuals_correct_parser.add_argument("--globaltag",
            help="Alternative globalTag. Otherwise the gt for sunmission is used")

