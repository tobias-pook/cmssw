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
        # Dict to map workflow modes to output file name
        output_file_dict ={ "timeboxes" : "DTTimeBoxes.root",
                            "residuals" : 'residuals.root',
                            "validation" : "DQM.root"
                            }
        # Dict to map workflow modes to output folders in main path
        self.outpath_workflow_mode_dict = {
                                            "timeboxes" : "TimeBoxes",
                                            "residuals" : "Residuals",
                                            "validation" : "TtrigValidation"
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
            raise NotImplementedError( errmsg.format(self.__class__.__name__,
                                                     function_name))
        log.debug("Preparing workflow with function %s" % function_name)
        # call chosen function
        fill_function()
        # dump used options

    ####################################################################
    #                     Prepare functions for timeboxes              #
    ####################################################################
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

        if self.options.inputDBTag:
            self.add_local_custom_db()
        self.prepare_common_submit()
        self.write_pset_file()

    def prepare_timeboxes_check(self):
        self.load_options_command("submit")

    def prepare_timeboxes_write(self):
        tag = self.prepare_common_write()
        merged_file = os.path.join(self.result_path, self.output_file)
        ttrig_uncorrected = "trig_uncorrected_"+ tag + ".db"
        ttrig_uncorrected = os.path.join(self.result_path, ttrig_uncorrected)
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

    ####################################################################
    #                      prepare functions for residuals             #
    ####################################################################
    def prepare_residuals_submit(self):
        self.pset_name = 'dtResidualCalibration_cfg.py'
        self.pset_template = 'CalibMuon.DTCalibration.dtResidualCalibration_cfg'
        if self.options.datasettype == "Cosmics":
            self.pset_template = 'CalibMuon.DTCalibration.dtResidualCalibration_cosmics_cfg'
        self.process = tools.loadCmsProcess(self.pset_template)
        #~ self.process.GlobalTag.globaltag = cms.string(self.options.globaltag)
        self.process.GlobalTag.globaltag = self.options.globaltag
        self.process.dtResidualCalibration.rootFileName = self.output_file
        self.prepare_common_submit()
        if self.options.inputCalibDB:
            self.add_local_calib_db()
        self.write_pset_file()

    def prepare_residuals_check(self):
        self.load_options_command("submit")

    def prepare_residuals_correction(self):
        self.pset_name = "dtTTrigResidualCorrection_cfg.py"
        self.pset_template = 'CalibMuon.DTCalibration.dtTTrigResidualCorrection_cfg'
        self.process = tools.loadCmsProcess(self.pset_template)
        self.load_options_command("submit")
        self.process.source.firstRun = cms.untracked.uint32(self.options.run)
        self.process.GlobalTag.globaltag = cms.string(str(self.options.globaltag))

        tag = self.prepare_common_write()
        if self.options.inputT0DB:
            log.warning("Option inputT0DB not supported for residual corrections")

        if self.options.inputDBTag:
            self.add_local_custom_db()
        if self.options.inputVDriftDB:
            self.add_local_vdrift_db()
        if self.options.inputCalibDB:
            self.add_local_calib_db()
        # Change DB label if running on Cosmics
        if self.options.datasettype == "Cosmics":
            self.process.dtTTrigResidualCorrection.dbLabel = 'cosmics'
            self.process.dtTTrigResidualCorrection.correctionAlgoConfig.dbLabel = 'cosmics'
        ttrig_ResidCorr_db = os.path.abspath( os.path.join(self.result_path,
                                              "ttrig_residuals_" + str(tag) + ".db"))
        self.process.PoolDBOutputService.connect = 'sqlite_file:%s' % ttrig_ResidCorr_db
        rootfile_path = os.path.abspath( os.path.join(self.result_path, self.output_file))
        merged_file = os.path.join(self.result_path, self.output_file)
        self.process.dtTTrigResidualCorrection.correctionAlgoConfig.residualsRootFile = merged_file

        self.write_pset_file()

    def correction(self):
        """ Wrapper function as there is no difference compared to write"""
        self.write()

    def prepare_residuals_all(self):
        # individual prepare functions for all tasks will be called in
        # main implementation of all
        self.all_commands=["submit", "check","correction"]


    ####################################################################
    #                   prepare functions for validation               #
    ####################################################################

    def prepare_validation_submit(self):
        self.required_options_dict["submit"].append("input-db")
        self.pset_name = 'dtCalibValidation_cfg.py'
        self.pset_template = 'CalibMuon.DTCalibration.dtCalibValidation_cfg'
        if self.options.datasettype == "Cosmics":
            self.pset_template = 'CalibMuon.DTCalibration.dtCalibValidation_cosmics_cfg'
        self.process = tools.loadCmsProcess(self.pset_template)
        self.process.GlobalTag.globaltag = cms.string(str(self.options.globaltag))
        self.prepare_common_submit()
        self.options.input_db = os.path.abspath(self.options.input_db)


    def prepare_validation_check(self):
        self.load_options_command("submit")

    def prepare_validation_summary(self):
        self.pset_name = 'dtTTrigValidSummary_cfg.py'
        self.pset_template = 'CalibMuon.DTCalibration.dtTTrigValidSummary_cfg'

    def summary(self):
        pass
    def prepare_validation_all(self):
        # individual prepare functions for all tasks will be called in
        # main implementation of all
        self.all_commands=["submit", "check","summary"]

    ####################################################################
    #                           CLI creation                           #
    ####################################################################
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
            help = "" )
        ttrig_residuals_subparser = ttrig_subparsers.add_parser( "residuals",
            help = "" )
        ttrig_validation_subparser = ttrig_subparsers.add_parser( "validation",
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

        ttrig_timeboxes_all_parser = ttrig_timeboxes_subparsers.add_parser(
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

        ttrig_residuals_all_parser = ttrig_residuals_subparsers.add_parser(
            "all",
            parents=[super(DTttrigWorkflow,cls).get_common_options_parser(),
                    super(DTttrigWorkflow,cls).get_submission_options_parser(),
                    super(DTttrigWorkflow,cls).get_check_options_parser(),
                    super(DTttrigWorkflow,cls).get_write_options_parser(),
                    super(DTttrigWorkflow,cls).get_local_input_db_options_parser(),
                    super(DTttrigWorkflow,cls).get_input_db_options_parser()],
            help = "Perform all steps: submit, check, correct")

        ################################################################
        #        Sub parser options for workflow mode validation       #
        ################################################################
        ttrig_validation_subparsers = ttrig_validation_subparser.add_subparsers( dest="command",
            help="Possible commands for residuals")

        ttrig_validation_submit_parser = ttrig_validation_subparsers.add_parser(
            "submit",
            parents=[super(DTttrigWorkflow,cls).get_common_options_parser(),
                    super(DTttrigWorkflow,cls).get_submission_options_parser(),
                    super(DTttrigWorkflow,cls).get_check_options_parser(),
                    super(DTttrigWorkflow,cls).get_local_input_db_options_parser(),
                    super(DTttrigWorkflow,cls).get_input_db_options_parser()],
            help = "Submit job to the GRID via crab3")
        ttrig_validation_submit_parser.add_argument("--input-db",
            help="Input database for validation. Expects path to timeboxes or residual db." )


        ttrig_validation_check_parser = ttrig_validation_subparsers.add_parser(
            "check",
            parents=[super(DTttrigWorkflow,cls).get_common_options_parser(),
                    super(DTttrigWorkflow,cls).get_check_options_parser(),],
            help = "Check status of submitted jobs")

        ttrig_validation_summary_parser = ttrig_validation_subparsers.add_parser(
            "summary",
            parents=[super(DTttrigWorkflow,cls).get_common_options_parser(),
                    super(DTttrigWorkflow,cls).get_write_options_parser()],
            help = "Create summary for validation")

        ttrig_validation_all_parser = ttrig_validation_subparsers.add_parser(
            "all",
            parents=[super(DTttrigWorkflow,cls).get_common_options_parser(),
                    super(DTttrigWorkflow,cls).get_submission_options_parser(),
                    super(DTttrigWorkflow,cls).get_check_options_parser(),
                    super(DTttrigWorkflow,cls).get_write_options_parser(),
                    super(DTttrigWorkflow,cls).get_local_input_db_options_parser(),
                    super(DTttrigWorkflow,cls).get_input_db_options_parser()],
            help = "Perform all steps: submit, check, summary")
        ttrig_validation_all_parser.add_argument("--input-db",
            help="Input database for validation. Expects path to timeboxes or residual db." )
