import os
import logging

import tools
import FWCore.ParameterSet.Config as cms
from DTWorkflow import DTWorkflow

log = logging.getLogger(__name__)

class DTvdriftWorkflow( DTWorkflow ):
    """ This class creates and performce / submits vdrift workflow jobs"""
    def __init__(self, options):
        # call parent constructor
        super( DTttrigWorkflow, self ).__init__( options )

        self.outpath_command_tag = "VdriftCalibration"
        self.outpath_workflow_mode_tag = ""
        output_file_dict ={ "segment" : "DTTimeBoxes.root",
                            }
        self.output_file = output_file_dict[self.options.workflow_mode]
        self.output_files = [self.output_file]

    def prepare_segment_submit(self):
        self.pset_name = 'dtVDriftSegmentCalibration_cfg.py'
        self.pset_template = 'CalibMuon.DTCalibration.dtResidualCalibration_cfg'
        if self.options.datasettype == "Cosmics":
            self.pset_template = 'CalibMuon.DTCalibration.dtVDriftSegmentCalibration_cosmics_cfg'

        self.prepare_common_submit()

        if self.options.inputTtrigDB:
            label = ''
            if self.options.datasettype == "Cosmics":
                label = 'cosmics'
            connect_path = 'sqlite_file:%s' % os.path.basename(self.config.inputTTrigDB)
            self.addPoolDBESSource( process = self.process,
                                    moduleName = 'tTrigDB',
                                    record = 'DTTtrigRcd',
                                    tag = 'ttrig',
                                    label = label,
                                    connect = connect_path)

        self.write_pset_file()

    def prepare_segment_submit(self):
        pass

    def prepare_segment_write(self):
        self.pset_name = 'dtVDriftSegmentWriter_cfg.py'
        self.pset_template = 'dtVDriftSegmentWriter_cfg.py'

        self.outpath_workflow_mode_tag = "Segments"
        if not command == "all":
            if not self.options.config_path:
                self.options.config_path = os.path.join(self.local_path,
                                                        self.get_config_name("write"))
            self.load_options( self.options.config_path )
        crabtask = self.crabFunctions.CrabTask(crab_config = self.crab_config_filepath)
        self.fill_options_from_crab_config()
        output_path = os.path.join( self.local_path, "unmerged_results" )
        self.get_output_files(crabtask, output_path)


    @classmethod
    def add_parser_options(cls, subparser_container):
        vdrift_parser = subparser_container.add_parser( "vdrift",
        #parents=[mutual_parent_parser, common_parent_parser],
        help = "" ) # What does ttrig

        ################################################################
        #                Sub parser options for workflow modes         #
        ################################################################
        vdrift_subparsers = vdrift_parser.add_subparsers( dest="workflow_mode",
            help="Possible workflow modes",)
        ## Add all workflow modes for ttrig
        vdrift_timeboxes_subparser = vdrift_subparsers.add_parser( "segment",
            #parents=[mutual_parent_parser, common_parent_parser],
            help = "" )
        ################################################################
        #        Sub parser options for workflow mode timeboxes        #
        ################################################################
        vdrift_segment_subparsers = vdrift_segment_subparser.add_subparsers( dest="command",
            help="Possible commands for timeboxes")
        vdrift_segment_submit_parser = ttrig_timeboxes_subparsers.add_parser(
            "submit",
            parents=[super(DTttrigWorkflow,cls).get_common_options_parser(),
                    super(DTttrigWorkflow,cls).get_submission_options_parser(),
                    super(DTttrigWorkflow,cls).get_local_input_db_options_parser(),
                    super(DTttrigWorkflow,cls).get_input_db_options_parser()],
            help = "Submit job to the GRID via crab3")
        vdrift_segment_submit_parser.add_argument("--inputTtrigDB",
            help="Local alternative calib ttrig db")

        vdrift_residuals_check_parser = ttrig_residuals_subparsers.add_parser(
            "check",
            parents=[super(DTttrigWorkflow,cls).get_common_options_parser(),
                    super(DTttrigWorkflow,cls).get_check_options_parser(),],
            help = "Check status of submitted jobs")
