#!/usr/bin/env python3
import os
import logging
from pathlib import Path
from argparse import ArgumentParser

logging.basicConfig(level=logging.DEBUG)

# --- Import Pegasus API ------------------------------------------------------
from Pegasus.api import *


class PipelineWorkflow:
    wf = None
    sc = None
    tc = None
    props = None

    dagfile = None
    wf_name = None
    wf_dir = None

    # --- Init ----------------------------------------------------------------
    def __init__(self, dagfile="workflow.yml"):
        self.dagfile = dagfile
        self.wf_name = "hierarchical"
        self.wf_dir = str(Path(".").parent.resolve())

    # --- Write files in directory --------------------------------------------
    def write(self):
        if not self.sc is None:
            self.sc.write()
        self.props.write()
        self.tc.write()
        self.wf.write()
        self.wf.plan(submit=True)

    # --- Configuration (Pegasus Properties) ----------------------------------
    def create_pegasus_properties(self):
        self.props = Properties()

        # properties that will be used by both the outer workflow  and inner diamond workflow
        self.props["pegasus.mode"] = "development"
        self.props.write()
        
        # properties that will be used by inner diamond workflow
        self.props["pegasus.catalog.transformation.file"] = "inner_diamond_workflow_tc.yml"
        self.props["pegasus.catalog.replica.file"] = "inner_diamond_workflow_rc.yml"
        self.props.write("inner_diamond_workflow.pegasus.properties")
        return

    # --- Site Catalogs --------------------------------------------------------
    def create_sites_catalog(self, exec_site_name="condorpool"):
        self.sc = SiteCatalog()

        shared_scratch_dir = os.path.join(self.wf_dir, "scratch")
        local_storage_dir = os.path.join(self.wf_dir, "output")

        local = Site("local").add_directories(
            Directory(Directory.SHARED_SCRATCH, shared_scratch_dir).add_file_servers(
                FileServer("file://" + shared_scratch_dir, Operation.ALL)
            ),
            Directory(Directory.LOCAL_STORAGE, local_storage_dir).add_file_servers(
                FileServer("file://" + local_storage_dir, Operation.ALL)
            ),
        )

        exec_site = (
            Site(exec_site_name)
            .add_pegasus_profile(style="condor")
            .add_condor_profile(universe="vanilla")
            .add_profiles(Namespace.PEGASUS, key="data.configuration", value="condorio")
        )

        self.sc.add_sites(local, exec_site)

    # --- Transformation Catalogs (Executables and Containers) -----------------
    def create_transformation_catalog(self, exec_site_name="condorpool"):
        self.tc = TransformationCatalog()

        curl = Transformation(
            "curl", site=exec_site_name, pfn="/usr/bin/curl", is_stageable=False
        )
        
        wc = Transformation(
            "wc", site=exec_site_name, pfn="/usr/bin/wc", is_stageable=False,
        )
        
        generate_diamond_wf = Transformation(
                        name="generate_inner_diamond_workflow",
                        site="condorpool",
                        pfn=os.path.join(self.wf_dir, "generate_inner_diamond_workflow.py"),
                        is_stageable=True,
                        arch=Arch.X86_64,
                        os_type=OS.LINUX,
                        os_release="rhel",
                        os_version="7"
                    )

        self.tc.add_transformations(curl, wc, generate_diamond_wf)
        
        # create transformation catalog for the inner diamond workflow
        preprocess = Transformation(
                name="preprocess",
                site="local",
                pfn="/usr/bin/pegasus-keg",
                is_stageable=True,
                arch=Arch.X86_64,
                os_type=OS.LINUX,
                os_release="rhel",
                os_version="7"
            )

        findrange = Transformation(
                name="findrange",
                site="local",
                pfn="/usr/bin/pegasus-keg",
                is_stageable=True,
                arch=Arch.X86_64,
                os_type=OS.LINUX,
                os_release="rhel",
                os_version="7"
            )

        analyze = Transformation(
                name="analyze",
                site="local",
                pfn="/usr/bin/pegasus-keg",
                is_stageable=True,
                arch=Arch.X86_64,
                os_type=OS.LINUX,
                os_release="rhel",
                os_version="7"
            )

        inner_diamond_workflow_tc = TransformationCatalog()
        inner_diamond_workflow_tc.add_transformations(preprocess, findrange, analyze)
        inner_diamond_workflow_tc.write("inner_diamond_workflow_tc.yml")
        

    # --- Create Workflow -----------------------------------------------------
    def create_workflow(self):
        
        # --- Replicas -----------------------------------------------------------------
        with open("f.a", "w") as f:
            f.write("Sample input file for the first inner dax job.")

        # replica catalog for the inner diamond workflow
        inner_diamond_workflow_rc = ReplicaCatalog()
        inner_diamond_workflow_rc.add_replica(site="local", lfn="f.a", pfn=self.wf_dir+"f.a")
        inner_diamond_workflow_rc.write("inner_diamond_workflow_rc.yml")

        # replica catalog for the outer workflow
        rc = ReplicaCatalog()
        rc.add_replica(site="local", lfn="inner_diamond_workflow.pegasus.properties",
                       pfn=self.wf_dir+"inner_diamond_workflow.pegasus.properties")
        rc.add_replica(site="local", lfn="inner_diamond_workflow_rc.yml", pfn=self.wf_dir+"inner_diamond_workflow_rc.yml")
        rc.add_replica(site="local", lfn="inner_diamond_workflow_tc.yml", pfn=self.wf_dir+"inner_diamond_workflow_tc.yml")
        rc.add_replica(site="local", lfn="sites.yml", pfn=self.wf_dir+"sites.yml")
        rc.add_replica(site="local", lfn="inner_sleep_workflow.yml", pfn=self.wf_dir+"inner_sleep_workflow.yml")
        rc.write()
        
        
        self.wf = Workflow(self.wf_name)

        webpage = File("pegasus.html")

        curl_job = (
            Job(curl)
            .add_args("-o", webpage, "http://pegasus.isi.edu")
            .add_outputs(webpage, stage_out=True, register_replica=True)
        )

        count = File("count.txt")

        wc_job = (
            Job("wc")
            .add_args("-l", webpage)
            .add_inputs(webpage)
            .set_stdout(count, stage_out=True, register_replica=True)
        )

        # job to generate the diamond workflow
        diamond_wf_file = File("inner_diamond_workflow.yml")
        generate_diamond_wf_job = Job("generate_inner_diamond_workflow")\
                            .add_outputs(diamond_wf_file)

        # job to plan and run the diamond workflow
        diamond_wf_job = SubWorkflow(file=diamond_wf_file, is_planned=False, _id="diamond_subworkflow")\
                    .add_args(
                        "--conf",
                        "inner_diamond_workflow.pegasus.properties",
                        "--output-sites",
                        "local",
                        "-vvv",
                        "--basename",
                        "inner"
                    )\
                    .add_inputs(
                        File("inner_diamond_workflow.pegasus.properties"),
                        File("inner_diamond_workflow_rc.yml"),
                        File("inner_diamond_workflow_tc.yml"),
                        File("sites.yml"),
                    )
        
        self.wf.add_jobs(curl_job, wc_job, generate_diamond_wf_job, diamond_wf_job)
        self.wf.add_dependency(generate_diamond_wf_job, children=[diamond_wf_job])


if __name__ == "__main__":
    parser = ArgumentParser(description="Pegasus Hierarchical Workflow")

    parser.add_argument(
        "-s",
        "--skip_sites_catalog",
        action="store_true",
        help="Skip site catalog creation",
    )
    parser.add_argument(
        "-e",
        "--execution_site_name",
        metavar="STR",
        type=str,
        default="condorpool",
        help="Execution site name (default: condorpool)",
    )
    parser.add_argument(
        "-o",
        "--output",
        metavar="STR",
        type=str,
        default="workflow.yml",
        help="Output file (default: workflow.yml)",
    )

    args = parser.parse_args()

    workflow = PipelineWorkflow(args.output)

    if not args.skip_sites_catalog:
        print("Creating execution sites...")
        workflow.create_sites_catalog(args.execution_site_name)

    print("Creating workflow properties...")
    workflow.create_pegasus_properties()
    
    print("Creating transformation catalog...")
    workflow.create_transformation_catalog(args.execution_site_name)

    print("Creating pipeline workflow dag...")
    workflow.create_workflow()

    workflow.write()