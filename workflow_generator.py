#!/usr/bin/env python3
import os
import logging
from pathlib import Path
from argparse import ArgumentParser

logging.basicConfig(level=logging.DEBUG)

# --- Import Pegasus API ------------------------------------------------------
from Pegasus.api import *
TOP_DIR = Path(__file__).resolve().parent

def generate_workflow(exec_site_name):
    # --- Properties ---------------------------------------------------------------
    # properties that will be used by both the outer workflow  and inner diamond workflow
    print("Creating workflow properties...")
    props = Properties()
    props.write()

    # properties that will be used by inner diamond workflow
    props["pegasus.catalog.transformation.file"] = "inner_diamond_workflow_tc.yml"
    props["pegasus.catalog.replica.file"] = "inner_diamond_workflow_rc.yml"
    props.write("inner_diamond_workflow.pegasus.properties")
    
    # --- Sites --------------------------------------------------------------------
    sc = SiteCatalog()
    print("Creating execution sites...")
    # local site
    local_site = Site(name="local", arch=Arch.X86_64, os_type=OS.LINUX, os_release="rhel", os_version="7")
    local_site.add_directories(
        Directory(Directory.SHARED_SCRATCH, TOP_DIR / "work/local-site/scratch")
            .add_file_servers(FileServer("file://{}".format(TOP_DIR / "work/local-site/scratch"), Operation.ALL)),
        Directory(Directory.LOCAL_STORAGE, TOP_DIR / "outputs/local-site")
            .add_file_servers(FileServer("file://{}".format(TOP_DIR / "outputs/local-site"), Operation.ALL))
            )

    exec_site = (
            Site(exec_site_name)
            .add_pegasus_profile(style="condor")
            .add_condor_profile(universe="vanilla")
            .add_profiles(Namespace.PEGASUS, key="data.configuration", value="condorio")
        )

    sc.add_sites(local_site, exec_site)
    sc.write()
    
    # --- Transformations ----------------------------------------------------------
    # create transformation catalog for the outer level workflow
    print("Creating transformation catalog...")
    wc = Transformation(
                        name="wc",
                        site="condorpool",
                        pfn="/usr/bin/wc",
                        is_stageable=False
        )

    curl = Transformation(
                        name="curl",
                        site="condorpool",
                        pfn="/usr/bin/curl",
                        is_stageable=False
        )

    tc = TransformationCatalog()
    tc.add_transformations(wc,curl)
    tc.write()
    
    # create transformation catalog for the inner diamond workflow
    preprocess = Transformation(
                name="preprocess",
                site="condorpool",
                pfn="/usr/bin/pegasus-keg",
                is_stageable=True
            )

    findrange = Transformation(
                name="findrange",
                site="condorpool",
                pfn="/usr/bin/pegasus-keg",
                is_stageable=True
            )

    analyze = Transformation(
                name="analyze",
                site="condorpool",
                pfn="/usr/bin/pegasus-keg",
                is_stageable=True
            )

    inner_diamond_workflow_tc = TransformationCatalog()
    inner_diamond_workflow_tc.add_transformations(preprocess, findrange, analyze)
    inner_diamond_workflow_tc.write("inner_diamond_workflow_tc.yml")
    
    # --- Replicas -----------------------------------------------------------------

    # replica catalog for the outer workflow
    rc = ReplicaCatalog()
    rc.add_replica(site="local", lfn="inner_diamond_workflow.pegasus.properties", pfn=TOP_DIR / "inner_diamond_workflow.pegasus.properties")
    rc.add_replica(site="local", lfn="inner_diamond_workflow_tc.yml", pfn=TOP_DIR / "inner_diamond_workflow_tc.yml")
    rc.add_replica(site="local", lfn="inner_diamond_workflow.yml", pfn=TOP_DIR / "inner_diamond_workflow.yml")
    rc.add_replica(site="local", lfn="sites.yml", pfn=TOP_DIR / "sites.yml")
    rc.write()
    
    # --- Workflow -----------------------------------------------------------------
    wf = Workflow("hierarchical-workflow")
    print("Creating pipeline workflow dag...")
    
    webpage = File("pegasus.html")

    curl_job = (
            Job("curl")
            .add_args("-o", webpage, "http://pegasus.isi.edu")
            .add_outputs(webpage, stage_out=True, register_replica=True)
        )


    fb1 = File("f.b1")
    fb2 = File("f.b2")
    fc1 = File("f.c1")
    fc2 = File("f.c2")
    fd = File("f.d")
    
    wf2 = (
    Workflow("blackdiamond")
        .add_jobs(
            Job(preprocess, namespace="diamond", version="4.0")
            .add_args("-a", "preprocess", "-T", "20", "-i", webpage, "-o", fb1, fb2)
            .add_inputs(webpage)
            .add_outputs(fb1, fb2),
            Job(findrange, namespace="diamond", version="4.0")
            .add_args("-a", "findrange", "-T", "20", "-i", fb1, "-o", fc1)
            .add_inputs(fb1)
            .add_outputs(fc1),
            Job(findrange, namespace="diamond", version="4.0")
            .add_args("-a", "findrange", "-T", "20", "-i", fb2, "-o", fc2)
            .add_inputs(fb2)
            .add_outputs(fc2),
            Job(analyze, namespace="diamond", version="4.0")
            .add_args("-a", "analyze", "-T", "20", "-i", fc1, fc2, "-o", fd)
            .add_inputs(fc1, fc2)
            .add_outputs(fd),
        )
        .write(file="inner_diamond_workflow.yml")
    )
    
    # job to plan and run the diamond workflow
    diamond_wf_job = SubWorkflow(file="inner_diamond_workflow.yml", is_planned=False, _id="diamond_subworkflow")\
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
                        File("inner_diamond_workflow.pegasus.properties", for_planning=True),
                        File("inner_diamond_workflow_tc.yml", for_planning=True),
                        File("sites.yml", for_planning=True),
                        webpage
                    )\
                    .add_outputs(fd)

    count = File("count.txt")
    wc_job = (
            Job(wc)
            .add_args("-l", fd)\
            .add_inputs(fd)\
            .set_stdout(count, stage_out=True, register_replica=True)
        )
    wf.add_jobs(curl_job,diamond_wf_job, wc_job)
    wf.add_dependency(curl_job, children=[diamond_wf_job])
    wf.add_dependency(diamond_wf_job, children=[wc_job])
    
    wf.write("workflow.yml")

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
    
    generate_workflow(args.execution_site_name)
    
