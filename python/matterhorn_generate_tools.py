# This script requires the following environment variables:
# - HARVARD_DATA_TOOLS_BASE: the directory where the harvard-data-tools
#     repository has been checked out. Point to the root of the repository, e.g.
#     /tmp/code/harvard-data-tools
# - HARVARD_DATA_GENERATED_OUTPUT: the directory where generated scripts and
#     .jar files should be stored.
# - CANVAS_DATA_SCHEMA_VERSION: The version of the Canvas Data schema for which
#     files will be generated. Format as a string, e.g. 1.2.0

import os
import shutil
import subprocess
import sys

GIT_BASE = os.environ['HARVARD_DATA_TOOLS_BASE']
GENERATED_CODE_DIR = os.environ['HARVARD_DATA_GENERATED_OUTPUT']
CURRENT_SCHEMA = os.environ['DATA_SCHEMA_VERSION']
CONFIG_PATHS = os.environ['CONFIG_PATHS']
PIPELINE_ID = os.environ['PIPELINE_ID']

DATA_CLIENT_DIR = "{0}/java/data_client".format(GIT_BASE)
DATA_TOOLS_DIR = "{0}/java/matterhorn_data_tools".format(GIT_BASE)
MATTERHORN_DATA_CLIENT_DIR = "{0}/java/matterhorn_data_client".format(GIT_BASE)

JAVA_BINDINGS_DIR = "{0}/java".format(GENERATED_CODE_DIR)

SCHEMA_JSON_DIR = "{0}/schema".format(GIT_BASE)

def check_return_code(return_code):
    if return_code != 0:
        sys.exit(return_code)

def compile_java(code_dir):
    command = ['mvn', 'clean', 'install']
    print "Running {0} in {1}".format(command, code_dir)
    process = subprocess.Popen(command, cwd=code_dir)
    process.wait()
    print "Return code: {0}".format(process.returncode)
    check_return_code(process.returncode)

def clean_up_files():
    os.rename(
        "{0}/target/matterhorn_data_tools-1.0.0.jar".format(DATA_TOOLS_DIR),
        "{0}/data_tools.jar".format(GENERATED_CODE_DIR)
    )
    shutil.rmtree("{0}/java".format(GENERATED_CODE_DIR))

def run_generator():
    generator_classpath = "{0}:{1}:{2}:{3}".format(
        "{0}/target/matterhorn_data_client-1.0.0.jar".format(MATTERHORN_DATA_CLIENT_DIR),
        "{0}/target/data_client-1.0.0.jar".format(DATA_CLIENT_DIR),
        SCHEMA_JSON_DIR,
        PIPELINE_ID
    )
    main_class = "edu.harvard.data.matterhorn.MatterhornCodeGenerator"
    command = [
        'java', '-cp', generator_classpath, main_class,
        CURRENT_SCHEMA,
        CONFIG_PATHS,
        GIT_BASE,
        GENERATED_CODE_DIR,
        PIPELINE_ID
    ]
    print "Running {0} in {1}".format(command, GENERATED_CODE_DIR)
    process = subprocess.Popen(command)
    process.wait()
    print "Return code: {0}".format(process.returncode)
    check_return_code(process.returncode)

compile_java(DATA_CLIENT_DIR)
compile_java(MATTERHORN_DATA_CLIENT_DIR)
run_generator()
compile_java(JAVA_BINDINGS_DIR)
compile_java(DATA_TOOLS_DIR)

clean_up_files()
