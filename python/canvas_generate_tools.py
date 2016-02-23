# This script requires the following environment variables:
# - HARVARD_DATA_TOOLS_BASE: the directory where the harvard-data-tools
#     repository has been checked out. Point to the root of the repository, e.g.
#     /tmp/code/harvard-data-tools
# - SECURE_PROPERTIES_LOCATION: a directory containing the secure.properties
#     file modelled after the template in:
#     java/aws_data_tools/src/main/resources/secure.properties.example
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
SECURE_PROPERTIES_LOCATION = os.environ['SECURE_PROPERTIES_LOCATION']
CURRENT_SCHEMA = os.environ['CANVAS_DATA_SCHEMA_VERSION']

DATA_CLIENT_DIR = "{0}/java/data_client".format(GIT_BASE)
DATA_TOOLS_DIR = "{0}/java/data_tools".format(GIT_BASE)

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
        "{0}/target/data_tools-1.0.0.jar".format(DATA_TOOLS_DIR),
        "{0}/data_tools.jar".format(GENERATED_CODE_DIR)
    )
    shutil.rmtree("{0}/java".format(GENERATED_CODE_DIR))

def run_generator():
    generator_classpath = "{0}:{1}:{2}".format(
        "{0}/target/data_client-1.0.0.jar".format(DATA_CLIENT_DIR),
        SCHEMA_JSON_DIR,
        SECURE_PROPERTIES_LOCATION
    )
    main_class = "edu.harvard.data.client.canvas.CanvasDataGenerator"
    command = [
        'java', '-cp', generator_classpath, main_class, CURRENT_SCHEMA,
        GIT_BASE,
        GENERATED_CODE_DIR
    ]
    print "Running {0}".format(command)
    process = subprocess.Popen(command)
    process.wait()
    print "Return code: {0}".format(process.returncode)
    check_return_code(process.returncode)

def copy_secure_properties():
    secure_file = "{0}/secure.properties".format(SECURE_PROPERTIES_LOCATION)
    resources_dir = "{0}/src/main/resources/secure.properties".format(DATA_TOOLS_DIR)
    shutil.copyfile(secure_file, resources_dir)

compile_java(DATA_CLIENT_DIR)
run_generator()
compile_java(JAVA_BINDINGS_DIR)
copy_secure_properties()
compile_java(DATA_TOOLS_DIR)

clean_up_files()
