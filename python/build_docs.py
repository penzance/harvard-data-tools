import os
import shutil
import subprocess
import sys

GIT_BASE = os.environ['HARVARD_DATA_TOOLS_BASE']
CANVAS_GENERATED_CODE = os.environ['HDT_CANVAS_GENERATED_CODE_DIR']
EDX_GENERATED_CODE = os.environ['HDT_EDX_GENERATED_CODE_DIR']
MATTERHORN_GENERATED_CODE = os.environ['HDT_MATTERHORN_GENERATED_CODE_DIR']

TEMP_DIR = '/tmp/hdt_javadoc'

primary_path = '{0}/java/data_client'.format(GIT_BASE)

paths = [
    '{0}/java/pipeline_complete_lambda'.format(GIT_BASE),

    '{0}/java/canvas_data_client'.format(GIT_BASE),
    '{0}/java/canvas_data_tools'.format(GIT_BASE),
    CANVAS_GENERATED_CODE,

    '{0}/java/matterhorn_data_client'.format(GIT_BASE),
    '{0}/java/matterhorn_data_tools'.format(GIT_BASE),
    MATTERHORN_GENERATED_CODE,

    # '{0}/java/edx_data_client'.format(GIT_BASE),
    # '{0}/java/edx_data_tools'.format(GIT_BASE),
    # EDX_GENERATED_CODE,
    ]

def check_return_code(return_code, cmd):
    if return_code != 0:
        print "Error. Unexpected return code {} from command {}".format(return_code, cmd)
        sys.exit(return_code)

def run_command(cmd, cwd=TEMP_DIR):
    print "Running {0}".format(cmd)
    process = subprocess.Popen(cmd, cwd=cwd)
    process.wait()
    check_return_code(process.returncode, cmd)

if not os.path.exists(TEMP_DIR):
    os.makedirs(TEMP_DIR)
shutil.copytree(primary_path, "{0}/code".format(TEMP_DIR))
for path in paths:
    run_command(['cp', '-r', "{0}/src/main/java".format(path), "{0}/code/src/main".format(TEMP_DIR)])
run_command(['mvn', 'javadoc:javadoc'], "{0}/code".format(TEMP_DIR))

run_command(['cp', '-r', "{0}/code/target/site/apidocs".format(TEMP_DIR), GIT_BASE])

shutil.rmtree(TEMP_DIR)