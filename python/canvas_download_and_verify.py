import json
import os
import subprocess
import sys

CURRENT_SCHEMA = os.environ['CANVAS_DATA_SCHEMA_VERSION']
GENERATED_CODE_DIR = os.environ['HARVARD_DATA_GENERATED_OUTPUT']
SECURE_PROPERTIES_LOCATION = os.environ['SECURE_PROPERTIES_LOCATION']
RESULT_METADATA = os.environ['CANVAS_DATA_RESULT_FILE']

MAIN_CLASS = 'edu.harvard.data.data_tools.DataCli'
CLASSPATH = "{0}/data_tools.jar:{1}".format(
        GENERATED_CODE_DIR,
        SECURE_PROPERTIES_LOCATION
    )

def run_command(args):
    command = ['java', '-Duser.timezone=GMT', '-Xmx32G', '-cp', CLASSPATH, MAIN_CLASS] + args
    print "Running {0}".format(command)
    process = subprocess.Popen(command)
    process.wait()
    print "Return code: {0}".format(process.returncode)
    return process.returncode

def bail(message):
    print message

def download_and_verify():
    status = run_command(['canvas', 'download', RESULT_METADATA])
    if status != 0:
        bail('Failed to download dump')
        return status

    with open(RESULT_METADATA) as result_file:
        download_result = json.load(result_file)

    dump_id = download_result['DUMP_ID']

    status = run_command(['canvas', 'compareschemas', dump_id, CURRENT_SCHEMA])
    if status != 0:
        bail('Failed on schema check')
        return status

    status = run_command(['canvas', 'verify', '0', '-i', dump_id])
    if status != 0:
        bail('Failed to verify dump')
        return status

    return 0

if __name__ == '__main__':
    return_code = download_and_verify()
    sys.exit(return_code)
