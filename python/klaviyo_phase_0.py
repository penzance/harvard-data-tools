import json
import os
import subprocess
import sys

GENERATED_CODE_DIR = os.environ['HARVARD_DATA_GENERATED_OUTPUT']
RESULT_METADATA = os.environ['KLAVIYO_DATA_RESULT_FILE']
GIT_BASE = os.environ['HARVARD_DATA_TOOLS_BASE']
DUMP_ID = os.environ.get('KLAVIYO_DATA_DUMP_ID', None)
THREAD_COUNT = os.environ.get('DATA_THREAD_COUNT', 1)

MAIN_CLASS = 'edu.harvard.data.klaviyo.cli.KlaviyoDataCli'
CLASSPATH = "{0}/data_tools.jar:{1}".format(
    GENERATED_CODE_DIR,
    "{0}/schema".format(GIT_BASE)
)


def run_command(args):
    command = ['java', '-Duser.timezone=GMT', '-Xmx32G', '-cp', CLASSPATH,
               MAIN_CLASS] + args
    print "Running {0}".format(command)
    process = subprocess.Popen(command)
    process.wait()
    print "Return code: {0}".format(process.returncode)
    return process.returncode


def bail(message):
    print message


def download_and_verify():
    if not DUMP_ID:
        status = run_command(['download', RESULT_METADATA])
        if status != 0:
            bail('Failed to download dump')
            return status

        with open(RESULT_METADATA) as result_file:
            download_result = json.load(result_file)
        dump_id = download_result['DUMP_ID']
    else:
        dump_id = DUMP_ID
        print "Skipping download for Dump ID: {0}".format(dump_id)

    status = run_command(['compareschemas', dump_id, CURRENT_SCHEMA])
    if status != 0:
        bail('Failed on schema check')
        return status

    status = run_command(
        ['-threads', THREAD_COUNT, 'postverify', '0', '-i', dump_id])
    if status != 0:
        bail('Failed to verify dump')
        return status

    status = run_command(['updateredshift', dump_id])
    if status != 0:
        bail('Failed to update Redshift schema')
        return status

    return 0


if __name__ == '__main__':
    return_code = download_and_verify()
    sys.exit(return_code)
