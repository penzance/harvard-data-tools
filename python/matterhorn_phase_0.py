import os
import subprocess
import sys

DROPBOX_LOCATION = os.environ['MATTERHORN_DROPBOX_LOCATION']
INCOMING_LOCATION = os.environ['MATTERHORN_INCOMING_LOCATION']
GENERATED_CODE_DIR = os.environ['HARVARD_DATA_GENERATED_OUTPUT']
SECURE_PROPERTIES_LOCATION = os.environ['SECURE_PROPERTIES_LOCATION']
GIT_BASE = os.environ['HARVARD_DATA_TOOLS_BASE']
THREAD_COUNT = os.environ.get('MATTERHORN_DATA_THREAD_COUNT', 1)

MAIN_CLASS = 'edu.harvard.data.matterhorn.cli.MatterhornDataCli'
CLASSPATH = "{0}/data_tools.jar:{1}:{2}".format(
    GENERATED_CODE_DIR,
    SECURE_PROPERTIES_LOCATION,
    "{0}/schema".format(GIT_BASE)
)

command = [
    'java', '-Duser.timezone=EST', '-Xmx32G', '-cp', CLASSPATH, MAIN_CLASS,
    "parse", DROPBOX_LOCATION, INCOMING_LOCATION
]
print "Running {0}".format(command)
process = subprocess.Popen(command)
process.wait()
return_code = process.returncode
print "Return code: {0}".format(return_code)
sys.exit(return_code)
