# Data Tools

Download latest non-downloaded dump:
* java -jar data_tools.jar edu.harvard.data.data_tools.DataCli canvas download /metadata/output/file

Verify a dump (Phase 0 - directly downloaded from Instructure):
* java -jar data_tools.jar edu.harvard.data.data_tools.DataCli canvas verify 0 -i dump_uuid

Compare schemas:
* java -jar data_tools.jar edu.harvard.data.data_tools.DataCli canvas compareschemas dump_uuid version_id

Run the hadoop jobs for phase 1:
* java -jar data_tools.jar edu.harvard.data.data_tools.DataCli canvas hadoop 1 /input/hdfs/dir /output/hdfs/dir

Verify at the end of phase 1:
* java -jar data_tools.jar edu.harvard.data.data_tools.DataCli canvas verify 1 -i dump_uuid

