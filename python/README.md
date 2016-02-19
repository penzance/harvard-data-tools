Environment variables for canvas_generate_tools.py:
  CANVAS_DATA_SCHEMA_VERSION # Schema version number - currently 1.5.0.
  HARVARD_DATA_GENERATED_OUTPUT # Directory to place all generated files.
  SECURE_PROPERTIES_LOCATION # Directory location of the generated secure.properties file.
  HARVARD_DATA_TOOLS_BASE # Root location for the harvard-data-tools repository checked out of Git.

Environment variables for canvas_download_and_verify.py:
  CANVAS_DATA_SCHEMA_VERSION # schema version number - currently 1.5.0.
  HARVARD_DATA_GENERATED_OUTPUT # directory where generated files were placed by canvas_generate_tools.py
  SECURE_PROPERTIES_LOCATION # directory location of the generated secure.properties file.
  CANVAS_DATA_RESULT_FILE # file location to store a JSON blob with information on a run.
