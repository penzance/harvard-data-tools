Setup instructions:
 - Create secure.properties in aws_data_tools/src/main/resources, following the template in secure.properties.example
 - Build the aws tools jar:
   cd java/data_client
   mvn clean install
   cd ../aws_data_tools
   mvn clean install

Run the jar with the appropriate command line options:

 - Download any new dumps from the Canvas Data API:
   java -jar target/aws_data_tools-1.0.0.jar download

 - Compare two schema versions:
   java -jar target/aws_data_tools-1.0.0.jar compareschemas 1.1.0 1.3.0

See /aws_data_tools/src/main/java/edu/harvard/canvas_data/aws_data_tools/cli/ReturnStatus.java for application return values.
