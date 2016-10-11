if ! hadoop fs -test -e hdfs_1; then hadoop fs -mkdir hdfs_1; fi
set -e
sudo mkdir -p /var/log/hive/user/hadoop # Workaround for Hive logging bug
sudo chown hive:hive -R /var/log/hive
hadoop fs -mv hdfs_0/all_possible_column_types hdfs_1/all_possible_column_types &>> /home/hadoop/phase_1_move_unmodified_files.out
hadoop fs -mv hdfs_0/table_with_identifier hdfs_1/table_with_identifier &>> /home/hadoop/phase_1_move_unmodified_files.out
hadoop fs -mv hdfs_0/table_with_multiplexed_identifier hdfs_1/table_with_multiplexed_identifier &>> /home/hadoop/phase_1_move_unmodified_files.out
