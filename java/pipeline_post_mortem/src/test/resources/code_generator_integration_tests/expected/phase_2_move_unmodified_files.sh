if ! hadoop fs -test -e hdfs_2; then hadoop fs -mkdir hdfs_2; fi
set -e
sudo mkdir -p /var/log/hive/user/hadoop # Workaround for Hive logging bug
sudo chown hive:hive -R /var/log/hive
hadoop fs -mv hdfs_1/all_possible_column_types hdfs_2/all_possible_column_types &>> /home/hadoop/phase_2_move_unmodified_files.out
hadoop fs -mv hdfs_1/table_with_identifier hdfs_2/table_with_identifier &>> /home/hadoop/phase_2_move_unmodified_files.out
hadoop fs -mv hdfs_1/table_with_multiplexed_identifier hdfs_2/table_with_multiplexed_identifier &>> /home/hadoop/phase_2_move_unmodified_files.out
