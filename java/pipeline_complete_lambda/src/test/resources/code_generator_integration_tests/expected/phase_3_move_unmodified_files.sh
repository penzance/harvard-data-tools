if ! hadoop fs -test -e hdfs_3; then hadoop fs -mkdir hdfs_3; fi
set -e
sudo mkdir -p /var/log/hive/user/hadoop # Workaround for Hive logging bug
sudo chown hive:hive -R /var/log/hive
hadoop fs -mv hdfs_2/all_possible_column_types hdfs_3/all_possible_column_types &>> /home/hadoop/phase_3_move_unmodified_files.out
hadoop fs -mv hdfs_2/like_table hdfs_3/like_table &>> /home/hadoop/phase_3_move_unmodified_files.out
hadoop fs -mv hdfs_2/like_table_with_additions hdfs_3/like_table_with_additions &>> /home/hadoop/phase_3_move_unmodified_files.out
hadoop fs -mv hdfs_2/simple_table hdfs_3/simple_table &>> /home/hadoop/phase_3_move_unmodified_files.out
hadoop fs -mv hdfs_2/table_with_identifier hdfs_3/table_with_identifier &>> /home/hadoop/phase_3_move_unmodified_files.out
hadoop fs -mv hdfs_2/table_with_multiplexed_identifier hdfs_3/table_with_multiplexed_identifier &>> /home/hadoop/phase_3_move_unmodified_files.out
