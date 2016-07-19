set -e
sudo mkdir -p /var/log/hive/user/hadoop # Workaround for Hive logging bug
sudo chown hive:hive -R /var/log/hive
hive -f $1/phase_2_hive_file.q &>> /home/hadoop/phase_2_hive.out
hive -f $1/phase_2_other_hive_file.q &>> /home/hadoop/phase_2_hive.out
