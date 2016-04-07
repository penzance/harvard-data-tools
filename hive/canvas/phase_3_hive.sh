set -e
sudo mkdir -p /var/log/hive/user/hadoop # Workaround for Hive logging bug
sudo chown hive:hive -R /var/log/hive
