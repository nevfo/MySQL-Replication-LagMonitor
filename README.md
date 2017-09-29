# MySQL-Replication-LagMonitor
Some useful MySQL DBA Tools

#Description
The script is used to check replication lags, replication speed, and ETA of catup up

#Usage
Step 1: Create user on both master and slave
   SQL: GRANT REPLICATION CLIENT ON *.* to lag_monitor@'%' IDENTIFIED BY 'lag_monitor';
Step 2: Run lag print tool by: python ./LagPrint.py [slaveHost] [slavePort] [masterHost] [masterPort]
   Sample: python ./LagPrint.py 127.0.0.1 3306 127.0.0.1 3316
