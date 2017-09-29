#!/usr/bin/python
# -*- coding: UTF-8 -*-

# Script Description:
#-----------------------------------------------------------------------------------------------------
# The script is used to check replication lags
# Author: Young Chen
# Date: 2017/09/27
#-----------------------------------------------------------------------------------------------------

import sys
import os
import time
import commands
import re
import pymysql
from pymysql.cursors import DictCursor
import traceback
reload(sys)
sys.setdefaultencoding('utf8')

colorTitle = "\x1B[{};{};{}m".format(4,31,1)
colorRed = "\x1B[{};{};{}m".format(0,31,1)
colorGray = "\x1B[{};{};{}m".format(0,32,1)
colorPurple = "\x1B[{};{};{}m".format(0,35,1)
colorcolumn = "\x1B[{};{};{}m".format(0,30,46)

os.system("clear")
print '\n{}{}\n\x1B[0m'.format(colorTitle,'Welcome to MySQL lag print tool')
print '\n{}{}\n\x1B[0m'.format(colorPurple,'Usage:')
print 'Step 1: Create user on both master and slave'
print ' ' * 8 + 'SQL: GRANT REPLICATION CLIENT ON *.* to lag_monitor@\'%\' IDENTIFIED BY \'lag_monitor\''
print 'Step 2: Run lag print tool by: python ./LagPrint.py [slaveHost] [slavePort] [masterHost] [masterPort] '
print ' ' * 8 + 'Sample: python ./LagPrint.py 127.0.0.1 11313 127.0.0.1 11312 \n\n'

if len(sys.argv) != 5 :
  exit(1)

slaveHost        = str(sys.argv[1])
slavePort        = int(sys.argv[2])
masterHost       = str(sys.argv[3])
masterPort       = int(sys.argv[4])
printIntival     = 5
Basename         = os.path.basename(sys.argv[0]).split('.')[0]
lastGap          = 0
listSpeed        = []
initFlag         = 0
slaveDB = {
      'host':slaveHost,
      'port':slavePort,
      'user':'lag_monitor',
      'password':'lag_monitor',
      'charset':'utf8',
      'cursorclass':pymysql.cursors.DictCursor,
      }

masterDB = {
      'host':masterHost,
      'port':masterPort,
      'user':'lag_monitor',
      'password':'lag_monitor',
      'charset':'utf8',
      'cursorclass':pymysql.cursors.DictCursor,
      }


try:
  slaveConn = pymysql.connect(**slaveDB)
  slaveCur  = slaveConn.cursor()
  masterConn = pymysql.connect(**masterDB)
  masterCur  = masterConn.cursor()
except Exception,e:
  print 'Connection error for - User:lag_monitor Password:lag_monitor InstanceSlave: {}:{} InstanceMaster:{}:{}'.format(slaveHost,slavePort,masterHost,masterPort)
  print e
  print '{}'.format(traceback.print_exc())
  exit(1)


#----------------------------------------------------------------------------------------------------------------
# Replication running check and SQL_DELAY mode check
#----------------------------------------------------------------------------------------------------------------
slaveSQL = "show slave status;"
statTrxSQL = " show global status where variable_name in('com_select','com_insert','com_delete','com_update');"
masterSQL = "show master logs;"
slaveCur.execute(slaveSQL)
slaveRes = slaveCur.fetchall()
if slaveRes[0]['Slave_IO_Running'] != 'Yes' or slaveRes[0]['Slave_SQL_Running'] != 'Yes':
  print 'Replication is stopped!'
  exit(2)

if slaveRes[0]['SQL_Delay'] != 0 :
  print '[NOTE] Delay replication is on, this may affect the result below !!!'


#print '-' * 70
printTitle =  '|' + 'DATETIME'.center(20,' ') \
            + '|' + 'LSN_GAP'.center(15,' ') \
            + '|' + 'EXEC_SPEED'.center(15,' ') \
            + '|' + 'CATCHUP_SPEED'.center(15,' ') \
            + '|' + 'ETA'.center(15,' ') \
            + '|' + 'SLAVE_QPS'.center(15,' ') \
            + '|' + 'SLAVE_TPS'.center(15,' ') \
            + '|'

while True:
  try:
    if initFlag % 30 == 0:
      print '{}{}\x1B[0m'.format(colorcolumn,printTitle)
    #----------------------------------------------------------------------------------------------------------------
    # Get slave status
    #----------------------------------------------------------------------------------------------------------------
    slaveCur.execute(slaveSQL)
    slaveRes = slaveCur.fetchall()
    relayLogFile = slaveRes[0]['Relay_Master_Log_File']
    relayExecPos = slaveRes[0]['Exec_Master_Log_Pos']

    slaveCur.execute(statTrxSQL)
    trxRes = slaveCur.fetchall()
    qpsNum = 0
    tpsNum = 0
    for i in range(len(trxRes)):
      if trxRes[i]['Variable_name'] in ('Com_delete','Com_insert','Com_update'):
        tpsNum += int(trxRes[i]['Value'])
      else:
        qpsNum = int(trxRes[i]['Value'])

    #print  '{} | {}'.format(relayLogFile,relayExecPos)

    #----------------------------------------------------------------------------------------------------------------
    # Get master status
    #----------------------------------------------------------------------------------------------------------------
    masterCur.execute(masterSQL)
    masterRes = masterCur.fetchall()
    restEvent = 0
    idxLogFile = 99999

    #----------------------------------------------------------------------------------------------------------------
    # Figure out result
    #----------------------------------------------------------------------------------------------------------------
    for i in range(len(masterRes)):
      if i > idxLogFile:
        restEvent += masterRes[i]['File_size']
      elif masterRes[i]['Log_name'] == relayLogFile:
        currentMaxPos = masterRes[i]['File_size']
        idxLogFile = i
      elif i == (len(masterRes) -1 ):
        print 'ERROR: Log gap is too huge and master logs has been removed from master server!!!'
        print 'ERROR: It is better to make a full backup on master and load into slave,then reset a new replication.'
        slaveCur.close()
        slaveConn.close()
        masterCur.close()
        masterConn.close()
        exit(1)
    #print '{}|{}|{}'.format(restEvent,currentMaxPos,relayExecPos)
    eventGap = restEvent + currentMaxPos - relayExecPos
    if initFlag != 0:
      if (relayExecPos - lastExecPos) > -1:
        execSpeed = (relayExecPos - lastExecPos) / printIntival
      else:
        execSpeed = 'Switch Log File'
      catchUpSpeed = (lastGap - eventGap) / printIntival
      listSpeed.append(catchUpSpeed)
      listSpeed = listSpeed[-10:]
      avgSpeed = reduce(lambda x,y: x+y, listSpeed) / len(listSpeed)
      if eventGap == 0 :
        eta = 'Now'
      elif catchUpSpeed < 1:
        eta = 'Forever'
      else:
        eta = str(eventGap / avgSpeed ) + ' Seconds'
      qpsAvgIn5 = (qpsNum - lastQpsNum) / printIntival
      tpsAvgIn5 = (tpsNum - lastTpsNum) / printIntival
    else:
      execSpeed = '-'
      catchUpSpeed = '-'
      eta = '-'
      qpsAvgIn5 = '-'
      tpsAvgIn5 = '-'

    print '|' + time.strftime("%Y/%m/%d %H:%M:%S").center(20,' ') \
        + '|' + str(eventGap).center(15,' ') \
        + '|' + str(execSpeed).center(15,' ') \
        + '|' + str(catchUpSpeed).center(15,' ') \
        + '|' + str(eta).center(15,' ') \
        + '|' + str(qpsAvgIn5).center(15,' ') \
        + '|' + str(tpsAvgIn5).center(15,' ') \
        + '|' #+ '{}|{}|{}'.format(restEvent,currentMaxPos,relayExecPos)
    lastGap = eventGap
    lastExecPos = relayExecPos
    lastQpsNum = qpsNum
    lastTpsNum = tpsNum
    initFlag += 1
    time.sleep(printIntival)
  except KeyboardInterrupt:  
    slaveCur.close()
    slaveConn.close()
    masterCur.close()
    masterConn.close()
    print '\nHave a nice day!'
    exit(0)