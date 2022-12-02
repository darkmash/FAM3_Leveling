import datetime
import glob
import logging
from logging.handlers import RotatingFileHandler
import os
import re
import math
import time
import numpy as np
import openpyxl as xl
from PyQt5 import QtCore, QtWidgets, QtGui
from PyQt5.QtCore import Qt, QCoreApplication
from PyQt5.QtGui import QDoubleValidator, QStandardItemModel, QIcon, QStandardItem, QIntValidator, QFont
from PyQt5.QtWidgets import QMainWindow, QMessageBox, QProgressBar, QPlainTextEdit, QWidget, QGridLayout, QGroupBox, QLineEdit, QSizePolicy, QToolButton, QLabel, QFrame, QListView, QMenuBar, QStatusBar, QPushButton, QCalendarWidget, QVBoxLayout, QFileDialog, QComboBox
from PyQt5.QtCore import pyqtSlot, pyqtSignal, QObject, QThread, QRect, QSize, QDate, QThreadPool
import pandas as pd
import cx_Oracle
import random

class MainClass(QObject):
    mainReturnError = pyqtSignal(Exception)
    mainReturnInfo = pyqtSignal(str)
    mainReturnWarning = pyqtSignal(str)
    mainReturnEnd = pyqtSignal(bool)

    def __init__(self, 
                debugFlag,
                debugDate,
                cb_main,
                list_masterFile, 
                maxCnt, 
                emgHoldList):
        super().__init__()
        self.isDebug = debugFlag
        self.debugDate = debugDate
        self.cb_main = cb_main
        self.list_masterFile = list_masterFile
        self.maxCnt = maxCnt
        self.emgHoldList = emgHoldList

    #워킹데이 체크 내부함수
    def checkWorkDay(self, df, today, compDate):
        dtToday = pd.to_datetime(datetime.datetime.strptime(today, '%Y%m%d'))
        dtComp = pd.to_datetime(compDate, unit='s')
        workDay = 0
        index = int(df.index[(df['Date']==dtComp)].tolist()[0])
        while dtToday > pd.to_datetime(df['Date'][index], unit='s'):
            if df['WorkingDay'][index] == 1:
                workDay -= 1    
            index += 1

        for i in df.index:
            dt = pd.to_datetime(df['Date'][i], unit='s')
            if dtToday < dt and dt <= dtComp:
                if df['WorkingDay'][i] == 1:
                    workDay += 1
        return workDay
    #콤마 삭제용 내부함수
    def delComma(self, value):
        return str(value).split('.')[0]

    #디비 불러오기 공통내부함수
    def readDB(self, ip, port, sid, userName, password, sql):
        location = r'.\\instantclient_21_6'
        os.environ["PATH"] = location + ";" + os.environ["PATH"]
        dsn = cx_Oracle.makedsn(ip, port, sid)
        db = cx_Oracle.connect(userName, password, dsn)
        cursor= db.cursor()
        cursor.execute(sql)
        out_data = cursor.fetchall()
        df_oracle = pd.DataFrame(out_data)
        col_names = [row[0] for row in cursor.description]
        df_oracle.columns = col_names
        return df_oracle

    #생산시간 합계용 내부함수
    def getSec(self, time_str):
        time_str = re.sub(r'[^0-9:]', '', str(time_str))
        if len(time_str) > 0:
            h, m, s = time_str.split(':')
            return int(h) * 3600 + int(m) * 60 + int(s)
        else:
            return 0
    #백슬래쉬 삭제용 내부함수
    def delBackslash(self, value):
        value = re.sub(r"\\c", "", str(value))
        return value

    #알람 상세 누적 기록용 내부함수
    def concatAlarmDetail(self, df_target, 
                            no, 
                            category, 
                            df_data, 
                            index, 
                            smtAssy, 
                            shortageCnt):
        """
        Args:
            df_target(DataFrame)    : 알람상세내역 DataFrame
            no(int)                 : 알람 번호
            category(str)           : 알람 분류
            df_data(DataFrame)      : 원본 DataFrame
            index(int)              : 원본 DataFrame의 인덱스
            smtAssy(str)            : Smt Assy 이름
            shortageCnt(int)        : 부족 수량
        Return:
            return(DataFrame)       : 알람상세 Merge결과 DataFrame 
        """
        if category == '1':
            return pd.concat([df_target, 
                                pd.DataFrame.from_records([{"No.":no,
                                                            "분류" : category,
                                                            "L/N" : df_data['Linkage Number'][index],
                                                            "MS CODE" : df_data['MS Code'][index], 
                                                            "SMT ASSY" : smtAssy, 
                                                            "수주수량" : df_data['미착공수주잔'][index], 
                                                            "부족수량" : shortageCnt, 
                                                            "검사호기" : '-', 
                                                            "대상 검사시간(초)" : 0, 
                                                            "필요시간(초)" : 0, 
                                                            "완성예정일" : df_data['Planned Prod. Completion date'][index]}
                                                            ])])
        elif category == '2':
            return pd.concat([df_target, 
                                pd.DataFrame.from_records([{"No.":no,
                                                            "분류" : category,
                                                            "L/N" : df_data['Linkage Number'][index],
                                                            "MS CODE" : df_data['MS Code'][index], 
                                                            "SMT ASSY" : '-', 
                                                            "수주수량" : df_data['미착공수주잔'][index], 
                                                            "부족수량" : shortageCnt, 
                                                            "검사호기" : df_data['INSPECTION_EQUIPMENT'][index], 
                                                            "대상 검사시간(초)" : df_data['TotalTime'][index], 
                                                            "필요시간(초)" : (df_data['미착공수주잔'][index] - df_data['설비능력반영_착공량'][index]) * df_data['TotalTime'][index], 
                                                            "완성예정일" : df_data['Planned Prod. Completion date'][index]}
                                                            ])])
        elif category == '기타1':
            return pd.concat([df_target, 
                                pd.DataFrame.from_records([{"No.":no,
                                                            "분류" : category,
                                                            "L/N" : df_data['Linkage Number'][index],
                                                            "MS CODE" : df_data['MS Code'][index], 
                                                            "SMT ASSY" : '미등록', 
                                                            "수주수량" : df_data['미착공수주잔'][index], 
                                                            "부족수량" : 0, 
                                                            "검사호기" : '-', 
                                                            "대상 검사시간(초)" : 0, 
                                                            "필요시간(초)" : 0, 
                                                            "완성예정일" : df_data['Planned Prod. Completion date'][index]}
                                                            ])])
        elif category == '기타2':
            return pd.concat([df_target, 
                                pd.DataFrame.from_records([{"No.":no,
                                                            "분류" : category,
                                                            "L/N" : df_data['Linkage Number'][index],
                                                            "MS CODE" : df_data['MS Code'][index], 
                                                            "SMT ASSY" : '-', 
                                                            "수주수량" : df_data['미착공수주잔'][index], 
                                                            "부족수량" : 0, 
                                                            "검사호기" : '-', 
                                                            "대상 검사시간(초)" : 0, 
                                                            "필요시간(초)" : 0, 
                                                            "완성예정일" : df_data['Planned Prod. Completion date'][index]}
                                                            ])])   
        elif category == '기타3':
            return pd.concat([df_target, 
                                pd.DataFrame.from_records([{"No.":no,
                                                            "분류" : category,
                                                            "L/N" : df_data['Linkage Number'][index],
                                                            "MS CODE" : df_data['MS Code'][index], 
                                                            "SMT ASSY" : smtAssy, 
                                                            "수주수량" : df_data['미착공수주잔'][index], 
                                                            "부족수량" : 0, 
                                                            "검사호기" : '-', 
                                                            "대상 검사시간(초)" : 0, 
                                                            "필요시간(초)" : 0, 
                                                            "완성예정일" : df_data['Planned Prod. Completion date'][index]}
                                                            ])])    
    def smtReflectInst(self, 
                        df_input, 
                        isRemain, 
                        dict_smtCnt, 
                        alarmDetailNo, 
                        df_alarmDetail, 
                        rowNo):
        """
        Args:
            df_input(DataFrame)         : 입력 DataFrame 
            isRemain(Bool)              : 잔여착공 여부 Flag
            dict_smtCnt(Dict)           : Smt잔여량 Dict
            alarmDetailNo(int)          : 알람 번호
            df_alarmDetail(DataFrame)   : 알람 상세 기록용 DataFrame
            rowNo(int)                  : 사용 Smt Assy 갯수
        Return:
            return(List)                 
                df_input(DataFrame)         : 입력 DataFrame (갱신 후)
                dict_smtCnt(Dict)           : Smt잔여량 Dict (갱신 후)
                alarmDetailNo(int)          : 알람 번호
                df_alarmDetail(DataFrame)   : 알람 상세 기록용 DataFrame (갱신 후)
        """
        instCol = '평준화_적용_착공량'
        resultCol = 'SMT반영_착공량'
        if isRemain:
            instCol = '잔여_착공량'
            resultCol = 'SMT반영_착공량_잔여'
        for i in df_input.index:
            for j in range(1,rowNo):
                if j == 1:
                    rowCnt = 1
                if str(df_input[f'ROW{str(j)}'][i]) != '' and str(df_input[f'ROW{str(j)}'][i]) != 'nan':
                    rowCnt = j
                else:
                    break
            minCnt = 9999
            for j in range(1,rowCnt+1):
                    smtAssyName = str(df_input[f'ROW{str(j)}'][i])
                    if df_input['SMT_MS_CODE'][i] != 'nan' and df_input['SMT_MS_CODE'][i] != 'None' and df_input['SMT_MS_CODE'][i] != '':
                        if smtAssyName != '' and smtAssyName != 'nan' and smtAssyName != 'None' :
                            if df_input['긴급오더'][i] == '대상' or df_input['당일착공'][i] == '대상':
                                if smtAssyName in dict_smtCnt:
                                    if dict_smtCnt[smtAssyName] < 0:
                                        # dict_smtCnt[smtAssyName] = 0
                                        diffCnt = df_input['미착공수주잔'][i]
                                        if dict_smtCnt[smtAssyName] + df_input['미착공수주잔'][i] > 0:
                                            diffCnt = 0 - dict_smtCnt[smtAssyName]
                                        if not isRemain:
                                            if dict_smtCnt[smtAssyName] > 0:
                                                df_alarmDetail = self.concatAlarmDetail(df_alarmDetail,
                                                                                        alarmDetailNo,
                                                                                        '1', 
                                                                                        df_input,
                                                                                        i, 
                                                                                        smtAssyName, 
                                                                                        diffCnt)
                                                alarmDetailNo += 1
                                else:
                                    df_alarmDetail = self.concatAlarmDetail(df_alarmDetail,
                                                                            alarmDetailNo,
                                                                            '기타3', 
                                                                            df_input,
                                                                            i, 
                                                                            smtAssyName, 
                                                                            0)
                                    alarmDetailNo += 1  
                            else:
                                if smtAssyName in dict_smtCnt:
                                    if dict_smtCnt[smtAssyName] >= df_input[instCol][i]:
                                        if minCnt > df_input[instCol][i]:
                                            minCnt = df_input[instCol][i]
                                    else: 
                                        if dict_smtCnt[smtAssyName] > 0:
                                            if minCnt > dict_smtCnt[smtAssyName]:
                                                minCnt = dict_smtCnt[smtAssyName]

                                        else:
                                            minCnt = 0
                                        if not isRemain:
                                            if dict_smtCnt[smtAssyName] > 0:
                                                df_alarmDetail = self.concatAlarmDetail(df_alarmDetail,
                                                                                        alarmDetailNo,
                                                                                        '1', 
                                                                                        df_input,
                                                                                        i, 
                                                                                        smtAssyName, 
                                                                                        df_input[instCol][i] - dict_smtCnt[smtAssyName])
                                                alarmDetailNo += 1
                                else:
                                    df_alarmDetail = self.concatAlarmDetail(df_alarmDetail,
                                                                            alarmDetailNo,
                                                                            '기타3', 
                                                                            df_input,
                                                                            i, 
                                                                            smtAssyName, 
                                                                            0)
                                    alarmDetailNo += 1  
                    else:
                        df_alarmDetail = self.concatAlarmDetail(df_alarmDetail,
                                                                    alarmDetailNo,
                                                                    '기타1', 
                                                                    df_input,
                                                                    i, 
                                                                    '미등록', 
                                                                    0)
                        alarmDetailNo += 1
            if minCnt != 9999:
                df_input[resultCol][i] = minCnt
            else:
                df_input[resultCol][i] = df_input[instCol][i]

            for j in range(1,rowCnt+1):
                if smtAssyName != '' and smtAssyName != 'nan' and smtAssyName != 'None':
                    smtAssyName = str(df_input[f'ROW{str(j)}'][i])
                    dict_smtCnt[smtAssyName] -= df_input[resultCol][i]
        return [df_input, dict_smtCnt, alarmDetailNo, df_alarmDetail]
    def ateReflectInst(self, 
                        df_input, 
                        isRemain, 
                        dict_ate, 
                        df_alarmDetail, 
                        alarmDetailNo, 
                        maxCnt):
        """
        Args:
            df_input(DataFrame)         : 입력 DataFrame 
            isRemain(Bool)              : 잔여착공 여부 Flag
            dict_ate(Dict)              : 잔여 검사설비능력 Dict
            alarmDetailNo(int)          : 알람 번호
            df_alarmDetail(DataFrame)   : 알람 상세 기록용 DataFrame
            maxCnt(int)                 : 최대착공량
        Return:
            return(List)                 
                df_input(DataFrame)         : 입력 DataFrame (갱신 후)
                dict_ate(Dict)              : 잔여 검사설비능력 Dict (갱신 후)
                alarmDetailNo(int)          : 알람 번호
                df_alarmDetail(DataFrame)   : 알람 상세 기록용 DataFrame (갱신 후)
                maxCnt(int)                 : 최대착공량 (갱신 후)
        """
        if isRemain:
            smtReflectCnt = 'SMT반영_착공량_잔여'
            tempAteCnt =  '임시수량_잔여'
            ateReflectCnt = '설비능력반영_착공량_잔여'
        else:
            smtReflectCnt = 'SMT반영_착공량'
            tempAteCnt =  '임시수량'
            ateReflectCnt = '설비능력반영_착공량'
        for i in df_input.index:
            if str(df_input['TotalTime'][i]) != '' and str(df_input['TotalTime'][i]) != 'nan':
                if str(df_input['INSPECTION_EQUIPMENT'][i]) != '' and str(df_input['INSPECTION_EQUIPMENT'][i]) != 'nan':
                    tempTime = 0
                    ateName = ''
                    #긴급오더 or 당일착공 대상은 검사설비 능력이 부족하여도 강제 착공. 그리고 알람을 기록
                    if df_input['긴급오더'][i] == '대상' or df_input['당일착공'][i] == '대상':
                        for ate in df_input['INSPECTION_EQUIPMENT'][i]:
                            if maxCnt >= 0 :
                                tempTime = dict_ate[ate]
                                ateName = ate
                                if ate == df_input['INSPECTION_EQUIPMENT'][i][0]:
                                    df_input[tempAteCnt][i] = df_input[smtReflectCnt][i]
                                if df_input[tempAteCnt][i] != 0:
                                    if not (dict_ate[ateName] >= df_input['TotalTime'][i] * df_input[tempAteCnt][i]) and not (dict_ate[ateName] >= df_input['TotalTime'][i]):
                                        df_alarmDetail = self.concatAlarmDetail(df_alarmDetail, 
                                                                                alarmDetailNo, 
                                                                                '2', 
                                                                                df_input, 
                                                                                i, 
                                                                                '-', 
                                                                                df_input['미착공수주잔'][i] - df_input[ateReflectCnt][i])
                                        alarmDetailNo += 1
                                    dict_ate[ateName] -= df_input['TotalTime'][i] * df_input[tempAteCnt][i]
                                    df_input[ateReflectCnt][i] += df_input[tempAteCnt][i]
                                    maxCnt -= df_input[tempAteCnt][i]
                                    df_input[tempAteCnt][i] = 0
                                    break
                                else:
                                    break
                        if maxCnt < 0:        
                            df_alarmDetail = self.concatAlarmDetail(df_alarmDetail,
                                                                    alarmDetailNo,
                                                                    '기타2', 
                                                                    df_input,
                                                                    i, 
                                                                    '-', 
                                                                    0)
                            alarmDetailNo += 1
                            break
                    #긴급오더 or 당일착공이 아닌 경우는 검사설비 능력을 반영하여 착공 실시
                    else:
                        if maxCnt < 0:
                            maxCnt = 0
                        for ate in df_input['INSPECTION_EQUIPMENT'][i]:
                            if tempTime < dict_ate[ate]:
                                tempTime = dict_ate[ate]
                                ateName = ate
                                if ate == df_input['INSPECTION_EQUIPMENT'][i][0]:
                                    df_input[tempAteCnt][i] = df_input[smtReflectCnt][i]
                                if df_input[tempAteCnt][i] != 0:
                                    if dict_ate[ateName] >= df_input['TotalTime'][i] * df_input[tempAteCnt][i]:
                                        if maxCnt >= df_input[tempAteCnt][i]:
                                            dict_ate[ateName] -= df_input['TotalTime'][i] * df_input[tempAteCnt][i]
                                            df_input[ateReflectCnt][i] += df_input[tempAteCnt][i]
                                            maxCnt -= df_input[tempAteCnt][i]
                                            df_input[tempAteCnt][i] = 0
                                            break
                                        else:
                                            dict_ate[ateName] -= df_input['TotalTime'][i] * maxCnt
                                            df_input[ateReflectCnt][i] += maxCnt
                                            df_input[tempAteCnt][i] -= maxCnt
                                            maxCnt = 0
                                            break
                                    elif dict_ate[ateName] >= df_input['TotalTime'][i]:
                                        tempCnt = int(df_input[tempAteCnt][i])
                                        for j in range(tempCnt,0,-1):
                                            if dict_ate[ateName] >= int(df_input['TotalTime'][i]) * j:
                                                if maxCnt >= j:
                                                    df_input[ateReflectCnt][i] = int(df_input[ateReflectCnt][i]) + j
                                                    dict_ate[ateName] -= int(df_input['TotalTime'][i]) * j
                                                    df_input[tempAteCnt][i] = tempCnt - j
                                                    maxCnt -= j
                                                    break
                                else:
                                    break
        return [df_input, dict_ate, alarmDetailNo, df_alarmDetail, maxCnt]
    def run(self):
        #pandas 경고없애기 옵션 적용
        pd.set_option('mode.chained_assignment', None)
        try:
            #긴급오더, 홀딩오더 불러오기
            emgLinkage = self.emgHoldList[0]
            emgmscode = self.emgHoldList[1]
            holdLinkage = self.emgHoldList[2]
            holdmscode = self.emgHoldList[3]
            #긴급오더, 홀딩오더 데이터프레임화
            df_emgLinkage = pd.DataFrame({'Linkage Number':emgLinkage})
            df_emgmscode = pd.DataFrame({'MS Code':emgmscode})
            df_holdLinkage = pd.DataFrame({'Linkage Number':holdLinkage})
            df_holdmscode = pd.DataFrame({'MS Code':holdmscode})
            #각 Linkage Number 컬럼의 타입을 일치시킴
            df_emgLinkage['Linkage Number'] = df_emgLinkage['Linkage Number'].astype(np.int64)
            df_holdLinkage['Linkage Number'] = df_holdLinkage['Linkage Number'].astype(np.int64)
            #긴급오더, 홍딩오더 Join 전 컬럼 추가
            df_emgLinkage['긴급오더'] = '대상'
            df_emgmscode['긴급오더'] = '대상'
            df_holdLinkage['홀딩오더'] = '대상'
            df_holdmscode['홀딩오더'] = '대상'
            #레벨링 리스트 불러오기(멀티프로세싱 적용 후, 분리 예정)
            df_levelingMain = pd.read_excel(self.list_masterFile[1])
            #미착공 대상만 추출(Main)
            df_levelingMainDropSEQ = df_levelingMain[df_levelingMain['Sequence No'].isnull()]
            df_levelingMainUndepSeq = df_levelingMain[df_levelingMain['Sequence No']=='Undep']
            df_levelingMainUncorSeq = df_levelingMain[df_levelingMain['Sequence No']=='Uncor']
            df_levelingMain = pd.concat([df_levelingMainDropSEQ, df_levelingMainUndepSeq, df_levelingMainUncorSeq])
            df_levelingMain['Linkage Number'] = df_levelingMain['Linkage Number'].astype(str)
            df_levelingMain = df_levelingMain.reset_index(drop=True)
            df_levelingMain['미착공수주잔'] = df_levelingMain.groupby('Linkage Number')['Linkage Number'].transform('size')
            # if self.isDebug:
            #     df_levelingMain.to_excel('.\\debug\\Main\\flow1_main.xlsx')
            # if self.isDebug:
            #     df_levelingMain.to_excel('.\\debug\\Main\\flow1.xlsx')
            df_sosFile = pd.read_excel(self.list_masterFile[0])
            df_sosFile['Linkage Number'] = df_sosFile['Linkage Number'].astype(str)
            if self.isDebug:
                df_sosFile.to_excel('.\\debug\\Main\\flow2.xlsx')
            #착공 대상 외 모델 삭제
            df_sosFile = df_sosFile.drop(df_sosFile[df_sosFile['MS Code'].str.contains('ZOTHER')].index)
            df_sosFile = df_sosFile.drop(df_sosFile[df_sosFile['MS Code'].str.contains('YZ')].index)
            df_sosFile = df_sosFile.drop(df_sosFile[df_sosFile['MS Code'].str.contains('SF')].index)
            df_sosFile = df_sosFile.drop(df_sosFile[df_sosFile['MS Code'].str.contains('KM')].index)
            df_sosFile = df_sosFile.drop(df_sosFile[df_sosFile['MS Code'].str.contains('TA80')].index)
            df_sosFile = df_sosFile.drop(df_sosFile[df_sosFile['MS Code'].str.contains('CT')].index)
            if self.isDebug:
                df_sosFile.to_excel('.\\debug\\Main\\flow3.xlsx')
            #워킹데이 캘린더 불러오기
            dfCalendar = pd.read_excel(self.list_masterFile[4])
            today = datetime.datetime.today().strftime('%Y%m%d')
            if self.isDebug:
                today = self.debugDate
            #진척 파일 - SOS2파일 Join
            df_sosFileMerge = pd.merge(df_sosFile, df_levelingMain).drop_duplicates(['Linkage Number'])
            df_sosFileMerge = df_sosFileMerge[['Linkage Number',
                                                'MS Code',
                                                'Planned Prod. Completion date',
                                                'Order Quantity',
                                                '미착공수주잔']]
            #미착공수주잔이 없는 데이터는 불요이므로 삭제
            df_sosFileMerge = df_sosFileMerge[df_sosFileMerge['미착공수주잔'] != 0]
            #위 파일을 완성지정일 기준 오름차순 정렬 및 인덱스 재설정
            df_sosFileMerge = df_sosFileMerge.sort_values(by=['Planned Prod. Completion date'],
                                                            ascending=[True])
            df_sosFileMerge = df_sosFileMerge.reset_index(drop=True)
            #대표모델 Column 생성
            df_sosFileMerge['대표모델'] = df_sosFileMerge['MS Code'].str[:9]
            #남은 워킹데이 Column 생성
            df_sosFileMerge['남은 워킹데이'] = 0
            #긴급오더, 홀딩오더 Linkage Number Column 타입 일치
            df_emgLinkage['Linkage Number'] = df_emgLinkage['Linkage Number'].astype(str)
            df_holdLinkage['Linkage Number'] = df_holdLinkage['Linkage Number'].astype(str)
            #긴급오더, 홀딩오더와 위 Sos파일을 Join
            df_MergeLink = pd.merge(df_sosFileMerge, df_emgLinkage, on='Linkage Number', how='left')
            dfMergemscode = pd.merge(df_sosFileMerge, df_emgmscode, on='MS Code', how='left')
            df_MergeLink = pd.merge(df_MergeLink, df_holdLinkage, on='Linkage Number', how='left')
            dfMergemscode = pd.merge(dfMergemscode, df_holdmscode, on='MS Code', how='left')
            df_MergeLink['긴급오더'] = df_MergeLink['긴급오더'].combine_first(dfMergemscode['긴급오더'])
            df_MergeLink['홀딩오더'] = df_MergeLink['홀딩오더'].combine_first(dfMergemscode['홀딩오더'])
            df_MergeLink['당일착공'] = ''
            #남은 워킹데이 체크 및 컬럼 추가
            for i in df_MergeLink.index:
                df_MergeLink['남은 워킹데이'][i] = self.checkWorkDay(dfCalendar, today, df_MergeLink['Planned Prod. Completion date'][i])
                if df_MergeLink['남은 워킹데이'][i] < 0:
                    df_MergeLink['긴급오더'][i] = '대상'
                elif df_MergeLink['남은 워킹데이'][i] == 0:
                    df_MergeLink['당일착공'][i] = '대상'
            df_MergeLink['Linkage Number'] = df_MergeLink['Linkage Number'].astype(str)
            if self.isDebug:
                df_MergeLink.to_excel('.\\debug\\Main\\flow4.xlsx')
            #프로그램 기동날짜의 전일을 계산 (Debug시에는 디버그용 LineEdit에 기록된 날짜를 사용)
            yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y%m%d')
            if self.isDebug:
                yesterday = (datetime.datetime.strptime(self.debugDate,'%Y%m%d') - datetime.timedelta(days=1)).strftime('%Y%m%d')
            #해당 날짜의 Smt Assy 남은 대수 확인
            df_SmtAssyInven = self.readDB('10.36.15.42',
                                            1521,
                                            'NEURON',
                                            'ymi_user',
                                            'ymi123!',
                                            "SELECT INV_D, PARTS_NO, CURRENT_INV_QTY FROM pdsg0040 where INV_D = TO_DATE("+ str(yesterday) +",'YYYYMMDD')")
            df_SmtAssyInven['현재수량'] = 0
            if self.isDebug:
                df_SmtAssyInven.to_excel('.\\debug\\Main\\flow5.xlsx')
            #2차 메인피킹 리스트 불러오기 및 Smt Assy 재고량 Df와 Join
            df_secOrderMainList = pd.read_excel(self.list_masterFile[6], skiprows=5)
            df_joinSmt = pd.merge(df_secOrderMainList, df_SmtAssyInven, how = 'right', left_on='ASSY NO', right_on='PARTS_NO')
            df_joinSmt['대수'] = df_joinSmt['대수'].fillna(0)
            #Smt Assy 현재 재고량에서 사용량 차감
            df_joinSmt['현재수량'] = df_joinSmt['CURRENT_INV_QTY'] - df_joinSmt['대수']
            df_joinSmt.to_excel('.\\debug\\Main\\flow6.xlsx')
            dict_smtCnt = {}
            #Smt Assy 재고량을 PARTS_NO를 Key로 Dict화
            for i in df_joinSmt.index:
                if df_joinSmt['현재수량'][i] < 0 :
                    df_joinSmt['현재수량'][i] = 0
                dict_smtCnt[df_joinSmt['PARTS_NO'][i]] = df_joinSmt['현재수량'][i]
            #검사시간DB를 가져옴(공수계산PRG용 DB)
            df_productTime = self.readDB('ymzn-bdv19az029-rds.cgbtxsdj6fjy.ap-northeast-1.rds.amazonaws.com',
                                            1521,
                                            'TPROD',
                                            'TEST_SCM',
                                            'test_scm',
                                            'SELECT * FROM FAM3_PRODUCT_TIME_TB')
            #전체 생산시간을 계산
            df_productTime['TotalTime'] = df_productTime['COMPONENT_SET'].apply(self.getSec) + df_productTime['MAEDZUKE'].apply(self.getSec) + df_productTime['MAUNT'].apply(self.getSec) + df_productTime['LEAD_CUTTING'].apply(self.getSec) + df_productTime['VISUAL_EXAMINATION'].apply(self.getSec) + df_productTime['PICKUP'].apply(self.getSec) + df_productTime['ASSAMBLY'].apply(self.getSec) + df_productTime['M_FUNCTION_CHECK'].apply(self.getSec) + df_productTime['A_FUNCTION_CHECK'].apply(self.getSec) + df_productTime['PERSON_EXAMINE'].apply(self.getSec)
            #대표모델 컬럼생성 및 중복 제거
            df_productTime['대표모델'] = df_productTime['MODEL'].str[:9]
            df_productTime = df_productTime.drop_duplicates(['대표모델'])
            df_productTime = df_productTime.reset_index(drop=True)
            if self.isDebug:
                df_productTime.to_excel('.\\debug\\Main\\flow7.xlsx')
            #DB로부터 메인라인의 MSCode별 사용 Smt Assy 가져옴
            df_pdbs = self.readDB('10.36.15.42',
                                    1521,
                                    'neuron',
                                    'ymfk_user',
                                    'ymfk_user',
                                    "SELECT SMT_MS_CODE, SMT_SMT_ASSY, SMT_CRP_GR_NO FROM sap.pdbs0010 WHERE SMT_CRP_GR_NO = '100L1311'" )
            #불필요한 데이터 삭제
            df_pdbs = df_pdbs[~df_pdbs['SMT_MS_CODE'].str.contains('AST')]
            df_pdbs = df_pdbs[~df_pdbs['SMT_MS_CODE'].str.contains('BMS')]
            df_pdbs = df_pdbs[~df_pdbs['SMT_MS_CODE'].str.contains('WEB')]
            if self.isDebug:
                df_pdbs.to_excel('.\\debug\\Main\\flow7-1.xlsx')
            #사용 Smt Assy를 병렬화
            gb = df_pdbs.groupby('SMT_MS_CODE')
            df_temp = pd.DataFrame([df_pdbs.loc[gb.groups[n], 'SMT_SMT_ASSY'].values for n in gb.groups], index=gb.groups.keys())
            df_temp.columns = ['ROW'+ str(i +1) for i in df_temp.columns]
            rowNo = len(df_temp.columns)
            df_temp = df_temp.reset_index()
            df_temp.rename(columns={'index':'SMT_MS_CODE'}, inplace=True)
            if self.isDebug:
                df_temp.to_excel('.\\debug\\Main\\flow7-2.xlsx')
            #검사설비를 List화
            df_ATEList = df_productTime.copy()
            df_ATEList = df_ATEList.drop_duplicates(['INSPECTION_EQUIPMENT'])
            df_ATEList = df_ATEList.reset_index(drop=True)
            df_ATEList['INSPECTION_EQUIPMENT'] = df_ATEList['INSPECTION_EQUIPMENT'].apply(self.delBackslash)
            df_ATEList['INSPECTION_EQUIPMENT'] = df_ATEList['INSPECTION_EQUIPMENT'].str.strip()
            df_productTime['INSPECTION_EQUIPMENT'] = df_productTime['INSPECTION_EQUIPMENT'].apply(self.delBackslash)
            df_productTime['INSPECTION_EQUIPMENT'] = df_productTime['INSPECTION_EQUIPMENT'].str.strip()
            if self.isDebug:
                df_ATEList.to_excel('.\\debug\\Main\\flow8.xlsx')
            dict_ate = {}
            max_ateCnt = 0
            overTime = 0
            #잔업에 대한 체크
            if self.cb_main == '잔업없음':
                overTime = 0
            else:
                overTime = int(re.sub(r'[^0-9]', '', str(self.cb_main)))
            #각 검사설비를 Key로 검사시간을 Dict화 (잔업까지 적용)
            for i in df_ATEList.index:
                if max_ateCnt < len(str(df_ATEList['INSPECTION_EQUIPMENT'][i])):
                    max_ateCnt = len(str(df_ATEList['INSPECTION_EQUIPMENT'][i]))
                for j in df_ATEList['INSPECTION_EQUIPMENT'][i]:
                    if overTime == 0:
                        dict_ate[j] = 460 * 60
                    else:
                        dict_ate[j] = (460+(60*overTime)) * 60
            #대표모델 별 검사시간 및 검사설비를 Join
            df_sosAddMainModel = pd.merge(df_MergeLink, df_productTime[['대표모델','TotalTime','INSPECTION_EQUIPMENT']], on='대표모델', how='left')
            df_sosAddMainModel = df_sosAddMainModel[~df_sosAddMainModel['INSPECTION_EQUIPMENT'].str.contains('None')]
            #모델별 사용 Smt Assy를 Join
            df_addSmtAssy = pd.merge(df_sosAddMainModel, df_temp, left_on='MS Code',right_on='SMT_MS_CODE', how='left')
            df_addSmtAssy = df_addSmtAssy.reset_index(drop=True)
            if self.isDebug:
                df_addSmtAssy.to_excel('.\\debug\\Main\\flow8-2.xlsx')
            df_addSmtAssy['대표모델별_최소착공필요량_per_일'] = 0
            dict_integCnt = {}
            dict_minContCnt = {}
            #대표모델 별 최소 착공 필요량을 계산
            for i in df_addSmtAssy.index:
                if df_addSmtAssy['대표모델'][i] in dict_integCnt:
                    dict_integCnt[df_addSmtAssy['대표모델'][i]] += int(df_addSmtAssy['미착공수주잔'][i])
                else:
                    dict_integCnt[df_addSmtAssy['대표모델'][i]] = int(df_addSmtAssy['미착공수주잔'][i])
                if df_addSmtAssy['남은 워킹데이'][i] == 0:
                    workDay = 1
                else:
                    workDay = df_addSmtAssy['남은 워킹데이'][i]
                
                if len(dict_minContCnt) > 0:
                    if df_addSmtAssy['대표모델'][i] in dict_minContCnt:
                        if dict_minContCnt[df_addSmtAssy['대표모델'][i]][0] < math.ceil(dict_integCnt[df_addSmtAssy['대표모델'][i]]/workDay):
                            dict_minContCnt[df_addSmtAssy['대표모델'][i]][0] = math.ceil(dict_integCnt[df_addSmtAssy['대표모델'][i]]/workDay)
                            dict_minContCnt[df_addSmtAssy['대표모델'][i]][1] = df_addSmtAssy['Planned Prod. Completion date'][i]
                    else:
                        dict_minContCnt[df_addSmtAssy['대표모델'][i]] = [math.ceil(dict_integCnt[df_addSmtAssy['대표모델'][i]]/workDay),
                                                                    df_addSmtAssy['Planned Prod. Completion date'][i]]
                else:
                    dict_minContCnt[df_addSmtAssy['대표모델'][i]] = [math.ceil(dict_integCnt[df_addSmtAssy['대표모델'][i]]/workDay),
                                                                    df_addSmtAssy['Planned Prod. Completion date'][i]]
                if workDay <= 0:
                    workDay = 1
                df_addSmtAssy['대표모델별_최소착공필요량_per_일'][i] = dict_integCnt[df_addSmtAssy['대표모델'][i]]/workDay
            if self.isDebug:
                df_addSmtAssy.to_excel('.\\debug\\Main\\flow9.xlsx')
            dict_minContCopy = dict_minContCnt.copy()
            #대표모델 별 최소착공 필요량을 기준으로 평준화 적용 착공량을 계산. 미착공수주잔에서 해당 평준화 적용 착공량을 제외한 수량은 잔여착공량으로 기재
            df_addSmtAssy['평준화_적용_착공량'] = 0
            for i in df_addSmtAssy.index:
                if df_addSmtAssy['대표모델'][i] in dict_minContCopy:
                    if dict_minContCopy[df_addSmtAssy['대표모델'][i]][0] >= int(df_addSmtAssy['미착공수주잔'][i]):
                        df_addSmtAssy['평준화_적용_착공량'][i] = int(df_addSmtAssy['미착공수주잔'][i])
                        dict_minContCopy[df_addSmtAssy['대표모델'][i]][0] -= int(df_addSmtAssy['미착공수주잔'][i])
                    else:
                        df_addSmtAssy['평준화_적용_착공량'][i] = dict_minContCopy[df_addSmtAssy['대표모델'][i]][0]
                        dict_minContCopy[df_addSmtAssy['대표모델'][i]][0] = 0
            df_addSmtAssy['잔여_착공량'] = df_addSmtAssy['미착공수주잔'] - df_addSmtAssy['평준화_적용_착공량']
            df_addSmtAssy = df_addSmtAssy.sort_values(by=['긴급오더',
                                                            '당일착공',
                                                            'Planned Prod. Completion date',
                                                            '평준화_적용_착공량'],
                                                            ascending=[False,
                                                                        False,
                                                                        True,
                                                                        False])
            if self.isDebug:
                df_addSmtAssy.to_excel('.\\debug\\Main\\flow10.xlsx')
            df_addSmtAssy['SMT반영_착공량'] = 0
            #알람 상세 DataFrame 생성
            df_alarmDetail = pd.DataFrame(columns=["No.", 
                                                    "분류", 
                                                    "L/N", 
                                                    "MS CODE", 
                                                    "SMT ASSY", 
                                                    "수주수량", 
                                                    "부족수량", 
                                                    "검사호기", 
                                                    "대상 검사시간(초)", 
                                                    "필요시간(초)", 
                                                    "완성예정일"])
            alarmDetailNo = 1
            #최소착공량에 대해 Smt적용 착공량 계산
            df_addSmtAssy, dict_smtCnt, alarmDetailNo, df_alarmDetail = self.smtReflectInst(df_addSmtAssy, 
                                                                                            False, 
                                                                                            dict_smtCnt, 
                                                                                            alarmDetailNo,
                                                                                            df_alarmDetail,
                                                                                            rowNo)
            if self.isDebug:
                # df_alarmDetail = df_alarmDetail.drop_duplicates(['MS CODE'])
                df_alarmDetail.to_excel('.\\debug\\Main\\df_alarmDetail.xlsx')
            #잔여 착공량에 대해 Smt적용 착공량 계산
            df_addSmtAssy['SMT반영_착공량_잔여'] = 0
            df_addSmtAssy, dict_smtCnt, alarmDetailNo, df_alarmDetail = self.smtReflectInst(df_addSmtAssy, 
                                                                                            True, 
                                                                                            dict_smtCnt, 
                                                                                            alarmDetailNo,
                                                                                            df_alarmDetail,
                                                                                            rowNo)
            if self.isDebug:
                df_addSmtAssy.to_excel('.\\debug\\Main\\flow11.xlsx')
            #설비능력 반영 착공량 계산
            df_addSmtAssy['임시수량'] = 0
            df_addSmtAssy['설비능력반영_착공량'] = 0
            df_addSmtAssy, dict_ate, alarmDetailNo, df_alarmDetail, self.maxCnt = self.ateReflectInst(df_addSmtAssy,
                                                                                                        False,
                                                                                                        dict_ate,
                                                                                                        df_alarmDetail,
                                                                                                        alarmDetailNo,
                                                                                                        self.maxCnt)
            #잔여 착공량에 대해 설비능력 반영 착공량 계산
            df_addSmtAssy['임시수량_잔여'] = 0
            df_addSmtAssy['설비능력반영_착공량_잔여'] = 0
            df_addSmtAssy, dict_ate, alarmDetailNo, df_alarmDetail, self.maxCnt = self.ateReflectInst(df_addSmtAssy,
                                                                                                        True,
                                                                                                        dict_ate,
                                                                                                        df_alarmDetail,
                                                                                                        alarmDetailNo,
                                                                                                        self.maxCnt)
            if self.isDebug:
                df_dict = pd.DataFrame(data=dict_ate, index=[0])
                df_dict = df_dict.T
                df_dict.to_excel('.\\debug\\Main\\dict_ate.xlsx')
            
            df_addSmtAssy = df_addSmtAssy.reset_index(drop=True)
            if self.isDebug:
                df_addSmtAssy.to_excel('.\\debug\\Main\\flow12.xlsx')
                df_alarmDetail = df_alarmDetail.reset_index(drop=True)
                df_alarmDetail.to_excel('.\\debug\\Main\\df_alarmDetail.xlsx')
            #알람 상세 결과에서 각 항목별로 요약
            #분류1 요약
            df_firstAlarm = df_alarmDetail[df_alarmDetail['분류'] == '1']
            df_firstAlarmSummary = df_firstAlarm.groupby("SMT ASSY")['부족수량'].sum()
            df_firstAlarmSummary = df_firstAlarmSummary.reset_index()
            df_firstAlarmSummary['수량'] = df_firstAlarmSummary['부족수량']
            df_firstAlarmSummary['분류'] = '1'
            df_firstAlarmSummary['MS CODE'] = '-'
            df_firstAlarmSummary['검사호기'] = '-'
            df_firstAlarmSummary['부족 시간'] = '-'
            df_firstAlarmSummary['Message'] = '[SMT ASSY : '+ df_firstAlarmSummary["SMT ASSY"]+ ']가 부족합니다. SMT ASSY 제작을 지시해주세요.'
            del df_firstAlarmSummary['부족수량']
            #분류2 요약
            df_secAlarm = df_alarmDetail[df_alarmDetail['분류'] == '2']
            df_secAlarmSummary = df_secAlarm.groupby("검사호기")['필요시간(초)'].sum()
            df_secAlarmSummary = df_secAlarmSummary.reset_index()
            df_secAlarmSummary['부족 시간'] = df_secAlarmSummary['필요시간(초)']
            df_secAlarmSummary['분류'] = '2'
            df_secAlarmSummary['MS CODE'] = '-'
            df_secAlarmSummary['SMT ASSY'] = '-'
            df_secAlarmSummary['수량'] = '-'
            df_secAlarmSummary['Message'] = '검사설비능력이 부족합니다. 생산 가능여부를 확인해 주세요.'
            del df_secAlarmSummary['필요시간(초)']
            #위 알람을 병합            
            df_alarmSummary = pd.concat([df_firstAlarmSummary, df_secAlarmSummary])

            #기타 알람에 대한 추가
            df_etcList = df_alarmDetail[(df_alarmDetail['분류'] == '기타1') | (df_alarmDetail['분류'] == '기타2') | (df_alarmDetail['분류'] == '기타3')]
            df_etcList = df_etcList.drop_duplicates(['MS CODE'])
            for i in df_etcList.index:
                if df_etcList['분류'][i] == '기타1':
                    df_alarmSummary = pd.concat([df_alarmSummary, 
                                                pd.DataFrame.from_records([{"분류" : df_etcList['분류'][i],
                                                                            "MS CODE" : df_etcList['MS CODE'][i],
                                                                            "SMT ASSY" : '-', 
                                                                            "수량" : 0,
                                                                            "검사호기" : '-', 
                                                                            "부족 시간" : 0, 
                                                                            "Message" : '해당 MS CODE에서 사용되는 SMT ASSY가 등록되지 않았습니다. 등록 후 다시 실행해주세요.'}
                                                                            ])])
                elif df_etcList['분류'][i] == '기타2':
                    df_alarmSummary = pd.concat([df_alarmSummary, 
                                                pd.DataFrame.from_records([{"분류" : df_etcList['분류'][i],
                                                                            "MS CODE" : df_etcList['MS CODE'][i],
                                                                            "SMT ASSY" : '-', 
                                                                            "수량" : 0,
                                                                            "검사호기" : '-', 
                                                                            "부족 시간" : 0, 
                                                                            "Message" : '긴급오더 및 당일착공 대상의 총 착공량이 입력한 최대착공량보다 큽니다. 최대착공량을 확인해주세요.'}
                                                                            ])])            
                elif df_etcList['분류'][i] == '기타3':
                    df_alarmSummary = pd.concat([df_alarmSummary, 
                                                pd.DataFrame.from_records([{"분류" : df_etcList['분류'][i],
                                                                            "MS CODE" : df_etcList['MS CODE'][i],
                                                                            "SMT ASSY" : '-', 
                                                                            "수량" : 0,
                                                                            "검사호기" : '-', 
                                                                            "부족 시간" : 0, 
                                                                            "Message" : 'SMT ASSY 정보가 등록되지 않아 재고를 확인할 수 없습니다. 등록 후 다시 실행해주세요.'}
                                                                            ])]) 
            df_alarmSummary = df_alarmSummary.reset_index(drop=True)
            df_alarmSummary = df_alarmSummary[['분류', 'MS CODE', 'SMT ASSY', '수량', '검사호기', '부족 시간', 'Message']]
            if self.isDebug:
                df_alarmSummary.to_excel('.\\debug\\Main\\df_alarmSummary.xlsx')
            #파일 한개로 출력
            with pd.ExcelWriter('.\\Output\\Alarm\\FAM3_AlarmList_'+today+'.xlsx') as writer:
                df_alarmSummary.to_excel(writer, sheet_name='정리', index=True)
                df_alarmDetail.to_excel(writer, sheet_name='상세', index=True)   
            #총착공량 컬럼으로 병합
            df_addSmtAssy['총착공량'] = df_addSmtAssy['설비능력반영_착공량'] + df_addSmtAssy['설비능력반영_착공량_잔여']
            df_addSmtAssy = df_addSmtAssy[df_addSmtAssy['총착공량'] != 0]
            if self.isDebug:
                df_addSmtAssy.to_excel('.\\debug\\Main\\flow13.xlsx')
            #최대착공량만큼 착공 못했을 경우, 메시지 출력
            if self.maxCnt > 0 :
                self.mainReturnWarning.emit(f'아직 착공하지 못한 모델이 [{int(self.maxCnt)}대] 남았습니다. 설비능력 부족이 예상됩니다. 확인해주세요.')
            #레벨링 리스트와 병합
            df_addSmtAssy = df_addSmtAssy.astype({'Linkage Number':'str'})
            df_levelingMain = df_levelingMain.astype({'Linkage Number':'str'})
            df_mergeOrder = pd.merge(df_addSmtAssy, df_levelingMain, on='Linkage Number', how='left')
            if self.isDebug:
                df_mergeOrder.to_excel('.\\debug\\Main\\flow14.xlsx')
            df_mergeOrderResult = pd.DataFrame().reindex_like(df_mergeOrder)
            df_mergeOrderResult = df_mergeOrderResult[0:0]
            #총착공량 만큼 개별화
            for i in df_addSmtAssy.index:
                for j in df_mergeOrder.index:
                    if df_addSmtAssy['Linkage Number'][i] == df_mergeOrder['Linkage Number'][j]:
                        if j > 0:
                            if df_mergeOrder['Linkage Number'][j] != df_mergeOrder['Linkage Number'][j-1]:
                                orderCnt = int(df_addSmtAssy['총착공량'][i])
                        else:
                            orderCnt = int(df_addSmtAssy['총착공량'][i])
                        if orderCnt > 0:
                            df_mergeOrderResult = df_mergeOrderResult.append(df_mergeOrder.iloc[j])
                            # df_mergeOrderResult = pd.concat([df_mergeOrderResult, df_mergeOrder.iloc[j]])
                            orderCnt -= 1
            #사이클링을 위해 검사설비별로 정리
            df_mergeOrderResult = df_mergeOrderResult.sort_values(by=['INSPECTION_EQUIPMENT'],
                                                                    ascending=[False])
            df_mergeOrderResult = df_mergeOrderResult.reset_index(drop=True)
            if self.isDebug:
                df_mergeOrderResult.to_excel('.\\debug\\Main\\flow15.xlsx')
            #긴급오더 제외하고 사이클 대상만 식별하여 검사장치별로 갯수 체크
            df_cycleCopy = df_mergeOrderResult[df_mergeOrderResult['긴급오더'].isnull()]
            df_cycleCopy['검사장치Cnt'] = df_cycleCopy.groupby('INSPECTION_EQUIPMENT')['INSPECTION_EQUIPMENT'].transform('size')
            df_cycleCopy = df_cycleCopy.sort_values(by=['검사장치Cnt'],
                                                    ascending=[False])
            df_cycleCopy = df_cycleCopy.reset_index(drop=True)
            #긴급오더 포함한 Df와 병합
            df_mergeOrderResult = pd.merge(df_mergeOrderResult, df_cycleCopy[['Planned Order', '검사장치Cnt']], on='Planned Order', how='left')
            df_mergeOrderResult = df_mergeOrderResult.sort_values(by=['검사장치Cnt'],
                                                                    ascending=[False])
            df_mergeOrderResult = df_mergeOrderResult.reset_index(drop=True)
            if self.isDebug:
                df_mergeOrderResult.to_excel('.\\debug\\Main\\flow15-1.xlsx')
            #최대 사이클 번호 체크
            maxCycle = float(df_cycleCopy['검사장치Cnt'][0])
            cycleGr = 1.0
            df_mergeOrderResult['사이클그룹'] = 0
            #각 검사장치별로 사이클 그룹을 작성하고, 최대 사이클과 비교하여 각 사이클그룹에서 배수처리
            for i in df_mergeOrderResult.index:
                if df_mergeOrderResult['긴급오더'][i] != '대상':
                    # multiCnt = math.floor(maxCycle/df_mergeOrderResult['검사장치Cnt'][i])
                    multiCnt = maxCycle/df_mergeOrderResult['검사장치Cnt'][i]
                    if i == 0:
                        df_mergeOrderResult['사이클그룹'][i] = cycleGr
                    else:
                        if df_mergeOrderResult['INSPECTION_EQUIPMENT'][i] != df_mergeOrderResult['INSPECTION_EQUIPMENT'][i-1]:
                            if i == 1:
                                cycleGr = 2.0 
                            else:
                                cycleGr = 1.0 
                        df_mergeOrderResult['사이클그룹'][i] = cycleGr * multiCnt
                    cycleGr += 1.0
                if cycleGr >= maxCycle:
                    cycleGr = 1.0
            #배정된 사이클 그룹 순으로 정렬
            df_mergeOrderResult = df_mergeOrderResult.sort_values(by=['사이클그룹'],
                                                                        ascending=[True])
            df_mergeOrderResult = df_mergeOrderResult.reset_index(drop=True)
            if self.isDebug:
                df_mergeOrderResult.to_excel('.\\debug\\Main\\flow16.xlsx')
            df_mergeOrderResult = df_mergeOrderResult.reset_index()
            for i in df_mergeOrderResult.index:
                if df_mergeOrderResult['긴급오더'][i] != '대상':
                    if i != 0 and (df_mergeOrderResult['INSPECTION_EQUIPMENT'][i] == df_mergeOrderResult['INSPECTION_EQUIPMENT'][i-1]):
                        for j in df_mergeOrderResult.index:
                            if (j != 0 and j < len(df_mergeOrderResult)-1) and (df_mergeOrderResult['INSPECTION_EQUIPMENT'][i] != df_mergeOrderResult['INSPECTION_EQUIPMENT'][j + 1]) and (df_mergeOrderResult['INSPECTION_EQUIPMENT'][i] != df_mergeOrderResult['INSPECTION_EQUIPMENT'][j]):
                                df_mergeOrderResult['index'][i] = (float(df_mergeOrderResult['index'][j]) + float(df_mergeOrderResult['index'][j+1]))/2
                                df_mergeOrderResult = df_mergeOrderResult.sort_values(by=['index'],
                                                                                      ascending=[True])
                                df_mergeOrderResult = df_mergeOrderResult.reset_index(drop=True)
                                break
                        
                                
            if self.isDebug:
                df_mergeOrderResult.to_excel('.\\debug\\Main\\flow17.xlsx')            

            df_mergeOrderResult['No (*)'] = (df_mergeOrderResult.index.astype(int) + 1) * 10
            df_mergeOrderResult['Planned Order'] = df_mergeOrderResult['Planned Order'].astype(int).astype(str).str.zfill(10)
            df_mergeOrderResult['Scheduled End Date'] = df_mergeOrderResult['Scheduled End Date'].astype(str).str.zfill(10)
            df_mergeOrderResult['Specified Start Date'] = df_mergeOrderResult['Specified Start Date'].astype(str).str.zfill(10)
            df_mergeOrderResult['Specified End Date'] = df_mergeOrderResult['Specified End Date'].astype(str).str.zfill(10)
            df_mergeOrderResult['Spec Freeze Date'] = df_mergeOrderResult['Spec Freeze Date'].astype(str).str.zfill(10)
            df_mergeOrderResult['Component Number'] = df_mergeOrderResult['Component Number'].astype(int).astype(str).str.zfill(4)

            df_mergeOrderResult = df_mergeOrderResult[['No (*)', 
                                                        'Sequence No', 
                                                        'Production Order', 
                                                        'Planned Order', 
                                                        'Manual', 
                                                        'Scheduled Start Date (*)', 
                                                        'Scheduled End Date', 
                                                        'Specified Start Date', 
                                                        'Specified End Date', 
                                                        'Demand destination country', 
                                                        'MS-CODE', 
                                                        'Allocate', 
                                                        'Spec Freeze Date', 
                                                        'Linkage Number', 
                                                        'Order Number', 
                                                        'Order Item', 
                                                        'Combination flag', 
                                                        'Project Definition', 
                                                        'Error message', 
                                                        'Leveling Group', 
                                                        'Leveling Class', 
                                                        'Planning Plant', 
                                                        'Component Number', 
                                                        'Serial Number']]

            outputFile = '.\\Output\\Result\\'+ today +'_Main.xlsx'
            df_mergeOrderResult.to_excel(outputFile, index=False)

            self.mainReturnEnd.emit(True)
            self.thread().quit()
        except Exception as e:
            self.mainReturnError.emit(e)
            self.thread().quit()
            return
        
class CustomFormatter(logging.Formatter):
    FORMATS = {
        logging.ERROR:   ('[%(asctime)s] %(levelname)s:%(message)s','yellow'),
        logging.DEBUG:   ('[%(asctime)s] %(levelname)s:%(message)s','white'),
        logging.INFO:    ('[%(asctime)s] %(levelname)s:%(message)s','white'),
        logging.WARNING: ('[%(asctime)s] %(levelname)s:%(message)s', 'yellow')
    }
    def format( self, record ):
        last_fmt = self._style._fmt
        opt = CustomFormatter.FORMATS.get(record.levelno)
        if opt:
            fmt, color = opt
            self._style._fmt = "<font color=\"{}\">{}</font>".format(QtGui.QColor(color).name(),fmt)
        res = logging.Formatter.format( self, record )
        self._style._fmt = last_fmt
        return res

class QTextEditLogger(logging.Handler):
    def __init__(self, parent=None):
        super().__init__()
        self.widget = QPlainTextEdit(parent)
        self.widget.setReadOnly(True)    
        self.widget.setGeometry(QRect(10, 260, 661, 161))
        self.widget.setStyleSheet('background-color: rgb(53, 53, 53);\ncolor: rgb(255, 255, 255);')
        self.widget.setObjectName('logBrowser')
        font = QFont()
        font.setFamily('Nanum Gothic')
        font.setBold(False)
        font.setPointSize(9)
        self.widget.setFont(font)
    def emit(self, record):
        msg = self.format(record)
        self.widget.appendHtml(msg) 
        # move scrollbar
        scrollbar = self.widget.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())

class CalendarWindow(QWidget):
    submitClicked = pyqtSignal(str)
    def __init__(self):
        super().__init__()
        self.initUI()
    def initUI(self):
        cal = QCalendarWidget(self)
        cal.setGridVisible(True)
        cal.clicked[QDate].connect(self.showDate)
        self.lb = QLabel(self)
        date = cal.selectedDate()
        self.lb.setText(date.toString("yyyy-MM-dd"))
        vbox = QVBoxLayout()
        vbox.addWidget(cal)
        vbox.addWidget(self.lb)
        self.submitBtn = QToolButton(self)
        sizePolicy = QSizePolicy(QSizePolicy.Ignored, 
                                    QSizePolicy.Fixed)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        self.submitBtn.setSizePolicy(sizePolicy)
        self.submitBtn.setMinimumSize(QSize(0, 35))
        self.submitBtn.setStyleSheet('background-color: rgb(63, 63, 63);\ncolor: rgb(255, 255, 255);')
        self.submitBtn.setObjectName('submitBtn')
        self.submitBtn.setText('착공지정일 결정')
        self.submitBtn.clicked.connect(self.confirm)
        vbox.addWidget(self.submitBtn)
        self.setLayout(vbox)
        self.setWindowTitle('캘린더')
        self.setGeometry(500,500,500,400)
        self.show()
    def showDate(self, date):
        self.lb.setText(date.toString("yyyy-MM-dd"))
    @pyqtSlot()
    def confirm(self):
        self.submitClicked.emit(self.lb.text())
        self.close()
class UISubWindow(QMainWindow):
    submitClicked = pyqtSignal(list)
    status = ''
    def __init__(self):
        super().__init__()
        self.setupUi()
    def setupUi(self):
        self.setObjectName('SubWindow')
        self.resize(600, 600)
        self.setStyleSheet('background-color: rgb(252, 252, 252);')
        self.centralwidget = QWidget(self)
        self.centralwidget.setObjectName('centralwidget')
        self.gridLayout2 = QGridLayout(self.centralwidget)
        self.gridLayout2.setObjectName('gridLayout2')
        self.gridLayout = QGridLayout()
        self.gridLayout.setObjectName('gridLayout')
        self.groupBox = QGroupBox(self.centralwidget)
        self.groupBox.setTitle('')
        self.groupBox.setObjectName('groupBox')
        self.gridLayout4 = QGridLayout(self.groupBox)
        self.gridLayout4.setObjectName('gridLayout4')
        self.gridLayout3 = QGridLayout()
        self.gridLayout3.setObjectName('gridLayout3')
        self.linkageInput = QLineEdit(self.groupBox)
        self.linkageInput.setMinimumSize(QSize(0, 25))
        self.linkageInput.setObjectName('linkageInput')
        self.linkageInput.setValidator(QDoubleValidator(self))
        self.gridLayout3.addWidget(self.linkageInput, 0, 1, 1, 3)
        self.linkageInputBtn = QPushButton(self.groupBox)
        self.linkageInputBtn.setMinimumSize(QSize(0,25))
        self.gridLayout3.addWidget(self.linkageInputBtn, 0, 4, 1, 2)
        self.linkageAddExcelBtn = QPushButton(self.groupBox)
        self.linkageAddExcelBtn.setMinimumSize(QSize(0,25))
        self.gridLayout3.addWidget(self.linkageAddExcelBtn, 0, 6, 1, 2)
        self.mscodeInput = QLineEdit(self.groupBox)
        self.mscodeInput.setMinimumSize(QSize(0, 25))
        self.mscodeInput.setObjectName('mscodeInput')
        self.mscodeInputBtn = QPushButton(self.groupBox)
        self.mscodeInputBtn.setMinimumSize(QSize(0,25))
        self.gridLayout3.addWidget(self.mscodeInput, 1, 1, 1, 3)
        self.gridLayout3.addWidget(self.mscodeInputBtn, 1, 4, 1, 2)
        self.mscodeAddExcelBtn = QPushButton(self.groupBox)
        self.mscodeAddExcelBtn.setMinimumSize(QSize(0,25))
        self.gridLayout3.addWidget(self.mscodeAddExcelBtn, 1, 6, 1, 2)
        sizePolicy = QSizePolicy(QSizePolicy.Ignored,
                                    QSizePolicy.Fixed)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        self.submitBtn = QToolButton(self.groupBox)
        sizePolicy.setHeightForWidth(self.submitBtn.sizePolicy().hasHeightForWidth())
        self.submitBtn.setSizePolicy(sizePolicy)
        self.submitBtn.setMinimumSize(QSize(100, 35))
        self.submitBtn.setStyleSheet('background-color: rgb(63, 63, 63);\ncolor: rgb(255, 255, 255);')
        self.submitBtn.setObjectName('submitBtn')
        self.gridLayout3.addWidget(self.submitBtn, 3, 5, 1, 2)
        
        self.label = QLabel(self.groupBox)
        self.label.setAlignment(Qt.AlignRight | 
                                Qt.AlignTrailing | 
                                Qt.AlignVCenter)
        self.label.setObjectName('label')
        self.gridLayout3.addWidget(self.label, 0, 0, 1, 1)
        self.label2 = QLabel(self.groupBox)
        self.label2.setAlignment(Qt.AlignRight | 
                                    Qt.AlignTrailing | 
                                    Qt.AlignVCenter)
        self.label2.setObjectName('label2')
        self.gridLayout3.addWidget(self.label2, 1, 0, 1, 1)
        self.line = QFrame(self.groupBox)
        self.line.setFrameShape(QFrame.HLine)
        self.line.setFrameShadow(QFrame.Sunken)
        self.line.setObjectName('line')
        self.gridLayout3.addWidget(self.line, 2, 0, 1, 10)
        self.gridLayout4.addLayout(self.gridLayout3, 0, 0, 1, 1)
        self.gridLayout.addWidget(self.groupBox, 0, 0, 1, 1)
        self.groupBox2 = QGroupBox(self.centralwidget)
        self.groupBox2.setTitle('')
        self.groupBox2.setObjectName('groupBox2')
        self.gridLayout6 = QGridLayout(self.groupBox2)
        self.gridLayout6.setObjectName('gridLayout6')
        self.gridLayout5 = QGridLayout()
        self.gridLayout5.setObjectName('gridLayout5')
        listViewModelLinkage = QStandardItemModel()
        self.listViewLinkage = QListView(self.groupBox2)
        self.listViewLinkage.setModel(listViewModelLinkage)
        self.gridLayout5.addWidget(self.listViewLinkage, 1, 0, 1, 1)
        self.label3 = QLabel(self.groupBox2)
        self.label3.setAlignment(Qt.AlignLeft | 
                                    Qt.AlignTrailing | 
                                    Qt.AlignVCenter)
        self.label3.setObjectName('label3')
        self.gridLayout5.addWidget(self.label3, 0, 0, 1, 1)
        self.vline = QFrame(self.groupBox2)
        self.vline.setFrameShape(QFrame.VLine)
        self.vline.setFrameShadow(QFrame.Sunken)
        self.vline.setObjectName('vline')
        self.gridLayout5.addWidget(self.vline, 1, 1, 1, 1)
        listViewModelmscode = QStandardItemModel()
        self.listViewmscode = QListView(self.groupBox2)
        self.listViewmscode.setModel(listViewModelmscode)
        self.gridLayout5.addWidget(self.listViewmscode, 1, 2, 1, 1)
        self.label4 = QLabel(self.groupBox2)
        self.label4.setAlignment(Qt.AlignLeft | 
                                    Qt.AlignTrailing | 
                                    Qt.AlignVCenter)
        self.label4.setObjectName('label4')
        self.gridLayout5.addWidget(self.label4, 0, 2, 1, 1)
        self.label5 = QLabel(self.groupBox2)
        self.label5.setAlignment(Qt.AlignLeft | 
                                    Qt.AlignTrailing | 
                                    Qt.AlignVCenter)
        self.label5.setObjectName('label5')       
        self.gridLayout5.addWidget(self.label5, 0, 3, 1, 1) 
        self.linkageDelBtn = QPushButton(self.groupBox2)
        self.linkageDelBtn.setMinimumSize(QSize(0,25))
        self.gridLayout5.addWidget(self.linkageDelBtn, 2, 0, 1, 1)
        self.mscodeDelBtn = QPushButton(self.groupBox2)
        self.mscodeDelBtn.setMinimumSize(QSize(0,25))
        self.gridLayout5.addWidget(self.mscodeDelBtn, 2, 2, 1, 1)
        self.gridLayout6.addLayout(self.gridLayout5, 0, 0, 1, 1)
        self.gridLayout.addWidget(self.groupBox2, 1, 0, 1, 1)
        self.gridLayout2.addLayout(self.gridLayout, 0, 0, 1, 1)
        self.setCentralWidget(self.centralwidget)
        self.menubar = QMenuBar(self)
        self.menubar.setGeometry(QRect(0, 0, 653, 21))
        self.menubar.setObjectName('menubar')
        self.setMenuBar(self.menubar)
        self.statusbar = QStatusBar(self)
        self.statusbar.setObjectName('statusbar')
        self.setStatusBar(self.statusbar)
        self.retranslateUi(self)
        self.mscodeInput.returnPressed.connect(self.addmscode)
        self.linkageInput.returnPressed.connect(self.addLinkage)
        self.linkageInputBtn.clicked.connect(self.addLinkage)
        self.mscodeInputBtn.clicked.connect(self.addmscode)
        self.linkageDelBtn.clicked.connect(self.delLinkage)
        self.mscodeDelBtn.clicked.connect(self.delmscode)
        self.submitBtn.clicked.connect(self.confirm)
        self.linkageAddExcelBtn.clicked.connect(self.addLinkageExcel)
        self.mscodeAddExcelBtn.clicked.connect(self.addmscodeExcel)
        self.retranslateUi(self)
        self.show()
    
    def retranslateUi(self, MainWindow):
        _translate = QCoreApplication.translate
        MainWindow.setWindowTitle(_translate('SubWindow', '긴급/홀딩오더 입력'))
        MainWindow.setWindowIcon(QIcon('.\\Logo\\logo.png'))
        self.label.setText(_translate('SubWindow', 'Linkage No 입력 :'))
        self.linkageInputBtn.setText(_translate('SubWindow', '추가'))
        self.label2.setText(_translate('SubWindow', 'MS-CODE 입력 :'))
        self.mscodeInputBtn.setText(_translate('SubWindow', '추가'))
        self.submitBtn.setText(_translate('SubWindow','추가 완료'))
        self.label3.setText(_translate('SubWindow', 'Linkage No List'))
        self.label4.setText(_translate('SubWindow', 'MS-Code List'))
        self.linkageDelBtn.setText(_translate('SubWindow', '삭제'))
        self.mscodeDelBtn.setText(_translate('SubWindow', '삭제'))
        self.linkageAddExcelBtn.setText(_translate('SubWindow', '엑셀 입력'))
        self.mscodeAddExcelBtn.setText(_translate('SubWindow', '엑셀 입력'))
    @pyqtSlot()
    def addLinkage(self):
        linkageNo = self.linkageInput.text()
        if len(linkageNo) == 16:
            if linkageNo.isdigit():
                model = self.listViewLinkage.model()
                linkageItem = QStandardItem()
                linkageItemModel = QStandardItemModel()
                dupFlag = False
                for i in range(model.rowCount()):
                    index = model.index(i,0)
                    item = model.data(index)
                    if item == linkageNo:
                        dupFlag = True
                    linkageItem = QStandardItem(item)
                    linkageItemModel.appendRow(linkageItem)
                if not dupFlag:
                    linkageItem = QStandardItem(linkageNo)
                    linkageItemModel.appendRow(linkageItem)
                    self.listViewLinkage.setModel(linkageItemModel)
                else:
                    QMessageBox.information(self, 'Error', '중복된 데이터가 있습니다.')
            else:
                QMessageBox.information(self, 'Error', '숫자만 입력해주세요.')
        elif len(linkageNo) == 0: 
            QMessageBox.information(self, 'Error', 'Linkage Number 데이터가 입력되지 않았습니다.')
        else:
            QMessageBox.information(self, 'Error', '16자리의 Linkage Number를 입력해주세요.')
    
    @pyqtSlot()
    def delLinkage(self):
        model = self.listViewLinkage.model()
        linkageItem = QStandardItem()
        linkageItemModel = QStandardItemModel()
        for index in self.listViewLinkage.selectedIndexes():
            selected_item = self.listViewLinkage.model().data(index)
            for i in range(model.rowCount()):
                index = model.index(i,0)
                item = model.data(index)
                linkageItem = QStandardItem(item)
                if selected_item != item:
                    linkageItemModel.appendRow(linkageItem)
            self.listViewLinkage.setModel(linkageItemModel)
    @pyqtSlot()
    def addmscode(self):
        mscode = self.mscodeInput.text()
        if len(mscode) > 0:
            model = self.listViewmscode.model()
            mscodeItem = QStandardItem()
            mscodeItemModel = QStandardItemModel()
            dupFlag = False
            for i in range(model.rowCount()):
                index = model.index(i,0)
                item = model.data(index)
                if item == mscode:
                    dupFlag = True
                mscodeItem = QStandardItem(item)
                mscodeItemModel.appendRow(mscodeItem)
            if not dupFlag:
                mscodeItem = QStandardItem(mscode)
                mscodeItemModel.appendRow(mscodeItem)
                self.listViewmscode.setModel(mscodeItemModel)
            else:
                QMessageBox.information(self, 'Error', '중복된 데이터가 있습니다.')
        else: 
            QMessageBox.information(self, 'Error', 'MS-CODE 데이터가 입력되지 않았습니다.')
    @pyqtSlot()
    def delmscode(self):
        model = self.listViewmscode.model()
        mscodeItem = QStandardItem()
        mscodeItemModel = QStandardItemModel()
        for index in self.listViewmscode.selectedIndexes():
            selected_item = self.listViewmscode.model().data(index)
            for i in range(model.rowCount()):
                index = model.index(i,0)
                item = model.data(index)
                mscodeItem = QStandardItem(item)
                if selected_item != item:
                    mscodeItemModel.appendRow(mscodeItem)
            self.listViewmscode.setModel(mscodeItemModel)
    @pyqtSlot()
    def addLinkageExcel(self):
        try:
            fileName = QFileDialog.getOpenFileName(self, 'Open File', './', 'Excel Files (*.xlsx)')[0]
            if fileName != "":
                df = pd.read_excel(fileName)
                for i in df.index:
                    linkageNo = str(df[df.columns[0]][i])
                    if len(linkageNo) == 16:
                        if linkageNo.isdigit():
                            model = self.listViewLinkage.model()
                            linkageItem = QStandardItem()
                            linkageItemModel = QStandardItemModel()
                            dupFlag = False
                            for i in range(model.rowCount()):
                                index = model.index(i,0)
                                item = model.data(index)
                                if item == linkageNo:
                                    dupFlag = True
                                linkageItem = QStandardItem(item)
                                linkageItemModel.appendRow(linkageItem)
                            if not dupFlag:
                                linkageItem = QStandardItem(linkageNo)
                                linkageItemModel.appendRow(linkageItem)
                                self.listViewLinkage.setModel(linkageItemModel)
                            else:
                                QMessageBox.information(self, 'Error', '중복된 데이터가 있습니다.')
                        else:
                            QMessageBox.information(self, 'Error', '숫자만 입력해주세요.')
                    elif len(linkageNo) == 0: 
                        QMessageBox.information(self, 'Error', 'Linkage Number 데이터가 입력되지 않았습니다.')
                    else:
                        QMessageBox.information(self, 'Error', '16자리의 Linkage Number를 입력해주세요.')
        except Exception as e:
            QMessageBox.information(self, 'Error', '에러발생 : ' + e)
    @pyqtSlot()
    def addmscodeExcel(self):
        try:
            fileName = QFileDialog.getOpenFileName(self, 'Open File', './', 'Excel Files (*.xlsx)')[0]
            if fileName != "":
                df = pd.read_excel(fileName)
                for i in df.index:
                    mscode = str(df[df.columns[0]][i])
                    if len(mscode) > 0:
                        model = self.listViewmscode.model()
                        mscodeItem = QStandardItem()
                        mscodeItemModel = QStandardItemModel()
                        dupFlag = False
                        for i in range(model.rowCount()):
                            index = model.index(i,0)
                            item = model.data(index)
                            if item == mscode:
                                dupFlag = True
                            mscodeItem = QStandardItem(item)
                            mscodeItemModel.appendRow(mscodeItem)
                        if not dupFlag:
                            mscodeItem = QStandardItem(mscode)
                            mscodeItemModel.appendRow(mscodeItem)
                            self.listViewmscode.setModel(mscodeItemModel)
                        else:
                            QMessageBox.information(self, 'Error', '중복된 데이터가 있습니다.')
                    else: 
                        QMessageBox.information(self, 'Error', 'MS-CODE 데이터가 입력되지 않았습니다.')
        except Exception as e:
            QMessageBox.information(self, 'Error', '에러발생 : ' + e)
    @pyqtSlot()
    def confirm(self):
        self.submitClicked.emit([self.listViewLinkage.model(), self.listViewmscode.model()])
        self.close()
class Ui_MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setupUi()
        
    def setupUi(self):
        logger = logging.getLogger(__name__)
        rfh = RotatingFileHandler(filename='./Log.log', 
                                    mode='a',
                                    maxBytes=5*1024*1024,
                                    backupCount=2,
                                    encoding=None,
                                    delay=0
                                    )
        logging.basicConfig(level=logging.DEBUG, 
                            format = '%(asctime)s:%(levelname)s:%(message)s', 
                            datefmt = '%m/%d/%Y %H:%M:%S',
                            handlers=[rfh])
        self.setObjectName('MainWindow')
        self.resize(900, 1000)
        self.setStyleSheet('background-color: rgb(252, 252, 252);')
        self.centralwidget = QWidget(self)
        self.centralwidget.setObjectName('centralwidget')
        self.gridLayout2 = QGridLayout(self.centralwidget)
        self.gridLayout2.setObjectName('gridLayout2')
        self.gridLayout = QGridLayout()
        self.gridLayout.setObjectName('gridLayout')
        self.groupBox = QGroupBox(self.centralwidget)
        self.groupBox.setTitle('')
        self.groupBox.setObjectName('groupBox')
        self.gridLayout4 = QGridLayout(self.groupBox)
        self.gridLayout4.setObjectName('gridLayout4')
        self.gridLayout3 = QGridLayout()
        self.gridLayout3.setObjectName('gridLayout3')
        self.mainOrderinput = QLineEdit(self.groupBox)
        self.mainOrderinput.setMinimumSize(QSize(0, 25))
        self.mainOrderinput.setObjectName('mainOrderinput')
        self.mainOrderinput.setValidator(QIntValidator(self))
        self.gridLayout3.addWidget(self.mainOrderinput, 0, 1, 1, 1)
        self.spOrderinput = QLineEdit(self.groupBox)
        self.spOrderinput.setMinimumSize(QSize(0, 25))
        self.spOrderinput.setObjectName('spOrderinput')
        self.spOrderinput.setValidator(QIntValidator(self))
        self.gridLayout3.addWidget(self.spOrderinput, 1, 1, 1, 1)
        self.powerOrderinput = QLineEdit(self.groupBox)
        self.powerOrderinput.setMinimumSize(QSize(0, 25))
        self.powerOrderinput.setObjectName('powerOrderinput')
        self.powerOrderinput.setValidator(QIntValidator(self))
        self.gridLayout3.addWidget(self.powerOrderinput, 2, 1, 1, 1)
        self.dateBtn = QToolButton(self.groupBox)
        self.dateBtn.setMinimumSize(QSize(0,25))
        self.dateBtn.setObjectName('dateBtn')
        self.gridLayout3.addWidget(self.dateBtn, 3, 1, 1, 1)
        self.emgFileInputBtn = QPushButton(self.groupBox)
        self.emgFileInputBtn.setMinimumSize(QSize(0,25))
        self.gridLayout3.addWidget(self.emgFileInputBtn, 4, 1, 1, 1)
        self.holdFileInputBtn = QPushButton(self.groupBox)
        self.holdFileInputBtn.setMinimumSize(QSize(0,25))
        self.gridLayout3.addWidget(self.holdFileInputBtn, 7, 1, 1, 1)
        self.label4 = QLabel(self.groupBox)
        self.label4.setAlignment(Qt.AlignRight | 
                                    Qt.AlignTrailing | 
                                    Qt.AlignVCenter)
        self.label4.setObjectName('label4')
        self.gridLayout3.addWidget(self.label4, 5, 1, 1, 1)
        self.label5 = QLabel(self.groupBox)
        self.label5.setAlignment(Qt.AlignRight | 
                                    Qt.AlignTrailing | 
                                    Qt.AlignVCenter)
        self.label5.setObjectName('label5')
        self.gridLayout3.addWidget(self.label5, 5, 2, 1, 1)
        self.label6 = QLabel(self.groupBox)
        self.label6.setAlignment(Qt.AlignRight | 
                                    Qt.AlignTrailing | 
                                    Qt.AlignVCenter)
        self.label6.setObjectName('label6')
        self.gridLayout3.addWidget(self.label6, 8, 1, 1, 1)
        self.label7 = QLabel(self.groupBox)
        self.label7.setAlignment(Qt.AlignRight | 
                                    Qt.AlignTrailing | 
                                    Qt.AlignVCenter)
        self.label7.setObjectName('label7')
        self.gridLayout3.addWidget(self.label7, 8, 2, 1, 1)
        listViewModelEmgLinkage = QStandardItemModel()
        self.listViewEmgLinkage = QListView(self.groupBox)
        self.listViewEmgLinkage.setModel(listViewModelEmgLinkage)
        self.gridLayout3.addWidget(self.listViewEmgLinkage, 6, 1, 1, 1)
        listViewModelEmgmscode = QStandardItemModel()
        self.listViewEmgmscode = QListView(self.groupBox)
        self.listViewEmgmscode.setModel(listViewModelEmgmscode)
        self.gridLayout3.addWidget(self.listViewEmgmscode, 6, 2, 1, 1)
        listViewModelHoldLinkage = QStandardItemModel()
        self.listViewHoldLinkage = QListView(self.groupBox)
        self.listViewHoldLinkage.setModel(listViewModelHoldLinkage)
        self.gridLayout3.addWidget(self.listViewHoldLinkage, 9, 1, 1, 1)
        listViewModelHoldmscode = QStandardItemModel()
        self.listViewHoldmscode = QListView(self.groupBox)
        self.listViewHoldmscode.setModel(listViewModelHoldmscode)
        self.gridLayout3.addWidget(self.listViewHoldmscode, 9, 2, 1, 1)
        self.labelBlank = QLabel(self.groupBox)
        self.labelBlank.setObjectName('labelBlank')
        self.gridLayout3.addWidget(self.labelBlank, 2, 4, 1, 1)
        self.progressbar = QProgressBar(self.groupBox)
        self.progressbar.setObjectName('progressbar')
        self.gridLayout3.addWidget(self.progressbar, 10, 1, 1, 2)
        self.runBtn = QToolButton(self.groupBox)
        sizePolicy = QSizePolicy(QSizePolicy.Ignored, 
                                    QSizePolicy.Fixed)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(self.runBtn.sizePolicy().hasHeightForWidth())
        self.runBtn.setSizePolicy(sizePolicy)
        self.runBtn.setMinimumSize(QSize(0, 35))
        self.runBtn.setStyleSheet('background-color: rgb(63, 63, 63);\ncolor: rgb(255, 255, 255);')
        self.runBtn.setObjectName('runBtn')
        self.gridLayout3.addWidget(self.runBtn, 10, 3, 1, 2)
        self.label = QLabel(self.groupBox)
        self.label.setAlignment(Qt.AlignRight | 
                                Qt.AlignTrailing | 
                                Qt.AlignVCenter)
        self.label.setObjectName('label')
        self.gridLayout3.addWidget(self.label, 0, 0, 1, 1)
        self.label9 = QLabel(self.groupBox)
        self.label9.setAlignment(Qt.AlignRight | 
                                Qt.AlignTrailing | 
                                Qt.AlignVCenter)
        self.label9.setObjectName('label9')
        self.gridLayout3.addWidget(self.label9, 1, 0, 1, 1)
        self.label10 = QLabel(self.groupBox)
        self.label10.setAlignment(Qt.AlignRight | 
                                Qt.AlignTrailing | 
                                Qt.AlignVCenter)
        self.label10.setObjectName('label10')
        self.gridLayout3.addWidget(self.label10, 2, 0, 1, 1)
        self.label11 = QLabel(self.groupBox)
        self.label11.setAlignment(Qt.AlignRight | 
                                Qt.AlignTrailing | 
                                Qt.AlignVCenter)
        self.label11.setObjectName('label11')
        self.gridLayout3.addWidget(self.label11, 0, 2, 1, 1)
        self.label12 = QLabel(self.groupBox)
        self.label12.setAlignment(Qt.AlignRight | 
                                Qt.AlignTrailing | 
                                Qt.AlignVCenter)
        self.label12.setObjectName('label12')
        self.gridLayout3.addWidget(self.label12, 1, 2, 1, 1)
        self.label13 = QLabel(self.groupBox)
        self.label13.setAlignment(Qt.AlignRight | 
                                Qt.AlignTrailing | 
                                Qt.AlignVCenter)
        self.label13.setObjectName('label13')
        self.gridLayout3.addWidget(self.label13, 2, 2, 1, 1)
        self.label8 = QLabel(self.groupBox)
        self.label8.setAlignment(Qt.AlignRight | 
                                Qt.AlignTrailing | 
                                Qt.AlignVCenter)
        self.label8.setObjectName('label8')
        self.gridLayout3.addWidget(self.label8, 3, 0, 1, 1) 
        self.labelDate = QLabel(self.groupBox)
        self.labelDate.setAlignment(Qt.AlignRight | 
                                Qt.AlignTrailing | 
                                Qt.AlignVCenter)
        self.labelDate.setObjectName('labelDate')
        self.gridLayout3.addWidget(self.labelDate, 3, 2, 1, 1) 
        self.label2 = QLabel(self.groupBox)
        self.label2.setAlignment(Qt.AlignRight | 
                                    Qt.AlignTrailing | 
                                    Qt.AlignVCenter)
        self.label2.setObjectName('label2')
        self.gridLayout3.addWidget(self.label2, 4, 0, 1, 1)
        self.label3 = QLabel(self.groupBox)
        self.label3.setAlignment(Qt.AlignRight | 
                                    Qt.AlignTrailing | 
                                    Qt.AlignVCenter)
        self.label3.setObjectName('label3')
        self.gridLayout3.addWidget(self.label3, 7, 0, 1, 1)
        self.line = QFrame(self.groupBox)
        self.line.setFrameShape(QFrame.HLine)
        self.line.setFrameShadow(QFrame.Sunken)
        self.line.setObjectName('line')
        self.cb_main = QComboBox(self.groupBox)
        self.gridLayout3.addWidget(self.cb_main, 0, 3, 1, 1)
        # self.cb_sp = QComboBox(self.groupBox)
        # self.gridLayout3.addWidget(self.cb_sp, 1, 3, 1, 1)
        # self.cb_power = QComboBox(self.groupBox)
        # self.gridLayout3.addWidget(self.cb_power, 2, 3, 1, 1)
        self.gridLayout3.addWidget(self.line, 11, 0, 1, 10)
        self.gridLayout4.addLayout(self.gridLayout3, 0, 0, 1, 1)
        self.gridLayout.addWidget(self.groupBox, 0, 0, 1, 1)
        self.groupBox2 = QGroupBox(self.centralwidget)
        self.groupBox2.setTitle('')
        self.groupBox2.setObjectName('groupBox2')
        self.gridLayout6 = QGridLayout(self.groupBox2)
        self.gridLayout6.setObjectName('gridLayout6')
        self.gridLayout5 = QGridLayout()
        self.gridLayout5.setObjectName('gridLayout5')
        self.logBrowser = QTextEditLogger(self.groupBox2)
        # self.logBrowser.setFormatter(
        #                             logging.Formatter('[%(asctime)s] %(levelname)s:%(message)s', 
        #                                                 datefmt='%Y-%m-%d %H:%M:%S')
        #                             )
        self.logBrowser.setFormatter(CustomFormatter())
        logging.getLogger().addHandler(self.logBrowser)
        logging.getLogger().setLevel(logging.INFO)
        self.gridLayout5.addWidget(self.logBrowser.widget, 0, 0, 1, 1)
        self.gridLayout6.addLayout(self.gridLayout5, 0, 0, 1, 1)
        self.gridLayout.addWidget(self.groupBox2, 1, 0, 1, 1)
        self.gridLayout2.addLayout(self.gridLayout, 0, 0, 1, 1)
        self.setCentralWidget(self.centralwidget)
        self.menubar = QMenuBar(self)
        self.menubar.setGeometry(QRect(0, 0, 653, 21))
        self.menubar.setObjectName('menubar')
        self.setMenuBar(self.menubar)
        self.statusbar = QStatusBar(self)
        self.statusbar.setObjectName('statusbar')
        self.setStatusBar(self.statusbar)
        self.retranslateUi(self)
        self.dateBtn.clicked.connect(self.selectStartDate)
        self.emgFileInputBtn.clicked.connect(self.emgWindow)
        self.holdFileInputBtn.clicked.connect(self.holdWindow)
        self.runBtn.clicked.connect(self.mainStartLeveling)
        #디버그용 플래그
        self.isDebug = True
        self.isFileReady = False

        if self.isDebug:
            self.debugDate = QLineEdit(self.groupBox)
            self.debugDate.setObjectName('debugDate')
            self.gridLayout3.addWidget(self.debugDate, 10, 0, 1, 1)
            self.debugDate.setPlaceholderText('디버그용 날짜입력')
        self.thread = QThread()
        self.thread.setTerminationEnabled(True)
        self.show()

    def retranslateUi(self, MainWindow):
        _translate = QCoreApplication.translate
        MainWindow.setWindowTitle(_translate('MainWindow', 'FA-M3 착공 평준화 자동화 프로그램 Rev0.00'))
        MainWindow.setWindowIcon(QIcon('.\\Logo\\logo.png'))
        self.label.setText(_translate('MainWindow', '메인 생산대수:'))
        self.label9.setText(_translate('MainWindow', '특수 생산대수:'))
        self.label10.setText(_translate('MainWindow', '전원 생산대수:'))
        self.label11.setText(_translate('MainWindow', '메인 잔업시간:'))
        # self.label12.setText(_translate('MainWindow', '특수 잔업시간:'))
        # self.label13.setText(_translate('MainWindow', '전원 잔업시간:'))       
        self.runBtn.setText(_translate('MainWindow', '실행'))
        self.label2.setText(_translate('MainWindow', '긴급오더 입력 :'))
        self.label3.setText(_translate('MainWindow', '홀딩오더 입력 :'))
        self.label4.setText(_translate('MainWindow', 'Linkage No List'))
        self.label5.setText(_translate('MainWindow', 'MSCode List'))
        self.label6.setText(_translate('MainWindow', 'Linkage No List'))
        self.label7.setText(_translate('MainWindow', 'MSCode List'))
        self.label8.setText(_translate('MainWndow', '착공지정일 입력 :'))
        self.labelDate.setText(_translate('MainWndow', '미선택'))
        self.dateBtn.setText(_translate('MainWindow', ' 착공지정일 선택 '))
        self.emgFileInputBtn.setText(_translate('MainWindow', '리스트 입력'))
        self.holdFileInputBtn.setText(_translate('MainWindow', '리스트 입력'))
        self.labelBlank.setText(_translate('MainWindow', '            '))
        self.cb_main.addItems(['잔업없음','1시간','2시간','3시간','4시간'])
        maxOrderInputFilePath = r'.\\착공량입력.xlsx'
        if not os.path.exists(maxOrderInputFilePath):
            logging.error('%s 파일이 없습니다. 착공량을 수동으로 입력해주세요.', maxOrderInputFilePath)
        else:
            df_orderInput = pd.read_excel(maxOrderInputFilePath)
            self.mainOrderinput.setText(str(df_orderInput['착공량'][0]))
            self.spOrderinput.setText(str(df_orderInput['착공량'][1]))
            self.powerOrderinput.setText(str(df_orderInput['착공량'][3]))

        logging.info('프로그램이 정상 기동했습니다')

    #착공지정일 캘린더 호출
    def selectStartDate(self):
        self.w = CalendarWindow()
        self.w.submitClicked.connect(self.getStartDate)
        self.w.show()
    
    #긴급오더 윈도우 호출
    @pyqtSlot()
    def emgWindow(self):
        self.w = UISubWindow()
        self.w.submitClicked.connect(self.getEmgListview)
        self.w.show()
    #홀딩오더 윈도우 호출
    @pyqtSlot()
    def holdWindow(self):
        self.w = UISubWindow()
        self.w.submitClicked.connect(self.getHoldListview)
        self.w.show()
    #긴급오더 리스트뷰 가져오기
    def getEmgListview(self, list):
        if len(list) > 0 :
            self.listViewEmgLinkage.setModel(list[0])
            self.listViewEmgmscode.setModel(list[1])
            logging.info('긴급오더 리스트를 정상적으로 불러왔습니다.')
        else:
            logging.error('긴급오더 리스트가 없습니다. 다시 한번 확인해주세요')
    
    #홀딩오더 리스트뷰 가져오기
    def getHoldListview(self, list):
        if len(list) > 0 :
            self.listViewHoldLinkage.setModel(list[0])
            self.listViewHoldmscode.setModel(list[1])
            logging.info('홀딩오더 리스트를 정상적으로 불러왔습니다.')
        else:
            logging.error('긴급오더 리스트가 없습니다. 다시 한번 확인해주세요')
    
    #프로그레스바 갱신
    def updateProgressbar(self, val):
        self.progressbar.setValue(val)
    #착공지정일 가져오기
    def getStartDate(self, date):
        if len(date) > 0 :
            self.labelDate.setText(date)
            logging.info('착공지정일이 %s 로 정상적으로 지정되었습니다.', date)
        else:
            logging.error('착공지정일이 선택되지 않았습니다.')

    def enableRunBtn(self):
        self.runBtn.setEnabled(True) 
        self.runBtn.setText('실행')
    def disableRunBtn(self):
        self.runBtn.setEnabled(False) 
        self.runBtn.setText('실행 중...')

    def mainShowError(self, str):
        logging.error(f'Main라인 에러 - {str}')
        self.enableRunBtn()

    def mainShowWarning(self, str):
        logging.warning(f'Main라인 경고 - {str}')

    def mainThreadEnd(self, isEnd):
        logging.info('착공이 완료되었습니다.')
        self.enableRunBtn()  

    @pyqtSlot()
    def mainStartLeveling(self):
        #마스터 데이터 불러오기 내부함수
        def loadMasterFile():
            self.isFileReady = True
            masterFileList = []
            date = datetime.datetime.today().strftime('%Y%m%d')
            if self.isDebug:
                date = self.debugDate.text()
            sosFilePath = r'.\\input\\Master_File\\' + date +r'\\SOS2.xlsx'
            mainFilePath = r'.\\input\\Master_File\\' + date +r'\\MAIN.xlsx'
            spFilePath = r'.\\input\\Master_File\\' + date +r'\\OTHER.xlsx'
            powerFilePath = r'.\\input\\Master_File\\' + date +r'\\POWER.xlsx'
            calendarFilePath = r'.\\Input\\Calendar_File\\FY' + date[2:4] + '_Calendar.xlsx'
            smtAssyFilePath = r'.\\input\\DB\\MSCode_SMT_Assy.xlsx'
            secMainListFilePath = r'.\\input\\Master_File\\' + date +r'\\100L1311('+date[4:8]+')MAIN_2차.xlsx'
            inspectFacFilePath = r'.\\input\\DB\\Inspect_Fac.xlsx'
            pathList = [sosFilePath, 
                        mainFilePath, 
                        spFilePath, 
                        powerFilePath, 
                        calendarFilePath, 
                        smtAssyFilePath, 
                        secMainListFilePath, 
                        inspectFacFilePath]
            for path in pathList:
                if os.path.exists(path):
                    file = glob.glob(path)[0]
                    masterFileList.append(file)
                else:
                    logging.error('%s 파일이 없습니다. 확인해주세요.', path)
                    self.runBtn.setEnabled(True)
                    self.isFileReady = False
            if self.isFileReady :
                logging.info('마스터 파일 및 캘린더 파일을 정상적으로 불러왔습니다.')
            return masterFileList
        
        self.disableRunBtn()
        # try:
        list_masterFile = loadMasterFile()
        list_emgHold = []
        list_emgHold.append([str(self.listViewEmgLinkage.model().data(self.listViewEmgLinkage.model().index(x,0))) for x in range(self.listViewEmgLinkage.model().rowCount())])
        list_emgHold.append([self.listViewEmgmscode.model().data(self.listViewEmgmscode.model().index(x,0)) for x in range(self.listViewEmgmscode.model().rowCount())])
        list_emgHold.append([str(self.listViewHoldLinkage.model().data(self.listViewHoldLinkage.model().index(x,0))) for x in range(self.listViewHoldLinkage.model().rowCount())])
        list_emgHold.append([self.listViewHoldmscode.model().data(self.listViewHoldmscode.model().index(x,0)) for x in range(self.listViewHoldmscode.model().rowCount())])

        if self.isFileReady :
            if len(self.mainOrderinput.text()) > 0:
                self.thread_main = MainClass(self.isDebug,
                                                self.debugDate.text(), 
                                                self.cb_main.currentText(),
                                                list_masterFile,
                                                float(self.mainOrderinput.text()), 
                                                list_emgHold)
                self.thread_main.moveToThread(self.thread)
                self.thread.started.connect(self.thread_main.run)
                self.thread_main.mainReturnError.connect(self.mainShowError)
                self.thread_main.mainReturnEnd.connect(self.mainThreadEnd)
                self.thread_main.mainReturnWarning.connect(self.mainShowWarning)
                self.thread.start()
            else:
                logging.info('메인기종 착공량이 입력되지 않아 메인기종 착공은 미실시 됩니다.')
        else:
            logging.warning('필수 파일이 없어 더이상 진행할 수 없습니다.')

if __name__ == '__main__':
    import sys
    app = QtWidgets.QApplication(sys.argv)
    ui = Ui_MainWindow()
    sys.exit(app.exec_())