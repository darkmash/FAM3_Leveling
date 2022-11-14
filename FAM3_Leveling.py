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

class ThreadClass(QObject):
    returnError = pyqtSignal(Exception)
    returnInfo = pyqtSignal(str)
    returnEnd = pyqtSignal(bool)

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
    
    def concatAlarmDetail(self, df_target, no, category, df_data, index, smtAssy, shortageCnt):
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
    def smtReflectInst(self, df_input, isRemain, dict_smtCnt, alarmDetailNo, df_alarmDetail):
        instCol = '평준화_적용_착공량'
        resultCol = 'SMT반영_착공량'
        if isRemain:
            instCol = '잔여_착공량'
            resultCol = 'SMT반영_착공량_잔여'
        for i in df_input.index:
            if df_input['PRODUCT_TYPE'][i] == 'MAIN' and 'CT' not in df_input['MS Code'][i]:
                for j in range(1,6):
                    if j == 1:
                        rowCnt = 1
                    if str(df_input[f'ROW{str(j)}'][i]) != '' and str(df_input[f'ROW{str(j)}'][i]) != 'nan':
                        rowCnt = j
                    else:
                        break
                minCnt = 9999
                for j in range(1,rowCnt+1):
                        smtAssyName = str(df_input[f'ROW{str(j)}'][i])
                        if smtAssyName != '' and smtAssyName != 'nan':
                            if df_input['긴급오더'][i] == '대상':
                                if dict_smtCnt[smtAssyName] < 0:
                                    diffCnt = df_input['미착공수주잔'][i]
                                    if dict_smtCnt[smtAssyName] + df_input['미착공수주잔'][i] > 0:
                                        diffCnt = 0 - dict_smtCnt[smtAssyName]

                                    df_alarmDetail = self.concatAlarmDetail(df_alarmDetail,
                                                                            alarmDetailNo,
                                                                            '1', 
                                                                            df_input,
                                                                            i, 
                                                                            smtAssyName, 
                                                                            diffCnt)
                                    alarmDetailNo += 1
                            else:
                                if dict_smtCnt[smtAssyName] >= df_input[instCol][i]:
                                    if minCnt > df_input[instCol][i]:
                                        minCnt = df_input[instCol][i]
                                else: 
                                    if dict_smtCnt[smtAssyName] > 0:
                                        if minCnt > dict_smtCnt[smtAssyName]:
                                            minCnt = dict_smtCnt[smtAssyName]

                                    else:
                                        minCnt = 0
                                    df_alarmDetail = self.concatAlarmDetail(df_alarmDetail,
                                                                            alarmDetailNo,
                                                                            '1', 
                                                                            df_input,
                                                                            i, 
                                                                            smtAssyName, 
                                                                            df_input['미착공수주잔'][i] - minCnt)
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
                    if smtAssyName != '' and smtAssyName != 'nan':
                        smtAssyName = str(df_input[f'ROW{str(j)}'][i])
                        dict_smtCnt[smtAssyName] -= df_input[resultCol][i]
        return [df_input, dict_smtCnt, alarmDetailNo, df_alarmDetail]

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
            df_levelingMain = pd.read_excel(self.list_masterFile[2])
            df_levelingSp = pd.read_excel(self.list_masterFile[3])
            df_levelingPower = pd.read_excel(self.list_masterFile[4])
            #미착공 대상만 추출(Main)
            df_levelingMainDropSEQ = df_levelingMain[df_levelingMain['Sequence No'].isnull()]
            df_levelingMainUndepSeq = df_levelingMain[df_levelingMain['Sequence No']=='Undep']
            df_levelingMainUncorSeq = df_levelingMain[df_levelingMain['Sequence No']=='Uncor']
            df_levelingMain = pd.concat([df_levelingMainDropSEQ, df_levelingMainUndepSeq, df_levelingMainUncorSeq])
            df_levelingMain = df_levelingMain.reset_index(drop=True)
            # df_levelingMain['미착공수량'] = df_levelingMain.groupby('Linkage Number')['Linkage Number'].transform('size')
            #미착공 대상만 추출(특수)
            df_levelingSpDropSEQ = df_levelingSp[df_levelingSp['Sequence No'].isnull()]
            df_levelingSpUndepSeq = df_levelingSp[df_levelingSp['Sequence No']=='Undep']
            df_levelingSpUncorSeq = df_levelingSp[df_levelingSp['Sequence No']=='Uncor']
            df_levelingSp = pd.concat([df_levelingSpDropSEQ, df_levelingSpUndepSeq, df_levelingSpUncorSeq])
            df_levelingSp = df_levelingSp.reset_index(drop=True)
            # df_levelingSp['미착공수량'] = df_levelingSp.groupby('Linkage Number')['Linkage Number'].transform('size')
            #미착공 대상만 추출(전원)
            df_levelingPowerDropSEQ = df_levelingPower[df_levelingPower['Sequence No'].isnull()]
            df_levelingPowerUndepSeq = df_levelingPower[df_levelingPower['Sequence No']=='Undep']
            df_levelingPowerUncorSeq = df_levelingPower[df_levelingPower['Sequence No']=='Uncor']
            df_levelingPower = pd.concat([df_levelingPowerDropSEQ, df_levelingPowerUndepSeq, df_levelingPowerUncorSeq])
            df_levelingPower = df_levelingPower.reset_index(drop=True)
            # df_levelingPower['미착공수량'] = df_levelingPower.groupby('Linkage Number')['Linkage Number'].transform('size')
            # if self.isDebug:
            #     df_levelingMain.to_excel('.\\debug\\flow1_main.xlsx')
            #     df_levelingSp.to_excel('.\\debug\\flow1_sp.xlsx')
            #     df_levelingPower.to_excel('.\\debug\\flow1_power.xlsx')
            # 미착공 수주잔 계산
            df_progressFile = pd.read_excel(self.list_masterFile[1], skiprows=3)
            df_progressFile = df_progressFile.drop(df_progressFile.index[len(df_progressFile.index) - 2:])
            df_progressFile['미착공수주잔'] = df_progressFile['수주\n수량'] - df_progressFile['생산\n지시\n수량']
            # df_progressFile = df_progressFile[df_progressFile['미착공수주잔'] != 0]
            df_progressFile['LINKAGE NO'] = df_progressFile['LINKAGE NO'].astype(str).apply(self.delComma)
            # if self.isDebug:
            #     df_progressFile.to_excel('.\\debug\\flow1.xlsx')
            df_sosFile = pd.read_excel(self.list_masterFile[0])
            df_sosFile['Linkage Number'] = df_sosFile['Linkage Number'].astype(str)
            # if self.isDebug:
                # df_sosFile.to_excel('.\\debug\\flow2.xlsx')
            #착공 대상 외 모델 삭제
            df_sosFile = df_sosFile.drop(df_sosFile[df_sosFile['MS Code'].str.contains('ZOTHER')].index)
            df_sosFile = df_sosFile.drop(df_sosFile[df_sosFile['MS Code'].str.contains('YZ')].index)
            df_sosFile = df_sosFile.drop(df_sosFile[df_sosFile['MS Code'].str.contains('SF')].index)
            df_sosFile = df_sosFile.drop(df_sosFile[df_sosFile['MS Code'].str.contains('KM')].index)
            df_sosFile = df_sosFile.drop(df_sosFile[df_sosFile['MS Code'].str.contains('TA80')].index)
            df_sosFile = df_sosFile.drop(df_sosFile[df_sosFile['MS Code'].str.contains('CT')].index)
            # if self.isDebug:
                # df_sosFile.to_excel('.\\debug\\flow3.xlsx')
            #워킹데이 캘린더 불러오기
            dfCalendar = pd.read_excel(self.list_masterFile[5])
            today = datetime.datetime.today().strftime('%Y%m%d')
            if self.isDebug:
                today = self.debugDate
            #진척 파일 - SOS2파일 Join
            df_sosFileMerge = pd.merge(df_sosFile, df_progressFile, left_on='Linkage Number', right_on='LINKAGE NO', how='left').drop_duplicates(['Linkage Number'])
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
            dfMergeLink = pd.merge(df_sosFileMerge, df_emgLinkage, on='Linkage Number', how='left')
            df_sosFileMerge['MS Code'] = df_sosFileMerge['MS Code'].apply(tuple)
            df_emgmscode['MS Code'] = df_emgmscode['MS Code'].apply(tuple)
            dfMergemscode = pd.merge(df_sosFileMerge, df_emgmscode, on='MS Code', how='left')
            dfMergeLink = pd.merge(dfMergeLink, df_holdLinkage, on='Linkage Number', how='left')
            dfMergemscode = pd.merge(dfMergemscode, df_holdmscode, on='MS Code', how='left')
            dfMergeLink['긴급오더'] = dfMergeLink['긴급오더'].combine_first(dfMergemscode['긴급오더'])
            dfMergeLink['홀딩오더'] = dfMergeLink['홀딩오더'].combine_first(dfMergemscode['홀딩오더'])
            for i in dfMergeLink.index:
                dfMergeLink['남은 워킹데이'][i] = self.checkWorkDay(dfCalendar, today, dfMergeLink['Planned Prod. Completion date'][i])
                if dfMergeLink['남은 워킹데이'][i] <= 0:
                    dfMergeLink['긴급오더'][i] = '대상'
            dfMergeLink = dfMergeLink[dfMergeLink['미착공수주잔'] != 0]
            if self.isDebug:
                dfMergeLink.to_excel('.\\debug\\flow4.xlsx')
            yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y%m%d')
            if self.isDebug:
                yesterday = (datetime.datetime.strptime(self.debugDate,'%Y%m%d') - datetime.timedelta(days=1)).strftime('%Y%m%d')
            df_SmtAssyInven = self.readDB('10.36.15.42',
                                            1521,
                                            'NEURON',
                                            'ymi_user',
                                            'ymi123!',
                                            "SELECT INV_D, PARTS_NO, CURRENT_INV_QTY FROM pdsg0040 where INV_D = TO_DATE("+ str(yesterday) +",'YYYYMMDD')")
            # df_SmtAssyInven.columns = ['INV_D','PARTS_NO','CURRENT_INV_QTY']
            df_SmtAssyInven['현재수량'] = 0
            # print(df_SmtAssyInven)
            # if self.isDebug:
            #     df_SmtAssyInven.to_excel('.\\debug\\flow5.xlsx')
            df_secOrderMainList = pd.read_excel(self.list_masterFile[7], skiprows=5)
            # print(df_secOrderMainList)
            df_joinSmt = pd.merge(df_secOrderMainList, df_SmtAssyInven, how = 'right', left_on='ASSY NO', right_on='PARTS_NO')
            df_joinSmt['대수'] = df_joinSmt['대수'].fillna(0)
            df_joinSmt['현재수량'] = df_joinSmt['CURRENT_INV_QTY'] - df_joinSmt['대수']
            df_joinSmt.to_excel('.\\debug\\flow6.xlsx')
            dict_smtCnt = {}
            for i in df_joinSmt.index:
                dict_smtCnt[df_joinSmt['PARTS_NO'][i]] = df_joinSmt['현재수량'][i]
            df_productTime = self.readDB('ymzn-bdv19az029-rds.cgbtxsdj6fjy.ap-northeast-1.rds.amazonaws.com',
                                            1521,
                                            'TPROD',
                                            'TEST_SCM',
                                            'test_scm',
                                            'SELECT * FROM FAM3_PRODUCT_TIME_TB')
            df_productTime['TotalTime'] = df_productTime['COMPONENT_SET'].apply(self.getSec) + df_productTime['MAEDZUKE'].apply(self.getSec) + df_productTime['MAUNT'].apply(self.getSec) + df_productTime['LEAD_CUTTING'].apply(self.getSec) + df_productTime['VISUAL_EXAMINATION'].apply(self.getSec) + df_productTime['PICKUP'].apply(self.getSec) + df_productTime['ASSAMBLY'].apply(self.getSec) + df_productTime['M_FUNCTION_CHECK'].apply(self.getSec) + df_productTime['A_FUNCTION_CHECK'].apply(self.getSec) + df_productTime['PERSON_EXAMINE'].apply(self.getSec)
            df_productTime['대표모델'] = df_productTime['MODEL'].str[:9]
            df_productTime = df_productTime.drop_duplicates(['대표모델'])
            df_productTime = df_productTime.reset_index(drop=True)
            if self.isDebug:
                df_productTime.to_excel('.\\debug\\flow7.xlsx')
            # print(df_productTime.columns)
            df_inspectATE = pd.read_excel(self.list_masterFile[8])
            df_ATEList = df_inspectATE.drop_duplicates(['ATE_NO'])
            df_ATEList = df_ATEList.reset_index(drop=True)
            if self.isDebug:
                df_ATEList.to_excel('.\\debug\\flow8.xlsx')
            dict_ate = {}
            max_ateCnt = 0
            overTime = 0
            if self.cb_main == '잔업없음':
                overTime = 0
            else:
                overTime = int(re.sub(r'[^0-9]', '', str(self.cb_main)))
            for i in df_ATEList.index:
                if max_ateCnt < len(str(df_ATEList['ATE_NO'][i])):
                    max_ateCnt = len(str(df_ATEList['ATE_NO'][i]))
                for j in df_ATEList['ATE_NO'][i]:
                    if overTime == 0:
                        dict_ate[j] = 460 * 60
                    else:
                        dict_ate[j] = (460+(60*overTime)) * 60
            # print(dict_ate)
            df_sosAddMainModel = pd.merge(dfMergeLink, df_inspectATE, left_on='대표모델', right_on='MSCODE', how='left')
            df_sosAddMainModel = pd.merge(df_sosAddMainModel, df_productTime[['대표모델','TotalTime','INSPECTION_EQUIPMENT']], on='대표모델', how='left')
            # df_sosAddMainModel.to_excel('.\\debug\\flow9.xlsx')
            df_mscodeSmtAssy = pd.read_excel(self.list_masterFile[6])
            df_addSmtAssy = pd.merge(df_sosAddMainModel, df_mscodeSmtAssy, left_on='MS Code', right_on='MS CODE', how='left')
            # for i in range(1,6):
            #     df_addSmtAssy = pd.merge(df_addSmtAssy, df_joinSmt[['PARTS_NO','현재수량']], left_on=f'ROW{str(i)}', right_on='PARTS_NO', how='left')
            #     df_addSmtAssy = df_addSmtAssy.rename(columns = {'현재수량':f'ROW{str(i)}_Cnt'})
            df_addSmtAssy = df_addSmtAssy.drop_duplicates(['Linkage Number'])
            df_addSmtAssy = df_addSmtAssy.reset_index(drop=True)
            # df_addSmtAssy['ATE_NO'] = ''
            # for i in df_addSmtAssy.index:
            #     for j in df_inspectATE.index:
            #         if df_addSmtAssy['대표모델'][i] == df_inspectATE['MSCODE'][j]:
            #             if str(df_addSmtAssy['PRODUCT_TYPE'][i]) == '' or str(df_addSmtAssy['PRODUCT_TYPE'][i]) == 'nan':
            #                 df_addSmtAssy['PRODUCT_TYPE'][i] = df_inspectATE['PRODUCT_TYPE'][j]
            #             if str(df_addSmtAssy['ATE_NO'][i]) == '' or str(df_addSmtAssy['ATE_NO'][i]) == 'nan': 
            #                 df_addSmtAssy['ATE_NO'][i] = df_inspectATE['ATE_NO'][j]
            #             else:
            #                 df_addSmtAssy['ATE_NO'][i] += ',' + df_inspectATE['ATE_NO'][j]
                        
            # df_addSmtAssy.to_excel('.\\debug\\flow9.xlsx')
            df_addSmtAssy['대표모델별_최소착공필요량_per_일'] = 0
            dict_integCnt = {}
            dict_minContCnt = {}
            
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
                df_addSmtAssy['대표모델별_최소착공필요량_per_일'][i] = dict_integCnt[df_addSmtAssy['대표모델'][i]]/workDay
            if self.isDebug:
                df_addSmtAssy.to_excel('.\\debug\\flow9.xlsx')
            
            dict_minContCopy = dict_minContCnt.copy()
            
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
                                                            'Planned Prod. Completion date',
                                                            '평준화_적용_착공량'],
                                                            ascending=[False,
                                                                        True,
                                                                        False])
            if self.isDebug:
                df_addSmtAssy.to_excel('.\\debug\\flow10.xlsx')
            rowCnt = 0
            df_addSmtAssy = df_addSmtAssy[df_addSmtAssy['미착공수주잔'] != 0]
            df_addSmtAssy = df_addSmtAssy[df_addSmtAssy['PRODUCT_TYPE']=='MAIN']
            df_addSmtAssy = df_addSmtAssy.reset_index(drop=True)
            df_addSmtAssy['SMT반영_착공량'] = 0
            df_alarmDetail = pd.DataFrame(columns=["No.", "분류", "L/N", "MS CODE", "SMT ASSY", "수주수량", "부족수량", "검사호기", "대상 검사시간(초)", "필요시간(초)", "완성예정일"])
            alarmDetailNo = 1
            df_addSmtAssy, dict_smtCnt, alarmDetailNo, df_alarmDetail = self.smtReflectInst(df_addSmtAssy, 
                                                                                            False, 
                                                                                            dict_smtCnt, 
                                                                                            alarmDetailNo,
                                                                                            df_alarmDetail)
            
            # for i in df_addSmtAssy.index:
            #     if df_addSmtAssy['PRODUCT_TYPE'][i] == 'MAIN' and 'CT' not in df_addSmtAssy['MS Code'][i]:
            #         for j in range(1,6):
            #             if j == 1:
            #                 rowCnt = 1
            #             if str(df_addSmtAssy[f'ROW{str(j)}'][i]) != '' and str(df_addSmtAssy[f'ROW{str(j)}'][i]) != 'nan':
            #                 rowCnt = j
            #             else:
            #                 break
            #         smtFlag = False    
            #         minCnt = 9999
            #         for j in range(1,rowCnt+1):
            #                 smtAssyName = str(df_addSmtAssy[f'ROW{str(j)}'][i])
            #                 if smtAssyName != '' and smtAssyName != 'nan':
            #                     if df_addSmtAssy['긴급오더'][i] == '대상':
            #                         # dict_smtCnt[smtAssyName] -= df_addSmtAssy['평준화_적용_착공량'][i]
            #                         # df_smtCopy = df_smtCopy.append(df_addSmtAssy.iloc[i])
            #                         if dict_smtCnt[smtAssyName] < 0:
            #                             diffCnt = df_addSmtAssy['미착공수주잔'][i]
            #                             if dict_smtCnt[smtAssyName] + df_addSmtAssy['미착공수주잔'][i] > 0:
            #                                 diffCnt = 0 - dict_smtCnt[smtAssyName]

            #                             df_alarmDetail = self.concatAlarmDetail(df_alarmDetail,
            #                                                                     alarmDetailNo,
            #                                                                     '1', 
            #                                                                     df_addSmtAssy,
            #                                                                     i, 
            #                                                                     smtAssyName, 
            #                                                                     diffCnt)
                                                                                
            #                             # df_alarmDetail = pd.concat([df_alarmDetail, 
            #                             #                             pd.DataFrame.from_records([{"No.":alarmDetailNo,
            #                             #                                                         "분류" :'1',
            #                             #                                                         "L/N" : df_addSmtAssy['Linkage Number'][i],
            #                             #                                                         "MS CODE" : df_addSmtAssy['MS Code'][i], 
            #                             #                                                         "SMT ASSY" : smtAssyName, 
            #                             #                                                         "수주수량" : df_addSmtAssy['미착공수주잔'][i], 
            #                             #                                                         "부족수량" : diffCnt, 
            #                             #                                                         "검사호기" : '-', 
            #                             #                                                         "대상 검사시간(초)" : '-', 
            #                             #                                                         "필요시간(초)" : '-', 
            #                             #                                                         "완성예정일" : df_addSmtAssy['Planned Prod. Completion date'][i]}
            #                             #                                                         ])])
            #                             alarmDetailNo += 1
            #                             # logging.warning('「당일착공 대상 : %s」, 「사양 : %s」을 착공하기에는 「SmtAssy : %s」가 「%i 대」부족합니다. SmtAssy 제작을 지시해주세요. 당일착공 대상이므로 착공은 진행합니다.',
            #                             #                 df_addSmtAssy['Linkage Number'][i],
            #                             #                 df_addSmtAssy['MS Code'][i],
            #                             #                 smtAssyName,
            #                             #                 0 - dict_smtCnt[smtAssyName])
            #                     else:
            #                         if dict_smtCnt[smtAssyName] >= df_addSmtAssy['평준화_적용_착공량'][i]:
            #                             if minCnt > df_addSmtAssy['평준화_적용_착공량'][i]:
            #                                 minCnt = df_addSmtAssy['평준화_적용_착공량'][i]
                                            
            #                             # dict_smtCnt[smtAssyName] -= df_addSmtAssy['미착공수량'][i]
            #                             # df_smtCopy = df_smtCopy.append(df_addSmtAssy.iloc[i])

            #                         else: 
            #                             if dict_smtCnt[smtAssyName] > 0:
            #                                 if minCnt > dict_smtCnt[smtAssyName]:
            #                                     minCnt = dict_smtCnt[smtAssyName]
            #                                     # df_alarmDetail = pd.concat([df_alarmDetail, 
            #                                     #                             pd.DataFrame.from_records([{"No.":alarmDetailNo,
            #                                     #                                                         "분류" :'1',
            #                                     #                                                         "L/N" : df_addSmtAssy['Linkage Number'][i],
            #                                     #                                                         "MS CODE" : df_addSmtAssy['MS Code'][i], 
            #                                     #                                                         "SMT ASSY" : smtAssyName, 
            #                                     #                                                         "수주수량" : df_addSmtAssy['미착공수주잔'][i], 
            #                                     #                                                         "부족수량" : df_addSmtAssy['미착공수주잔'][i] - minCnt, 
            #                                     #                                                         "검사호기" : '-', 
            #                                     #                                                         "대상 검사시간(초)" : '-', 
            #                                     #                                                         "필요시간(초)" : '-', 
            #                                     #                                                         "완성예정일" : df_addSmtAssy['Planned Prod. Completion date'][i]}
            #                                     #                                                         ])])
            #                                     # alarmDetailNo += 1
            #                                 # df_addSmtAssy['미착공수량'][i] = dict_smtCnt[smtAssyName]
            #                                 # dict_smtCnt[smtAssyName] -= df_addSmtAssy['미착공수량'][i]
            #                                 # df_smtCopy = df_smtCopy.append(df_addSmtAssy.iloc[i])

            #                             else:
            #                                 minCnt = 0
            #                             df_alarmDetail = self.concatAlarmDetail(df_alarmDetail,
            #                                                                     alarmDetailNo,
            #                                                                     '1', 
            #                                                                     df_addSmtAssy,
            #                                                                     i, 
            #                                                                     smtAssyName, 
            #                                                                     df_addSmtAssy['미착공수주잔'][i] - minCnt)
            #                             alarmDetailNo += 1
                                        
            #                             # logging.warning('「사양 : %s」을 착공하기에는 「SmtAssy : %s」가 부족합니다. SmtAssy 제작을 지시해주세요.',
            #                             #                 df_addSmtAssy['MS Code'][i],
            #                             #                 smtAssyName)
            #                 else:
            #                     df_alarmDetail = self.concatAlarmDetail(df_alarmDetail,
            #                                                             alarmDetailNo,
            #                                                             '기타1', 
            #                                                             df_addSmtAssy,
            #                                                             i, 
            #                                                             '미등록', 
            #                                                             0)

            #                     # df_alarmDetail = pd.concat([df_alarmDetail, 
            #                     #                             pd.DataFrame.from_records([{"No.":alarmDetailNo,
            #                     #                                                         "분류" :'기타1',
            #                     #                                                         "L/N" : df_addSmtAssy['Linkage Number'][i],
            #                     #                                                         "MS CODE" : df_addSmtAssy['MS Code'][i], 
            #                     #                                                         "SMT ASSY" : '미등록', 
            #                     #                                                         "수주수량" : df_addSmtAssy['미착공수주잔'][i], 
            #                     #                                                         "부족수량" : 0, 
            #                     #                                                         "검사호기" : '-', 
            #                     #                                                         "대상 검사시간(초)" : '-', 
            #                     #                                                         "필요시간(초)" : '-', 
            #                     #                                                         "완성예정일" : df_addSmtAssy['Planned Prod. Completion date'][i]}
            #                     #                                                         ])])
            #                     alarmDetailNo += 1
            #                     # logging.warning('「사양 : %s」의 SmtAssy가 %s 파일에 등록되지 않았습니다. 등록 후, 다시 실행해주세요.',
            #                     #                 df_addSmtAssy['MS Code'][i],
            #                     #                 self.list_masterFile[6])
            #         if minCnt != 9999:
            #             df_addSmtAssy['SMT반영_착공량'][i] = minCnt
            #         else:
            #             df_addSmtAssy['SMT반영_착공량'][i] = df_addSmtAssy['평준화_적용_착공량'][i]

            #         for j in range(1,rowCnt+1):
            #             if smtAssyName != '' and smtAssyName != 'nan':
            #                 smtAssyName = str(df_addSmtAssy[f'ROW{str(j)}'][i])
            #                 dict_smtCnt[smtAssyName] -= df_addSmtAssy['SMT반영_착공량'][i]

            if self.isDebug:
                df_alarmDetail = df_alarmDetail.reset_index(drop=True)
                df_alarmDetail.to_excel('.\\debug\\df_alarmDetail.xlsx')
            df_addSmtAssy['SMT반영_착공량_잔여'] = 0

            df_addSmtAssy, dict_smtCnt, alarmDetailNo, df_alarmDetail = self.smtReflectInst(df_addSmtAssy, 
                                                                                            True, 
                                                                                            dict_smtCnt, 
                                                                                            alarmDetailNo,
                                                                                            df_alarmDetail)
            # for i in df_addSmtAssy.index:
            #     if (df_addSmtAssy['PRODUCT_TYPE'][i] == 'MAIN') and ('CT' not in df_addSmtAssy['MS Code'][i]):
            #         for j in range(1,6):
            #             if j == 1:
            #                 rowCnt = 1
            #             if str(df_addSmtAssy[f'ROW{str(j)}'][i]) != '' and str(df_addSmtAssy[f'ROW{str(j)}'][i]) != 'nan':
            #                 rowCnt = j
            #             else:
            #                 break
            #         smtFlag = False    
            #         minCnt = 9999
            #         for j in range(1,rowCnt+1):
            #                 smtAssyName = str(df_addSmtAssy[f'ROW{str(j)}'][i])
            #                 if smtAssyName != '' and smtAssyName != 'nan':
            #                     if dict_smtCnt[smtAssyName] >= df_addSmtAssy['잔여_착공량'][i]:
            #                         if minCnt > df_addSmtAssy['잔여_착공량'][i]:
            #                             minCnt = df_addSmtAssy['잔여_착공량'][i]
                                        
            #                         # dict_smtCnt[smtAssyName] -= df_addSmtAssy['미착공수량'][i]
            #                         # df_smtCopy = df_smtCopy.append(df_addSmtAssy.iloc[i])
            #                     else:
            #                         if dict_smtCnt[smtAssyName] > 0:
            #                             if minCnt > dict_smtCnt[smtAssyName]:
            #                                 minCnt = dict_smtCnt[smtAssyName]

            #                                 # df_alarmDetail = pd.concat([df_alarmDetail, 
            #                                 #                             pd.DataFrame.from_records([{"No.":alarmDetailNo,
            #                                 #                                                         "분류" :'1',
            #                                 #                                                         "L/N" : df_addSmtAssy['Linkage Number'][i],
            #                                 #                                                         "MS CODE" : df_addSmtAssy['MS Code'][i], 
            #                                 #                                                         "SMT ASSY" : smtAssyName, 
            #                                 #                                                         "수주수량" : df_addSmtAssy['미착공수주잔'][i], 
            #                                 #                                                         "부족수량" : df_addSmtAssy['미착공수주잔'][i] - minCnt, 
            #                                 #                                                         "검사호기" : '-', 
            #                                 #                                                         "대상 검사시간(초)" : '-', 
            #                                 #                                                         "필요시간(초)" : '-', 
            #                                 #                                                         "완성예정일" : df_addSmtAssy['Planned Prod. Completion date'][i]}
            #                                 #                                                         ])])
            #                                 # alarmDetailNo += 1
            #                             # df_addSmtAssy['미착공수량'][i] = dict_smtCnt[smtAssyName]
            #                             # dict_smtCnt[smtAssyName] -= df_addSmtAssy['미착공수량'][i]
            #                             # df_smtCopy = df_smtCopy.append(df_addSmtAssy.iloc[i])
                                        
            #                         else:
            #                             minCnt = 0
                                        
            #                             # df_alarmDetail = pd.concat([df_alarmDetail, 
            #                             #                             pd.DataFrame.from_records([{"No.":alarmDetailNo,
            #                             #                                                         "분류" :'1',
            #                             #                                                         "L/N" : df_addSmtAssy['Linkage Number'][i],
            #                             #                                                         "MS CODE" : df_addSmtAssy['MS Code'][i], 
            #                             #                                                         "SMT ASSY" : smtAssyName, 
            #                             #                                                         "수주수량" : df_addSmtAssy['미착공수주잔'][i], 
            #                             #                                                         "부족수량" : df_addSmtAssy['미착공수주잔'][i] - minCnt, 
            #                             #                                                         "검사호기" : '-', 
            #                             #                                                         "대상 검사시간(초)" : '-', 
            #                             #                                                         "필요시간(초)" : '-', 
            #                             #                                                         "완성예정일" : df_addSmtAssy['Planned Prod. Completion date'][i]}
            #                             #                                                         ])])
            #                             df_alarmDetail = self.concatAlarmDetail(df_alarmDetail,
            #                                                                     alarmDetailNo,
            #                                                                     '1', 
            #                                                                     df_addSmtAssy,
            #                                                                     i, 
            #                                                                     smtAssyName, 
            #                                                                     df_addSmtAssy['미착공수주잔'][i] - minCnt)
            #                             alarmDetailNo += 1
            #                             # logging.warning('「사양 : %s」을 착공하기에는 「SmtAssy : %s」가 부족합니다. SmtAssy 제작을 지시해주세요.',
            #                             #                 df_addSmtAssy['MS Code'][i],
            #                             #                 smtAssyName)
            #                 else:
            #                     df_alarmDetail = self.concatAlarmDetail(df_alarmDetail,
            #                                                             alarmDetailNo,
            #                                                             '기타1', 
            #                                                             df_addSmtAssy,
            #                                                             i, 
            #                                                             '미등록', 
            #                                                             0)
            #                     # df_alarmDetail = pd.concat([df_alarmDetail, 
            #                     #                             pd.DataFrame.from_records([{"No.":alarmDetailNo,
            #                     #                                                         "분류" :'기타1',
            #                     #                                                         "L/N" : df_addSmtAssy['Linkage Number'][i],
            #                     #                                                         "MS CODE" : df_addSmtAssy['MS Code'][i], 
            #                     #                                                         "SMT ASSY" : '미등록', 
            #                     #                                                         "수주수량" : df_addSmtAssy['미착공수주잔'][i], 
            #                     #                                                         "부족수량" : 0, 
            #                     #                                                         "검사호기" : '-', 
            #                     #                                                         "대상 검사시간(초)" : '-', 
            #                     #                                                         "필요시간(초)" : '-', 
            #                     #                                                         "완성예정일" : df_addSmtAssy['Planned Prod. Completion date'][i]}
            #                     #                                                         ])])
            #                     alarmDetailNo += 1
            #                     # logging.warning('「사양 : %s」의 SmtAssy가 %s 파일에 등록되지 않았습니다. 등록 후, 다시 실행해주세요.',
            #                     #                 df_addSmtAssy['MS Code'][i],
            #                     #                 self.list_masterFile[6])
            #         if minCnt != 9999:
            #             df_addSmtAssy['SMT반영_착공량_잔여'][i] = minCnt
            #         else:
            #             df_addSmtAssy['SMT반영_착공량_잔여'][i] = df_addSmtAssy['잔여_착공량'][i]
                                    
            #         for j in range(1,rowCnt+1):
            #             if smtAssyName != '' and smtAssyName != 'nan':
            #                 smtAssyName = str(df_addSmtAssy[f'ROW{str(j)}'][i])
            #                 dict_smtCnt[smtAssyName] -= df_addSmtAssy['SMT반영_착공량_잔여'][i]
            #         # if str(df_addSmtAssy['ATE_NO'][i]) !='' and str(df_addSmtAssy['ATE_NO'][i]) !='nan':
            #         #     for j in range(0,len(str(df_addSmtAssy['ATE_NO'][i]))):
            #         #         df_addSmtAssy['ATE_NO'][i][j]
            if self.isDebug:
                df_addSmtAssy.to_excel('.\\debug\\flow11.xlsx')
            df_addSmtAssy['임시수량'] = 0
            df_addSmtAssy['설비능력반영_착공량'] = 0
            
            # for i in df_addSmtAssy.index:  
            #     if str(df_addSmtAssy['TotalTime'][i]) != '' and str(df_addSmtAssy['TotalTime'][i]) != 'nan':
            #         if str(df_addSmtAssy['ATE_NO'][i]) != '' and str(df_addSmtAssy['ATE_NO'][i]) != 'nan':
            #             tempTime = 0
            #             ateName = ''
            #             for ate in df_addSmtAssy['ATE_NO'][i]:
            #                 if tempTime < dict_ate[ate]:
            #                     tempTime = dict_ate[ate]
            #                     ateName = ate
            #             if dict_ate[ateName] >= df_addSmtAssy['TotalTime'][i] * df_addSmtAssy['SMT반영_착공량'][i]:
            #                 dict_ate[ateName] -= df_addSmtAssy['TotalTime'][i] * df_addSmtAssy['SMT반영_착공량'][i]
            #                 df_addSmtAssy['설비능력반영_착공량'][i] = df_addSmtAssy['SMT반영_착공량'][i]
            #             elif dict_ate[ateName] >= df_addSmtAssy['TotalTime'][i]:
            #                 tempCnt = int(df_addSmtAssy['SMT반영_착공량'][i])
            #                 for j in range(tempCnt,0,-1):
            #                     # print(dict_ate[ateName])
            #                     # print(int(df_addSmtAssy['TotalTime'][i]) * j)
            #                     if dict_ate[ateName] >= int(df_addSmtAssy['TotalTime'][i]) * j:
            #                         df_addSmtAssy['설비능력반영_착공량'][i] = j
            #                         dict_ate[ateName] -= int(df_addSmtAssy['TotalTime'][i]) * j
            #                         break
            if self.isDebug:
                df_addSmtAssy['잔여착공량_Debug'] = 0
            for i in df_addSmtAssy.index:
                if 'CT' not in df_addSmtAssy['MS Code'][i]:
                    if str(df_addSmtAssy['TotalTime'][i]) != '' and str(df_addSmtAssy['TotalTime'][i]) != 'nan':
                        if str(df_addSmtAssy['ATE_NO'][i]) != '' and str(df_addSmtAssy['ATE_NO'][i]) != 'nan':
                            tempTime = 0
                            ateName = ''
                            if df_addSmtAssy['긴급오더'][i] == '대상':
                                for ate in df_addSmtAssy['ATE_NO'][i]:
                                    tempTime = dict_ate[ate]
                                    ateName = ate
                                    if ate == df_addSmtAssy['ATE_NO'][i][0]:
                                        df_addSmtAssy['임시수량'][i] = df_addSmtAssy['SMT반영_착공량'][i]
                                    if df_addSmtAssy['임시수량'][i] != 0:
                                        if dict_ate[ateName] >= df_addSmtAssy['TotalTime'][i] * df_addSmtAssy['임시수량'][i]:
                                            if self.maxCnt >= df_addSmtAssy['임시수량'][i]:
                                                dict_ate[ateName] -= df_addSmtAssy['TotalTime'][i] * df_addSmtAssy['임시수량'][i]
                                                df_addSmtAssy['설비능력반영_착공량'][i] += df_addSmtAssy['임시수량'][i]
                                                self.maxCnt -= df_addSmtAssy['임시수량'][i]
                                                df_addSmtAssy['임시수량'][i] = 0
                                                break
                                            else:
                                                dict_ate[ateName] -= df_addSmtAssy['TotalTime'][i] * self.maxCnt
                                                df_addSmtAssy['설비능력반영_착공량'][i] += self.maxCnt
                                                df_addSmtAssy['임시수량'][i] -= self.maxCnt
                                                self.maxCnt = 0
                                                break
                                        elif dict_ate[ateName] >= df_addSmtAssy['TotalTime'][i]:
                                            tempCnt = int(df_addSmtAssy['임시수량'][i])
                                            for j in range(tempCnt,0,-1):
                                                # print(dict_ate[ateName])
                                                # print(int(df_addSmtAssy['TotalTime'][i]) * j)
                                                if dict_ate[ateName] >= int(df_addSmtAssy['TotalTime'][i]) * j:
                                                    if self.maxCnt >= j:
                                                        df_addSmtAssy['설비능력반영_착공량'][i] = int(df_addSmtAssy['설비능력반영_착공량'][i]) + j
                                                        dict_ate[ateName] -= int(df_addSmtAssy['TotalTime'][i]) * j
                                                        df_addSmtAssy['임시수량'][i] = tempCnt - j
                                                        self.maxCnt -= j
                                                        break
                                    else:
                                        break
                                if df_addSmtAssy['미착공수주잔'][i] - df_addSmtAssy['설비능력반영_착공량'][i] > 0:
                                    df_alarmDetail = pd.concat([df_alarmDetail, 
                                                            pd.DataFrame.from_records([{"No.":alarmDetailNo,
                                                                                        "분류" :'2',
                                                                                        "L/N" : df_addSmtAssy['Linkage Number'][i],
                                                                                        "MS CODE" : df_addSmtAssy['MS Code'][i], 
                                                                                        "SMT ASSY" : '-', 
                                                                                        "수주수량" : df_addSmtAssy['미착공수주잔'][i], 
                                                                                        "부족수량" : df_addSmtAssy['미착공수주잔'][i] - df_addSmtAssy['설비능력반영_착공량'][i], 
                                                                                        "검사호기" : df_addSmtAssy['ATE_NO'][i], 
                                                                                        "대상 검사시간(초)" : df_addSmtAssy['TotalTime'][i], 
                                                                                        "필요시간(초)" : (df_addSmtAssy['미착공수주잔'][i] - df_addSmtAssy['설비능력반영_착공량'][i]) * df_addSmtAssy['TotalTime'][i], 
                                                                                        "완성예정일" : df_addSmtAssy['Planned Prod. Completion date'][i]}
                                                                                        ])])
                                    alarmDetailNo += 1
                            else:
                                for ate in df_addSmtAssy['ATE_NO'][i]:
                                    if tempTime < dict_ate[ate]:
                                        tempTime = dict_ate[ate]
                                        ateName = ate
                                        if ate == df_addSmtAssy['ATE_NO'][i][0]:
                                            df_addSmtAssy['임시수량'][i] = df_addSmtAssy['SMT반영_착공량'][i]
                                        if df_addSmtAssy['임시수량'][i] != 0:
                                            if dict_ate[ateName] >= df_addSmtAssy['TotalTime'][i] * df_addSmtAssy['임시수량'][i]:
                                                if self.maxCnt >= df_addSmtAssy['임시수량'][i]:
                                                    dict_ate[ateName] -= df_addSmtAssy['TotalTime'][i] * df_addSmtAssy['임시수량'][i]
                                                    df_addSmtAssy['설비능력반영_착공량'][i] += df_addSmtAssy['임시수량'][i]
                                                    self.maxCnt -= df_addSmtAssy['임시수량'][i]
                                                    df_addSmtAssy['임시수량'][i] = 0
                                                    break
                                                else:
                                                    dict_ate[ateName] -= df_addSmtAssy['TotalTime'][i] * self.maxCnt
                                                    df_addSmtAssy['설비능력반영_착공량'][i] += self.maxCnt
                                                    df_addSmtAssy['임시수량'][i] -= self.maxCnt
                                                    self.maxCnt = 0
                                                    break
                                            elif dict_ate[ateName] >= df_addSmtAssy['TotalTime'][i]:
                                                tempCnt = int(df_addSmtAssy['임시수량'][i])
                                                for j in range(tempCnt,0,-1):
                                                    # print(dict_ate[ateName])
                                                    # print(int(df_addSmtAssy['TotalTime'][i]) * j)
                                                    if dict_ate[ateName] >= int(df_addSmtAssy['TotalTime'][i]) * j:
                                                        if self.maxCnt >= j:
                                                            df_addSmtAssy['설비능력반영_착공량'][i] = int(df_addSmtAssy['설비능력반영_착공량'][i]) + j
                                                            dict_ate[ateName] -= int(df_addSmtAssy['TotalTime'][i]) * j
                                                            df_addSmtAssy['임시수량'][i] = tempCnt - j
                                                            self.maxCnt -= j
                                                            break
                                        else:
                                            break
            df_addSmtAssy['임시수량_잔여'] = 0
            df_addSmtAssy['설비능력반영_착공량_잔여'] = 0
            df_addSmtAssy['총착공량'] = 0
            for i in df_addSmtAssy.index:
                if 'CT' not in df_addSmtAssy['MS Code'][i]:
                    if str(df_addSmtAssy['TotalTime'][i]) != '' and str(df_addSmtAssy['TotalTime'][i]) != 'nan':
                        if str(df_addSmtAssy['ATE_NO'][i]) != '' and str(df_addSmtAssy['ATE_NO'][i]) != 'nan':
                            tempTime = 0
                            ateName = ''
                            for ate in df_addSmtAssy['ATE_NO'][i]:
                                if tempTime < dict_ate[ate]:
                                    tempTime = dict_ate[ate]
                                    ateName = ate
                                    if ate == df_addSmtAssy['ATE_NO'][i][0]:
                                        df_addSmtAssy['임시수량_잔여'][i] = df_addSmtAssy['SMT반영_착공량_잔여'][i]
                                    if df_addSmtAssy['임시수량_잔여'][i] != 0:
                                        if dict_ate[ateName] >= df_addSmtAssy['TotalTime'][i] * df_addSmtAssy['임시수량_잔여'][i]:
                                            if self.maxCnt >= df_addSmtAssy['임시수량_잔여'][i]:
                                                dict_ate[ateName] -= df_addSmtAssy['TotalTime'][i] * df_addSmtAssy['임시수량_잔여'][i]
                                                df_addSmtAssy['설비능력반영_착공량_잔여'][i] += df_addSmtAssy['임시수량_잔여'][i]
                                                self.maxCnt -= df_addSmtAssy['임시수량_잔여'][i]
                                                df_addSmtAssy['임시수량_잔여'][i] = 0
                                                break
                                            else:
                                                dict_ate[ateName] -= df_addSmtAssy['TotalTime'][i] * self.maxCnt
                                                df_addSmtAssy['설비능력반영_착공량'][i] += self.maxCnt
                                                df_addSmtAssy['임시수량_잔여'][i] -= self.maxCnt
                                                self.maxCnt = 0
                                                break
                                        elif dict_ate[ateName] >= df_addSmtAssy['TotalTime'][i]:
                                            tempCnt = int(df_addSmtAssy['임시수량_잔여'][i])
                                            for j in range(tempCnt,0,-1):
                                                # print(dict_ate[ateName])
                                                # print(int(df_addSmtAssy['TotalTime'][i]) * j)
                                                if dict_ate[ateName] >= int(df_addSmtAssy['TotalTime'][i]) * j:
                                                    if self.maxCnt >= j:
                                                        df_addSmtAssy['설비능력반영_착공량_잔여'][i] = int(df_addSmtAssy['설비능력반영_착공량_잔여'][i]) + j
                                                        dict_ate[ateName] -= int(df_addSmtAssy['TotalTime'][i]) * j
                                                        df_addSmtAssy['임시수량_잔여'][i] = tempCnt - j
                                                        self.maxCnt -= j
                                                        break
                                    else:
                                        break     
                                    if self.isDebug:
                                        df_addSmtAssy['잔여착공량_Debug'][i] = self.maxCnt
                            # print(i)
                            # print(f'설비명 : {ateName}')
                            # print('남은시간 : ' + str((dict_ate[ateName])))
            # print(dict_ate)
            if self.isDebug:
                df_addSmtAssy.to_excel('.\\debug\\flow12.xlsx')
                df_alarmDetail = df_alarmDetail.reset_index(drop=True)
                df_alarmDetail.to_excel('.\\debug\\df_alarmDetail.xlsx')
            # df_addSmtAssy['대표모델별_누적착공량'] = ''
            # dict_integAteCnt = {}
            # for i in df_addSmtAssy.index:
            #     if 'CT' not in df_addSmtAssy['MS Code'][i]:
            #         if df_addSmtAssy['대표모델'][i] in dict_integAteCnt:
            #             dict_integAteCnt[df_addSmtAssy['대표모델'][i]] += int(df_addSmtAssy['설비능력반영_착공량'][i])
            #         else:
            #             dict_integAteCnt[df_addSmtAssy['대표모델'][i]] = int(df_addSmtAssy['설비능력반영_착공량'][i])
            #         df_addSmtAssy['대표모델별_누적착공량'][i] = dict_integAteCnt[df_addSmtAssy['대표모델'][i]]
            # for key, value in dict_minContCnt.items():
            #     if key in dict_integAteCnt:
            #         if value[0] > dict_integAteCnt[key]:
            #             logging.warning('「%s」 사양이 「완성지정일: %s」 까지 오늘 「착공수량: %i 대」로는 착공량 부족이 예상됩니다. 최소 필요 착공량은 「%i 대」 입니다.', 
            #                 key, 
            #                 str(value[1]),
            #                 dict_integAteCnt[key],
            #                 math.ceil(value[0]))      
            # df_addSmtAssy.to_excel('.\\debug\\flow13.xlsx')
            # self.runBtn.setEnabled(True)

            self.returnEnd.emit(True)
            self.thread().quit()
        except Exception as e:
            self.returnError.emit(e)
            self.thread().quit()
            return
        
class CustomFormatter(logging.Formatter):
    FORMATS = {
        logging.ERROR:   ('[%(asctime)s] %(levelname)s:%(message)s','white'),
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
        self.runBtn.clicked.connect(self.startLeveling)
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
        # self.cb_sp.addItems(['잔업없음','1시간','2시간','3시간','4시간'])
        # self.cb_power.addItems(['잔업없음','1시간','2시간','3시간','4시간'])
        # try:
        #     self.df_productTime = self.loadProductTimeDb()
        #     # self.df_productTime.to_excel(r'.\result.xlsx')
        # except Exception as e:
        #     logging.error('검사시간DB 불러오기에 실패했습니다. 관리자에게 문의해주세요.')
        #     logging.exception(e, exc_info=True)      
        # try:
        #     self.df_smt = self.loadSmtDb
        # except Exception as e:
        #     logging.error('SMT Assy 재고량 DB 불러오기에 실패했습니다. 관리자에게 문의해주세요.')
        #     logging.exception(e, exc_info=True)   
        logging.info('프로그램이 정상 기동했습니다')
    # #생산시간 DB로부터 불러오기
    # def loadProductTimeDb(self):
    #     location = r'.\\instantclient_21_6'
    #     os.environ["PATH"] = location + ";" + os.environ["PATH"]
    #     dsn = cx_Oracle.makedsn("ymzn-bdv19az029-rds.cgbtxsdj6fjy.ap-northeast-1.rds.amazonaws.com", 1521, "tprod")
    #     db = cx_Oracle.connect("TEST_SCM","test_scm", dsn)
    #     cursor= db.cursor()
    #     cursor.execute("SELECT MODEL, COMPONENT_SET, MAEDZUKE, MAUNT, LEAD_CUTTING, VISUAL_EXAMINATION, PICKUP, ASSAMBLY, M_FUNCTION_CHECK, A_FUNCTION_CHECK, PERSON_EXAMINE, INSPECTION_EQUIPMENT FROM FAM3_PRODUCT_TIME_TB")
    #     out_data = cursor.fetchall()
    #     df_productTime = pd.DataFrame(out_data)
    #     df_productTime.columns = ["MODEL", "COMPONENT_SET", "MAEDZUKE", "MAUNT", "LEAD_CUTTING", "VISUAL_EXAMINATION", "PICKUP", "ASSAMBLY", "M_FUNCTION_CHECK", "A_FUNCTION_CHECK", "PERSON_EXAMINE", "INSPECTION_EQUIPMENT"]
    #     return df_productTime
    # #SMT Assy 재고 DB로부터 불러오기
    # def loadSmtDb(self):
    #     location = r'.\\instantclient_21_6'
    #     os.environ["PATH"] = location + ";" + os.environ["PATH"]
    #     dsn = cx_Oracle.makedsn("10.36.15.42", 1521, "NEURON")
    #     db = cx_Oracle.connect("ymi_user","ymi123!", dsn)
    #     cursor= db.cursor()
    #     cursor.execute("SELECT INV_D, PARTS_NO, CURRENT_INV_QTY FROM pdsg0040 where INV_D = TO_DATE(TO_CHAR(SYSDATE-1,'YYYYMMDD'),'YYYYMMDD')")
    #     out_data = cursor.fetchall()
    #     df_smt = pd.DataFrame(out_data)
    #     df_smt.columns = ["출력일", "PARTS NO", "TOTAL 재고"]
    #     return df_smt
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

    def showError(self, str):
        logging.warning(str)
        self.runBtn.setEnabled(True)   

    def mainThreadEnd(self, isEnd):
        logging.info('착공이 완료되었습니다.')
        self.runBtn.setEnabled(True)   

    @pyqtSlot()
    def startLeveling(self):
        #마스터 데이터 불러오기 내부함수
        def loadMasterFile():
            self.isFileReady = True
            masterFileList = []
            date = datetime.datetime.today().strftime('%Y%m%d')
            if self.isDebug:
                date = self.debugDate.text()
            sosFilePath = r'.\\input\\Master_File\\' + date +r'\\SOS2.xlsx'
            progressFilePath = r'.\\input\\Master_File\\' + date +r'\\진척.xlsx'
            mainFilePath = r'.\\input\\Master_File\\' + date +r'\\MAIN.xlsx'
            spFilePath = r'.\\input\\Master_File\\' + date +r'\\OTHER.xlsx'
            powerFilePath = r'.\\input\\Master_File\\' + date +r'\\POWER.xlsx'
            calendarFilePath = r'.\\Input\\Calendar_File\\FY' + date[2:4] + '_Calendar.xlsx'
            smtAssyFilePath = r'.\\input\\DB\\MSCode_SMT_Assy.xlsx'
            # usedSmtAssyFilePath = r'.\\input\\DB\\MSCode_SMT_Assy.xlsx'
            secMainListFilePath = r'.\\input\\Master_File\\' + date +r'\\100L1311('+date[4:8]+')MAIN_2차.xlsx'
            inspectFacFilePath = r'.\\input\\DB\\Inspect_Fac.xlsx'
            pathList = [sosFilePath, 
                        progressFilePath, 
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
        
        self.runBtn.setEnabled(False)   
        # try:
        list_masterFile = loadMasterFile()
        list_emgHold = []
        list_emgHold.append([str(self.listViewEmgLinkage.model().data(self.listViewEmgLinkage.model().index(x,0))) for x in range(self.listViewEmgLinkage.model().rowCount())])
        list_emgHold.append([[self.listViewEmgmscode.model().data(self.listViewEmgmscode.model().index(x,0)) for x in range(self.listViewEmgmscode.model().rowCount())]])
        list_emgHold.append([str(self.listViewHoldLinkage.model().data(self.listViewHoldLinkage.model().index(x,0))) for x in range(self.listViewHoldLinkage.model().rowCount())])
        list_emgHold.append([self.listViewHoldmscode.model().data(self.listViewHoldmscode.model().index(x,0)) for x in range(self.listViewHoldmscode.model().rowCount())])

        if self.isFileReady :
            if len(self.mainOrderinput.text()) > 0:
                self.thread_main = ThreadClass(self.isDebug,
                                            self.debugDate.text(), 
                                            self.cb_main.currentText(),
                                            list_masterFile,
                                            float(self.mainOrderinput.text()), 
                                            list_emgHold)
                self.thread_main.moveToThread(self.thread)
                self.thread.started.connect(self.thread_main.run)
                self.thread_main.returnError.connect(self.showError)
                self.thread_main.returnEnd.connect(self.mainThreadEnd)
                self.thread.start()
            else:
                logging.info('메인기종 착공량이 입력되지 않아 메인기종 착공은 미실시 됩니다.')
        else:
            logging.warning('필수 파일이 없어 더이상 진행할 수 없습니다.')


        #     if len(list_masterFile) > 0 :
        #         mainOrderCnt = 0.0
        #         spOrderCnt = 0.0
        #         powerOrderCnt = 0.0
        #         #착공량 미입력시의 처리 (추후 멀티프로세싱 적용 시를 위한 처리)
        #         if len(self.mainOrderinput.text()) <= 0:
        #             logging.info('메인기종 착공량이 입력되지 않아 메인기종 착공은 미실시 됩니다.')
        #         else:
        #             mainOrderCnt = float(self.mainOrderinput.text())
        #         if len(self.spOrderinput.text()) <= 0:
        #             logging.info('특수기종 착공량이 입력되지 않아 특수기종 착공은 미실시 됩니다.')
        #         else:
        #             spOrderCnt = float(self.spOrderinput.text())
        #         if len(self.powerOrderinput.text()) <= 0:
        #             logging.info('전원기종 착공량이 입력되지 않아 전원기종 착공은 미실시 됩니다.')            
        #         else:
        #             powerOrderCnt = float(self.powerOrderinput.text())
        #         #긴급오더, 홀딩오더 불러오기
        #         emgLinkage = [str(self.listViewEmgLinkage.model().data(self.listViewEmgLinkage.model().index(x,0))) for x in range(self.listViewEmgLinkage.model().rowCount())]
        #         emgmscode = [self.listViewEmgmscode.model().data(self.listViewEmgmscode.model().index(x,0)) for x in range(self.listViewEmgmscode.model().rowCount())]
        #         holdLinkage = [str(self.listViewHoldLinkage.model().data(self.listViewHoldLinkage.model().index(x,0))) for x in range(self.listViewHoldLinkage.model().rowCount())]
        #         holdmscode = [self.listViewHoldmscode.model().data(self.listViewHoldmscode.model().index(x,0)) for x in range(self.listViewHoldmscode.model().rowCount())]        
        #         #긴급오더, 홀딩오더 데이터프레임화
        #         df_emgLinkage = pd.DataFrame({'Linkage Number':emgLinkage})
        #         df_emgmscode = pd.DataFrame({'MS Code':emgmscode})
        #         df_holdLinkage = pd.DataFrame({'Linkage Number':holdLinkage})
        #         df_holdmscode = pd.DataFrame({'MS Code':holdmscode})
        #         #각 Linkage Number 컬럼의 타입을 일치시킴
        #         df_emgLinkage['Linkage Number'] = df_emgLinkage['Linkage Number'].astype(np.int64)
        #         df_holdLinkage['Linkage Number'] = df_holdLinkage['Linkage Number'].astype(np.int64)
                
        #         #긴급오더, 홍딩오더 Join 전 컬럼 추가
        #         df_emgLinkage['긴급오더'] = '대상'
        #         df_emgmscode['긴급오더'] = '대상'
        #         df_holdLinkage['홀딩오더'] = '대상'
        #         df_holdmscode['홀딩오더'] = '대상'
        #         #레벨링 리스트 불러오기(멀티프로세싱 적용 후, 분리 예정)
        #         df_levelingMain = pd.read_excel(list_masterFile[2])
        #         df_levelingSp = pd.read_excel(list_masterFile[3])
        #         df_levelingPower = pd.read_excel(list_masterFile[4])
        #         #미착공 대상만 추출(Main)
        #         df_levelingMainDropSEQ = df_levelingMain[df_levelingMain['Sequence No'].isnull()]
        #         df_levelingMainUndepSeq = df_levelingMain[df_levelingMain['Sequence No']=='Undep']
        #         df_levelingMainUncorSeq = df_levelingMain[df_levelingMain['Sequence No']=='Uncor']
        #         df_levelingMain = pd.concat([df_levelingMainDropSEQ, df_levelingMainUndepSeq, df_levelingMainUncorSeq])
        #         df_levelingMain = df_levelingMain.reset_index(drop=True)
        #         # df_levelingMain['미착공수량'] = df_levelingMain.groupby('Linkage Number')['Linkage Number'].transform('size')
        #         #미착공 대상만 추출(특수)
        #         df_levelingSpDropSEQ = df_levelingSp[df_levelingSp['Sequence No'].isnull()]
        #         df_levelingSpUndepSeq = df_levelingSp[df_levelingSp['Sequence No']=='Undep']
        #         df_levelingSpUncorSeq = df_levelingSp[df_levelingSp['Sequence No']=='Uncor']
        #         df_levelingSp = pd.concat([df_levelingSpDropSEQ, df_levelingSpUndepSeq, df_levelingSpUncorSeq])
        #         df_levelingSp = df_levelingSp.reset_index(drop=True)
        #         # df_levelingSp['미착공수량'] = df_levelingSp.groupby('Linkage Number')['Linkage Number'].transform('size')
        #         #미착공 대상만 추출(전원)
        #         df_levelingPowerDropSEQ = df_levelingPower[df_levelingPower['Sequence No'].isnull()]
        #         df_levelingPowerUndepSeq = df_levelingPower[df_levelingPower['Sequence No']=='Undep']
        #         df_levelingPowerUncorSeq = df_levelingPower[df_levelingPower['Sequence No']=='Uncor']
        #         df_levelingPower = pd.concat([df_levelingPowerDropSEQ, df_levelingPowerUndepSeq, df_levelingPowerUncorSeq])
        #         df_levelingPower = df_levelingPower.reset_index(drop=True)
        #         # df_levelingPower['미착공수량'] = df_levelingPower.groupby('Linkage Number')['Linkage Number'].transform('size')
        #         # if self.isDebug:
        #         #     df_levelingMain.to_excel('.\\debug\\flow1_main.xlsx')
        #         #     df_levelingSp.to_excel('.\\debug\\flow1_sp.xlsx')
        #         #     df_levelingPower.to_excel('.\\debug\\flow1_power.xlsx')
        #         # 미착공 수주잔 계산
        #         df_progressFile = pd.read_excel(list_masterFile[1], skiprows=3)
        #         df_progressFile = df_progressFile.drop(df_progressFile.index[len(df_progressFile.index) - 2:])
        #         df_progressFile['미착공수주잔'] = df_progressFile['수주\n수량'] - df_progressFile['생산\n지시\n수량']
        #         # df_progressFile = df_progressFile[df_progressFile['미착공수주잔'] != 0]
        #         df_progressFile['LINKAGE NO'] = df_progressFile['LINKAGE NO'].astype(str).apply(delComma)
        #         # if self.isDebug:
        #         #     df_progressFile.to_excel('.\\debug\\flow1.xlsx')
        #         df_sosFile = pd.read_excel(list_masterFile[0])
        #         df_sosFile['Linkage Number'] = df_sosFile['Linkage Number'].astype(str)
        #         # if self.isDebug:
        #             # df_sosFile.to_excel('.\\debug\\flow2.xlsx')
        #         #착공 대상 외 모델 삭제
        #         df_sosFile = df_sosFile.drop(df_sosFile[df_sosFile['MS Code'].str.contains('ZOTHER')].index)
        #         df_sosFile = df_sosFile.drop(df_sosFile[df_sosFile['MS Code'].str.contains('YZ')].index)
        #         df_sosFile = df_sosFile.drop(df_sosFile[df_sosFile['MS Code'].str.contains('SF')].index)
        #         df_sosFile = df_sosFile.drop(df_sosFile[df_sosFile['MS Code'].str.contains('KM')].index)
        #         df_sosFile = df_sosFile.drop(df_sosFile[df_sosFile['MS Code'].str.contains('TA80')].index)
        #         # if self.isDebug:
        #             # df_sosFile.to_excel('.\\debug\\flow3.xlsx')
        #         #워킹데이 캘린더 불러오기
        #         dfCalendar = pd.read_excel(list_masterFile[5])
        #         today = datetime.datetime.today().strftime('%Y%m%d')
        #         if self.isDebug:
        #             today = self.debugDate.text()
        #         #진척 파일 - SOS2파일 Join
        #         df_sosFileMerge = pd.merge(df_sosFile, df_progressFile, left_on='Linkage Number', right_on='LINKAGE NO', how='left').drop_duplicates(['Linkage Number'])
        #         #위 파일을 완성지정일 기준 오름차순 정렬 및 인덱스 재설정
        #         df_sosFileMerge = df_sosFileMerge.sort_values(by=['Planned Prod. Completion date'],
        #                                                         ascending=[True])
        #         df_sosFileMerge = df_sosFileMerge.reset_index(drop=True)
                
        #         #대표모델 Column 생성
        #         df_sosFileMerge['대표모델'] = df_sosFileMerge['MS Code'].str[:9]
        #         #남은 워킹데이 Column 생성
        #         df_sosFileMerge['남은 워킹데이'] = 0
        #         #긴급오더, 홀딩오더 Linkage Number Column 타입 일치
        #         df_emgLinkage['Linkage Number'] = df_emgLinkage['Linkage Number'].astype(str)
        #         df_holdLinkage['Linkage Number'] = df_holdLinkage['Linkage Number'].astype(str)
        #         #긴급오더, 홀딩오더와 위 Sos파일을 Join
        #         dfMergeLink = pd.merge(df_sosFileMerge, df_emgLinkage, on='Linkage Number', how='left')
        #         dfMergemscode = pd.merge(df_sosFileMerge, df_emgmscode, on='MS Code', how='left')
        #         dfMergeLink = pd.merge(dfMergeLink, df_holdLinkage, on='Linkage Number', how='left')
        #         dfMergemscode = pd.merge(dfMergemscode, df_holdmscode, on='MS Code', how='left')
        #         dfMergeLink['긴급오더'] = dfMergeLink['긴급오더'].combine_first(dfMergemscode['긴급오더'])
        #         dfMergeLink['홀딩오더'] = dfMergeLink['홀딩오더'].combine_first(dfMergemscode['홀딩오더'])
        #         for i in dfMergeLink.index:
        #             dfMergeLink['남은 워킹데이'][i] = checkWorkDay(dfCalendar, today, dfMergeLink['Planned Prod. Completion date'][i])
        #             if dfMergeLink['남은 워킹데이'][i] <= 0:
        #                 dfMergeLink['긴급오더'][i] = '대상'
        #         dfMergeLink = dfMergeLink[dfMergeLink['미착공수주잔'] != 0]
        #         if self.isDebug:
        #             dfMergeLink.to_excel('.\\debug\\flow4.xlsx')
        #         yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y%m%d')
        #         if self.isDebug:
        #             yesterday = (datetime.datetime.strptime(self.debugDate.text(),'%Y%m%d') - datetime.timedelta(days=1)).strftime('%Y%m%d')
        #         df_SmtAssyInven = readDB('10.36.15.42',
        #                                 1521,
        #                                 'NEURON',
        #                                 'ymi_user',
        #                                 'ymi123!',
        #                                 "SELECT INV_D, PARTS_NO, CURRENT_INV_QTY FROM pdsg0040 where INV_D = TO_DATE("+ str(yesterday) +",'YYYYMMDD')")
        #         # df_SmtAssyInven.columns = ['INV_D','PARTS_NO','CURRENT_INV_QTY']
        #         df_SmtAssyInven['현재수량'] = 0
        #         # print(df_SmtAssyInven)
        #         # if self.isDebug:
        #         #     df_SmtAssyInven.to_excel('.\\debug\\flow5.xlsx')
        #         df_secOrderMainList = pd.read_excel(list_masterFile[7], skiprows=5)
        #         # print(df_secOrderMainList)
        #         df_joinSmt = pd.merge(df_secOrderMainList, df_SmtAssyInven, how = 'right', left_on='ASSY NO', right_on='PARTS_NO')
        #         df_joinSmt['대수'] = df_joinSmt['대수'].fillna(0)
        #         df_joinSmt['현재수량'] = df_joinSmt['CURRENT_INV_QTY'] - df_joinSmt['대수']
        #         df_joinSmt.to_excel('.\\debug\\flow6.xlsx')
        #         dict_smtCnt = {}
        #         for i in df_joinSmt.index:
        #             dict_smtCnt[df_joinSmt['PARTS_NO'][i]] = df_joinSmt['현재수량'][i]
        #         df_productTime = readDB('ymzn-bdv19az029-rds.cgbtxsdj6fjy.ap-northeast-1.rds.amazonaws.com',
        #                                 1521,
        #                                 'TPROD',
        #                                 'TEST_SCM',
        #                                 'test_scm',
        #                                 'SELECT * FROM FAM3_PRODUCT_TIME_TB')
        #         df_productTime['TotalTime'] = df_productTime['COMPONENT_SET'].apply(getSec) + df_productTime['MAEDZUKE'].apply(getSec) + df_productTime['MAUNT'].apply(getSec) + df_productTime['LEAD_CUTTING'].apply(getSec) + df_productTime['VISUAL_EXAMINATION'].apply(getSec) + df_productTime['PICKUP'].apply(getSec) + df_productTime['ASSAMBLY'].apply(getSec) + df_productTime['M_FUNCTION_CHECK'].apply(getSec) + df_productTime['A_FUNCTION_CHECK'].apply(getSec) + df_productTime['PERSON_EXAMINE'].apply(getSec)
        #         df_productTime['대표모델'] = df_productTime['MODEL'].str[:9]
        #         df_productTime = df_productTime.drop_duplicates(['대표모델'])
        #         df_productTime = df_productTime.reset_index(drop=True)
        #         df_productTime.to_excel('.\\debug\\flow7.xlsx')
        #         # print(df_productTime.columns)
        #         df_inspectATE = pd.read_excel(list_masterFile[8])
        #         df_ATEList = df_inspectATE.drop_duplicates(['ATE_NO'])
        #         df_ATEList = df_ATEList.reset_index(drop=True)
        #         df_ATEList.to_excel('.\\debug\\flow8.xlsx')
        #         dict_ate = {}
        #         max_ateCnt = 0
        #         overTime = 0
        #         if self.cb_main.currentText() == '잔업없음':
        #             overTime = 0
        #         else:
        #             overTime = int(re.sub(r'[^0-9]', '', str(self.cb_main.currentText())))
        #         for i in df_ATEList.index:
        #             if max_ateCnt < len(str(df_ATEList['ATE_NO'][i])):
        #                 max_ateCnt = len(str(df_ATEList['ATE_NO'][i]))
        #             for j in df_ATEList['ATE_NO'][i]:
        #                 if overTime == 0:
        #                     dict_ate[j] = 460 * 60
        #                 else:
        #                     dict_ate[j] = (460+(60*overTime)) * 60
        #         # print(dict_ate)
        #         df_sosAddMainModel = pd.merge(dfMergeLink, df_inspectATE, left_on='대표모델', right_on='MSCODE', how='left')
        #         df_sosAddMainModel = pd.merge(df_sosAddMainModel, df_productTime[['대표모델','TotalTime','INSPECTION_EQUIPMENT']], on='대표모델', how='left')
        #         # df_sosAddMainModel.to_excel('.\\debug\\flow9.xlsx')
        #         df_mscodeSmtAssy = pd.read_excel(list_masterFile[6])
        #         df_addSmtAssy = pd.merge(df_sosAddMainModel, df_mscodeSmtAssy, left_on='MS Code', right_on='MS CODE', how='left')
        #         # for i in range(1,6):
        #         #     df_addSmtAssy = pd.merge(df_addSmtAssy, df_joinSmt[['PARTS_NO','현재수량']], left_on=f'ROW{str(i)}', right_on='PARTS_NO', how='left')
        #         #     df_addSmtAssy = df_addSmtAssy.rename(columns = {'현재수량':f'ROW{str(i)}_Cnt'})
        #         df_addSmtAssy = df_addSmtAssy.drop_duplicates(['Linkage Number'])
        #         df_addSmtAssy = df_addSmtAssy.reset_index(drop=True)
        #         # df_addSmtAssy['ATE_NO'] = ''
        #         # for i in df_addSmtAssy.index:
        #         #     for j in df_inspectATE.index:
        #         #         if df_addSmtAssy['대표모델'][i] == df_inspectATE['MSCODE'][j]:
        #         #             if str(df_addSmtAssy['PRODUCT_TYPE'][i]) == '' or str(df_addSmtAssy['PRODUCT_TYPE'][i]) == 'nan':
        #         #                 df_addSmtAssy['PRODUCT_TYPE'][i] = df_inspectATE['PRODUCT_TYPE'][j]
        #         #             if str(df_addSmtAssy['ATE_NO'][i]) == '' or str(df_addSmtAssy['ATE_NO'][i]) == 'nan': 
        #         #                 df_addSmtAssy['ATE_NO'][i] = df_inspectATE['ATE_NO'][j]
        #         #             else:
        #         #                 df_addSmtAssy['ATE_NO'][i] += ',' + df_inspectATE['ATE_NO'][j]
                            
        #         # df_addSmtAssy.to_excel('.\\debug\\flow9.xlsx')
        #         df_addSmtAssy['대표모델별_최소착공필요량_per_일'] = 0
        #         dict_integCnt = {}
        #         dict_minContCnt = {}
                
        #         for i in df_addSmtAssy.index:
        #             if df_addSmtAssy['대표모델'][i] in dict_integCnt:
        #                 dict_integCnt[df_addSmtAssy['대표모델'][i]] += int(df_addSmtAssy['미착공수주잔'][i])
        #             else:
        #                 dict_integCnt[df_addSmtAssy['대표모델'][i]] = int(df_addSmtAssy['미착공수주잔'][i])
        #             if df_addSmtAssy['남은 워킹데이'][i] == 0:
        #                 workDay = 1
        #             else:
        #                 workDay = df_addSmtAssy['남은 워킹데이'][i]
                    
        #             if len(dict_minContCnt) > 0:
        #                 if df_addSmtAssy['대표모델'][i] in dict_minContCnt:
        #                     if dict_minContCnt[df_addSmtAssy['대표모델'][i]][0] < math.ceil(dict_integCnt[df_addSmtAssy['대표모델'][i]]/workDay):
        #                         dict_minContCnt[df_addSmtAssy['대표모델'][i]][0] = math.ceil(dict_integCnt[df_addSmtAssy['대표모델'][i]]/workDay)
        #                         dict_minContCnt[df_addSmtAssy['대표모델'][i]][1] = df_addSmtAssy['Planned Prod. Completion date'][i]
        #                 else:
        #                     dict_minContCnt[df_addSmtAssy['대표모델'][i]] = [math.ceil(dict_integCnt[df_addSmtAssy['대표모델'][i]]/workDay),
        #                                                                 df_addSmtAssy['Planned Prod. Completion date'][i]]
        #             else:
        #                 dict_minContCnt[df_addSmtAssy['대표모델'][i]] = [math.ceil(dict_integCnt[df_addSmtAssy['대표모델'][i]]/workDay),
        #                                                                 df_addSmtAssy['Planned Prod. Completion date'][i]]
        #             df_addSmtAssy['대표모델별_최소착공필요량_per_일'][i] = dict_integCnt[df_addSmtAssy['대표모델'][i]]/workDay
        #         df_addSmtAssy.to_excel('.\\debug\\flow9.xlsx')
                
        #         dict_minContCopy = dict_minContCnt.copy()
                
        #         df_addSmtAssy['평준화_적용_착공량'] = 0
        #         for i in df_addSmtAssy.index:
        #             if df_addSmtAssy['대표모델'][i] in dict_minContCopy:
        #                 if dict_minContCopy[df_addSmtAssy['대표모델'][i]][0] >= int(df_addSmtAssy['미착공수주잔'][i]):
        #                     df_addSmtAssy['평준화_적용_착공량'][i] = int(df_addSmtAssy['미착공수주잔'][i])
        #                     dict_minContCopy[df_addSmtAssy['대표모델'][i]][0] -= int(df_addSmtAssy['미착공수주잔'][i])
        #                 else:
        #                     df_addSmtAssy['평준화_적용_착공량'][i] = dict_minContCopy[df_addSmtAssy['대표모델'][i]][0]
        #                     dict_minContCopy[df_addSmtAssy['대표모델'][i]][0] = 0
        #         df_addSmtAssy['잔여_착공량'] = df_addSmtAssy['미착공수주잔'] - df_addSmtAssy['평준화_적용_착공량']
        #         df_addSmtAssy = df_addSmtAssy.sort_values(by=['긴급오더',
        #                                                         'Planned Prod. Completion date',
        #                                                         '평준화_적용_착공량'],
        #                                                         ascending=[False,
        #                                                                     True,
        #                                                                     False])
        #         df_addSmtAssy.to_excel('.\\debug\\flow10.xlsx')
        #         rowCnt = 0
        #         df_addSmtAssy = df_addSmtAssy[df_addSmtAssy['PRODUCT_TYPE']=='MAIN']
        #         df_addSmtAssy = df_addSmtAssy.reset_index(drop=True)
        #         df_addSmtAssy['SMT반영_착공량'] = 0
        #         for i in df_addSmtAssy.index:
        #             if df_addSmtAssy['PRODUCT_TYPE'][i] == 'MAIN' and 'CT' not in df_addSmtAssy['MS Code'][i]:
        #                 for j in range(1,6):
        #                     if j == 1:
        #                         rowCnt = 1
        #                     if str(df_addSmtAssy[f'ROW{str(j)}'][i]) != '' and str(df_addSmtAssy[f'ROW{str(j)}'][i]) != 'nan':
        #                         rowCnt = j
        #                     else:
        #                         break
        #                 smtFlag = False    
        #                 minCnt = 9999
        #                 for j in range(1,rowCnt+1):
        #                         smtAssyName = str(df_addSmtAssy[f'ROW{str(j)}'][i])
        #                         if smtAssyName != '' and smtAssyName != 'nan':
        #                             if df_addSmtAssy['긴급오더'][i] == '대상':
        #                                 # dict_smtCnt[smtAssyName] -= df_addSmtAssy['평준화_적용_착공량'][i]
        #                                 # df_smtCopy = df_smtCopy.append(df_addSmtAssy.iloc[i])
        #                                 if dict_smtCnt[smtAssyName] < 0:
        #                                     logging.warning('「당일착공 대상 : %s」, 「사양 : %s」을 착공하기에는 「SmtAssy : %s」가 「%i 대」부족합니다. SmtAssy 제작을 지시해주세요. 당일착공 대상이므로 착공은 진행합니다.',
        #                                                     df_addSmtAssy['Linkage Number'][i],
        #                                                     df_addSmtAssy['MS Code'][i],
        #                                                     smtAssyName,
        #                                                     0 - dict_smtCnt[smtAssyName])
        #                             else:
        #                                 if dict_smtCnt[smtAssyName] >= df_addSmtAssy['평준화_적용_착공량'][i]:
        #                                     if minCnt > df_addSmtAssy['평준화_적용_착공량'][i]:
        #                                         minCnt = df_addSmtAssy['평준화_적용_착공량'][i]
                                                
        #                                     # dict_smtCnt[smtAssyName] -= df_addSmtAssy['미착공수량'][i]
        #                                     # df_smtCopy = df_smtCopy.append(df_addSmtAssy.iloc[i])
        #                                 elif dict_smtCnt[smtAssyName] > 0:
        #                                     if minCnt > dict_smtCnt[smtAssyName]:
        #                                         minCnt = dict_smtCnt[smtAssyName]
        #                                     # df_addSmtAssy['미착공수량'][i] = dict_smtCnt[smtAssyName]
        #                                     # dict_smtCnt[smtAssyName] -= df_addSmtAssy['미착공수량'][i]
        #                                     # df_smtCopy = df_smtCopy.append(df_addSmtAssy.iloc[i])
        #                                 else:
        #                                     minCnt = 0
        #                                     logging.warning('「사양 : %s」을 착공하기에는 「SmtAssy : %s」가 부족합니다. SmtAssy 제작을 지시해주세요.',
        #                                                     df_addSmtAssy['MS Code'][i],
        #                                                     smtAssyName)
        #                         else:
        #                             logging.warning('「사양 : %s」의 SmtAssy가 %s 파일에 등록되지 않았습니다. 등록 후, 다시 실행해주세요.',
        #                                             df_addSmtAssy['MS Code'][i],
        #                                             list_masterFile[6])
        #                 if minCnt != 9999:
        #                     df_addSmtAssy['SMT반영_착공량'][i] = minCnt
        #                 else:
        #                     df_addSmtAssy['SMT반영_착공량'][i] = df_addSmtAssy['평준화_적용_착공량'][i]
                                        
        #                 for j in range(1,rowCnt+1):
        #                     if smtAssyName != '' and smtAssyName != 'nan':
        #                         smtAssyName = str(df_addSmtAssy[f'ROW{str(j)}'][i])
        #                         dict_smtCnt[smtAssyName] -= df_addSmtAssy['SMT반영_착공량'][i]
        #         df_addSmtAssy['SMT반영_착공량_잔여'] = 0
        #         for i in df_addSmtAssy.index:
        #             if df_addSmtAssy['PRODUCT_TYPE'][i] == 'MAIN' and 'CT' not in df_addSmtAssy['MS Code'][i]:
        #                 for j in range(1,6):
        #                     if j == 1:
        #                         rowCnt = 1
        #                     if str(df_addSmtAssy[f'ROW{str(j)}'][i]) != '' and str(df_addSmtAssy[f'ROW{str(j)}'][i]) != 'nan':
        #                         rowCnt = j
        #                     else:
        #                         break
        #                 smtFlag = False    
        #                 minCnt = 9999
        #                 for j in range(1,rowCnt+1):
        #                         smtAssyName = str(df_addSmtAssy[f'ROW{str(j)}'][i])
        #                         if smtAssyName != '' and smtAssyName != 'nan':
        #                             if dict_smtCnt[smtAssyName] >= df_addSmtAssy['잔여_착공량'][i]:
        #                                 if minCnt > df_addSmtAssy['잔여_착공량'][i]:
        #                                     minCnt = df_addSmtAssy['잔여_착공량'][i]
                                            
        #                                 # dict_smtCnt[smtAssyName] -= df_addSmtAssy['미착공수량'][i]
        #                                 # df_smtCopy = df_smtCopy.append(df_addSmtAssy.iloc[i])
        #                             elif dict_smtCnt[smtAssyName] > 0:
        #                                 if minCnt > dict_smtCnt[smtAssyName]:
        #                                     minCnt = dict_smtCnt[smtAssyName]
        #                                 # df_addSmtAssy['미착공수량'][i] = dict_smtCnt[smtAssyName]
        #                                 # dict_smtCnt[smtAssyName] -= df_addSmtAssy['미착공수량'][i]
        #                                 # df_smtCopy = df_smtCopy.append(df_addSmtAssy.iloc[i])
        #                             else:
        #                                 minCnt = 0
        #                                 logging.warning('「사양 : %s」을 착공하기에는 「SmtAssy : %s」가 부족합니다. SmtAssy 제작을 지시해주세요.',
        #                                                 df_addSmtAssy['MS Code'][i],
        #                                                 smtAssyName)
        #                         else:
        #                             logging.warning('「사양 : %s」의 SmtAssy가 %s 파일에 등록되지 않았습니다. 등록 후, 다시 실행해주세요.',
        #                                             df_addSmtAssy['MS Code'][i],
        #                                             list_masterFile[6])
        #                 if minCnt != 9999:
        #                     df_addSmtAssy['SMT반영_착공량_잔여'][i] = minCnt
        #                 else:
        #                     df_addSmtAssy['SMT반영_착공량_잔여'][i] = df_addSmtAssy['잔여_착공량'][i]
                                        
        #                 for j in range(1,rowCnt+1):
        #                     if smtAssyName != '' and smtAssyName != 'nan':
        #                         smtAssyName = str(df_addSmtAssy[f'ROW{str(j)}'][i])
        #                         dict_smtCnt[smtAssyName] -= df_addSmtAssy['SMT반영_착공량_잔여'][i]
        #                 # if str(df_addSmtAssy['ATE_NO'][i]) !='' and str(df_addSmtAssy['ATE_NO'][i]) !='nan':
        #                 #     for j in range(0,len(str(df_addSmtAssy['ATE_NO'][i]))):
        #                 #         df_addSmtAssy['ATE_NO'][i][j]
        #         df_addSmtAssy.to_excel('.\\debug\\flow11.xlsx')
        #         df_addSmtAssy['임시수량'] = 0
        #         df_addSmtAssy['설비능력반영_착공량'] = 0
                
        #         # for i in df_addSmtAssy.index:  
        #         #     if str(df_addSmtAssy['TotalTime'][i]) != '' and str(df_addSmtAssy['TotalTime'][i]) != 'nan':
        #         #         if str(df_addSmtAssy['ATE_NO'][i]) != '' and str(df_addSmtAssy['ATE_NO'][i]) != 'nan':
        #         #             tempTime = 0
        #         #             ateName = ''
        #         #             for ate in df_addSmtAssy['ATE_NO'][i]:
        #         #                 if tempTime < dict_ate[ate]:
        #         #                     tempTime = dict_ate[ate]
        #         #                     ateName = ate
        #         #             if dict_ate[ateName] >= df_addSmtAssy['TotalTime'][i] * df_addSmtAssy['SMT반영_착공량'][i]:
        #         #                 dict_ate[ateName] -= df_addSmtAssy['TotalTime'][i] * df_addSmtAssy['SMT반영_착공량'][i]
        #         #                 df_addSmtAssy['설비능력반영_착공량'][i] = df_addSmtAssy['SMT반영_착공량'][i]
        #         #             elif dict_ate[ateName] >= df_addSmtAssy['TotalTime'][i]:
        #         #                 tempCnt = int(df_addSmtAssy['SMT반영_착공량'][i])
        #         #                 for j in range(tempCnt,0,-1):
        #         #                     # print(dict_ate[ateName])
        #         #                     # print(int(df_addSmtAssy['TotalTime'][i]) * j)
        #         #                     if dict_ate[ateName] >= int(df_addSmtAssy['TotalTime'][i]) * j:
        #         #                         df_addSmtAssy['설비능력반영_착공량'][i] = j
        #         #                         dict_ate[ateName] -= int(df_addSmtAssy['TotalTime'][i]) * j
        #         #                         break
        #         df_addSmtAssy['총착공량'] = 0
        #         for i in df_addSmtAssy.index:
        #             if 'CT' not in df_addSmtAssy['MS Code'][i]:
        #                 if str(df_addSmtAssy['TotalTime'][i]) != '' and str(df_addSmtAssy['TotalTime'][i]) != 'nan':
        #                     if str(df_addSmtAssy['ATE_NO'][i]) != '' and str(df_addSmtAssy['ATE_NO'][i]) != 'nan':
        #                         tempTime = 0
        #                         ateName = ''
        #                         for ate in df_addSmtAssy['ATE_NO'][i]:
        #                             if tempTime < dict_ate[ate]:
        #                                 tempTime = dict_ate[ate]
        #                                 ateName = ate
        #                                 if ate == df_addSmtAssy['ATE_NO'][i][0]:
        #                                     df_addSmtAssy['임시수량'][i] = df_addSmtAssy['SMT반영_착공량'][i]
        #                                 if df_addSmtAssy['임시수량'][i] != 0:
        #                                     if dict_ate[ateName] >= df_addSmtAssy['TotalTime'][i] * df_addSmtAssy['임시수량'][i]:
        #                                         if mainOrderCnt >= df_addSmtAssy['임시수량'][i]:
        #                                             dict_ate[ateName] -= df_addSmtAssy['TotalTime'][i] * df_addSmtAssy['임시수량'][i]
        #                                             df_addSmtAssy['설비능력반영_착공량'][i] += df_addSmtAssy['임시수량'][i]
        #                                             mainOrderCnt -= df_addSmtAssy['임시수량'][i]
        #                                             df_addSmtAssy['임시수량'][i] = 0
        #                                             break
        #                                         else:
        #                                             dict_ate[ateName] -= df_addSmtAssy['TotalTime'][i] * mainOrderCnt
        #                                             df_addSmtAssy['설비능력반영_착공량'][i] += mainOrderCnt
        #                                             df_addSmtAssy['임시수량'][i] -= mainOrderCnt
        #                                             mainOrderCnt = 0
        #                                             break
        #                                     elif dict_ate[ateName] >= df_addSmtAssy['TotalTime'][i]:
        #                                         tempCnt = int(df_addSmtAssy['임시수량'][i])
        #                                         for j in range(tempCnt,0,-1):
        #                                             # print(dict_ate[ateName])
        #                                             # print(int(df_addSmtAssy['TotalTime'][i]) * j)
        #                                             if dict_ate[ateName] >= int(df_addSmtAssy['TotalTime'][i]) * j:
        #                                                 if mainOrderCnt >= j:
        #                                                     df_addSmtAssy['설비능력반영_착공량'][i] = int(df_addSmtAssy['설비능력반영_착공량'][i]) + j
        #                                                     dict_ate[ateName] -= int(df_addSmtAssy['TotalTime'][i]) * j
        #                                                     df_addSmtAssy['임시수량'][i] = tempCnt - j
        #                                                     mainOrderCnt -= j
        #                                                     break
        #                                 else:
        #                                     break
        #         df_addSmtAssy['임시수량_잔여'] = 0
        #         df_addSmtAssy['설비능력반영_착공량_잔여'] = 0
        #         for i in df_addSmtAssy.index:
        #             if 'CT' not in df_addSmtAssy['MS Code'][i]:
        #                 if str(df_addSmtAssy['TotalTime'][i]) != '' and str(df_addSmtAssy['TotalTime'][i]) != 'nan':
        #                     if str(df_addSmtAssy['ATE_NO'][i]) != '' and str(df_addSmtAssy['ATE_NO'][i]) != 'nan':
        #                         tempTime = 0
        #                         ateName = ''
        #                         for ate in df_addSmtAssy['ATE_NO'][i]:
        #                             if tempTime < dict_ate[ate]:
        #                                 tempTime = dict_ate[ate]
        #                                 ateName = ate
        #                                 if ate == df_addSmtAssy['ATE_NO'][i][0]:
        #                                     df_addSmtAssy['임시수량_잔여'][i] = df_addSmtAssy['SMT반영_착공량_잔여'][i]
        #                                 if df_addSmtAssy['임시수량_잔여'][i] != 0:
        #                                     if dict_ate[ateName] >= df_addSmtAssy['TotalTime'][i] * df_addSmtAssy['임시수량_잔여'][i]:
        #                                         if mainOrderCnt >= df_addSmtAssy['임시수량_잔여'][i]:
        #                                             dict_ate[ateName] -= df_addSmtAssy['TotalTime'][i] * df_addSmtAssy['임시수량_잔여'][i]
        #                                             df_addSmtAssy['설비능력반영_착공량_잔여'][i] += df_addSmtAssy['임시수량_잔여'][i]
        #                                             mainOrderCnt -= df_addSmtAssy['임시수량_잔여'][i]
        #                                             df_addSmtAssy['임시수량_잔여'][i] = 0
        #                                             break
        #                                         else:
        #                                             dict_ate[ateName] -= df_addSmtAssy['TotalTime'][i] * mainOrderCnt
        #                                             df_addSmtAssy['설비능력반영_착공량'][i] += mainOrderCnt
        #                                             df_addSmtAssy['임시수량_잔여'][i] -= mainOrderCnt
        #                                             mainOrderCnt = 0
        #                                             break
        #                                     elif dict_ate[ateName] >= df_addSmtAssy['TotalTime'][i]:
        #                                         tempCnt = int(df_addSmtAssy['임시수량_잔여'][i])
        #                                         for j in range(tempCnt,0,-1):
        #                                             # print(dict_ate[ateName])
        #                                             # print(int(df_addSmtAssy['TotalTime'][i]) * j)
        #                                             if dict_ate[ateName] >= int(df_addSmtAssy['TotalTime'][i]) * j:
        #                                                 if mainOrderCnt >= j:
        #                                                     df_addSmtAssy['설비능력반영_착공량_잔여'][i] = int(df_addSmtAssy['설비능력반영_착공량_잔여'][i]) + j
        #                                                     dict_ate[ateName] -= int(df_addSmtAssy['TotalTime'][i]) * j
        #                                                     df_addSmtAssy['임시수량_잔여'][i] = tempCnt - j
        #                                                     mainOrderCnt -= j
        #                                                     break
        #                                 else:
        #                                     break     
        #                                 df_addSmtAssy['총착공량'][i] = mainOrderCnt
        #                         # print(i)
        #                         # print(f'설비명 : {ateName}')
        #                         # print('남은시간 : ' + str((dict_ate[ateName])))
        #         # print(dict_ate)
        #         df_addSmtAssy.to_excel('.\\debug\\flow12.xlsx')
        #         # df_addSmtAssy['대표모델별_누적착공량'] = ''
        #         # dict_integAteCnt = {}
        #         # for i in df_addSmtAssy.index:
        #         #     if 'CT' not in df_addSmtAssy['MS Code'][i]:
        #         #         if df_addSmtAssy['대표모델'][i] in dict_integAteCnt:
        #         #             dict_integAteCnt[df_addSmtAssy['대표모델'][i]] += int(df_addSmtAssy['설비능력반영_착공량'][i])
        #         #         else:
        #         #             dict_integAteCnt[df_addSmtAssy['대표모델'][i]] = int(df_addSmtAssy['설비능력반영_착공량'][i])
        #         #         df_addSmtAssy['대표모델별_누적착공량'][i] = dict_integAteCnt[df_addSmtAssy['대표모델'][i]]
        #         # for key, value in dict_minContCnt.items():
        #         #     if key in dict_integAteCnt:
        #         #         if value[0] > dict_integAteCnt[key]:
        #         #             logging.warning('「%s」 사양이 「완성지정일: %s」 까지 오늘 「착공수량: %i 대」로는 착공량 부족이 예상됩니다. 최소 필요 착공량은 「%i 대」 입니다.', 
        #         #                 key, 
        #         #                 str(value[1]),
        #         #                 dict_integAteCnt[key],
        #         #                 math.ceil(value[0]))      
        #         # df_addSmtAssy.to_excel('.\\debug\\flow13.xlsx')
        #     self.runBtn.setEnabled(True)
        # except Exception as e:
        #     logging.exception(e, exc_info=True)                     
        #     self.runBtn.setEnabled(True)
if __name__ == '__main__':
    import sys
    app = QtWidgets.QApplication(sys.argv)
    ui = Ui_MainWindow()
    sys.exit(app.exec_())