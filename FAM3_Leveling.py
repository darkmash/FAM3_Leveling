import datetime
import glob
import logging
from logging.handlers import RotatingFileHandler
import os
import re
import math
import numpy as np
from PyQt5 import QtWidgets, QtGui
from PyQt5.QtCore import Qt, QCoreApplication
from PyQt5.QtGui import QDoubleValidator, QStandardItemModel, QIcon, QStandardItem, QIntValidator, QFont
from PyQt5.QtWidgets import QMainWindow, QMessageBox, QProgressBar, QPlainTextEdit, QWidget, QGridLayout, QGroupBox, QLineEdit, QSizePolicy, QToolButton, QLabel, QFrame, QListView, QMenuBar, QStatusBar, QPushButton, QCalendarWidget, QVBoxLayout, QFileDialog, QComboBox
from PyQt5.QtCore import pyqtSlot, pyqtSignal, QObject, QThread, QRect, QSize, QDate
import pandas as pd
import cx_Oracle
from pathlib import Path
import debugpy
import time
from configparser import ConfigParser


# 메인라인 동작 쓰레드
class MainThread(QObject):
    # 클래스 외부에서 사용할 수 있도록 시그널 선언
    mainReturnError = pyqtSignal(Exception)
    mainReturnInfo = pyqtSignal(str)
    mainReturnWarning = pyqtSignal(str)
    mainReturnEnd = pyqtSignal(bool)
    mainReturnDf = pyqtSignal(pd.DataFrame)
    mainReturnPb = pyqtSignal(int)
    mainReturnMaxPb = pyqtSignal(int)
    mainReturnEmgLinkage = pyqtSignal(dict)
    mainReturnEmgMscode = pyqtSignal(dict)

    # 초기화
    def __init__(self, debugFlag, date, constDate, list_masterFile, moduleMaxCnt, emgHoldList, cb_round, df_etcOrderInput):
        super().__init__(),
        self.isDebug = debugFlag
        self.date = date
        self.constDate = constDate
        self.list_masterFile = list_masterFile
        self.moduleMaxCnt = moduleMaxCnt
        self.emgHoldList = emgHoldList
        self.cb_round = cb_round
        self.df_etcOrderInput = df_etcOrderInput

    # 워킹데이 체크 내부함수
    def checkWorkDay(self, df, today, compDate):
        dtToday = pd.to_datetime(datetime.datetime.strptime(today, '%Y%m%d'))
        dtComp = pd.to_datetime(compDate, unit='s')
        workDay = 0
        if len(df.index[(df['Date'] == dtComp)].tolist()) > 0:
            index = int(df.index[(df['Date'] == dtComp)].tolist()[0])
            # 위에서 찾은 완성지정일로부터 프로그램 구동 당일까지 워킹데이를 계산.
            while dtToday > pd.to_datetime(df['Date'][index], unit='s'):
                if df['WorkingDay'][index] == 1:
                    workDay -= 1
                index += 1
            # 프로그램 구동 당일 ~ 완성지정일 까지의 워킹데이를 계산
            for i in df.index:
                dt = pd.to_datetime(df['Date'][i], unit='s')
                if dtToday < dt and dt <= dtComp:
                    if df['WorkingDay'][i] == 1:
                        workDay += 1
        else:
            self.mainReturnWarning.emit(f'FY{today[2:4]}_Calendar.xlsx 파일에 {str(dtComp.date())} 날짜의 워킹데이 데이터가 없습니다. 대한민국 휴일을 기준으로 근무일을 계산합니다. 이후, 해당 파일에 사력을 추가해주세요')
            workDay = np.busday_count(begindates=dtToday.date(), enddates=dtComp.date())
        return workDay

    # 콤마 삭제용 내부함수
    def delComma(self, value):
        return str(value).split('.')[0]

    # 디비 불러오기 공통내부함수
    def readDB(self, ip, port, sid, userName, password, sql):
        # 오라클 클라이언트 폴더 지정 (프로그램 파일에 이미 넣어둠)
        location = r'.\\instantclient_21_7'
        # 해당 폴더를 환경변수에 지정
        os.environ["PATH"] = location + ";" + os.environ["PATH"]
        # 입력된 Dsn 설정 및 접속정보로 오라클 접속
        dsn = cx_Oracle.makedsn(ip, port, sid)
        db = cx_Oracle.connect(userName, password, dsn)
        cursor = db.cursor()
        # Sql 실행
        cursor.execute(sql)
        out_data = cursor.fetchall()
        # 결과값을 Dataframe화
        df_oracle = pd.DataFrame(out_data)
        # 컬럼명 지정
        col_names = [row[0] for row in cursor.description]
        df_oracle.columns = col_names
        return df_oracle

    # 생산시간 합계용 내부함수
    def getSec(self, time_str):
        time_str = re.sub(r'[^0-9:]', '', str(time_str))
        if len(time_str) > 0:
            h, m, s = time_str.split(':')
            return int(h) * 3600 + int(m) * 60 + int(s)
        else:
            return 0

    # 백슬래쉬 삭제용 내부함수
    def delBackslash(self, value):
        value = re.sub(r"\\c", "", str(value))
        return value

    # 초단위의 시간을 시/분/초로 분할
    def convertSecToTime(self, seconds):
        seconds = seconds % (24 * 3600)
        hour = seconds // 3600
        seconds %= 3600
        minutes = seconds // 60
        seconds %= 60
        return "%d:%02d:%02d" % (hour, minutes, seconds)

    # 알람 상세 누적 기록용 내부함수
    def concatAlarmDetail(self, df_target, no, category, df_data, index, smtAssy, shortageCnt):
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
        df_result = pd.DataFrame()
        if category == '1':
            df_result = pd.concat([df_target,
                                    pd.DataFrame.from_records([{"No.": no,
                                                                "분류": category,
                                                                "L/N": df_data['Linkage Number'][index],
                                                                "MS CODE": df_data['MS Code'][index],
                                                                "SMT ASSY": smtAssy,
                                                                "수주수량": df_data['미착공수주잔'][index],
                                                                "부족수량": shortageCnt,
                                                                "검사호기": '-',
                                                                "대상 검사시간(초)": 0,
                                                                "필요시간(초)": 0,
                                                                "완성예정일": df_data['Planned Prod. Completion date'][index]}])])
        elif category == '2':
            df_result = pd.concat([df_target,
                                    pd.DataFrame.from_records([{"No.": no,
                                                                "분류": category,
                                                                "L/N": df_data['Linkage Number'][index],
                                                                "MS CODE": df_data['MS Code'][index],
                                                                "SMT ASSY": '-',
                                                                "수주수량": df_data['미착공수주잔'][index],
                                                                "부족수량": shortageCnt,
                                                                "검사호기": df_data['INSPECTION_EQUIPMENT'][index],
                                                                "대상 검사시간(초)": df_data['TotalTime'][index],
                                                                "필요시간(초)": shortageCnt * df_data['TotalTime'][index],
                                                                "완성예정일": df_data['Planned Prod. Completion date'][index]}])])
        elif category == '기타1':
            df_result = pd.concat([df_target,
                                    pd.DataFrame.from_records([{"No.": no,
                                                                "분류": category,
                                                                "L/N": df_data['Linkage Number'][index],
                                                                "MS CODE": df_data['MS Code'][index],
                                                                "SMT ASSY": '미등록',
                                                                "수주수량": df_data['미착공수주잔'][index],
                                                                "부족수량": 0,
                                                                "검사호기": '-',
                                                                "대상 검사시간(초)": 0,
                                                                "필요시간(초)": 0,
                                                                "완성예정일": df_data['Planned Prod. Completion date'][index]}])])
        elif category == '기타2':
            df_result = pd.concat([df_target,
                                    pd.DataFrame.from_records([{"No.": no,
                                                                "분류": category,
                                                                "L/N": df_data['Linkage Number'][index],
                                                                "MS CODE": df_data['MS Code'][index],
                                                                "SMT ASSY": '-',
                                                                "수주수량": df_data['미착공수주잔'][index],
                                                                "부족수량": shortageCnt,
                                                                "검사호기": '-',
                                                                "대상 검사시간(초)": 0,
                                                                "필요시간(초)": 0,
                                                                "완성예정일": df_data['Planned Prod. Completion date'][index]}])])
        elif category == '기타3':
            df_result = pd.concat([df_target,
                                    pd.DataFrame.from_records([{"No.": no,
                                                                "분류": category,
                                                                "L/N": df_data['Linkage Number'][index],
                                                                "MS CODE": df_data['MS Code'][index],
                                                                "SMT ASSY": smtAssy,
                                                                "수주수량": df_data['미착공수주잔'][index],
                                                                "부족수량": shortageCnt,
                                                                "검사호기": '-',
                                                                "대상 검사시간(초)": 0,
                                                                "필요시간(초)": 0,
                                                                "완성예정일": df_data['Planned Prod. Completion date'][index]}])])
        elif category == '기타4':
            df_result = pd.concat([df_target,
                                    pd.DataFrame.from_records([{"No.": no,
                                                                "분류": category,
                                                                "L/N": df_data['Linkage Number'][index],
                                                                "MS CODE": df_data['MS Code'][index],
                                                                "SMT ASSY": smtAssy,
                                                                "수주수량": df_data['미착공수주잔'][index],
                                                                "부족수량": shortageCnt,
                                                                "검사호기": '-',
                                                                "대상 검사시간(초)": 0,
                                                                "필요시간(초)": 0,
                                                                "완성예정일": df_data['Planned Prod. Completion date'][index]}])])
        return [df_result, no + 1]

    # SMT Assy 반영 착공로직
    def smtReflectInst(self, df_input, isRemain, dict_smtCnt, alarmDetailNo, df_alarmDetail, rowNo):
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
        # 행별로 확인
        for i in df_input.index:
            # 사용 Smt Assy 개수 확인
            for j in range(1, rowNo):
                if j == 1:
                    rowCnt = 1
                if (str(df_input[f'ROW{str(j)}'][i]) != '' and str(df_input[f'ROW{str(j)}'][i]) != 'nan'):
                    rowCnt = j
                else:
                    break
            if rowNo == 1:
                rowCnt = 1
            minCnt = 9999
            # 각 SmtAssy 별로 착공 가능 대수 확인
            for j in range(1, rowCnt + 1):
                smtAssyName = str(df_input[f'ROW{str(j)}'][i])
                if (df_input['SMT_MS_CODE'][i] != 'nan' and df_input['SMT_MS_CODE'][i] != 'None' and df_input['SMT_MS_CODE'][i] != ''):
                    if (smtAssyName != '' and smtAssyName != 'nan' and smtAssyName != 'None'):
                        # 긴급오더 혹은 당일착공 대상일 경우, SMT Assy 잔량에 관계없이 착공 실시.
                        # SMT Assy가 부족할 경우에는 분류1 알람을 발생.
                        if df_input['긴급오더'][i] == '대상' or df_input['당일착공'][i] == '대상':
                            # MS Code와 연결된 SMT Assy가 있을 경우, 정상적으로 로직을 실행
                            if smtAssyName in dict_smtCnt:
                                if dict_smtCnt[smtAssyName] < 0:
                                    diffCnt = df_input['미착공수주잔'][i]
                                    if dict_smtCnt[smtAssyName] + df_input['미착공수주잔'][i] > 0:
                                        diffCnt = 0 - dict_smtCnt[smtAssyName]
                                    if not isRemain:
                                        if dict_smtCnt[smtAssyName] > 0:
                                            df_alarmDetail, alarmDetailNo = self.concatAlarmDetail(df_alarmDetail, alarmDetailNo, '1', df_input, i, smtAssyName, diffCnt)
                            # SMT Assy가 DB에 등록되지 않은 경우, 기타3 알람을 출력.
                            else:
                                minCnt = 0
                                df_alarmDetail, alarmDetailNo = self.concatAlarmDetail(df_alarmDetail, alarmDetailNo, '기타3', df_input, i, smtAssyName, 0)
                        # 긴급오더 혹은 당일착공 대상이 아닐 경우, SMT Assy 잔량을 확인 후, SMT Assy 잔량이 부족할 경우, 부족한 양만큼 착공.
                        else:
                            # 사용하는 SmtAssy가 이미 등록된 SmtAssy일 경우의 로직
                            if smtAssyName in dict_smtCnt:
                                # 최소필요착공량보다 SmtAssy 수량이 여유 있는 경우, 그대로 착공
                                if dict_smtCnt[smtAssyName] >= df_input[instCol][i]:
                                    # 사용하는 SmtAssy가 다수 일 경우를 고려하여 최소수량 확인
                                    if minCnt > df_input[instCol][i]:
                                        minCnt = df_input[instCol][i]
                                # SmtAssy 수량의 여유가 없는 경우
                                else:
                                    # 최소수량과 SmtAssy수량을 다시 비교
                                    if dict_smtCnt[smtAssyName] > 0:
                                        if minCnt > dict_smtCnt[smtAssyName]:
                                            minCnt = dict_smtCnt[smtAssyName]
                                    # SmtAssy수량이 0개 인 경우, 최소수량을 0으로 전환
                                    else:
                                        minCnt = 0
                                    # 최소착공필요량 전체에 비해 SmtAssy수량이 부족한 경우, 알람을 출력.
                                    if not isRemain:
                                        if dict_smtCnt[smtAssyName] > 0:
                                            df_alarmDetail, alarmDetailNo = self.concatAlarmDetail(df_alarmDetail, alarmDetailNo, '1', df_input, i, smtAssyName, df_input[instCol][i] - dict_smtCnt[smtAssyName])
                            # SMT Assy가 DB에 등록되지 않은 경우, 기타3 알람을 출력.
                            else:
                                minCnt = 0
                                df_alarmDetail, alarmDetailNo = self.concatAlarmDetail(df_alarmDetail, alarmDetailNo, '기타3', df_input, i, smtAssyName, 0)
                # MS Code와 연결된 SMT Assy가 등록되지 않았을 경우, 기타1 알람을 출력.
                else:
                    minCnt = 0
                    df_alarmDetail, alarmDetailNo = self.concatAlarmDetail(df_alarmDetail, alarmDetailNo, '기타1', df_input, i, '미등록', 0)
            # 최소 수량을 1번이라도 갱신한 경우, 결과컬럼의 값을 minCnt로 대체
            if minCnt != 9999:
                df_input[resultCol][i] = minCnt
            # 갱신하지 않았을 경우, 기존 입력값을 그대로 출력
            else:
                df_input[resultCol][i] = df_input[instCol][i]
            # 사용되는 각 Smt Assy 수량에서 결과값을 빼기위한 로직
            for j in range(1, rowCnt + 1):
                if (smtAssyName != '' and smtAssyName != 'nan' and smtAssyName != 'None'):
                    smtAssyName = str(df_input[f'ROW{str(j)}'][i])
                    dict_smtCnt[smtAssyName] -= df_input[resultCol][i]
        return [df_input, dict_smtCnt, alarmDetailNo, df_alarmDetail]

    # 검사설비 반영 착공로직
    def ateReflectInst(self, df_input, isRemain, dict_ate, df_alarmDetail, alarmDetailNo, moduleMaxCnt, limitCtCnt):
        """
        Args:
            df_input(DataFrame)         : 입력 DataFrame
            isRemain(Bool)              : 잔여착공 여부 Flag
            dict_ate(Dict)              : 잔여 검사설비능력 Dict
            alarmDetailNo(int)          : 알람 번호
            df_alarmDetail(DataFrame)   : 알람 상세 기록용 DataFrame
            moduleMaxCnt(int)           : 최대착공량
            limitCtCnt(int)             : CT제한 착공량
        Return:
            return(List)
                df_input(DataFrame)         : 입력 DataFrame (갱신 후)
                dict_ate(Dict)              : 잔여 검사설비능력 Dict (갱신 후)
                alarmDetailNo(int)          : 알람 번호
                df_alarmDetail(DataFrame)   : 알람 상세 기록용 DataFrame (갱신 후)
                moduleMaxCnt(int)           : 최대착공량 (갱신 후)
                limitCtCnt(int)             : CT제한 착공량 (갱신 후)
        """
        # 여유 착공량인지 확인 후, 컬럼명을 지정
        if isRemain:
            smtReflectCnt = 'SMT반영_착공량_잔여'
            tempAteCnt = '임시수량_잔여'
            ateReflectCnt = '설비능력반영_착공량_잔여'
        else:
            smtReflectCnt = 'SMT반영_착공량'
            tempAteCnt = '임시수량'
            ateReflectCnt = '설비능력반영_착공량'
        for i in df_input.index:
            # 디버그로 남은착공량의 과정을 보기 위해 데이터프레임에 기록
            df_input['남은착공량'][i] = moduleMaxCnt
            # 검사시간이 있는 모델만 적용
            if (str(df_input['TotalTime'][i]) != '') and (str(df_input['TotalTime'][i]) != 'nan'):
                # 검사설비가 있는 모델만 적용
                if (str(df_input['INSPECTION_EQUIPMENT'][i]) != '') and (str(df_input['INSPECTION_EQUIPMENT'][i]) != 'nan'):
                    # 임시 검사시간과 검사설비를 가지고 있는 변수 선언
                    tempTime = 0
                    ateName = ''
                    # 긴급오더 or 당일착공 대상은 검사설비 능력이 부족하여도 강제 착공. 그리고 알람을 기록
                    if (str(df_input['긴급오더'][i]) == '대상') or (str(df_input['당일착공'][i]) == '대상'):
                        # 대상 검사설비를 한개씩 분할
                        ateList = list(df_input['INSPECTION_EQUIPMENT'][i])
                        dict_temp = {}
                        # 입력되어진 검사설비 시간을 임시 검사설비 딕셔너리에 넣음.
                        for ate in ateList:
                            dict_temp[ate] = dict_ate[ate]
                        # 여유있는 검사설비 순으로 정렬
                        dict_temp = sorted(dict_temp.items(), key=lambda item: item[1], reverse=True)
                        # 여유있는 검사설비 순으로 검시시간을 계산하여 넣는다.
                        for ate in dict_temp:
                            # 최대 착공량이 0 초과일 경우에만 해당 로직을 실행.
                            if moduleMaxCnt > 0:
                                # 일단 가장 여유있는 검사설비와 그 시간을 임시로 가져옴
                                tempTime = dict_ate[ate[0]]
                                ateName = ate[0]
                                # if ate[0] == df_input['INSPECTION_EQUIPMENT'][i][0]:
                                # 임시 수량 컬럼에 Smt 반영 착공량을 입력
                                df_input[tempAteCnt][i] = df_input[smtReflectCnt][i]
                                if df_input[tempAteCnt][i] != 0:
                                    # 해당 검사설비능력에서 착공분만큼 삭감
                                    dict_ate[ateName] -= df_input['TotalTime'][i] * df_input[tempAteCnt][i]
                                    df_input[ateReflectCnt][i] += df_input[tempAteCnt][i]
                                    # 특수모듈인 경우에는 전체 착공랴에서 빼지 않음
                                    if df_input['특수대상'][i] != '대상':
                                        moduleMaxCnt -= df_input[tempAteCnt][i]
                                    # 임시수량은 초기화
                                    df_input[tempAteCnt][i] = 0
                                    # CT사양의 경우 별도 CT제한수량에서도 삭감
                                    if '/CT' in df_input['MS Code'][i]:
                                        limitCtCnt -= df_input[tempAteCnt][i]
                                    break
                                else:
                                    break
                        # 최대착공량이 0 미만일 경우, 알람 출력
                        if moduleMaxCnt < 0:
                            df_alarmDetail, alarmDetailNo = self.concatAlarmDetail(df_alarmDetail, alarmDetailNo, '기타2', df_input, i, '-', 0)
                        # CT제한대수가 0 미만일 경우, 알람 출력
                        if limitCtCnt < 0:
                            df_alarmDetail, alarmDetailNo = self.concatAlarmDetail(df_alarmDetail, alarmDetailNo, '기타4', df_input, i, '-', 0)
                            # break
                        # 검사설비능력이 0미만일 경우, 알람 출력
                        if ateName != '' and dict_ate[ateName] < 0:
                            df_alarmDetail, alarmDetailNo = self.concatAlarmDetail(df_alarmDetail, alarmDetailNo, '2', df_input, i, '-', math.floor((0 - dict_ate[ateName]) / df_input['TotalTime'][i]))
                            dict_ate[ateName] = 0
                        # 긴급오더 or 당일착공이 아닌 경우는 검사설비 능력을 반영하여 착공 실시
                    else:
                        # 긴급오더 처리 시, 0미만으로 떨어진 각 카운터를 0으로 초기화
                        if moduleMaxCnt < 0:
                            moduleMaxCnt = 0
                        if limitCtCnt < 0:
                            limitCtCnt = 0
                        # 첫 착공인지 확인하는 플래그 선언
                        isFirst = True
                        ateList = list(df_input['INSPECTION_EQUIPMENT'][i])
                        dict_temp = {}
                        for ate in ateList:
                            dict_temp[ate] = dict_ate[ate]
                        dict_temp = sorted(dict_temp.items(), key=lambda item: item[1], reverse=True)
                        for ate in dict_temp:
                            if tempTime <= dict_ate[ate[0]]:
                                tempTime = dict_ate[ate[0]]
                                ateName = ate[0]
                                # if ate[0] == df_input['INSPECTION_EQUIPMENT'][i][0]:
                                # 비교리스트에 [Smt반영 착공량], [최대착공량]을 입력
                                compareList = [df_input[smtReflectCnt][i], moduleMaxCnt]
                                # LinkageNumber별 첫번째 착공이 아닐 경우(임시수량 있을 경우), [임시수량]도 비교리스트에 입력
                                if not isFirst:
                                    compareList.append(df_input[tempAteCnt][i])
                                # CT사양일 경우, [CT제한수량]도 비교리스트에 입력
                                if '/CT' in df_input['MS Code'][i]:
                                    compareList.append(limitCtCnt)
                                # 비교리스트 중 Min값을 임시수량으로 입력
                                df_input[tempAteCnt][i] = min(compareList)

                                if df_input[tempAteCnt][i] != 0:
                                    # 검사 설비 능력이 해당 LinkageNumber의 착공수량(임시수량)을 커버가능할 경우, 착공수량 전부를 착공대상으로 선정
                                    if dict_ate[ateName] >= df_input['TotalTime'][i] * df_input[tempAteCnt][i]:
                                        dict_ate[ateName] -= df_input['TotalTime'][i] * df_input[tempAteCnt][i]
                                        df_input[ateReflectCnt][i] += df_input[tempAteCnt][i]
                                        if df_input['특수대상'][i] != '대상':
                                            moduleMaxCnt -= df_input[tempAteCnt][i]
                                        if '/CT' in df_input['MS Code'][i]:
                                            limitCtCnt -= df_input[tempAteCnt][i]
                                        df_input[tempAteCnt][i] = 0
                                        break
                                    # 검사 설비 능력이 해당 LinkageNumber의 착공수량(임시수량)을 커버 불가능할 경우, 가능한 수량까지를 착공대상으로 선정
                                    elif dict_ate[ateName] >= df_input['TotalTime'][i]:
                                        tempCnt = int(df_input[tempAteCnt][i])
                                        # 착공수량으로부터 역순으로 Loop문을 실행시켜, 검사설비 능력이 가능한 최대 한도를 확인 후, 그 수량만큼 착공대상으로 선정.
                                        for j in range(tempCnt, 0, -1):
                                            if dict_ate[ateName] >= int(df_input['TotalTime'][i]) * j:
                                                if moduleMaxCnt >= j:
                                                    df_input[ateReflectCnt][i] = int(df_input[ateReflectCnt][i]) + j
                                                    dict_ate[ateName] -= int(df_input['TotalTime'][i]) * j
                                                    df_input[tempAteCnt][i] = tempCnt - j
                                                    if df_input['특수대상'][i] != '대상':
                                                        moduleMaxCnt -= j
                                                    if '/CT' in df_input['MS Code'][i]:
                                                        limitCtCnt -= j
                                                    isFirst = False
                                                    break
                                # else:
                                #     break
            # CT사양의 최소필요착공량을 착공 못할 경우, 알람을 발생 시킴
            if not isRemain and (df_input[smtReflectCnt][i] > df_input[ateReflectCnt][i]):
                if '/CT' in df_input['MS Code'][i] and limitCtCnt == 0 and df_input[smtReflectCnt][i] > 0:
                    df_alarmDetail, alarmDetailNo = self.concatAlarmDetail(df_alarmDetail, alarmDetailNo, '기타4', df_input, i, '-', df_input[smtReflectCnt][i] - df_input[ateReflectCnt][i])

        return [df_input, dict_ate, alarmDetailNo, df_alarmDetail, moduleMaxCnt, limitCtCnt]

    def run(self):
        # pandas 경고없애기 옵션 적용
        pd.set_option('mode.chained_assignment', None)
        try:
            start = time.time()
            # 쓰레드 디버깅을 위한 처리
            if self.isDebug:
                debugpy.debug_this_thread()
            # 프로그레스바의 최대값 설정
            maxPb = 210
            self.mainReturnMaxPb.emit(maxPb)
            # 최대착공량 0 초과 입력할 경우에만 로직 실행
            if self.moduleMaxCnt > 0:
                progress = 0
                self.mainReturnPb.emit(progress)
                # 긴급오더, 홀딩오더 불러오기
                emgLinkage = self.emgHoldList[0]
                emgmscode = self.emgHoldList[1]
                holdLinkage = self.emgHoldList[2]
                holdmscode = self.emgHoldList[3]
                # 긴급오더, 홀딩오더 데이터프레임화
                df_emgLinkage = pd.DataFrame({'Linkage Number': emgLinkage})
                df_emgmscode = pd.DataFrame({'MS Code': emgmscode})
                df_holdLinkage = pd.DataFrame({'Linkage Number': holdLinkage})
                df_holdmscode = pd.DataFrame({'MS Code': holdmscode})
                # 각 Linkage Number 컬럼의 타입을 일치시킴
                df_emgLinkage['Linkage Number'] = df_emgLinkage['Linkage Number'].astype(np.int64)
                df_holdLinkage['Linkage Number'] = df_holdLinkage['Linkage Number'].astype(np.int64)
                # 긴급오더, 홍딩오더 Join 전 컬럼 추가
                df_emgLinkage['긴급오더'] = '대상'
                df_emgmscode['긴급오더'] = '대상'
                df_holdLinkage['홀딩오더'] = '대상'
                df_holdmscode['홀딩오더'] = '대상'
                # 레벨링 리스트 불러오기(멀티프로세싱 적용 후, 분리 예정)
                df_levelingMain = pd.read_excel(self.list_masterFile[1])
                # 레벨링 리스트의 착공 당일의 마지막 No를 가져오기 위한 처리
                df_constDate = df_levelingMain[df_levelingMain['Scheduled Start Date (*)'] == self.constDate]
                df_constDate = df_constDate[df_constDate['Sequence No'].notnull()]
                if len(df_constDate) > 0:
                    df_constDate = df_constDate[df_constDate['Sequence No'].str.contains('D0')]
                if len(df_constDate) > 0:
                    maxNo = df_constDate['No (*)'].max()
                else:
                    maxNo = 0
                # 미착공 대상만 추출(Main)
                df_levelingMainDropSeq = df_levelingMain[df_levelingMain['Sequence No'].isnull()]
                df_levelingMainUndepSeq = df_levelingMain[df_levelingMain['Sequence No'] == 'Undep']
                df_levelingMainUncorSeq = df_levelingMain[df_levelingMain['Sequence No'] == 'Uncor']
                df_levelingMain = pd.concat([df_levelingMainDropSeq, df_levelingMainUndepSeq, df_levelingMainUncorSeq])
                df_levelingMain['Linkage Number'] = df_levelingMain['Linkage Number'].astype(str)
                df_levelingMain = df_levelingMain.reset_index(drop=True)
                df_levelingMain['미착공수주잔'] = df_levelingMain.groupby('Linkage Number')['Linkage Number'].transform('size')
                # 특수모듈이면서 메인검사장치를 사용하는 모듈의 조건처리
                df_levelingMain['특수대상'] = ''
                df_spCondition = pd.read_excel(self.list_masterFile[7])
                df_ateP = df_spCondition[df_spCondition['검사호기'] == 'P']
                df_ateP['1차_MAX_그룹'] = df_ateP['1차_MAX_그룹'].fillna(method='ffill')
                df_ateP['2차_MAX_그룹'] = df_ateP['2차_MAX_그룹'].fillna(method='ffill')
                df_ateP['1차_MAX'] = df_ateP['1차_MAX'].fillna(method='ffill')
                df_ateP['2차_MAX'] = df_ateP['2차_MAX'].fillna(method='ffill')
                df_ateP['우선착공'] = df_ateP['우선착공'].fillna(method='ffill')
                df_ateP['특수대상'] = '대상'
                dict_ateP1stCnt = {}
                dict_ateP2ndCnt = {}
                for i in df_ateP.index:
                    if str(df_ateP['1차_MAX_그룹'][i]) != '-' and str(df_ateP['1차_MAX_그룹'][i]) != '' and str(df_ateP['1차_MAX_그룹'][i]) != 'nan':
                        dict_ateP1stCnt[df_ateP['1차_MAX_그룹'][i]] = int(df_ateP['1차_MAX'][i])
                    if str(df_ateP['2차_MAX_그룹'][i]) != '-' and str(df_ateP['2차_MAX_그룹'][i]) != '' and str(df_ateP['2차_MAX_그룹'][i]) != 'nan':
                        dict_ateP2ndCnt[df_ateP['2차_MAX_그룹'][i]] = int(df_ateP['2차_MAX'][i])
                list_ateP = df_ateP['MODEL'].tolist()
                str_where = ""
                for list in list_ateP:
                    str_where += f" OR INSTR(SMT_MS_CODE, '{list}') > 0"
                if Path(self.list_masterFile[2]).is_file():
                    df_levelingSp = pd.read_excel(self.list_masterFile[2])
                    # 미착공 대상만 추출(특수_모듈)
                    df_levelingSpDropSeq = df_levelingSp[df_levelingSp['Sequence No'].isnull()]
                    df_levelingSpUndepSeq = df_levelingSp[df_levelingSp['Sequence No'] == 'Undep']
                    df_levelingSpUncorSeq = df_levelingSp[df_levelingSp['Sequence No'] == 'Uncor']
                    df_levelingSp = pd.concat([df_levelingSpDropSeq, df_levelingSpUndepSeq, df_levelingSpUncorSeq])
                    df_levelingSp['대표모델6자리'] = df_levelingSp['MS-CODE'].str[:6]
                    df_levelingSp = pd.merge(df_levelingSp, df_ateP, how='right', left_on='대표모델6자리', right_on='MODEL')
                    df_levelingSp['Linkage Number'] = df_levelingSp['Linkage Number'].astype(str)
                    df_levelingSp = df_levelingSp.reset_index(drop=True)
                    df_levelingSp['미착공수주잔'] = df_levelingSp.groupby('Linkage Number')['Linkage Number'].transform('size')
                progress += round(maxPb / 21)
                self.mainReturnPb.emit(progress)
                # if self.isDebug:
                #     df_levelingMain.to_excel('.\\debug\\Main\\flow1.xlsx')
                df_sosFile = pd.read_excel(self.list_masterFile[0])
                df_sosFile['Linkage Number'] = df_sosFile['Linkage Number'].astype(str)
                if self.isDebug:
                    df_sosFile.to_excel('.\\debug\\Main\\flow2.xlsx')
                # 착공 대상 외 모델 삭제
                df_sosFile = df_sosFile.drop(df_sosFile[df_sosFile['MS Code'].str.contains('ZOTHER')].index)
                df_sosFile = df_sosFile.drop(df_sosFile[df_sosFile['MS Code'].str.contains('YZ')].index)
                df_sosFile = df_sosFile.drop(df_sosFile[df_sosFile['MS Code'].str.contains('SF')].index)
                df_sosFile = df_sosFile.drop(df_sosFile[df_sosFile['MS Code'].str.contains('KM')].index)
                df_sosFile = df_sosFile.drop(df_sosFile[df_sosFile['MS Code'].str.contains('TA80')].index)
                # CT사양은 1차에서만 착공내리도록 처리
                if self.cb_round != '1차':
                    df_sosFile = df_sosFile.drop(df_sosFile[df_sosFile['MS Code'].str.contains('CT')].index)
                progress += round(maxPb / 21)
                self.mainReturnPb.emit(progress)
                if self.isDebug:
                    df_sosFile.to_excel('.\\debug\\Main\\flow3.xlsx')
                # 워킹데이 캘린더 불러오기
                dfCalendar = pd.read_excel(self.list_masterFile[4])
                today = datetime.datetime.today().strftime('%Y%m%d')
                if self.isDebug:
                    today = self.date
                # 진척 파일 - SOS2파일 Join
                df_sosFileMerge = pd.merge(df_sosFile, df_levelingMain).drop_duplicates(['Linkage Number'])
                if Path(self.list_masterFile[2]).is_file():
                    df_sosFileMergeSp = pd.merge(df_sosFile, df_levelingSp).drop_duplicates(['Linkage Number'])
                    df_sosFileMerge = pd.concat([df_sosFileMerge, df_sosFileMergeSp])
                else:
                    df_sosFileMerge['1차_MAX_그룹'] = ''
                    df_sosFileMerge['2차_MAX_그룹'] = ''
                    df_sosFileMerge['1차_MAX'] = ''
                    df_sosFileMerge['2차_MAX'] = ''
                    df_sosFileMerge['우선착공'] = ''
                df_sosFileMerge = df_sosFileMerge[['Linkage Number',
                                                    'MS Code',
                                                    'Planned Prod. Completion date',
                                                    'Order Quantity',
                                                    '미착공수주잔',
                                                    '특수대상',
                                                    '우선착공',
                                                    '1차_MAX_그룹',
                                                    '2차_MAX_그룹',
                                                    '1차_MAX',
                                                    '2차_MAX']]
                # 미착공수주잔이 없는 데이터는 불요이므로 삭제
                df_sosFileMerge = df_sosFileMerge[df_sosFileMerge['미착공수주잔'] != 0]
                # 위 파일을 완성지정일 기준 오름차순 정렬 및 인덱스 재설정
                df_sosFileMerge = df_sosFileMerge.sort_values(by=['Planned Prod. Completion date'], ascending=[True])
                df_sosFileMerge = df_sosFileMerge.reset_index(drop=True)
                # 대표모델 Column 생성
                df_sosFileMerge['대표모델'] = df_sosFileMerge['MS Code'].str[:9]
                # 남은 워킹데이 Column 생성
                df_sosFileMerge['남은 워킹데이'] = 0
                # 긴급오더, 홀딩오더 Linkage Number Column 타입 일치
                df_emgLinkage['Linkage Number'] = df_emgLinkage['Linkage Number'].astype(str)
                df_holdLinkage['Linkage Number'] = df_holdLinkage['Linkage Number'].astype(str)
                # 긴급오더, 홀딩오더와 위 Sos파일을 Join
                df_MergeLink = pd.merge(df_sosFileMerge, df_emgLinkage, on='Linkage Number', how='left')
                df_Mergemscode = pd.merge(df_sosFileMerge, df_emgmscode, on='MS Code', how='left')
                df_MergeLink = pd.merge(df_MergeLink, df_holdLinkage, on='Linkage Number', how='left')
                df_Mergemscode = pd.merge(df_Mergemscode, df_holdmscode, on='MS Code', how='left')
                df_MergeLink['긴급오더'] = df_MergeLink['긴급오더'].combine_first(df_Mergemscode['긴급오더'])
                df_MergeLink['홀딩오더'] = df_MergeLink['홀딩오더'].combine_first(df_Mergemscode['홀딩오더'])
                df_MergeLink['당일착공'] = ''
                df_MergeLink['완성지정일_원본'] = df_MergeLink['Planned Prod. Completion date']
                # CT사양은 기존 완성지정일보다 4일 더 빠르게 착공내려야 하기 때문에 보정처리
                df_MergeLink.loc[df_MergeLink['MS Code'].str.contains('/CT'), 'Planned Prod. Completion date'] = df_MergeLink['완성지정일_원본'] - datetime.timedelta(days=4)
                df_MergeLink = df_MergeLink.sort_values(by=['Planned Prod. Completion date'], ascending=[True])
                df_MergeLink = df_MergeLink.reset_index(drop=True)
                # 남은 워킹데이 체크 및 컬럼 추가
                for i in df_MergeLink.index:
                    df_MergeLink['남은 워킹데이'][i] = self.checkWorkDay(dfCalendar, today, df_MergeLink['Planned Prod. Completion date'][i])
                    if df_MergeLink['남은 워킹데이'][i] < 1:
                        df_MergeLink['긴급오더'][i] = '대상'
                    elif df_MergeLink['남은 워킹데이'][i] == 1:
                        df_MergeLink['당일착공'][i] = '대상'
                progress += round(maxPb / 21)
                self.mainReturnPb.emit(progress)
                df_MergeLink['Linkage Number'] = df_MergeLink['Linkage Number'].astype(str)
                # 홀딩오더는 제외
                df_MergeLink = df_MergeLink[df_MergeLink['홀딩오더'].isnull()]
                if self.isDebug:
                    df_MergeLink.to_excel('.\\debug\\Main\\flow4.xlsx')
                # 프로그램 기동날짜의 전일을 계산 (Debug시에는 디버그용 LineEdit에 기록된 날짜를 사용)
                yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y%m%d')
                if self.isDebug:
                    yesterday = (datetime.datetime.strptime(self.date, '%Y%m%d') - datetime.timedelta(days=1)).strftime('%Y%m%d')
                # 설정파일 불러오기
                parser = ConfigParser()
                parser.read(self.list_masterFile[16], encoding='euc-kr')
                smtAssyDbHost = parser.get('SMT Assy DB정보', 'Host')
                smtAssyDbPort = parser.getint('SMT Assy DB정보', 'Port')
                smtAssyDbSID = parser.get('SMT Assy DB정보', 'SID')
                smtAssyDbUser = parser.get('SMT Assy DB정보', 'Username')
                smtAssyDbPw = parser.get('SMT Assy DB정보', 'Password')
                # 해당 날짜의 Smt Assy 남은 대수 확인
                df_SmtAssyInven = self.readDB(smtAssyDbHost,
                                                smtAssyDbPort,
                                                smtAssyDbSID,
                                                smtAssyDbUser,
                                                smtAssyDbPw,
                                                "SELECT INV_D, PARTS_NO, CURRENT_INV_QTY FROM pdsg0040 where INV_D = TO_DATE(" + str(yesterday) + ",'YYYYMMDD')")
                df_SmtAssyInven['현재수량'] = 0
                progress += round(maxPb / 21)
                self.mainReturnPb.emit(progress)
                if self.isDebug:
                    df_SmtAssyInven.to_excel('.\\debug\\Main\\flow5.xlsx')
                # 2차 메인피킹 리스트 불러오기 및 Smt Assy 재고량 Df와 Join
                if Path(self.list_masterFile[5]).is_file() or Path(self.list_masterFile[14]).is_file() or Path(self.list_masterFile[15]).is_file():
                    df_secOrderList = pd.DataFrame(columns=['ASSY NO', '대수', 'SMT STORE ADDRESS'])
                    if Path(self.list_masterFile[5]).is_file():
                        df_secOrderMainList = pd.read_excel(self.list_masterFile[5], skiprows=5)
                        df_secOrderList = pd.concat([df_secOrderList, df_secOrderMainList])
                    if Path(self.list_masterFile[14]).is_file():
                        df_secOrderPowerList = pd.read_excel(self.list_masterFile[14], skiprows=5)
                        df_secOrderList = pd.concat([df_secOrderList, df_secOrderPowerList])
                    if Path(self.list_masterFile[15]).is_file():
                        df_secOrderSpList = pd.read_excel(self.list_masterFile[15], skiprows=5)
                        df_secOrderList = pd.concat([df_secOrderList, df_secOrderSpList])
                    df_joinSmt = pd.merge(df_secOrderList, df_SmtAssyInven, how='right', left_on='ASSY NO', right_on='PARTS_NO')
                    df_joinSmt['대수'] = df_joinSmt['대수'].fillna(0)
                    # Smt Assy 현재 재고량에서 사용량 차감
                    df_joinSmt['현재수량'] = df_joinSmt['CURRENT_INV_QTY'] - df_joinSmt['대수']
                else:
                    df_joinSmt = df_SmtAssyInven.copy()
                    df_joinSmt['현재수량'] = df_joinSmt['CURRENT_INV_QTY']
                progress += round(maxPb / 21)
                self.mainReturnPb.emit(progress)
                if self.isDebug:
                    df_joinSmt.to_excel('.\\debug\\Main\\flow6.xlsx')
                dict_smtCnt = {}
                # Smt Assy 재고량을 PARTS_NO를 Key로 Dict화
                for i in df_joinSmt.index:
                    if df_joinSmt['현재수량'][i] < 0:
                        df_joinSmt['현재수량'][i] = 0
                    dict_smtCnt[df_joinSmt['PARTS_NO'][i]] = df_joinSmt['현재수량'][i]
                # 설정파일 불러오기
                fam3PdTimeDbHost = parser.get('FAM3공수계산DB 정보', 'Host')
                fam3PdTimeDbPort = parser.getint('FAM3공수계산DB 정보', 'Port')
                fam3PdTimeDbSID = parser.get('FAM3공수계산DB 정보', 'SID')
                fam3PdTimeDbUser = parser.get('FAM3공수계산DB 정보', 'Username')
                fam3PdTimeDbPw = parser.get('FAM3공수계산DB 정보', 'Password')
                # 검사시간DB를 가져옴(공수계산PRG용 DB)
                df_productTime = self.readDB(fam3PdTimeDbHost, fam3PdTimeDbPort, fam3PdTimeDbSID, fam3PdTimeDbUser, fam3PdTimeDbPw, 'SELECT * FROM FAM3_PRODUCT_TIME_TB')
                # 전체 검사시간을 계산
                df_productTime['TotalTime'] = (df_productTime['M_FUNCTION_CHECK'].apply(self.getSec) + df_productTime['A_FUNCTION_CHECK'].apply(self.getSec))
                # 대표모델 컬럼생성 및 중복 제거
                df_productTime['대표모델'] = df_productTime['MODEL'].str[:9]
                df_productTime = df_productTime.drop_duplicates(['대표모델'])
                df_productTime = df_productTime.reset_index(drop=True)
                progress += round(maxPb / 21)
                self.mainReturnPb.emit(progress)
                if self.isDebug:
                    df_productTime.to_excel('.\\debug\\Main\\flow7.xlsx')
                # 설정파일 불러오기
                pdbsDbHost = parser.get('MSCODE별 SMT Assy DB정보', 'Host')
                pdbsDbPort = parser.getint('MSCODE별 SMT Assy DB정보', 'Port')
                pdbsDbSID = parser.get('MSCODE별 SMT Assy DB정보', 'SID')
                pdbsDbUser = parser.get('MSCODE별 SMT Assy DB정보', 'Username')
                pdbsDbPw = parser.get('MSCODE별 SMT Assy DB정보', 'Password')
                # DB로부터 메인라인의 MSCode별 사용 Smt Assy 가져옴
                df_pdbs = self.readDB(pdbsDbHost,
                                        pdbsDbPort,
                                        pdbsDbSID,
                                        pdbsDbUser,
                                        pdbsDbPw,
                                        "SELECT SMT_MS_CODE, SMT_SMT_ASSY, SMT_CRP_GR_NO FROM sap.pdbs0010 WHERE SMT_CRP_GR_NO = '100L1311'" + str_where)
                # 불필요한 데이터 삭제
                df_pdbs = df_pdbs[~df_pdbs['SMT_MS_CODE'].str.contains('AST')]
                df_pdbs = df_pdbs[~df_pdbs['SMT_MS_CODE'].str.contains('BMS')]
                df_pdbs = df_pdbs[~df_pdbs['SMT_MS_CODE'].str.contains('WEB')]
                progress += round(maxPb / 21)
                self.mainReturnPb.emit(progress)
                if self.isDebug:
                    df_pdbs.to_excel('.\\debug\\Main\\flow7-1.xlsx')
                # 사용 Smt Assy를 병렬화
                gb = df_pdbs.groupby('SMT_MS_CODE')
                df_temp = pd.DataFrame([df_pdbs.loc[gb.groups[n], 'SMT_SMT_ASSY'].values for n in gb.groups], index=gb.groups.keys())
                df_temp.columns = ['ROW' + str(i + 1) for i in df_temp.columns]
                rowNo = len(df_temp.columns)
                df_temp = df_temp.reset_index()
                df_temp.rename(columns={'index': 'SMT_MS_CODE'}, inplace=True)
                progress += round(maxPb / 21)
                self.mainReturnPb.emit(progress)
                if self.isDebug:
                    df_temp.to_excel('.\\debug\\Main\\flow7-2.xlsx')
                # 검사설비를 List화
                # df_ATEList = df_productTime.copy()
                # df_ATEList = df_ATEList.drop_duplicates(['INSPECTION_EQUIPMENT'])
                # df_ATEList = df_ATEList.reset_index(drop=True)
                # df_ATEList['INSPECTION_EQUIPMENT'] = df_ATEList['INSPECTION_EQUIPMENT'].apply(self.delBackslash)
                # df_ATEList['INSPECTION_EQUIPMENT'] = df_ATEList['INSPECTION_EQUIPMENT'].str.strip()
                df_productTime['INSPECTION_EQUIPMENT'] = df_productTime['INSPECTION_EQUIPMENT'].apply(self.delBackslash)
                df_productTime['INSPECTION_EQUIPMENT'] = df_productTime['INSPECTION_EQUIPMENT'].str.strip()
                progress += round(maxPb / 21)
                self.mainReturnPb.emit(progress)
                # if self.isDebug:
                #     df_ATEList.to_excel('.\\debug\\Main\\flow8.xlsx')
                df_ATEList = pd.read_excel(self.list_masterFile[12])
                dict_ate = {}
                list_priorityAte = []
                # 각 검사설비를 Key로 검사시간을 Dict화
                for i in df_ATEList.index:
                    dict_ate[df_ATEList['검사호기 분류'][i]] = (60 * df_ATEList['가동시간'][i]) * 60
                    if str(df_ATEList['설비능력 MAX 사용'][i]) != 'nan' and str(df_ATEList['설비능력 MAX 사용'][i]) != '':
                        list_priorityAte.append(str(df_ATEList['검사호기 분류'][i]))
                # 대표모델 별 검사시간 및 검사설비를 Join
                df_sosAddMainModel = pd.merge(df_MergeLink, df_productTime[['대표모델', 'TotalTime', 'INSPECTION_EQUIPMENT']], on='대표모델', how='left')
                df_sosAddMainModel = df_sosAddMainModel[~df_sosAddMainModel['INSPECTION_EQUIPMENT'].str.contains('None')]
                # 모델별 사용 Smt Assy를 Join
                df_addSmtAssy = pd.merge(df_sosAddMainModel, df_temp, left_on='MS Code', right_on='SMT_MS_CODE', how='left')
                df_addSmtAssy = df_addSmtAssy.reset_index(drop=True)
                progress += round(maxPb / 21)
                self.mainReturnPb.emit(progress)
                if self.isDebug:
                    df_addSmtAssy.to_excel('.\\debug\\Main\\flow8-2.xlsx')
                df_addSmtAssy['대표모델별_최소착공필요량_per_일'] = 0
                dict_integCnt = {}
                dict_minContCnt = {}
                # 대표모델 별 최소 착공 필요량을 계산
                for i in df_addSmtAssy.index:
                    # 특수모듈의 경우, 대표모델을 특수모듈 조건표의 그룹명으로 설정
                    if str(df_addSmtAssy['1차_MAX_그룹'][i]) != '' and str(df_addSmtAssy['1차_MAX_그룹'][i]) != '-' and str(df_addSmtAssy['1차_MAX_그룹'][i]) != 'nan':
                        df_addSmtAssy['대표모델'][i] = df_addSmtAssy['1차_MAX_그룹'][i]
                    # 각 대표모델 별 적산착공량을 계산하여 딕셔너리에 저장
                    if df_addSmtAssy['대표모델'][i] in dict_integCnt:
                        dict_integCnt[df_addSmtAssy['대표모델'][i]] += int(df_addSmtAssy['미착공수주잔'][i])
                    else:
                        dict_integCnt[df_addSmtAssy['대표모델'][i]] = int(df_addSmtAssy['미착공수주잔'][i])
                    # 이미 완성지정일을 지난경우, 워킹데이 계산을 위해 워킹데이를 1로 설정
                    if df_addSmtAssy['남은 워킹데이'][i] <= 0:
                        workDay = 1
                    else:
                        workDay = df_addSmtAssy['남은 워킹데이'][i]
                    # 완성지정일별 최소필요착공량 계산 후, 딕셔너리에 리스트(대표모델, 완성지정일)로 저장
                    if str(df_addSmtAssy['1차_MAX_그룹'][i]) != '' and str(df_addSmtAssy['1차_MAX_그룹'][i]) != 'nan' and str(df_addSmtAssy['1차_MAX_그룹'][i]) != '-':
                        dict_minContCnt[df_addSmtAssy['대표모델'][i]] = [df_addSmtAssy['1차_MAX'][i], df_addSmtAssy['Planned Prod. Completion date'][i]]
                    elif len(dict_minContCnt) > 0:
                        if df_addSmtAssy['대표모델'][i] in dict_minContCnt:
                            if dict_minContCnt[df_addSmtAssy['대표모델'][i]][0] < math.ceil(dict_integCnt[df_addSmtAssy['대표모델'][i]] / workDay):
                                dict_minContCnt[df_addSmtAssy['대표모델'][i]][0] = math.ceil(dict_integCnt[df_addSmtAssy['대표모델'][i]] / workDay)
                                dict_minContCnt[df_addSmtAssy['대표모델'][i]][1] = df_addSmtAssy['Planned Prod. Completion date'][i]
                        else:
                            dict_minContCnt[df_addSmtAssy['대표모델'][i]] = [math.ceil(dict_integCnt[df_addSmtAssy['대표모델'][i]] / workDay), df_addSmtAssy['Planned Prod. Completion date'][i]]
                    else:
                        dict_minContCnt[df_addSmtAssy['대표모델'][i]] = [math.ceil(dict_integCnt[df_addSmtAssy['대표모델'][i]] / workDay), df_addSmtAssy['Planned Prod. Completion date'][i]]
                    if workDay <= 0:
                        workDay = 1
                    # 위에서 계산한 최소필요착공량을 컬럼화시켜 데이터프레임에 입력
                    if str(df_addSmtAssy['1차_MAX_그룹'][i]) != '' and str(df_addSmtAssy['1차_MAX_그룹'][i]) != 'nan' and str(df_addSmtAssy['1차_MAX_그룹'][i]) != '-':
                        df_addSmtAssy['대표모델별_최소착공필요량_per_일'][i] = df_addSmtAssy['1차_MAX'][i]
                    else:
                        df_addSmtAssy['대표모델별_최소착공필요량_per_일'][i] = dict_integCnt[df_addSmtAssy['대표모델'][i]] / workDay
                progress += round(maxPb / 21)
                self.mainReturnPb.emit(progress)
                # 검사설비능력 최적화 후, 분할된 Row를 다시 합치기 위해 분할 전 데이터를 복사하여 저장
                df_flow9 = df_addSmtAssy.copy()
                if self.isDebug:
                    df_addSmtAssy.to_excel('.\\debug\\Main\\flow9.xlsx')
                df_addSmtAssyCopy = pd.DataFrame(columns=df_addSmtAssy.columns)
                # 검사설비능력 최적화를 위하여 미착공수주잔을 20대씩 분할 시킴. (ex. 83대 일 경우, Row를 20, 20, 20, 20, 3으로 5분할)
                for i in df_addSmtAssy.index:
                    div = df_addSmtAssy['미착공수주잔'][i] // 20
                    mod = df_addSmtAssy['미착공수주잔'][i] % 20
                    df_temp = pd.DataFrame(columns=df_addSmtAssy.columns)
                    for j in range(0, div + 1):
                        if j != div:
                            df_temp = df_temp.append(df_addSmtAssy.iloc[i])
                            df_temp = df_temp.reset_index(drop=True)
                            df_temp['미착공수주잔'][j] = 20
                        elif mod > 0:
                            df_temp = df_temp.append(df_addSmtAssy.iloc[i])
                            df_temp = df_temp.reset_index(drop=True)
                            df_temp['미착공수주잔'][j] = mod
                    df_addSmtAssyCopy = pd.concat([df_addSmtAssyCopy, df_temp])
                df_addSmtAssy = df_addSmtAssyCopy.reset_index(drop=True)
                if self.isDebug:
                    df_addSmtAssy.to_excel('.\\debug\\Main\\flow9-1.xlsx')
                dict_minContCopy = dict_minContCnt.copy()
                # 대표모델 별 최소착공 필요량을 기준으로 평준화 적용 착공량을 계산. 미착공수주잔에서 해당 평준화 적용 착공량을 제외한 수량은 잔여착공량으로 기재
                df_addSmtAssy['평준화_적용_착공량'] = 0
                for i in df_addSmtAssy.index:
                    if df_addSmtAssy['긴급오더'][i] == '대상':
                        df_addSmtAssy['평준화_적용_착공량'][i] = int(df_addSmtAssy['미착공수주잔'][i])
                        if df_addSmtAssy['대표모델'][i] in dict_minContCopy:
                            if dict_minContCopy[df_addSmtAssy['대표모델'][i]][0] >= int(df_addSmtAssy['미착공수주잔'][i]):
                                dict_minContCopy[df_addSmtAssy['대표모델'][i]][0] -= int(df_addSmtAssy['미착공수주잔'][i])
                            else:
                                dict_minContCopy[df_addSmtAssy['대표모델'][i]][0] = 0
                    elif df_addSmtAssy['대표모델'][i] in dict_minContCopy:
                        if dict_minContCopy[df_addSmtAssy['대표모델'][i]][0] >= int(df_addSmtAssy['미착공수주잔'][i]):
                            df_addSmtAssy['평준화_적용_착공량'][i] = int(df_addSmtAssy['미착공수주잔'][i])
                            dict_minContCopy[df_addSmtAssy['대표모델'][i]][0] -= int(df_addSmtAssy['미착공수주잔'][i])
                        else:
                            df_addSmtAssy['평준화_적용_착공량'][i] = dict_minContCopy[df_addSmtAssy['대표모델'][i]][0]
                            dict_minContCopy[df_addSmtAssy['대표모델'][i]][0] = 0
                df_addSmtAssy['잔여_착공량'] = df_addSmtAssy['미착공수주잔'] - df_addSmtAssy['평준화_적용_착공량']
                df_addSmtAssy = df_addSmtAssy.sort_values(by=['우선착공', '긴급오더', '당일착공', 'Planned Prod. Completion date', '평준화_적용_착공량'], ascending=[False, False, False, True, False])
                df_addSmtAssy = df_addSmtAssy.reset_index(drop=True)
                progress += round(maxPb / 21)
                self.mainReturnPb.emit(progress)
                if self.isDebug:
                    df_addSmtAssy.to_excel('.\\debug\\Main\\flow10.xlsx')
                df_addSmtAssy['SMT반영_착공량'] = 0
                # 알람 상세 DataFrame 생성
                df_alarmDetail = pd.DataFrame(columns=["No.", "분류", "L/N", "MS CODE", "SMT ASSY", "수주수량", "부족수량", "검사호기", "대상 검사시간(초)", "필요시간(초)", "완성예정일"])
                alarmDetailNo = 1
                # 최소착공량에 대해 Smt적용 착공량 계산
                df_addSmtAssy, dict_smtCnt, alarmDetailNo, df_alarmDetail = self.smtReflectInst(df_addSmtAssy, False, dict_smtCnt, alarmDetailNo, df_alarmDetail, rowNo)
                if self.isDebug:
                    df_alarmDetail.to_excel('.\\debug\\Main\\df_alarmDetail.xlsx')
                # 잔여 착공량에 대해 Smt적용 착공량 계산
                df_addSmtAssy['SMT반영_착공량_잔여'] = 0
                df_addSmtAssy, dict_smtCnt, alarmDetailNo, df_alarmDetail = self.smtReflectInst(df_addSmtAssy, True, dict_smtCnt, alarmDetailNo, df_alarmDetail, rowNo)
                progress += round(maxPb / 21)
                self.mainReturnPb.emit(progress)
                if self.isDebug:
                    df_addSmtAssy.to_excel('.\\debug\\Main\\flow11.xlsx')
                df_addSmtAssy['임시수량'] = 0
                df_addSmtAssy['설비능력반영_착공량'] = 0
                df_addSmtAssy['임시수량_잔여'] = 0
                df_addSmtAssy['설비능력반영_착공량_잔여'] = 0
                df_addSmtAssy['남은착공량'] = 0
                df_addSmtAssy = df_addSmtAssy.reset_index()
                # 검사설비 우선사용 계산을 위해 데이터프레임 신규 선언
                df_priority = pd.DataFrame(columns=df_addSmtAssy.columns)
                df_unPriority = pd.DataFrame(columns=df_addSmtAssy.columns)
                # 우선사용 검사설비 대상을 찾아 우선사용/비사용 데이터 프레임으로 분할
                if len(list_priorityAte) > 0:
                    for ate in list_priorityAte:
                        df_priority = pd.concat([df_priority, df_addSmtAssy[df_addSmtAssy['INSPECTION_EQUIPMENT'].str.contains(ate)]])
                        df_priority = pd.concat([df_priority, df_addSmtAssy[df_addSmtAssy['긴급오더'].notnull()]])
                        df_priority = pd.concat([df_priority, df_addSmtAssy[df_addSmtAssy['당일착공'].str.contains('대상')]])
                        if len(df_unPriority) > 0:
                            df_unPriority = pd.merge(df_unPriority, df_addSmtAssy[~df_addSmtAssy['INSPECTION_EQUIPMENT'].str.contains(ate)], how='inner')
                            df_unPriority = df_unPriority[df_unPriority['긴급오더'].isnull()]
                            df_unPriority = df_unPriority[~df_unPriority['당일착공'].str.contains('대상')]
                            df_unPriority = df_unPriority.drop_duplicates(['index'])
                        else:
                            df_unPriority = df_addSmtAssy[~df_addSmtAssy['INSPECTION_EQUIPMENT'].str.contains(ate)]
                            df_unPriority = df_unPriority[df_unPriority['긴급오더'].isnull()]
                            df_unPriority = df_unPriority[~df_unPriority['당일착공'].str.contains('대상')]
                            df_unPriority = df_unPriority.drop_duplicates(['index'])
                        df_priority = df_priority.drop_duplicates(['index'])
                else:
                    df_unPriority = df_addSmtAssy.copy()
                if self.isDebug:
                    df_priority.to_excel('.\\debug\\Main\\flow11-1.xlsx')
                    df_unPriority.to_excel('.\\debug\\Main\\flow11-2.xlsx')
                # CT제한 조건표를 불러오기
                df_limitCtCond = pd.read_excel(self.list_masterFile[13])
                limitCtCnt = df_limitCtCond[df_limitCtCond['상세구분'] == 'MAIN']['허용수량'].values[0]
                # 설비능력 반영 착공량 계산
                # print(df_priority.head())
                # print(df_unPriority.head())
                df_priority, dict_ate, alarmDetailNo, df_alarmDetail, self.moduleMaxCnt, limitCtCnt = self.ateReflectInst(df_priority, False, dict_ate, df_alarmDetail, alarmDetailNo, self.moduleMaxCnt, limitCtCnt)
                # if self.isDebug:
                #     df_priority.to_excel('.\\debug\\Main\\flow11-3.xlsx')
                # 잔여 착공량에 대해 설비능력 반영 착공량 계산
                df_priority, dict_ate, alarmDetailNo, df_alarmDetail, self.moduleMaxCnt, limitCtCnt = self.ateReflectInst(df_priority, True, dict_ate, df_alarmDetail, alarmDetailNo, self.moduleMaxCnt, limitCtCnt)
                # 설비능력 반영 착공량 계산
                df_unPriority, dict_ate, alarmDetailNo, df_alarmDetail, self.moduleMaxCnt, limitCtCnt = self.ateReflectInst(df_unPriority, False, dict_ate, df_alarmDetail, alarmDetailNo, self.moduleMaxCnt, limitCtCnt)
                # 잔여 착공량에 대해 설비능력 반영 착공량 계산
                df_unPriority, dict_ate, alarmDetailNo, df_alarmDetail, self.moduleMaxCnt, limitCtCnt = self.ateReflectInst(df_unPriority, True, dict_ate, df_alarmDetail, alarmDetailNo, self.moduleMaxCnt, limitCtCnt)
                # 검사설비 우선사용/비사용 데이터프레임을 다시 통합.
                df_addSmtAssy = pd.concat([df_priority, df_unPriority])
                # 잔여 검사설비능력 출력을 위하여 데이터프레임 선언
                df_dict = pd.DataFrame(data=dict_ate, index=[0])
                df_dict = df_dict.T
                if self.isDebug:
                    df_dict.to_excel('.\\debug\\Main\\dict_ate.xlsx')
                df_dict = df_dict.reset_index()
                df_dict.columns = ['검사설비', '남은 시간']
                df_dict['남은 시간'] = df_dict['남은 시간'].apply(self.convertSecToTime)
                df_dict = pd.merge(df_dict, df_ATEList, how='left', left_on='검사설비', right_on='검사호기 분류')
                df_dict = df_dict[['검사설비', '가동시간', '남은 시간']]
                now = time.strftime('%H%M%S')
                # 현재 시간의 잔여검사설비 출력
                if not os.path.exists(f'.\\Output\\Result\\{str(today)}'):
                    os.makedirs(f'.\\Output\\Result\\{str(today)}')
                if not os.path.exists(f'.\\Output\\Result\\{str(today)}\\{self.cb_round}'):
                    os.makedirs(f'.\\Output\\Result\\{str(today)}\\{self.cb_round}')
                df_dict.to_excel(f'.\\Output\\Result\\\\{str(today)}\\{str(self.cb_round)}\\잔여검사설비능력_{now}.xlsx')
                progress += round(maxPb / 21)
                self.mainReturnPb.emit(progress)
                df_addSmtAssy = df_addSmtAssy.reset_index(drop=True)
                if self.isDebug:
                    df_addSmtAssy.to_excel('.\\debug\\Main\\flow12.xlsx')
                    df_alarmDetail = df_alarmDetail.reset_index(drop=True)
                    df_alarmDetail.to_excel('.\\debug\\Main\\df_alarmDetail.xlsx')
                # 알람 상세 결과에서 각 항목별로 요약
                if len(df_alarmDetail) > 0:
                    # 분류1 요약
                    df_firstAlarm = df_alarmDetail[df_alarmDetail['분류'] == '1']
                    df_firstAlarmSummary = df_firstAlarm.groupby("SMT ASSY")['부족수량'].sum()
                    df_firstAlarmSummary = df_firstAlarmSummary.reset_index()
                    df_firstAlarmSummary['수량'] = df_firstAlarmSummary['부족수량']
                    df_firstAlarmSummary['분류'] = '1'
                    df_firstAlarmSummary['MS CODE'] = '-'
                    df_firstAlarmSummary['검사호기'] = '-'
                    df_firstAlarmSummary['부족 시간'] = '-'
                    df_firstAlarmSummary['Message'] = '[SMT ASSY : ' + df_firstAlarmSummary["SMT ASSY"] + ']가 부족합니다. SMT ASSY 제작을 지시해주세요.'
                    del df_firstAlarmSummary['부족수량']
                    # 분류2 요약
                    df_secAlarm = df_alarmDetail[df_alarmDetail['분류'] == '2']
                    df_secAlarmSummary = df_secAlarm.groupby("검사호기")['필요시간(초)'].sum()
                    df_secAlarmSummary = df_secAlarmSummary.reset_index()
                    df_secAlarmSummary['부족 시간'] = df_secAlarmSummary['필요시간(초)']
                    df_secAlarmSummary['부족 시간'] = df_secAlarmSummary['부족 시간'].apply(self.convertSecToTime)
                    df_secAlarmSummary['분류'] = '2'
                    df_secAlarmSummary['MS CODE'] = '-'
                    df_secAlarmSummary['SMT ASSY'] = '-'
                    df_secAlarmSummary['수량'] = '-'
                    df_secAlarmSummary['Message'] = '검사설비능력이 부족합니다. 생산 가능여부를 확인해 주세요.'
                    del df_secAlarmSummary['필요시간(초)']
                    # 분류 기타2 요약
                    df_etc2Alarm = df_alarmDetail[df_alarmDetail['분류'] == '기타2']
                    df_etc2AlarmSummary = df_etc2Alarm.groupby('MS CODE')['부족수량'].sum()
                    df_etc2AlarmSummary = df_etc2AlarmSummary.reset_index()
                    df_etc2AlarmSummary['수량'] = df_etc2AlarmSummary['부족수량']
                    df_etc2AlarmSummary['분류'] = '기타2'
                    df_etc2AlarmSummary['SMT ASSY'] = '-'
                    df_etc2AlarmSummary['검사호기'] = '-'
                    df_etc2AlarmSummary['부족 시간'] = '-'
                    df_etc2AlarmSummary['Message'] = '긴급오더 및 당일착공 대상의 총 착공량이 입력한 최대착공량보다 큽니다. 최대착공량을 확인해주세요.'
                    del df_etc2AlarmSummary['부족수량']
                    # 분류 기타4 요약
                    df_etc4Alarm = df_alarmDetail[df_alarmDetail['분류'] == '기타4']
                    df_etc4AlarmSummary = df_etc4Alarm.groupby('MS CODE')['부족수량'].sum()
                    df_etc4AlarmSummary = df_etc4AlarmSummary.reset_index()
                    df_etc4AlarmSummary['수량'] = df_etc4AlarmSummary['부족수량']
                    df_etc4AlarmSummary['분류'] = '기타4'
                    df_etc4AlarmSummary['SMT ASSY'] = '-'
                    df_etc4AlarmSummary['검사호기'] = '-'
                    df_etc4AlarmSummary['부족 시간'] = '-'
                    df_etc4AlarmSummary['Message'] = '설정된 CT 제한대수보다 최소 착공 필요량이 많습니다. 설정된 CT 제한대수를 확인해주세요.'
                    # 위 알람을 병합
                    df_alarmSummary = pd.concat([df_firstAlarmSummary, df_secAlarmSummary, df_etc2AlarmSummary, df_etc4AlarmSummary])
                    # 기타 알람에 대한 추가
                    df_etcList = df_alarmDetail[(df_alarmDetail['분류'] == '기타1') | (df_alarmDetail['분류'] == '기타3')]
                    df_etcList = df_etcList.drop_duplicates(['MS CODE'])
                    for i in df_etcList.index:
                        if df_etcList['분류'][i] == '기타1':
                            df_alarmSummary = pd.concat([df_alarmSummary,
                                                        pd.DataFrame.from_records([{"분류": df_etcList['분류'][i],
                                                                                    "MS CODE": df_etcList['MS CODE'][i],
                                                                                    "SMT ASSY": '-',
                                                                                    "수량": 0,
                                                                                    "검사호기": '-',
                                                                                    "부족 시간": 0,
                                                                                    "Message": '해당 MS CODE에서 사용되는 SMT ASSY가 등록되지 않았습니다. 등록 후 다시 실행해주세요.'}])])
                        elif df_etcList['분류'][i] == '기타3':
                            df_alarmSummary = pd.concat([df_alarmSummary,
                                                        pd.DataFrame.from_records([{"분류": df_etcList['분류'][i],
                                                                                    "MS CODE": df_etcList['MS CODE'][i],
                                                                                    "SMT ASSY": '-',
                                                                                    "수량": 0,
                                                                                    "검사호기": '-',
                                                                                    "부족 시간": 0,
                                                                                    "Message": 'SMT ASSY 정보가 등록되지 않아 재고를 확인할 수 없습니다. 등록 후 다시 실행해주세요.'}])])
                    df_alarmSummary = df_alarmSummary.reset_index(drop=True)
                    df_alarmSummary = df_alarmSummary[['분류', 'MS CODE', 'SMT ASSY', '수량', '검사호기', '부족 시간', 'Message']]
                    if self.isDebug:
                        df_alarmSummary.to_excel('.\\debug\\Main\\df_alarmSummary.xlsx')
                    if not os.path.exists(f'.\\Output\\Alarm\\{str(today)}\\{self.cb_round}'):
                        os.makedirs(f'.\\Output\\Alarm\\{str(today)}\\{self.cb_round}')
                    df_alarmExplain = pd.DataFrame({'분류': ['1', '2', '기타1', '기타2', '기타3', '기타4'],
                                                            '분류별 상황': ['DB상의 Smt Assy가 부족하여 해당 MS-Code를 착공 내릴 수 없는 경우',
                                                            '당일 착공분(or 긴급착공분)에 대해 검사설비 능력이 부족할 경우',
                                                            'MS-Code와 일치하는 Smt Assy가 마스터 파일에 없는 경우',
                                                            '긴급오더 대상 착공시 최대착공량(사용자입력공수)이 부족할 경우',
                                                            'SMT ASSY 정보가 DB에 미등록된 경우',
                                                            '당일 최소 착공필요량 > CT제한 대수인 경우']})
                    # 파일 한개로 출력
                    with pd.ExcelWriter(f'.\\Output\\Alarm\\{str(today)}\\{self.cb_round}\\FAM3_AlarmList_{str(today)}_Main.xlsx') as writer:
                        df_alarmSummary.to_excel(writer, sheet_name='정리', index=True)
                        df_alarmDetail.to_excel(writer, sheet_name='상세', index=True)
                        df_alarmExplain.to_excel(writer, sheet_name='설명', index=False)
                addColumnList = ['평준화_적용_착공량', '잔여_착공량', 'SMT반영_착공량', 'SMT반영_착공량_잔여', '설비능력반영_착공량', '설비능력반영_착공량_잔여']
                # 20개씩 분할되었던 Row를 하나로 통합하는 작업 실시
                for column in addColumnList:
                    df_group = df_addSmtAssy.groupby('Linkage Number')[column].sum()
                    df_group = df_group.reset_index()
                    df_group.columns = ['Linkage Number', column]
                    df_flow9 = pd.merge(df_flow9, df_group[['Linkage Number', column]], on='Linkage Number', how='left')
                    df_flow9[column] = df_flow9[column].fillna(0)
                df_addSmtAssy = df_flow9.copy()
                df_addSmtAssy = df_addSmtAssy.sort_values(by=['우선착공', '긴급오더', '당일착공', 'Planned Prod. Completion date', '설비능력반영_착공량', '설비능력반영_착공량_잔여'], ascending=[False, False, False, True, False, False])
                df_addSmtAssy = df_addSmtAssy.reset_index(drop=True)
                df_addSmtAssy['MODEL'] = df_addSmtAssy['MS Code'].str[:6]
                if self.isDebug:
                    df_addSmtAssy.to_excel('.\\debug\\Main\\flow12-1.xlsx')
                # 총착공량 컬럼으로 병합
                df_addSmtAssy['총착공량'] = df_addSmtAssy['설비능력반영_착공량'] + df_addSmtAssy['설비능력반영_착공량_잔여']
                df_addSmtAssy = df_addSmtAssy[df_addSmtAssy['총착공량'] != 0]
                # 홀딩리스트 파일 불러오기
                df_holdingList = pd.read_excel(self.list_masterFile[17])
                # 홀딩리스트와 비교하여 조건에 해당하는 경우, 알람 메시지 출력
                for i in df_holdingList.index:
                    message = ""
                    if len(df_addSmtAssy[df_addSmtAssy['MODEL'] == df_holdingList['MODEL'][i]]['총착공량'].values) > 0:
                        totalCnt = 0
                        for cnt in df_addSmtAssy[df_addSmtAssy['MODEL'] == df_holdingList['MODEL'][i]]['총착공량'].values:
                            totalCnt += cnt
                        message = f"{df_holdingList['MODEL'][i]} {int(totalCnt)}대 {df_holdingList['REMARK'][i]}"
                    if len(df_addSmtAssy[df_addSmtAssy['Linkage Number'] == df_holdingList['LINKAGENO'][i]]['총착공량'].values) > 0:
                        message = f"{df_holdingList['LINKAGENO'][i]} {int(df_addSmtAssy[df_addSmtAssy['Linkage Number'] == df_holdingList['LINKAGENO'][i]]['총착공량'].values[0])}대 {df_holdingList['REMARK'][i]}"
                    if len(df_addSmtAssy[df_addSmtAssy['MS Code'] == df_holdingList['MS-CODE'][i]]['총착공량'].values) > 0:
                        totalCnt = 0
                        for cnt in df_addSmtAssy[df_addSmtAssy['MS Code'] == df_holdingList['MS-CODE'][i]]['총착공량'].values:
                            totalCnt += cnt
                        message = f"{df_holdingList['MS-CODE'][i]} {int(totalCnt)}대 {df_holdingList['REMARK'][i]}"
                    if len(message) > 0:
                        self.mainReturnWarning.emit(message)
                progress += round(maxPb / 21)
                self.mainReturnPb.emit(progress)
                if self.isDebug:
                    df_addSmtAssy.to_excel('.\\debug\\Main\\flow13.xlsx')
                df_returnSp = df_addSmtAssy[df_addSmtAssy['특수대상'] == '대상']
                self.mainReturnDf.emit(df_returnSp)
                df_addSmtAssy = df_addSmtAssy[df_addSmtAssy['특수대상'] != '대상']
                # 최대착공량만큼 착공 못했을 경우, 메시지 출력
                if self.moduleMaxCnt > 0:
                    self.mainReturnWarning.emit(f'아직 착공하지 못한 모델이 [{int(self.moduleMaxCnt)}대] 남았습니다. 설비능력 부족이 예상됩니다. 확인해주세요.')
                # 레벨링 리스트와 병합
                df_addSmtAssy = df_addSmtAssy.astype({'Linkage Number': 'str'})
                df_levelingMain = df_levelingMain.astype({'Linkage Number': 'str'})
                df_mergeOrder = pd.merge(df_addSmtAssy, df_levelingMain, on='Linkage Number', how='left')
                progress += round(maxPb / 21)
                self.mainReturnPb.emit(progress)
                if self.isDebug:
                    df_mergeOrder.to_excel('.\\debug\\Main\\flow14.xlsx')
                df_mergeOrderResult = pd.DataFrame().reindex_like(df_mergeOrder)
                df_mergeOrderResult = df_mergeOrderResult[0:0]
                # 총착공량 만큼 개별화
                for i in df_addSmtAssy.index:
                    for j in df_mergeOrder.index:
                        if df_addSmtAssy['Linkage Number'][i] == df_mergeOrder['Linkage Number'][j]:
                            if j > 0:
                                if df_mergeOrder['Linkage Number'][j] != df_mergeOrder['Linkage Number'][j - 1]:
                                    orderCnt = int(df_addSmtAssy['총착공량'][i])
                            else:
                                orderCnt = int(df_addSmtAssy['총착공량'][i])
                            if orderCnt > 0:
                                df_mergeOrderResult = df_mergeOrderResult.append(df_mergeOrder.iloc[j])
                                orderCnt -= 1
                # 사이클링을 위해 검사설비별로 정리
                df_mergeOrderResult = df_mergeOrderResult.sort_values(by=['INSPECTION_EQUIPMENT'], ascending=[False])
                df_mergeOrderResult = df_mergeOrderResult.reset_index(drop=True)
                progress += round(maxPb / 21)
                self.mainReturnPb.emit(progress)
                if self.isDebug:
                    df_mergeOrderResult.to_excel('.\\debug\\Main\\flow15.xlsx')
                # 긴급오더 제외하고 사이클 대상만 식별하여 검사장치별로 갯수 체크
                df_unCt = df_mergeOrderResult[df_mergeOrderResult['MS Code'].str.contains('/CT')]
                df_mergeOrderResult = df_mergeOrderResult[~df_mergeOrderResult['MS Code'].str.contains('/CT')]
                df_cycleCopy = df_mergeOrderResult[df_mergeOrderResult['긴급오더'].isnull()]
                df_cycleCopy['검사장치Cnt'] = df_cycleCopy.groupby('INSPECTION_EQUIPMENT')['INSPECTION_EQUIPMENT'].transform('size')
                df_cycleCopy = df_cycleCopy.sort_values(by=['검사장치Cnt'], ascending=[False])
                df_cycleCopy = df_cycleCopy.reset_index(drop=True)
                # 긴급오더 포함한 Df와 병합
                df_mergeOrderResult = pd.merge(df_mergeOrderResult, df_cycleCopy[['Planned Order', '검사장치Cnt']], on='Planned Order', how='left')
                df_mergeOrderResult = df_mergeOrderResult.sort_values(by=['검사장치Cnt'], ascending=[False])
                df_mergeOrderResult = df_mergeOrderResult.reset_index(drop=True)
                progress += round(maxPb / 21)
                self.mainReturnPb.emit(progress)
                if self.isDebug:
                    df_mergeOrderResult.to_excel('.\\debug\\Main\\flow15-1.xlsx')
                # 최대 사이클 번호 체크
                maxCycle = float(df_cycleCopy['검사장치Cnt'][0])
                cycleGr = 1.0
                df_mergeOrderResult['사이클그룹'] = 0
                # 각 검사장치별로 사이클 그룹을 작성하고, 최대 사이클과 비교하여 각 사이클그룹에서 배수처리
                for i in df_mergeOrderResult.index:
                    if df_mergeOrderResult['긴급오더'][i] != '대상':
                        multiCnt = maxCycle / df_mergeOrderResult['검사장치Cnt'][i]
                        if i == 0:
                            df_mergeOrderResult['사이클그룹'][i] = cycleGr
                        else:
                            if df_mergeOrderResult['INSPECTION_EQUIPMENT'][i] != df_mergeOrderResult['INSPECTION_EQUIPMENT'][i - 1]:
                                if i == 1:
                                    cycleGr = 2.0
                                else:
                                    cycleGr = 1.0
                            df_mergeOrderResult['사이클그룹'][i] = cycleGr * multiCnt
                        cycleGr += 1.0
                    if cycleGr >= maxCycle:
                        cycleGr = 1.0
                # 배정된 사이클 그룹 순으로 정렬
                df_mergeOrderResult = df_mergeOrderResult.sort_values(by=['사이클그룹'], ascending=[True])
                df_mergeOrderResult = df_mergeOrderResult.reset_index(drop=True)
                progress += round(maxPb / 21)
                self.mainReturnPb.emit(progress)
                if self.isDebug:
                    df_mergeOrderResult.to_excel('.\\debug\\Main\\flow16.xlsx')
                df_mergeOrderResult = df_mergeOrderResult.reset_index()
                # 연속으로 같은 검사설비가 오지 않도록 순서를 재조정
                for i in df_mergeOrderResult.index:
                    if df_mergeOrderResult['긴급오더'][i] != '대상':
                        if (i != 0 and (df_mergeOrderResult['INSPECTION_EQUIPMENT'][i] == df_mergeOrderResult['INSPECTION_EQUIPMENT'][i - 1])):
                            for j in df_mergeOrderResult.index:
                                if df_mergeOrderResult['긴급오더'][j] != '대상':
                                    if ((j != 0 and j < len(df_mergeOrderResult) - 1) and (df_mergeOrderResult['INSPECTION_EQUIPMENT'][i] != df_mergeOrderResult['INSPECTION_EQUIPMENT'][j + 1]) and (df_mergeOrderResult['INSPECTION_EQUIPMENT'][i] != df_mergeOrderResult['INSPECTION_EQUIPMENT'][j])):
                                        df_mergeOrderResult['index'][i] = (float(df_mergeOrderResult['index'][j]) + float(df_mergeOrderResult['index'][j + 1])) / 2
                                        df_mergeOrderResult = df_mergeOrderResult.sort_values(by=['index'], ascending=[True])
                                        df_mergeOrderResult = df_mergeOrderResult.reset_index(drop=True)
                                        break
                df_unCt['index'] = 0
                df_unCt['사이클그룹'] = 0
                # CT대상인 모델과 비대상 모델을 다시 병합. (CT는 사이클그룹이 가장최상위)
                df_mergeOrderResult = pd.concat([df_unCt, df_mergeOrderResult])
                df_mergeOrderResult = df_mergeOrderResult.reset_index(drop=True)
                progress += round(maxPb / 21)
                self.mainReturnPb.emit(progress)
                if self.isDebug:
                    df_mergeOrderResult.to_excel('.\\debug\\Main\\flow17.xlsx')
                df_mergeOrderResult['No (*)'] = int(maxNo) + (df_mergeOrderResult.index.astype(int) + 1) * 10
                df_mergeOrderResult['Scheduled Start Date (*)'] = self.constDate
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
                dict_emgLinkage = {}
                dict_emgMscode = {}
                for i in df_emgLinkage.index:
                    if len(df_mergeOrderResult[df_mergeOrderResult['Linkage Number'] == df_emgLinkage['Linkage Number'][i]]['Linkage Number'].values) > 0:
                        dict_emgLinkage[df_emgLinkage['Linkage Number'][i]] = True
                    else:
                        dict_emgLinkage[df_emgLinkage['Linkage Number'][i]] = False
                for i in df_emgmscode.index:
                    if len(df_mergeOrderResult[df_mergeOrderResult['MS Code'] == df_emgmscode['MS-CODE'][i]]['MS Code'].values) > 0:
                        dict_emgMscode[df_emgLinkage['MS-CODE'][i]] = True
                    else:
                        dict_emgMscode[df_emgLinkage['MS-CODE'][i]] = False

                self.mainReturnEmgLinkage.emit(dict_emgLinkage)
                self.mainReturnEmgMscode.emit(dict_emgMscode)

                progress += round(maxPb / 21)
                self.mainReturnPb.emit(progress)

                outputFile = f'.\\Output\\Result\\{str(today)}\\{self.cb_round}\\{str(today)}_Main.xlsx'
                df_mergeOrderResult.to_excel(outputFile, index=False)
            else:
                df_returnSp = pd.DataFrame()
                self.mainReturnDf.emit(df_returnSp)
                self.mainReturnPb.emit(maxPb)
            # if self.isDebug:
            end = time.time()
            print(f"{end - start:.5f} sec")
            self.mainReturnEnd.emit(True)
            return
        except Exception as e:
            self.mainReturnError.emit(e)
            return


class PowerThread(QObject):
    powerReturnError = pyqtSignal(Exception)
    powerReturnInfo = pyqtSignal(str)
    powerReturnEnd = pyqtSignal(bool)
    powerReturnWarning = pyqtSignal(str)
    powerReturnPb = pyqtSignal(int)
    powerReturnMaxPb = pyqtSignal(int)
    powerReturnEmgLinkage = pyqtSignal(dict)
    powerReturnEmgMscode = pyqtSignal(dict)

    def __init__(self, debugFlag, date, constDate, list_masterFile, moduleMaxCnt, emgHoldList, cb_round, df_etcOrderInput):
        super().__init__()
        self.isDebug = debugFlag
        self.date = date
        self.constDate = constDate
        self.list_masterFile = list_masterFile
        self.moduleMaxCnt = moduleMaxCnt
        self.emgHoldList = emgHoldList
        self.cb_round = cb_round
        self.df_etcOrderInput = df_etcOrderInput

    # 워킹데이 체크 내부함수
    def checkWorkDay(self, df, today, compDate):
        dtToday = pd.to_datetime(datetime.datetime.strptime(today, '%Y%m%d'))
        dtComp = pd.to_datetime(compDate, unit='s')
        workDay = 0
        if len(df.index[(df['Date'] == dtComp)].tolist()) > 0:
            index = int(df.index[(df['Date'] == dtComp)].tolist()[0])
            # 위에서 찾은 완성지정일로부터 프로그램 구동 당일까지 워킹데이를 계산.
            while dtToday > pd.to_datetime(df['Date'][index], unit='s'):
                if df['WorkingDay'][index] == 1:
                    workDay -= 1
                index += 1
            # 프로그램 구동 당일 ~ 완성지정일 까지의 워킹데이를 계산
            for i in df.index:
                dt = pd.to_datetime(df['Date'][i], unit='s')
                if dtToday < dt and dt <= dtComp:
                    if df['WorkingDay'][i] == 1:
                        workDay += 1
        else:
            self.powerReturnWarning.emit(f'FY{today[2:4]}_Calendar.xlsx 파일에 {str(dtComp.date())} 날짜의 워킹데이 데이터가 없습니다. 대한민국 휴일을 기준으로 근무일을 계산합니다. 이후, 해당 파일에 사력을 추가해주세요')
            workDay = np.busday_count(begindates=dtToday.date(), enddates=dtComp.date())
        return workDay

    # 콤마 삭제용 내부함수
    def delComma(self, value):
        return str(value).split('.')[0]

    # 디비 불러오기 공통내부함수
    def readDB(self, ip, port, sid, userName, password, sql):
        location = r'.\\instantclient_21_7'
        os.environ["PATH"] = location + ";" + os.environ["PATH"]
        dsn = cx_Oracle.makedsn(ip, port, sid)
        db = cx_Oracle.connect(userName, password, dsn)
        cursor = db.cursor()
        cursor.execute(sql)
        out_data = cursor.fetchall()
        df_oracle = pd.DataFrame(out_data)
        col_names = [row[0] for row in cursor.description]
        df_oracle.columns = col_names
        return df_oracle

    # 생산시간 합계용 내부함수
    def getSec(self, time_str):
        time_str = re.sub(r'[^0-9:]', '', str(time_str))
        if len(time_str) > 0:
            h, m, s = time_str.split(':')
            return int(h) * 3600 + int(m) * 60 + int(s)
        else:
            return 0

    def concatAlarmDetail(self, df_target, no, category, df_data, index, smtAssy, shortageCnt):
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
        df_result = pd.DataFrame()
        if category == '1':
            df_result = pd.concat([df_target,
                                    pd.DataFrame.from_records([{"No.": no,
                                                                "분류": category,
                                                                "L/N": df_data['Linkage Number'][index],
                                                                "MS CODE": df_data['MS Code'][index],
                                                                "SMT ASSY": smtAssy,
                                                                "수주수량": df_data['미착공수주잔'][index],
                                                                "부족수량": shortageCnt,
                                                                "검사호기": '-',
                                                                "대상 검사시간(초)": 0,
                                                                "필요시간(초)": 0,
                                                                "완성예정일": df_data['Planned Prod. Completion date'][index]}])])
        elif '2-' in category:
            df_result = pd.concat([df_target,
                                    pd.DataFrame.from_records([{"No.": no,
                                                                "분류": category,
                                                                "L/N": df_data['Linkage Number'][index],
                                                                "MS CODE": df_data['MS Code'][index],
                                                                "SMT ASSY": '-',
                                                                "수주수량": df_data['미착공수주잔'][index],
                                                                "부족수량": shortageCnt,
                                                                "검사호기": '-',
                                                                "대상 검사시간(초)": 0,
                                                                "필요시간(초)": 0,
                                                                "완성예정일": df_data['Planned Prod. Completion date'][index]}])])
        elif category == '기타1':
            df_result = pd.concat([df_target,
                                    pd.DataFrame.from_records([{"No.": no,
                                                                "분류": category,
                                                                "L/N": df_data['Linkage Number'][index],
                                                                "MS CODE": df_data['MS Code'][index],
                                                                "SMT ASSY": '미등록',
                                                                "수주수량": df_data['미착공수주잔'][index],
                                                                "부족수량": 0,
                                                                "검사호기": '-',
                                                                "대상 검사시간(초)": 0,
                                                                "필요시간(초)": 0,
                                                                "완성예정일": df_data['Planned Prod. Completion date'][index]}])])
        elif category == '기타2':
            df_result = pd.concat([df_target,
                                    pd.DataFrame.from_records([{"No.": no,
                                                                "분류": category,
                                                                "L/N": df_data['Linkage Number'][index],
                                                                "MS CODE": df_data['MS Code'][index],
                                                                "SMT ASSY": '-',
                                                                "수주수량": df_data['미착공수주잔'][index],
                                                                "부족수량": shortageCnt,
                                                                "검사호기": '-',
                                                                "대상 검사시간(초)": 0,
                                                                "필요시간(초)": 0,
                                                                "완성예정일": df_data['Planned Prod. Completion date'][index]}])])
        elif category == '기타3':
            df_result = pd.concat([df_target,
                                    pd.DataFrame.from_records([{"No.": no,
                                                                "분류": category,
                                                                "L/N": df_data['Linkage Number'][index],
                                                                "MS CODE": df_data['MS Code'][index],
                                                                "SMT ASSY": smtAssy,
                                                                "수주수량": df_data['미착공수주잔'][index],
                                                                "부족수량": 0,
                                                                "검사호기": '-',
                                                                "대상 검사시간(초)": 0,
                                                                "필요시간(초)": 0,
                                                                "완성예정일": df_data['Planned Prod. Completion date'][index]}])])
        elif category == '기타4':
            df_result = pd.concat([df_target,
                                    pd.DataFrame.from_records([{"No.": no,
                                                                "분류": category,
                                                                "L/N": df_data['Linkage Number'][index],
                                                                "MS CODE": df_data['MS Code'][index],
                                                                "SMT ASSY": smtAssy,
                                                                "수주수량": df_data['미착공수주잔'][index],
                                                                "부족수량": shortageCnt,
                                                                "검사호기": '-',
                                                                "대상 검사시간(초)": 0,
                                                                "필요시간(초)": 0,
                                                                "완성예정일": df_data['Planned Prod. Completion date'][index]}])])
        return [df_result, no + 1]

    def smtReflectInst(self, df_input, isRemain, dict_smtCnt, alarmDetailNo, df_alarmDetail, rowNo):
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
        # 행별로 확인
        for i in df_input.index:
            # BU는 SMT Assy를 확인하지 않음.
            if df_input['MS Code'][i][:4] == 'F3BU':
                df_input[resultCol][i] = df_input[instCol][i]
            else:
                # 사용 Smt Assy 개수 확인
                for j in range(1, rowNo):
                    if j == 1:
                        rowCnt = 1
                    if (str(df_input[f'ROW{str(j)}'][i]) != '' and str(df_input[f'ROW{str(j)}'][i]) != 'nan'):
                        rowCnt = j
                    else:
                        break
                if rowNo == 1:
                    rowCnt = 1
                minCnt = 9999
                # 각 SmtAssy 별로 착공 가능 대수 확인
                for j in range(1, rowCnt + 1):
                    smtAssyName = str(df_input[f'ROW{str(j)}'][i])
                    if (df_input['SMT_MS_CODE'][i] != 'nan' and df_input['SMT_MS_CODE'][i] != 'None' and df_input['SMT_MS_CODE'][i] != ''):
                        if (smtAssyName != '' and smtAssyName != 'nan' and smtAssyName != 'None'):
                            # 긴급오더 혹은 당일착공 대상일 경우, SMT Assy 잔량에 관계없이 착공 실시.

                            if (df_input['긴급오더'][i] == '대상' or df_input['당일착공'][i] == '대상' and not isRemain):
                                # MS Code와 연결된 SMT Assy가 있을 경우, 정상적으로 로직을 실행
                                if smtAssyName in dict_smtCnt:
                                    if dict_smtCnt[smtAssyName] < 0:
                                        diffCnt = df_input['미착공수주잔'][i]
                                        if dict_smtCnt[smtAssyName] + df_input['미착공수주잔'][i] > 0:
                                            diffCnt = 0 - dict_smtCnt[smtAssyName]
                                        # SMT Assy가 부족할 경우에는 분류1 알람을 발생.
                                        if not isRemain:
                                            if dict_smtCnt[smtAssyName] > 0:
                                                df_alarmDetail, alarmDetailNo = self.concatAlarmDetail(df_alarmDetail, alarmDetailNo, '1', df_input, i, smtAssyName, diffCnt)
                                # SMT Assy가 DB에 등록되지 않은 경우, 기타3 알람을 출력.
                                else:
                                    minCnt = 0
                                    df_alarmDetail, alarmDetailNo = self.concatAlarmDetail(df_alarmDetail, alarmDetailNo, '기타3', df_input, i, smtAssyName, 0)
                            # 긴급오더 혹은 당일착공 대상이 아닐 경우, SMT Assy 잔량을 확인 후, SMT Assy 잔량이 부족할 경우, 부족한 양만큼 착공.
                            else:
                                # 사용하는 SmtAssy가 이미 등록된 SmtAssy일 경우의 로직
                                if smtAssyName in dict_smtCnt:
                                    # 최소필요착공량보다 SmtAssy 수량이 여유 있는 경우, 그대로 착공
                                    if dict_smtCnt[smtAssyName] >= df_input[instCol][i]:
                                        # 사용하는 SmtAssy가 다수 일 경우를 고려하여 최소수량 확인
                                        if minCnt > df_input[instCol][i]:
                                            minCnt = df_input[instCol][i]
                                    # SmtAssy 수량의 여유가 없는 경우
                                    else:
                                        # 최소수량과 SmtAssy수량을 다시 비교
                                        if dict_smtCnt[smtAssyName] > 0:
                                            if minCnt > dict_smtCnt[smtAssyName]:
                                                minCnt = dict_smtCnt[smtAssyName]
                                        # SmtAssy수량이 0개 인 경우, 최소수량을 0으로 전환
                                        else:
                                            minCnt = 0
                                        # 최소착공필요량 전체에 비해 SmtAssy수량이 부족한 경우, 알람을 출력.
                                        if not isRemain:
                                            if dict_smtCnt[smtAssyName] > 0:
                                                df_alarmDetail, alarmDetailNo = self.concatAlarmDetail(df_alarmDetail,
                                                                                                        alarmDetailNo,
                                                                                                        '1',
                                                                                                        df_input,
                                                                                                        i,
                                                                                                        smtAssyName,
                                                                                                        df_input[instCol][i] - dict_smtCnt[smtAssyName])
                                # SMT Assy가 DB에 등록되지 않은 경우, 기타3 알람을 출력.
                                else:
                                    minCnt = 0
                                    df_alarmDetail, alarmDetailNo = self.concatAlarmDetail(df_alarmDetail, alarmDetailNo, '기타3', df_input, i, smtAssyName, 0)
                    # MS Code와 연결된 SMT Assy가 등록되지 않았을 경우, 기타1 알람을 출력.
                    else:
                        minCnt = 0
                        df_alarmDetail, alarmDetailNo = self.concatAlarmDetail(df_alarmDetail, alarmDetailNo, '기타1', df_input, i, '미등록', 0)
                # 최소 수량을 1번이라도 갱신한 경우, 결과컬럼의 값을 minCnt로 대체
                if minCnt != 9999:
                    df_input[resultCol][i] = minCnt
                # 갱신하지 않았을 경우, 기존 입력값을 그대로 출력
                else:
                    df_input[resultCol][i] = df_input[instCol][i]
                # 사용되는 각 Smt Assy 수량에서 결과값을 빼기위한 로직
                for j in range(1, rowCnt + 1):
                    if (smtAssyName != '' and smtAssyName != 'nan' and smtAssyName != 'None'):
                        smtAssyName = str(df_input[f'ROW{str(j)}'][i])
                        if smtAssyName in dict_smtCnt:
                            dict_smtCnt[smtAssyName] -= df_input[resultCol][i]
        return [df_input, dict_smtCnt, alarmDetailNo, df_alarmDetail]

    def ratioReflectInst(self, df_input, isRemain, dict_ratioCnt, dict_maxCnt, alarmDetailNo, df_alarmDetail, limitCtCnt, dict_alarmRatioCnt, dict_alarmMaxCnt):
        """
        Args:
            df_input(DataFrame)         : 입력 DataFrame
            isRemain(Bool)              : 잔여착공 여부 Flag
            dict_ratioCnt(Dict)         : 그룹별 제한비율 딕셔너리
            dict_maxCnt(Dict)           : 대표모델별 제한대수 딕셔너리
            alarmDetailNo(int)          : 알람 기록용 번호
            df_alarmDetail(DataFrame)   : 알람 상세 기록용 DataFrame
            limitCtCnt(int)             : CT 제한대수
            dict_alarmRatioCnt(Dict)    : 알람 출력용 제한비율 딕셔너리
            dict_alarmMaxCnt(int)       : 알람 출력용 대표모델별 제한대수 딕셔너리
        Return:
            return(List)
                df_input(DataFrame)         : 입력 DataFrame (갱신 후)
                dict_ratioCnt(Dict)         : 그룹별 제한비율 딕셔너리 (갱신 후)
                dict_maxCnt(Dict)           : 대표모델별 제한대수 딕셔너리 (갱신 후)
                alarmDetailNo(int)          : 알람 기록용 번호 (갱신 후)
                df_alarmDetail(DataFrame)   : 알람 상세 기록용 DataFrame (갱신 후)
                limitCtCnt(int)             : CT 제한대수 (갱신 후)
                dict_alarmRatioCnt(Dict)    : 알람 출력용 제한비율 딕셔너리 (갱신 후)
                dict_alarmMaxCnt(int)       : 알람 출력용 대표모델별 제한대수 딕셔너리 (갱신 후)
        """
        instCol = 'SMT반영_착공량'
        resultCol1 = '설비능력반영_착공량'
        resultCol2 = '설비능력반영_착공공수'
        if isRemain:
            instCol = 'SMT반영_착공량_잔여'
            resultCol1 = '설비능력반영_착공량_잔여'
            resultCol2 = '설비능력반영_착공공수_잔여'
        for i in df_input.index:
            if df_input[instCol][i] != 0:
                # 모듈 구분 있는 경우에만 실행
                if (str(df_input['상세구분'][i]) != '' and str(df_input['상세구분'][i]) != 'nan'):
                    # 긴급오더 일 경우, 모든 조건을 무시하고 SMT반영 착공량 그대로 착공실시
                    if (str(df_input['긴급오더'][i]) == '대상' or str(df_input['당일착공'][i]) == '대상'):
                        # SMT반영 착공량을 그대로 착공하고 각 조건의 제한대수 딕셔너리에서 차감
                        df_input[resultCol2][i] = df_input[instCol][i] * df_input['공수'][i]
                        df_input[resultCol1][i] = df_input[instCol][i]
                        self.moduleMaxCnt -= df_input[resultCol2][i]
                        dict_ratioCnt[str(df_input['상세구분'][i])] -= float(df_input[instCol][i]) * df_input['공수'][i]
                        dict_alarmRatioCnt[str(df_input['상세구분'][i])] -= float(df_input[instCol][i]) * df_input['공수'][i]
                        dict_maxCnt[str(df_input['상세구분'][i])] -= float(df_input[instCol][i]) * df_input['공수'][i]
                        dict_alarmMaxCnt[str(df_input['상세구분'][i])] -= float(df_input[instCol][i]) * df_input['공수'][i]
                        # CT사양일 경우, CT제한대수를 차감
                        if '/CT' in df_input['MS Code'][i]:
                            limitCtCnt -= df_input[resultCol2][i]
                            # CT제한대수가 0 미만일 경우, 기타4 알람 기록
                            if limitCtCnt < 0:
                                df_alarmDetail, alarmDetailNo = self.concatAlarmDetail(df_alarmDetail, alarmDetailNo, '기타4', df_input, i, '-', 0 - limitCtCnt)
                        # 비율제한이 0 미만일 경우, 2-1 알람 기록
                        if dict_ratioCnt[str(df_input['상세구분'][i])] < 0:
                            df_alarmDetail, alarmDetailNo = self.concatAlarmDetail(df_alarmDetail, alarmDetailNo, '2-1', df_input, i, '-', 0 - (dict_ratioCnt[str(df_input['상세구분'][i])]))
                        # 모델별 제한대수가 0 미만일 경우, 2-2 알람 기록
                        if str(df_input['MAX대수'][i]) != '' and str(df_input['MAX대수'][i]) != 'nan' and str(df_input['MAX대수'][i]) != '-':
                            if dict_maxCnt[str(df_input['MODEL'][i])] < 0:
                                df_alarmDetail, alarmDetailNo = self.concatAlarmDetail(df_alarmDetail, alarmDetailNo, '2-2', df_input, i, '-', 0 - dict_maxCnt[str(df_input['MODEL'][i])])
                        # 최대 착공량이 0 미만일 경우, 기타2 알람 기록
                        if self.moduleMaxCnt < 0:
                            df_alarmDetail, alarmDetailNo = self.concatAlarmDetail(df_alarmDetail, alarmDetailNo, '기타2', df_input, i, '-', 0 - self.moduleMaxCnt)
                    # 긴급오더 아닌 경우의 로직
                    else:
                        # 리스트에 [SMT반영 착공량], [최대 착공량], [비율제한대수] 를 입력
                        compareList = [df_input[instCol][i], self.moduleMaxCnt, dict_ratioCnt[str(df_input['상세구분'][i])]]
                        # 모델별 제한대수가 있는 모델이면 리스트에 [모델별 제한대수] 추가
                        if str(df_input['MAX대수'][i]) != '' and str(df_input['MAX대수'][i]) != 'nan' and str(df_input['MAX대수'][i]) != '-':
                            compareList.append(dict_maxCnt[str(df_input['MODEL'][i])])
                        # CT사양일 경우, 리스트에 [CT제한대수] 추가
                        if '/CT' in df_input['MS Code'][i]:
                            compareList.append(limitCtCnt)
                        # 리스트 중, 최소값을 착공결과로 출력 / 공수도 같이 출력
                        df_input[resultCol2][i] = min(compareList)
                        df_input[resultCol1][i] = df_input[resultCol2][i] / df_input['공수'][i]
                        # 최소필요착공량 대상이며 착공 불가능한 상황인 경우, 상황에 맞는 알람을 출력
                        if not isRemain and df_input[instCol][i] > 0 and (df_input[instCol][i] != df_input[resultCol1][i]):
                            if df_input[instCol][i] > dict_alarmRatioCnt[str(df_input['상세구분'][i])]:
                                df_alarmDetail, alarmDetailNo = self.concatAlarmDetail(df_alarmDetail, alarmDetailNo, '2-1', df_input, i, '-', df_input[instCol][i] - dict_alarmRatioCnt[str(df_input['상세구분'][i])])
                            if str(df_input['MAX대수'][i]) != '' and str(df_input['MAX대수'][i]) != 'nan' and str(df_input['MAX대수'][i]) != '-':
                                if df_input[instCol][i] > dict_alarmMaxCnt[str(df_input['MODEL'][i])]:
                                    df_alarmDetail, alarmDetailNo = self.concatAlarmDetail(df_alarmDetail, alarmDetailNo, '2-2', df_input, i, '-', df_input[instCol][i] - dict_alarmMaxCnt[str(df_input['MODEL'][i])])
                            if '/CT' in df_input['MS Code'][i]:
                                if df_input[instCol][i] > limitCtCnt:
                                    df_alarmDetail, alarmDetailNo = self.concatAlarmDetail(df_alarmDetail, alarmDetailNo, '기타4', df_input, i, '-', df_input[instCol][i] - df_input[resultCol1][i])
                        # 각 조건에 맞는 딕셔너리에서 차감
                        if str(df_input['MAX대수'][i]) != '' and str(df_input['MAX대수'][i]) != 'nan' and str(df_input['MAX대수'][i]) != '-':
                            dict_maxCnt[str(df_input['MODEL'][i])] -= df_input[resultCol2][i]
                            dict_alarmMaxCnt[str(df_input['MODEL'][i])] -= df_input[resultCol2][i]
                        if '/CT' in df_input['MS Code'][i]:
                            limitCtCnt -= df_input[resultCol2][i]
                        dict_ratioCnt[str(df_input['상세구분'][i])] -= df_input[resultCol2][i]
                        dict_alarmRatioCnt[str(df_input['상세구분'][i])] -= df_input[resultCol2][i]
                        self.moduleMaxCnt -= df_input[resultCol2][i]
                    # 긴급오더 등으로 0 아래로 내려가 있는 변수들을 0으로 재정의
                    if self.moduleMaxCnt < 0:
                        self.moduleMaxCnt = 0
                    if limitCtCnt < 0:
                        limitCtCnt = 0
                    if str(df_input['MAX대수'][i]) != '' and str(df_input['MAX대수'][i]) != 'nan' and str(df_input['MAX대수'][i]) != '-':
                        if dict_ratioCnt[str(df_input['상세구분'][i])] < 0:
                            dict_ratioCnt[str(df_input['상세구분'][i])] = 0
                        if dict_maxCnt[str(df_input['MODEL'][i])] < 0:
                            dict_maxCnt[str(df_input['MODEL'][i])] = 0

        return [df_input, dict_ratioCnt, dict_maxCnt, alarmDetailNo, df_alarmDetail, limitCtCnt, dict_alarmRatioCnt, dict_alarmMaxCnt]

    def run(self):
        # pandas 경고없애기 옵션 적용
        pd.set_option('mode.chained_assignment', None)
        try:
            alaramMaxCnt = self.moduleMaxCnt + int(self.df_etcOrderInput['착공량'][0])
            maxPb = 200
            self.powerReturnMaxPb.emit(maxPb)
            if self.moduleMaxCnt > 0:
                progress = 0
                self.powerReturnPb.emit(progress)
                # 긴급오더, 홀딩오더 불러오기
                emgLinkage = self.emgHoldList[0]
                emgmscode = self.emgHoldList[1]
                holdLinkage = self.emgHoldList[2]
                holdmscode = self.emgHoldList[3]
                # 긴급오더, 홀딩오더 데이터프레임화
                df_emgLinkage = pd.DataFrame({'Linkage Number': emgLinkage})
                df_emgmscode = pd.DataFrame({'MS Code': emgmscode})
                df_holdLinkage = pd.DataFrame({'Linkage Number': holdLinkage})
                df_holdmscode = pd.DataFrame({'MS Code': holdmscode})
                # 각 Linkage Number 컬럼의 타입을 일치시킴
                df_emgLinkage['Linkage Number'] = df_emgLinkage['Linkage Number'].astype(np.int64)
                df_holdLinkage['Linkage Number'] = df_holdLinkage['Linkage Number'].astype(np.int64)
                # 긴급오더, 홍딩오더 Join 전 컬럼 추가
                df_emgLinkage['긴급오더'] = '대상'
                df_emgmscode['긴급오더'] = '대상'
                df_holdLinkage['홀딩오더'] = '대상'
                df_holdmscode['홀딩오더'] = '대상'
                # 레벨링 리스트 불러오기(멀티프로세싱 적용 후, 분리 예정)
                df_levelingPower = pd.read_excel(self.list_masterFile[3])
                # 레벨링 리스트의 착공 당일의 마지막 No를 가져오기 위한 처리
                df_constDate = df_levelingPower[df_levelingPower['Scheduled Start Date (*)'] == self.constDate]
                df_constDate = df_constDate[df_constDate['Sequence No'].notnull()]
                if len(df_constDate) > 0:
                    df_constDate = df_constDate[df_constDate['Sequence No'].str.contains('D0')]
                if len(df_constDate) > 0:
                    maxNo = df_constDate['No (*)'].max()
                else:
                    maxNo = 0
                # 미착공 대상만 추출(Main)
                df_levelingPowerDropSeq = df_levelingPower[df_levelingPower['Sequence No'].isnull()]
                df_levelingPowerUndepSeq = df_levelingPower[df_levelingPower['Sequence No'] == 'Undep']
                df_levelingPowerUncorSeq = df_levelingPower[df_levelingPower['Sequence No'] == 'Uncor']
                df_levelingPower = pd.concat([df_levelingPowerDropSeq, df_levelingPowerUndepSeq, df_levelingPowerUncorSeq])
                df_levelingPower['Linkage Number'] = df_levelingPower['Linkage Number'].astype(str)
                df_levelingPower = df_levelingPower.reset_index(drop=True)
                df_levelingPower['미착공수주잔'] = df_levelingPower.groupby('Linkage Number')['Linkage Number'].transform('size')
                progress += round(maxPb / 20)
                self.powerReturnPb.emit(progress)
                if self.isDebug:
                    df_levelingPower.to_excel('.\\debug\\Power\\flow1.xlsx')
                df_sosFile = pd.read_excel(self.list_masterFile[0])
                df_sosFile['Linkage Number'] = df_sosFile['Linkage Number'].astype(str)
                progress += round(maxPb / 20)
                self.powerReturnPb.emit(progress)
                if self.isDebug:
                    df_sosFile.to_excel('.\\debug\\Power\\flow2.xlsx')
                # 착공 대상 외 모델 삭제
                df_sosFile = df_sosFile.drop(df_sosFile[df_sosFile['MS Code'].str.contains('ZOTHER')].index)
                df_sosFile = df_sosFile.drop(df_sosFile[df_sosFile['MS Code'].str.contains('YZ')].index)
                df_sosFile = df_sosFile.drop(df_sosFile[df_sosFile['MS Code'].str.contains('SF')].index)
                df_sosFile = df_sosFile.drop(df_sosFile[df_sosFile['MS Code'].str.contains('KM')].index)
                df_sosFile = df_sosFile.drop(df_sosFile[df_sosFile['MS Code'].str.contains('TA80')].index)
                # df_sosFile = df_sosFile.drop(df_sosFile[df_sosFile['MS Code'].str.contains('CT')].index)
                if self.cb_round != '1차':
                    df_sosFile = df_sosFile.drop(df_sosFile[df_sosFile['MS Code'].str.contains('CT')].index)
                df_sosFile = df_sosFile.reset_index(drop=True)
                progress += round(maxPb / 20)
                self.powerReturnPb.emit(progress)
                # if self.isDebug:
                #     df_sosFile.to_excel('.\\debug\\Power\\flow3.xlsx')
                # 워킹데이 캘린더 불러오기
                dfCalendar = pd.read_excel(self.list_masterFile[4])
                today = datetime.datetime.today().strftime('%Y%m%d')
                if self.isDebug:
                    today = self.date
                # 진척 파일 - SOS2파일 Join
                df_sosFileMerge = pd.merge(df_sosFile, df_levelingPower).drop_duplicates(['Linkage Number'])
                df_sosFileMerge = df_sosFileMerge[['Linkage Number', 'MS Code', 'Planned Prod. Completion date', 'Order Quantity', '미착공수주잔']]
                # 미착공수주잔이 없는 데이터는 불요이므로 삭제
                df_sosFileMerge = df_sosFileMerge[df_sosFileMerge['미착공수주잔'] != 0]
                # 위 파일을 완성지정일 기준 오름차순 정렬 및 인덱스 재설정
                df_sosFileMerge = df_sosFileMerge.sort_values(by=['Planned Prod. Completion date'], ascending=[True])
                df_sosFileMerge = df_sosFileMerge.reset_index(drop=True)
                # 대표모델 Column 생성
                df_sosFileMerge['대표모델'] = df_sosFileMerge['MS Code'].str[:9]
                # 남은 워킹데이 Column 생성
                df_sosFileMerge['남은 워킹데이'] = 0
                df_sosFileMerge['당일착공'] = ''
                # 긴급오더, 홀딩오더 Linkage Number Column 타입 일치
                df_emgLinkage['Linkage Number'] = df_emgLinkage['Linkage Number'].astype(str)
                df_holdLinkage['Linkage Number'] = df_holdLinkage['Linkage Number'].astype(str)
                # 긴급오더, 홀딩오더와 위 Sos파일을 Join
                df_MergeLink = pd.merge(df_sosFileMerge, df_emgLinkage, on='Linkage Number', how='left')
                df_Mergemscode = pd.merge(df_sosFileMerge, df_emgmscode, on='MS Code', how='left')
                df_MergeLink = pd.merge(df_MergeLink, df_holdLinkage, on='Linkage Number', how='left')
                df_Mergemscode = pd.merge(df_Mergemscode, df_holdmscode, on='MS Code', how='left')
                df_MergeLink['긴급오더'] = df_MergeLink['긴급오더'].combine_first(df_Mergemscode['긴급오더'])
                df_MergeLink['홀딩오더'] = df_MergeLink['홀딩오더'].combine_first(df_Mergemscode['홀딩오더'])
                df_MergeLink['완성지정일_원본'] = df_MergeLink['Planned Prod. Completion date']
                # CT사양은 기존 완성지정일보다 4일 더 빠르게 착공내려야 하기 때문에 보정처리
                df_MergeLink.loc[df_MergeLink['MS Code'].str.contains('/CT'), 'Planned Prod. Completion date'] = df_MergeLink['완성지정일_원본'] - datetime.timedelta(days=4)
                df_MergeLink = df_MergeLink.sort_values(by=['Planned Prod. Completion date'], ascending=[True])
                df_MergeLink = df_MergeLink.reset_index(drop=True)
                # 남은 워킹데이 체크 및 컬럼 추가
                for i in df_MergeLink.index:
                    df_MergeLink['남은 워킹데이'][i] = self.checkWorkDay(dfCalendar, today, df_MergeLink['Planned Prod. Completion date'][i])
                    if df_MergeLink['남은 워킹데이'][i] < 1:
                        df_MergeLink['긴급오더'][i] = '대상'
                    elif df_MergeLink['남은 워킹데이'][i] == 1:
                        df_MergeLink['당일착공'][i] = '대상'
                df_MergeLink['Linkage Number'] = df_MergeLink['Linkage Number'].astype(str)
                # 홀딩오더는 제외
                df_MergeLink = df_MergeLink[df_MergeLink['홀딩오더'].isnull()]
                progress += round(maxPb / 20)
                self.powerReturnPb.emit(progress)
                if self.isDebug:
                    df_MergeLink.to_excel('.\\debug\\Power\\flow4.xlsx')
                # 프로그램 기동날짜의 전일을 계산 (Debug시에는 디버그용 LineEdit에 기록된 날짜를 사용)
                yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y%m%d')
                if self.isDebug:
                    yesterday = (datetime.datetime.strptime(self.date, '%Y%m%d') - datetime.timedelta(days=1)).strftime('%Y%m%d')
                # 설정파일 불러오기
                parser = ConfigParser()
                parser.read(self.list_masterFile[16], encoding='euc-kr')
                smtAssyDbHost = parser.get('SMT Assy DB정보', 'Host')
                smtAssyDbPort = parser.getint('SMT Assy DB정보', 'Port')
                smtAssyDbSID = parser.get('SMT Assy DB정보', 'SID')
                smtAssyDbUser = parser.get('SMT Assy DB정보', 'Username')
                smtAssyDbPw = parser.get('SMT Assy DB정보', 'Password')
                # 해당 날짜의 Smt Assy 남은 대수 확인
                df_SmtAssyInven = self.readDB(smtAssyDbHost,
                                                smtAssyDbPort,
                                                smtAssyDbSID,
                                                smtAssyDbUser,
                                                smtAssyDbPw,
                                                "SELECT INV_D, PARTS_NO, CURRENT_INV_QTY FROM pdsg0040 where INV_D = TO_DATE(" + str(yesterday) + ",'YYYYMMDD')")
                progress += round(maxPb / 20)
                self.powerReturnPb.emit(progress)
                if self.isDebug:
                    df_SmtAssyInven.to_excel('.\\debug\\Power\\flow5.xlsx')
                # 2차 메인피킹 리스트 불러오기 및 Smt Assy 재고량 Df와 Join
                if Path(self.list_masterFile[5]).is_file() or Path(self.list_masterFile[14]).is_file() or Path(self.list_masterFile[15]).is_file():
                    df_secOrderList = pd.DataFrame(columns=['ASSY NO', '대수', 'SMT STORE ADDRESS'])
                    if Path(self.list_masterFile[5]).is_file():
                        df_secOrderMainList = pd.read_excel(self.list_masterFile[5], skiprows=5)
                        df_secOrderList = pd.concat([df_secOrderList, df_secOrderMainList])
                    if Path(self.list_masterFile[14]).is_file():
                        df_secOrderPowerList = pd.read_excel(self.list_masterFile[14], skiprows=5)
                        df_secOrderList = pd.concat([df_secOrderList, df_secOrderPowerList])
                    if Path(self.list_masterFile[15]).is_file():
                        df_secOrderSpList = pd.read_excel(self.list_masterFile[15], skiprows=5)
                        df_secOrderList = pd.concat([df_secOrderList, df_secOrderSpList])
                    df_joinSmt = pd.merge(df_secOrderList, df_SmtAssyInven, how='right', left_on='ASSY NO', right_on='PARTS_NO')
                    df_joinSmt['대수'] = df_joinSmt['대수'].fillna(0)
                    # Smt Assy 현재 재고량에서 사용량 차감
                    df_joinSmt['현재수량'] = df_joinSmt['CURRENT_INV_QTY'] - df_joinSmt['대수']
                else:
                    df_joinSmt = df_SmtAssyInven.copy()
                    df_joinSmt['현재수량'] = df_joinSmt['CURRENT_INV_QTY']
                progress += round(maxPb / 20)
                self.powerReturnPb.emit(progress)
                if self.isDebug:
                    df_joinSmt.to_excel('.\\debug\\Power\\flow6.xlsx')
                dict_smtCnt = {}
                # Smt Assy 재고량을 PARTS_NO를 Key로 Dict화
                for i in df_joinSmt.index:
                    if df_joinSmt['현재수량'][i] < 0:
                        df_joinSmt['현재수량'][i] = 0
                    dict_smtCnt[df_joinSmt['PARTS_NO'][i]] = df_joinSmt['현재수량'][i]
                df_sosAddPowerModel = df_MergeLink
                # 설정파일 불러오기
                pdbsDbHost = parser.get('MSCODE별 SMT Assy DB정보', 'Host')
                pdbsDbPort = parser.getint('MSCODE별 SMT Assy DB정보', 'Port')
                pdbsDbSID = parser.get('MSCODE별 SMT Assy DB정보', 'SID')
                pdbsDbUser = parser.get('MSCODE별 SMT Assy DB정보', 'Username')
                pdbsDbPw = parser.get('MSCODE별 SMT Assy DB정보', 'Password')
                # DB로부터 메인라인의 MSCode별 사용 Smt Assy 가져옴
                df_pdbs = self.readDB(pdbsDbHost,
                                        pdbsDbPort,
                                        pdbsDbSID,
                                        pdbsDbUser,
                                        pdbsDbPw,
                                        "SELECT SMT_MS_CODE, SMT_SMT_ASSY, SMT_CRP_GR_NO FROM sap.pdbs0010 WHERE SMT_CRP_GR_NO = '100L1313'")
                # 불필요한 데이터 삭제
                df_pdbs = df_pdbs[~df_pdbs['SMT_MS_CODE'].str.contains('AST')]
                df_pdbs = df_pdbs[~df_pdbs['SMT_MS_CODE'].str.contains('BMS')]
                df_pdbs = df_pdbs[~df_pdbs['SMT_MS_CODE'].str.contains('WEB')]
                # 사용 Smt Assy를 병렬화
                gb = df_pdbs.groupby('SMT_MS_CODE')
                df_temp = pd.DataFrame([df_pdbs.loc[gb.groups[n], 'SMT_SMT_ASSY'].values for n in gb.groups], index=gb.groups.keys())
                df_temp.columns = ['ROW' + str(i + 1) for i in df_temp.columns]
                rowNo = len(df_temp.columns)
                df_temp = df_temp.reset_index()
                df_temp.rename(columns={'index': 'SMT_MS_CODE'}, inplace=True)
                progress += round(maxPb / 20)
                self.powerReturnPb.emit(progress)
                if self.isDebug:
                    df_temp.to_excel('.\\debug\\Power\\flow6-1.xlsx')
                # 모델별 사용 Smt Assy를 Join
                df_addSmtAssy = pd.merge(df_sosAddPowerModel, df_temp, left_on='MS Code', right_on='SMT_MS_CODE', how='left')
                df_addSmtAssy = df_addSmtAssy.drop_duplicates(['Linkage Number'])
                df_addSmtAssy = df_addSmtAssy.reset_index(drop=True)
                progress += round(maxPb / 20)
                self.powerReturnPb.emit(progress)
                if self.isDebug:
                    df_addSmtAssy.to_excel('.\\debug\\Power\\flow7.xlsx')
                df_addSmtAssy['대표모델별_최소착공필요량_per_일'] = 0
                dict_integCnt = {}
                dict_minContCnt = {}
                # 대표모델 별 최소 착공 필요량을 계산
                for i in df_addSmtAssy.index:
                    # 각 대표모델 별 적산착공량을 계산하여 딕셔너리에 저장
                    if df_addSmtAssy['대표모델'][i] in dict_integCnt:
                        dict_integCnt[df_addSmtAssy['대표모델'][i]] += int(df_addSmtAssy['미착공수주잔'][i])
                    else:
                        dict_integCnt[df_addSmtAssy['대표모델'][i]] = int(df_addSmtAssy['미착공수주잔'][i])
                    # 이미 완성지정일을 지난경우, 워킹데이 계산을 위해 워킹데이를 1로 설정
                    if df_addSmtAssy['남은 워킹데이'][i] <= 0:
                        workDay = 1
                    else:
                        workDay = df_addSmtAssy['남은 워킹데이'][i]
                    # 완성지정일별 최소필요착공량 계산 후, 딕셔너리에 리스트(대표모델, 완성지정일)로 저장
                    if len(dict_minContCnt) > 0:
                        if df_addSmtAssy['대표모델'][i] in dict_minContCnt:
                            if dict_minContCnt[df_addSmtAssy['대표모델'][i]][0] < math.ceil(dict_integCnt[df_addSmtAssy['대표모델'][i]] / workDay):
                                dict_minContCnt[df_addSmtAssy['대표모델'][i]][0] = math.ceil(dict_integCnt[df_addSmtAssy['대표모델'][i]] / workDay)
                                dict_minContCnt[df_addSmtAssy['대표모델'][i]][1] = df_addSmtAssy['Planned Prod. Completion date'][i]
                        else:
                            dict_minContCnt[df_addSmtAssy['대표모델'][i]] = [math.ceil(dict_integCnt[df_addSmtAssy['대표모델'][i]] / workDay),
                                                                        df_addSmtAssy['Planned Prod. Completion date'][i]]
                    else:
                        dict_minContCnt[df_addSmtAssy['대표모델'][i]] = [math.ceil(dict_integCnt[df_addSmtAssy['대표모델'][i]] / workDay),
                                                                        df_addSmtAssy['Planned Prod. Completion date'][i]]
                    if workDay <= 0:
                        workDay = 1
                    # 위에서 계산한 최소필요착공량을 컬럼화시켜 데이터프레임에 입력
                    df_addSmtAssy['대표모델별_최소착공필요량_per_일'][i] = dict_integCnt[df_addSmtAssy['대표모델'][i]] / workDay
                progress += round(maxPb / 20)
                self.powerReturnPb.emit(progress)
                if self.isDebug:
                    df_addSmtAssy.to_excel('.\\debug\\Power\\flow9.xlsx')
                dict_minContCopy = dict_minContCnt.copy()
                # 대표모델 별 최소착공 필요량을 기준으로 평준화 적용 착공량을 계산. 미착공수주잔에서 해당 평준화 적용 착공량을 제외한 수량은 잔여착공량으로 기재
                df_addSmtAssy['평준화_적용_착공량'] = 0
                for i in df_addSmtAssy.index:
                    if df_addSmtAssy['긴급오더'][i] == '대상':
                        df_addSmtAssy['평준화_적용_착공량'][i] = int(df_addSmtAssy['미착공수주잔'][i])
                        if df_addSmtAssy['대표모델'][i] in dict_minContCopy:
                            if dict_minContCopy[df_addSmtAssy['대표모델'][i]][0] >= int(df_addSmtAssy['미착공수주잔'][i]):
                                dict_minContCopy[df_addSmtAssy['대표모델'][i]][0] -= int(df_addSmtAssy['미착공수주잔'][i])
                            else:
                                dict_minContCopy[df_addSmtAssy['대표모델'][i]][0] = 0
                    elif df_addSmtAssy['대표모델'][i] in dict_minContCopy:
                        if dict_minContCopy[df_addSmtAssy['대표모델'][i]][0] >= int(df_addSmtAssy['미착공수주잔'][i]):
                            df_addSmtAssy['평준화_적용_착공량'][i] = int(df_addSmtAssy['미착공수주잔'][i])
                            dict_minContCopy[df_addSmtAssy['대표모델'][i]][0] -= int(df_addSmtAssy['미착공수주잔'][i])
                        else:
                            df_addSmtAssy['평준화_적용_착공량'][i] = dict_minContCopy[df_addSmtAssy['대표모델'][i]][0]
                            dict_minContCopy[df_addSmtAssy['대표모델'][i]][0] = 0
                df_addSmtAssy['잔여_착공량'] = df_addSmtAssy['미착공수주잔'] - df_addSmtAssy['평준화_적용_착공량']
                df_addSmtAssy = df_addSmtAssy.sort_values(by=['긴급오더', '당일착공', 'Planned Prod. Completion date', '평준화_적용_착공량'],
                                                            ascending=[False, False, True, False])
                progress += round(maxPb / 20)
                self.powerReturnPb.emit(progress)
                if self.isDebug:
                    df_addSmtAssy.to_excel('.\\debug\\Power\\flow10.xlsx')
                df_addSmtAssyPower = df_addSmtAssy
                df_addSmtAssyPower['SMT반영_착공량'] = 0
                # 알람 상세 DataFrame 생성
                df_alarmDetail = pd.DataFrame(columns=["No.", "분류", "L/N", "MS CODE", "SMT ASSY", "수주수량", "부족수량", "검사호기", "대상 검사시간(초)", "필요시간(초)", "완성예정일"])
                alarmDetailNo = 1
                # 최소착공량에 대해 Smt적용 착공량 계산
                df_addSmtAssy, dict_smtCnt, alarmDetailNo, df_alarmDetail = self.smtReflectInst(df_addSmtAssy, False, dict_smtCnt, alarmDetailNo, df_alarmDetail, rowNo)
                if self.isDebug:
                    df_alarmDetail.to_excel('.\\debug\\Power\\df_alarmDetail.xlsx')
                # 잔여 착공량에 대해 Smt적용 착공량 계산
                df_addSmtAssy['SMT반영_착공량_잔여'] = 0
                df_addSmtAssy, dict_smtCnt, alarmDetailNo, df_alarmDetail = self.smtReflectInst(df_addSmtAssy, True, dict_smtCnt, alarmDetailNo, df_alarmDetail, rowNo)
                progress += round(maxPb / 20)
                self.powerReturnPb.emit(progress)
                if self.isDebug:
                    df_addSmtAssy.to_excel('.\\debug\\Power\\flow11.xlsx')
                df_addSmtAssyPower = df_addSmtAssy.copy()
                df_addSmtAssyPower['Linkage Number'] = df_addSmtAssyPower['Linkage Number'].astype(str)
                df_addSmtAssyPower['MODEL'] = df_addSmtAssyPower['MS Code'].str[:6]
                # 전원 조건표 불러오기
                df_powerCondition = pd.read_excel(self.list_masterFile[6])
                df_powerCondition['상세구분'] = df_powerCondition['상세구분'].fillna(method='ffill')
                df_powerCondition['최대허용비율'] = df_powerCondition['최대허용비율'].fillna(method='ffill')
                df_mergeCondition = pd.merge(df_addSmtAssyPower, df_powerCondition, on='MODEL', how='left')
                df_mergeCondition['MAX대수'] = df_mergeCondition['MAX대수'].fillna('-')
                df_mergeCondition['공수'] = df_mergeCondition['공수'].fillna(1)
                df_mergeCondition = df_mergeCondition.sort_values(by=['우선착공'], ascending=[False])
                df_mergeCondition = df_mergeCondition.reset_index(drop=True)
                progress += round(maxPb / 20)
                self.powerReturnPb.emit(progress)
                if self.isDebug:
                    df_mergeCondition.to_excel('.\\debug\\Power\\flow11-1.xlsx')
                dict_ratioCnt = {}
                dict_alarmRatioCnt = {}
                dict_maxCnt = {}
                dict_alarmMaxCnt = {}
                df_mergeCondition['설비능력반영_착공공수'] = 0
                df_mergeCondition['설비능력반영_착공공수_잔여'] = 0
                df_mergeCondition['설비능력반영_착공량'] = 0
                df_mergeCondition['설비능력반영_착공량_잔여'] = 0
                # 조건표에 있는 내용을 각 LinkageNo에 적용
                for i in df_powerCondition.index:
                    dict_ratioCnt[str(df_powerCondition['상세구분'][i])] = round(float(df_powerCondition['최대허용비율'][i]) * self.moduleMaxCnt)
                    dict_alarmRatioCnt[str(df_powerCondition['상세구분'][i])] = round(float(df_powerCondition['최대허용비율'][i]) * alaramMaxCnt)
                    if str(df_powerCondition['MAX대수'][i]) != '' and str(df_powerCondition['MAX대수'][i]) != '-':
                        dict_maxCnt[str(df_powerCondition['MODEL'][i])] = round(float(df_powerCondition['최대허용비율'][i]) * self.moduleMaxCnt * float(df_powerCondition['MAX대수'][i]))
                        dict_alarmMaxCnt[str(df_powerCondition['MODEL'][i])] = round(float(df_powerCondition['최대허용비율'][i]) * alaramMaxCnt * float(df_powerCondition['MAX대수'][i]))
                # CT제한 조건표 불러오기
                df_limitCtCond = pd.read_excel(self.list_masterFile[13])
                limitCtCnt = df_limitCtCond[df_limitCtCond['상세구분'] == 'POWER']['허용수량'].values[0]
                # 비율제한 적용 (최소필요착공량)
                df_mergeCondition, dict_ratioCnt, dict_maxCnt, alarmDetailNo, df_alarmDetail, limitCtCnt, dict_alarmRatioCnt, dict_alarmMaxCnt = self.ratioReflectInst(df_mergeCondition,
                                                                                                                                                                        False,
                                                                                                                                                                        dict_ratioCnt,
                                                                                                                                                                        dict_maxCnt,
                                                                                                                                                                        alarmDetailNo,
                                                                                                                                                                        df_alarmDetail,
                                                                                                                                                                        limitCtCnt,
                                                                                                                                                                        dict_alarmRatioCnt,
                                                                                                                                                                        dict_alarmMaxCnt)
                # 비율제한 적용 (여유분)
                df_mergeCondition, dict_ratioCnt, dict_maxCnt, alarmDetailNo, df_alarmDetail, limitCtCnt, dict_alarmRatioCnt, dict_alarmMaxCnt = self.ratioReflectInst(df_mergeCondition,
                                                                                                                                                                        True,
                                                                                                                                                                        dict_ratioCnt,
                                                                                                                                                                        dict_maxCnt,
                                                                                                                                                                        alarmDetailNo,
                                                                                                                                                                        df_alarmDetail,
                                                                                                                                                                        limitCtCnt,
                                                                                                                                                                        dict_alarmRatioCnt,
                                                                                                                                                                        dict_alarmMaxCnt)
                progress += round(maxPb / 20)
                self.powerReturnPb.emit(progress)
                if self.isDebug:
                    df_mergeCondition.to_excel('.\\debug\\Power\\flow12.xlsx')
                    df_alarmDetail = df_alarmDetail.reset_index(drop=True)
                    df_alarmDetail.to_excel('.\\debug\\Power\\df_alarmDetail.xlsx')
                # 알람 상세 결과에서 각 항목별로 요약
                # 분류1 요약
                if len(df_alarmDetail) > 0:
                    df_firstAlarm = df_alarmDetail[df_alarmDetail['분류'] == '1']
                    df_firstAlarmSummary = df_firstAlarm.groupby("SMT ASSY")['부족수량'].sum()
                    df_firstAlarmSummary = df_firstAlarmSummary.reset_index()
                    df_firstAlarmSummary['수량'] = df_firstAlarmSummary['부족수량']
                    df_firstAlarmSummary['분류'] = '1'
                    df_firstAlarmSummary['MS CODE'] = '-'
                    df_firstAlarmSummary['검사호기'] = '-'
                    df_firstAlarmSummary['부족 시간'] = '-'
                    df_firstAlarmSummary['Message'] = '[SMT ASSY : ' + df_firstAlarmSummary["SMT ASSY"] + ']가 부족합니다. SMT ASSY 제작을 지시해주세요.'
                    del df_firstAlarmSummary['부족수량']
                    # 분류2-1 요약
                    df_secOneAlarm = df_alarmDetail[df_alarmDetail['분류'] == '2-1']
                    df_secOneAlarmSummary = df_secOneAlarm.groupby("MS CODE")['부족수량'].sum()
                    df_secOneAlarmSummary = df_secOneAlarmSummary.reset_index()
                    df_secOneAlarmSummary['수량'] = df_secOneAlarmSummary['부족수량']
                    df_secOneAlarmSummary['분류'] = '2-1'
                    df_secOneAlarmSummary['SMT ASSY'] = '-'
                    df_secOneAlarmSummary['부족 시간'] = '-'
                    df_secOneAlarmSummary['Message'] = '당일 최소 필요생산 대수에 대하여 생산 불가능한 모델이 있습니다. 생산 허용비율을 확인해 주세요.'
                    del df_secOneAlarmSummary['부족수량']
                    # 분류2-2 요약
                    df_secTwoAlarm = df_alarmDetail[df_alarmDetail['분류'] == '2-2']
                    df_secTwoAlarmSummary = df_secTwoAlarm.groupby("MS CODE")['부족수량'].sum()
                    df_secTwoAlarmSummary = df_secTwoAlarmSummary.reset_index()
                    df_secTwoAlarmSummary['수량'] = df_secTwoAlarmSummary['부족수량']
                    df_secTwoAlarmSummary['분류'] = '2-2'
                    df_secTwoAlarmSummary['SMT ASSY'] = '-'
                    df_secTwoAlarmSummary['부족 시간'] = '-'
                    df_secTwoAlarmSummary['Message'] = '당일 최소 필요생산 대수에 대하여 생산 불가능한 모델이 있습니다. 모델별 MAX 대수를 확인해 주세요.'
                    del df_secTwoAlarmSummary['부족수량']
                    # 분류 기타2 요약
                    df_etc2Alarm = df_alarmDetail[df_alarmDetail['분류'] == '기타2']
                    df_etc2AlarmSummary = df_etc2Alarm.groupby('MS CODE')['부족수량'].sum()
                    df_etc2AlarmSummary = df_etc2AlarmSummary.reset_index()
                    df_etc2AlarmSummary['수량'] = df_etc2AlarmSummary['부족수량']
                    df_etc2AlarmSummary['분류'] = '기타2'
                    df_etc2AlarmSummary['SMT ASSY'] = '-'
                    df_etc2AlarmSummary['검사호기'] = '-'
                    df_etc2AlarmSummary['부족 시간'] = '-'
                    df_etc2AlarmSummary['Message'] = '긴급오더 및 당일착공 대상의 총 착공량이 입력한 최대착공량보다 큽니다. 최대착공량을 확인해주세요.'
                    del df_etc2AlarmSummary['부족수량']
                    # 분류 기타4 요약
                    df_etc4Alarm = df_alarmDetail[df_alarmDetail['분류'] == '기타4']
                    df_etc4AlarmSummary = df_etc4Alarm.groupby('MS CODE')['부족수량'].sum()
                    df_etc4AlarmSummary = df_etc4AlarmSummary.reset_index()
                    df_etc4AlarmSummary['수량'] = df_etc4AlarmSummary['부족수량']
                    df_etc4AlarmSummary['분류'] = '기타4'
                    df_etc4AlarmSummary['SMT ASSY'] = '-'
                    df_etc4AlarmSummary['검사호기'] = '-'
                    df_etc4AlarmSummary['부족 시간'] = '-'
                    df_etc4AlarmSummary['Message'] = '설정된 CT 제한대수보다 최소 착공 필요량이 많습니다. 설정된 CT 제한대수를 확인해주세요.'
                    del df_etc4AlarmSummary['부족수량']
                    # 위 알람을 병합
                    df_alarmSummary = pd.concat([df_firstAlarmSummary, df_secOneAlarmSummary, df_secTwoAlarmSummary, df_etc2AlarmSummary, df_etc4AlarmSummary])
                    # 기타 알람에 대한 추가
                    df_etcList = df_alarmDetail[(df_alarmDetail['분류'] == '기타1') | (df_alarmDetail['분류'] == '기타3')]
                    df_etcList = df_etcList.drop_duplicates(['MS CODE'])
                    df_etcList = df_etcList.reset_index(drop=True)
                    for i in df_etcList.index:
                        if df_etcList['분류'][i] == '기타1':
                            df_alarmSummary = pd.concat([df_alarmSummary,
                                                        pd.DataFrame.from_records([{"분류": df_etcList['분류'][i],
                                                                                    "MS CODE": df_etcList['MS CODE'][i],
                                                                                    "SMT ASSY": '-',
                                                                                    "수량": 0,
                                                                                    "검사호기": '-',
                                                                                    "부족 시간": 0,
                                                                                    "Message": '해당 MS CODE에서 사용되는 SMT ASSY가 등록되지 않았습니다. 등록 후 다시 실행해주세요.'}])])
                        elif df_etcList['분류'][i] == '기타3':
                            df_alarmSummary = pd.concat([df_alarmSummary,
                                                        pd.DataFrame.from_records([{"분류": df_etcList['분류'][i],
                                                                                    "MS CODE": df_etcList['MS CODE'][i],
                                                                                    "SMT ASSY": '-',
                                                                                    "수량": 0,
                                                                                    "검사호기": '-',
                                                                                    "부족 시간": 0,
                                                                                    "Message": 'SMT ASSY 정보가 등록되지 않아 재고를 확인할 수 없습니다. 등록 후 다시 실행해주세요.'}])])
                    df_alarmSummary = df_alarmSummary.reset_index(drop=True)
                    df_alarmSummary = df_alarmSummary[['분류', 'MS CODE', 'SMT ASSY', '수량', '검사호기', '부족 시간', 'Message']]
                    if self.isDebug:
                        df_alarmSummary.to_excel('.\\debug\\Power\\df_alarmSummary.xlsx')
                    df_alarmExplain = pd.DataFrame({'분류': ['1', '2-1', '2-2', '기타1', '기타2', '기타3', '기타4'],
                                            '분류별 상황': ['DB상의 Smt Assy가 부족하여 해당 MS-Code를 착공 내릴 수 없는 경우',
                                            '당일 최소 착공필요량 > 모델별 생산 허용 비율인 경우',
                                            '당일 최소 착공필요량 > 모델별 MAX 대수인 경우',
                                            'MS-Code와 일치하는 Smt Assy가 마스터 파일에 없는 경우',
                                            '긴급오더 대상 착공시 최대착공량(사용자입력공수)이 부족할 경우',
                                            'SMT ASSY 정보가 DB에 미등록된 경우',
                                            '당일 최소 착공필요량 > CT제한 대수인 경우']})
                    # 파일 한개로 출력
                    if not os.path.exists(f'.\\Output\\Alarm\\{str(today)}\\{self.cb_round}'):
                        os.makedirs(f'.\\Output\\Alarm\\{str(today)}\\{self.cb_round}')
                    with pd.ExcelWriter(f'.\\Output\\Alarm\\{str(today)}\\{self.cb_round}\\FAM3_AlarmList_{str(today)}_Power.xlsx') as writer:
                        df_alarmSummary.to_excel(writer, sheet_name='정리', index=True)
                        df_alarmDetail.to_excel(writer, sheet_name='상세', index=True)
                        df_alarmExplain.to_excel(writer, sheet_name='설명', index=False)
                # 총착공량 컬럼으로 병합
                df_mergeCondition['총착공량'] = df_mergeCondition['설비능력반영_착공량'] + df_mergeCondition['설비능력반영_착공량_잔여']
                df_mergeCondition = df_mergeCondition[df_mergeCondition['총착공량'] != 0]
                df_mergeCondition['MODEL'] = df_mergeCondition['MS Code'].str[:6]
                # 홀딩리스트 불러오기
                df_holdingList = pd.read_excel(self.list_masterFile[17])
                # 홀딩리스트와 비교하여 조건에 해당하는 경우, 알람 메시지 출력
                for i in df_holdingList.index:
                    message = ""
                    if len(df_mergeCondition[df_mergeCondition['MODEL'] == df_holdingList['MODEL'][i]]['총착공량'].values) > 0:
                        totalCnt = 0
                        for cnt in df_mergeCondition[df_mergeCondition['MODEL'] == df_holdingList['MODEL'][i]]['총착공량'].values:
                            totalCnt += cnt
                        message = f"{df_holdingList['MODEL'][i]} {int(totalCnt)}대 {df_holdingList['REMARK'][i]}"
                    if len(df_mergeCondition[df_mergeCondition['Linkage Number'] == df_holdingList['LINKAGENO'][i]]['총착공량'].values) > 0:
                        message = f"{df_holdingList['LINKAGENO'][i]} {int(df_mergeCondition[df_mergeCondition['Linkage Number'] == df_holdingList['LINKAGENO'][i]]['총착공량'].values[0])}대 {df_holdingList['REMARK'][i]}"
                    if len(df_mergeCondition[df_mergeCondition['MS Code'] == df_holdingList['MS-CODE'][i]]['총착공량'].values) > 0:
                        totalCnt = 0
                        for cnt in df_mergeCondition[df_mergeCondition['MS Code'] == df_holdingList['MS-CODE'][i]]['총착공량'].values:
                            totalCnt += cnt
                        message = f"{df_holdingList['MS-CODE'][i]} {int(totalCnt)}대 {df_holdingList['REMARK'][i]}"
                    if len(message) > 0:
                        self.powerReturnWarning.emit(message)
                progress += round(maxPb / 20)
                self.powerReturnPb.emit(progress)
                if self.isDebug:
                    df_mergeCondition.to_excel('.\\debug\\Power\\flow13.xlsx')
                # 최대착공량만큼 착공 못했을 경우, 메시지 출력
                if math.floor(self.moduleMaxCnt) > 0:
                    self.powerReturnWarning.emit(f'아직 착공하지 못한 모델이 [{math.floor(self.moduleMaxCnt)}대] 남았습니다. 모델별 제한 대수 혹은 최대 착공량 의 설정 이상이 예상됩니다. 확인해주세요.')
                # 레벨링 리스트와 병합
                df_mergeCondition = df_mergeCondition.astype({'Linkage Number': 'str'})
                df_levelingPower = df_levelingPower.astype({'Linkage Number': 'str'})
                df_mergeOrder = pd.merge(df_mergeCondition, df_levelingPower, on='Linkage Number', how='right')
                progress += round(maxPb / 20)
                self.powerReturnPb.emit(progress)
                if self.isDebug:
                    df_mergeOrder.to_excel('.\\debug\\Power\\flow14.xlsx')
                df_mergeOrderResult = pd.DataFrame().reindex_like(df_mergeOrder)
                df_mergeOrderResult = df_mergeOrderResult[0:0]
                # 총착공량 만큼 개별화
                for i in df_mergeCondition.index:
                    for j in df_mergeOrder.index:
                        if df_mergeCondition['Linkage Number'][i] == df_mergeOrder['Linkage Number'][j]:
                            if j > 0:
                                if df_mergeOrder['Linkage Number'][j] != df_mergeOrder['Linkage Number'][j - 1]:
                                    orderCnt = int(df_mergeCondition['총착공량'][i])
                            else:
                                orderCnt = int(df_mergeCondition['총착공량'][i])
                            if orderCnt > 0:
                                df_mergeOrderResult = df_mergeOrderResult.append(df_mergeOrder.iloc[j])
                                orderCnt -= 1
                # 사이클링을 위해 검사설비별로 정리
                df_mergeOrderResult = df_mergeOrderResult.sort_values(by=['MODEL'], ascending=[False])
                df_mergeOrderResult = df_mergeOrderResult.reset_index(drop=True)
                progress += round(maxPb / 20)
                self.powerReturnPb.emit(progress)
                if self.isDebug:
                    df_mergeOrderResult.to_excel('.\\debug\\Power\\flow15.xlsx')
                df_unCt = df_mergeOrderResult[df_mergeOrderResult['MS Code'].str.contains('/CT')]
                df_mergeOrderResult = df_mergeOrderResult[~df_mergeOrderResult['MS Code'].str.contains('/CT')]
                # 긴급오더 제외하고 사이클 대상만 식별하여 검사장치별로 갯수 체크
                df_cycleCopy = df_mergeOrderResult[df_mergeOrderResult['긴급오더'].isnull()]
                df_cycleBuForward = df_cycleCopy[df_cycleCopy['상세구분'] == 'BASE']
                df_cycleBuForward = df_cycleCopy[df_cycleCopy['상세구분'] == 'BASE'].sort_values(by=['MODEL'], ascending=[True])
                df_cycleBuForward = df_cycleBuForward.reset_index(drop=True)
                df_cycleBuBack = df_cycleCopy[df_cycleCopy['상세구분'] == 'BASE'].sort_values(by=['MODEL'], ascending=[False])
                df_cycleBuBack = df_cycleBuBack.reset_index(drop=True)
                df_cyclePuForward = df_cycleCopy[df_cycleCopy['상세구분'] == 'POWER']
                df_cyclePuForward = df_cycleCopy[df_cycleCopy['상세구분'] == 'POWER'].sort_values(by=['MODEL'], ascending=[True])
                df_cyclePuForward = df_cyclePuForward.reset_index(drop=True)
                df_cyclePuBack = df_cycleCopy[df_cycleCopy['상세구분'] == 'POWER'].sort_values(by=['MODEL'], ascending=[False])
                df_cyclePuBack = df_cyclePuBack.reset_index(drop=True)
                df_cycleBuCopy = pd.DataFrame(columns=df_cycleCopy.columns)
                df_cyclePuCopy = pd.DataFrame(columns=df_cycleCopy.columns)
                # BU/PU 구분지어 별도 파일을 생성. 사양을 지그재그방식으로 순서를 재조정한다. (생산요청사항)
                for i in df_cycleBuForward.index:
                    df_dupCheck = df_cycleBuCopy['Serial Number'].str.contains(df_cycleBuForward['Serial Number'][i]).sum()
                    if len(df_cycleBuForward) > len(df_cycleBuCopy):
                        if df_dupCheck == 0:
                            df_cycleBuCopy = df_cycleBuCopy.append(df_cycleBuForward.iloc[i])
                    else:
                        break
                    df_dupCheck = df_cycleBuCopy['Serial Number'].str.contains(df_cycleBuBack['Serial Number'][i]).sum()
                    if len(df_cycleBuForward) > len(df_cycleBuCopy):
                        if df_dupCheck == 0:
                            df_cycleBuCopy = df_cycleBuCopy.append(df_cycleBuBack.iloc[i])
                    else:
                        break
                for i in df_cyclePuForward.index:
                    df_dupCheck = df_cyclePuCopy['Serial Number'].str.contains(df_cyclePuForward['Serial Number'][i]).sum()
                    if len(df_cyclePuForward) > len(df_cyclePuCopy):
                        if df_dupCheck == 0:
                            df_cyclePuCopy = df_cyclePuCopy.append(df_cyclePuForward.iloc[i])
                    else:
                        break
                    df_dupCheck = df_cyclePuCopy['Serial Number'].str.contains(df_cyclePuBack['Serial Number'][i]).sum()
                    if len(df_cyclePuForward) > len(df_cyclePuCopy):
                        if df_dupCheck == 0:
                            df_cyclePuCopy = df_cyclePuCopy.append(df_cyclePuBack.iloc[i])
                    else:
                        break
                df_cycleBuCopy = df_cycleBuCopy.reset_index()
                df_cyclePuCopy = df_cyclePuCopy.reset_index()
                if self.isDebug:
                    df_cycleBuCopy.to_excel('.\\debug\\Power\\flow15-2.xlsx')
                    df_cyclePuCopy.to_excel('.\\debug\\Power\\flow15-3.xlsx')
                # 분리했던 BU/PU 파일을 하나로 병합
                df_cycleCopy = pd.concat([df_cycleBuCopy, df_cyclePuCopy])
                df_cycleCopy['ModelCnt'] = df_cycleCopy.groupby('상세구분')['상세구분'].transform('size')
                df_cycleCopy = df_cycleCopy.sort_values(by=['ModelCnt', 'index'], ascending=[False, True])
                df_cycleCopy = df_cycleCopy.reset_index(drop=True)
                # 긴급오더 포함한 Df와 병합
                # df_mergeOrderResult = pd.merge(df_mergeOrderResult, df_cycleCopy[['Planned Order', 'ModelCnt']], on='Planned Order', how='left')
                df_mergeOrderResult = pd.concat([df_mergeOrderResult[df_mergeOrderResult['긴급오더'] == '대상'], df_cycleCopy])
                df_mergeOrderResult = df_mergeOrderResult.sort_values(by=['ModelCnt', 'index'], ascending=[False, True])
                df_mergeOrderResult = df_mergeOrderResult.reset_index(drop=True)
                progress += round(maxPb / 20)
                self.powerReturnPb.emit(progress)
                if self.isDebug:
                    df_mergeOrderResult.to_excel('.\\debug\\Power\\flow15-1.xlsx')
                # 최대 사이클 번호 체크
                maxCycle = float(df_cycleCopy['ModelCnt'][0])
                cycleGr = 1.0
                df_mergeOrderResult['사이클그룹'] = 0
                # 각 검사장치별로 사이클 그룹을 작성하고, 최대 사이클과 비교하여 각 사이클그룹에서 배수처리
                for i in df_mergeOrderResult.index:
                    if df_mergeOrderResult['긴급오더'][i] != '대상':
                        multiCnt = maxCycle / df_mergeOrderResult['ModelCnt'][i]
                        if i == 0:
                            df_mergeOrderResult['사이클그룹'][i] = cycleGr
                        else:
                            if df_mergeOrderResult['상세구분'][i] != df_mergeOrderResult['상세구분'][i - 1]:
                                if i == 1:
                                    cycleGr = 2.0
                                else:
                                    cycleGr = 1.0
                            df_mergeOrderResult['사이클그룹'][i] = cycleGr * multiCnt
                        cycleGr += 1.0
                    if cycleGr >= maxCycle:
                        cycleGr = 1.0
                # 배정된 사이클 그룹 순으로 정렬
                df_mergeOrderResult = df_mergeOrderResult.sort_values(by=['사이클그룹', 'index'], ascending=[True, True])
                df_mergeOrderResult = df_mergeOrderResult.reset_index(drop=True)
                progress += round(maxPb / 20)
                self.powerReturnPb.emit(progress)
                if self.isDebug:
                    df_mergeOrderResult.to_excel('.\\debug\\Power\\flow16.xlsx')
                df_mergeOrderResult = df_mergeOrderResult.reset_index()
                # 연속으로 같은 검사설비가 오지 않도록 순서를 재조정
                for i in df_mergeOrderResult.index:
                    if df_mergeOrderResult['긴급오더'][i] != '대상':
                        if (i != 0 and (df_mergeOrderResult['상세구분'][i] == df_mergeOrderResult['상세구분'][i - 1])):
                            for j in df_mergeOrderResult.index:
                                if df_mergeOrderResult['긴급오더'][j] != '대상':
                                    if ((j != 0 and j < len(df_mergeOrderResult) - 1) and (df_mergeOrderResult['상세구분'][i] != df_mergeOrderResult['상세구분'][j + 1]) and (df_mergeOrderResult['상세구분'][i] != df_mergeOrderResult['상세구분'][j])):
                                        df_mergeOrderResult['level_0'][i] = (float(df_mergeOrderResult['level_0'][j]) + float(df_mergeOrderResult['level_0'][j + 1])) / 2
                                        df_mergeOrderResult = df_mergeOrderResult.sort_values(by=['level_0'], ascending=[True])
                                        df_mergeOrderResult = df_mergeOrderResult.reset_index(drop=True)
                                        break

                df_unCt['index'] = 0
                df_unCt['사이클그룹'] = 0
                df_mergeOrderResult = pd.concat([df_unCt, df_mergeOrderResult])
                df_mergeOrderResult = df_mergeOrderResult.reset_index(drop=True)
                progress += round(maxPb / 20)
                self.powerReturnPb.emit(progress)
                if self.isDebug:
                    df_mergeOrderResult.to_excel('.\\debug\\Power\\flow17.xlsx')
                df_mergeOrderResult['No (*)'] = int(maxNo) + (df_mergeOrderResult.index.astype(int) + 1) * 10
                df_mergeOrderResult['Scheduled Start Date (*)'] = self.constDate
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
                dict_emgLinkage = {}
                dict_emgMscode = {}
                for i in df_emgLinkage.index:
                    if len(df_mergeOrderResult[df_mergeOrderResult['Linkage Number'] == df_emgLinkage['Linkage Number'][i]]['Linkage Number'].values) > 0:
                        dict_emgLinkage[df_emgLinkage['Linkage Number'][i]] = True
                    else:
                        dict_emgLinkage[df_emgLinkage['Linkage Number'][i]] = False
                for i in df_emgmscode.index:
                    if len(df_mergeOrderResult[df_mergeOrderResult['MS Code'] == df_emgmscode['MS-CODE'][i]]['MS Code'].values) > 0:
                        dict_emgMscode[df_emgLinkage['MS-CODE'][i]] = True
                    else:
                        dict_emgMscode[df_emgLinkage['MS-CODE'][i]] = False

                self.powerReturnEmgLinkage.emit(dict_emgLinkage)
                self.powerReturnEmgMscode.emit(dict_emgMscode)
                progress += round(maxPb / 20)
                self.powerReturnPb.emit(progress)
                if not os.path.exists(f'.\\Output\\Result\\{str(today)}'):
                    os.makedirs(f'.\\Output\\Result\\{str(today)}')
                if not os.path.exists(f'.\\Output\\Result\\{str(today)}\\{self.cb_round}'):
                    os.makedirs(f'.\\Output\\Result\\{str(today)}\\{self.cb_round}')
                outputFile = f'.\\Output\\Result\\{str(today)}\\{self.cb_round}\\{str(today)}_Power.xlsx'
                df_mergeOrderResult.to_excel(outputFile, index=False)
            else:
                self.powerReturnPb.emit(maxPb)
            self.powerReturnEnd.emit(True)
            return
        except Exception as e:
            self.powerReturnError.emit(e)
            return


class SpThread(QObject):
    spReturnError = pyqtSignal(Exception)
    spReturnInfo = pyqtSignal(str)
    spReturnWarning = pyqtSignal(str)
    spReturnEnd = pyqtSignal(bool)
    spReturnPb = pyqtSignal(int)
    spReturnMaxPb = pyqtSignal(int)
    spReturnEmgLinkage = pyqtSignal(dict)
    spReturnEmgMscode = pyqtSignal(dict)

    def __init__(self, debugFlag, date, constDate, list_masterFile, moduleMaxCnt, nonModuleMaxCnt, emgHoldList, df_receiveMain, cb_round, df_etcOrderInput):
        super().__init__()
        self.isDebug = debugFlag
        self.date = date
        self.constDate = constDate
        self.list_masterFile = list_masterFile
        self.moduleMaxCnt = moduleMaxCnt
        self.nonModuleMaxCnt = nonModuleMaxCnt
        self.emgHoldList = emgHoldList
        self.df_receiveMain = df_receiveMain
        self.cb_round = cb_round
        self.df_etcOrderInput = df_etcOrderInput

    # 워킹데이 체크 내부함수
    def checkWorkDay(self, df, today, compDate):
        dtToday = pd.to_datetime(datetime.datetime.strptime(today, '%Y%m%d'))
        dtComp = pd.to_datetime(compDate, unit='s')
        workDay = 0
        if len(df.index[(df['Date'] == dtComp)].tolist()) > 0:
            index = int(df.index[(df['Date'] == dtComp)].tolist()[0])
            # 위에서 찾은 완성지정일로부터 프로그램 구동 당일까지 워킹데이를 계산.
            while dtToday > pd.to_datetime(df['Date'][index], unit='s'):
                if df['WorkingDay'][index] == 1:
                    workDay -= 1
                index += 1
            # 프로그램 구동 당일 ~ 완성지정일 까지의 워킹데이를 계산
            for i in df.index:
                dt = pd.to_datetime(df['Date'][i], unit='s')
                if dtToday < dt and dt <= dtComp:
                    if df['WorkingDay'][i] == 1:
                        workDay += 1
        else:
            self.spReturnWarning.emit(f'FY{today[2:4]}_Calendar.xlsx 파일에 {str(dtComp.date())} 날짜의 워킹데이 데이터가 없습니다. 대한민국 휴일을 기준으로 근무일을 계산합니다. 이후, 해당 파일에 사력을 추가해주세요')
            workDay = np.busday_count(begindates=dtToday.date(), enddates=dtComp.date())
        return workDay

    # 콤마 삭제용 내부함수
    def delComma(self, value):
        return str(value).split('.')[0]

    # 하이픈 삭제
    def delHypen(self, value):
        return str(value).split('-')[0]

    # 디비 불러오기 공통내부함수
    def readDB(self, ip, port, sid, userName, password, sql):
        location = r'.\\instantclient_21_7'
        os.environ["PATH"] = location + ";" + os.environ["PATH"]
        dsn = cx_Oracle.makedsn(ip, port, sid)
        db = cx_Oracle.connect(userName, password, dsn)
        cursor = db.cursor()
        cursor.execute(sql)
        out_data = cursor.fetchall()
        df_oracle = pd.DataFrame(out_data)
        col_names = [row[0] for row in cursor.description]
        df_oracle.columns = col_names
        return df_oracle

    # 생산시간 합계용 내부함수
    def getSec(self, time_str):
        time_str = re.sub(r'[^0-9:]', '', str(time_str))
        if len(time_str) > 0:
            h, m, s = time_str.split(':')
            return int(h) * 3600 + int(m) * 60 + int(s)
        else:
            return 0

    # 백슬래쉬 삭제용 내부함수
    def delBackslash(self, value):
        value = re.sub(r"\\c", "", str(value))
        return value

    def concatAlarmDetail(self, df_target, no, category, df_data, index, smtAssy, shortageCnt):
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
        df_result = pd.DataFrame()
        if category == '1':
            df_result = pd.concat([df_target,
                                    pd.DataFrame.from_records([{"No.": no,
                                                                "분류": category,
                                                                "L/N": df_data['Linkage Number'][index],
                                                                "MS CODE": df_data['MS Code'][index],
                                                                "SMT ASSY": smtAssy,
                                                                "수주수량": df_data['미착공수주잔'][index],
                                                                "부족수량": shortageCnt,
                                                                "검사호기(그룹)": '-',
                                                                "대상 검사시간(초)": 0,
                                                                "필요시간(초)": 0,
                                                                "완성예정일": df_data['Planned Prod. Completion date'][index]}])])
        elif category == '2':
            df_result = pd.concat([df_target,
                                    pd.DataFrame.from_records([{"No.": no,
                                                                "분류": category,
                                                                "L/N": df_data['Linkage Number'][index],
                                                                "MS CODE": df_data['MS Code'][index],
                                                                "SMT ASSY": '-',
                                                                "수주수량": df_data['미착공수주잔'][index],
                                                                "부족수량": shortageCnt,
                                                                "검사호기(그룹)": smtAssy,
                                                                "대상 검사시간(초)": 0,
                                                                "필요시간(초)": 0,
                                                                "완성예정일": df_data['Planned Prod. Completion date'][index]}])])
        elif category == '기타1':
            df_result = pd.concat([df_target,
                                pd.DataFrame.from_records([{"No.": no,
                                                            "분류": category,
                                                            "L/N": df_data['Linkage Number'][index],
                                                            "MS CODE": df_data['MS Code'][index],
                                                            "SMT ASSY": '미등록',
                                                            "수주수량": df_data['미착공수주잔'][index],
                                                            "부족수량": 0,
                                                            "검사호기(그룹)": '-',
                                                            "대상 검사시간(초)": 0,
                                                            "필요시간(초)": 0,
                                                            "완성예정일": df_data['Planned Prod. Completion date'][index]}])])
        elif category == '기타2':
            df_result = pd.concat([df_target,
                                    pd.DataFrame.from_records([{"No.": no,
                                                                "분류": category,
                                                                "L/N": df_data['Linkage Number'][index],
                                                                "MS CODE": df_data['MS Code'][index],
                                                                "SMT ASSY": '-',
                                                                "수주수량": df_data['미착공수주잔'][index],
                                                                "부족수량": shortageCnt,
                                                                "검사호기(그룹)": '-',
                                                                "대상 검사시간(초)": 0,
                                                                "필요시간(초)": 0,
                                                                "완성예정일": df_data['Planned Prod. Completion date'][index]}])])
        elif category == '기타3':
            df_result = pd.concat([df_target,
                        pd.DataFrame.from_records([{"No.": no,
                                                    "분류": category,
                                                    "L/N": df_data['Linkage Number'][index],
                                                    "MS CODE": df_data['MS Code'][index],
                                                    "SMT ASSY": smtAssy,
                                                    "수주수량": df_data['미착공수주잔'][index],
                                                    "부족수량": 0,
                                                    "검사호기(그룹)": '-',
                                                    "대상 검사시간(초)": 0,
                                                    "필요시간(초)": 0,
                                                    "완성예정일": df_data['Planned Prod. Completion date'][index]}])])
        elif category == '기타4':
            df_result = pd.concat([df_target,
                                pd.DataFrame.from_records([{"No.": no,
                                                            "분류": category,
                                                            "L/N": df_data['Linkage Number'][index],
                                                            "MS CODE": df_data['MS Code'][index],
                                                            "SMT ASSY": smtAssy,
                                                            "수주수량": df_data['미착공수주잔'][index],
                                                            "부족수량": shortageCnt,
                                                            "검사호기": '-',
                                                            "대상 검사시간(초)": 0,
                                                            "필요시간(초)": 0,
                                                            "완성예정일": df_data['Planned Prod. Completion date'][index]}])])
        return [df_result, no + 1]

    def smtReflectInst(self, df_input, isRemain, dict_smtCnt, alarmDetailNo, df_alarmDetail, rowNo):
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
        # 행별로 확인
        for i in df_input.index:
            # 사용 Smt Assy 개수 확인
            for j in range(1, rowNo):
                if j == 1:
                    rowCnt = 1
                if (str(df_input[f'ROW{str(j)}'][i]) != '' and str(df_input[f'ROW{str(j)}'][i]) != 'None' and str(df_input[f'ROW{str(j)}'][i]) != 'nan'):
                    rowCnt = j
                else:
                    break
            minCnt = 9999
            isManageSMT = True
            # 각 SmtAssy 별로 착공 가능 대수 확인
            for j in range(1, rowCnt + 1):
                if str(df_input[f'SMT비관리대상{str(j)}'][i]) == 'True':
                    isManageSMT = False
            for j in range(1, rowCnt + 1):
                smtAssyName = str(df_input[f'ROW{str(j)}'][i])
                if (df_input['MS Code'][i] != 'nan' and df_input['MS Code'][i] != 'None' and df_input['MS Code'][i] != ''):
                    if (smtAssyName != '' and smtAssyName != 'None' and smtAssyName != 'nan'):
                        # SMT관리 대상만 한정하여 로직을 실행
                        if isManageSMT:
                            # 긴급오더 혹은 당일착공 대상일 경우, SMT Assy 잔량에 관계없이 착공 실시.
                            if df_input['긴급오더'][i] == '대상' or df_input['당일착공'][i] == '대상':
                                # MS Code와 연결된 SMT Assy가 있을 경우, 정상적으로 로직을 실행
                                if smtAssyName in dict_smtCnt:
                                    if dict_smtCnt[smtAssyName] < 0:
                                        diffCnt = df_input['미착공수주잔'][i]
                                        if dict_smtCnt[smtAssyName] + df_input['미착공수주잔'][i] > 0:
                                            diffCnt = 0 - dict_smtCnt[smtAssyName]
                                        # SMT Assy가 부족할 경우에는 분류1 알람을 발생.
                                        if not isRemain:
                                            if dict_smtCnt[smtAssyName] > 0:
                                                df_alarmDetail, alarmDetailNo = self.concatAlarmDetail(df_alarmDetail, alarmDetailNo, '1', df_input, i, smtAssyName, diffCnt)
                                # SMT Assy가 DB에 등록되지 않은 경우, 기타3 알람을 출력.
                                else:
                                    minCnt = 0
                                    df_alarmDetail, alarmDetailNo = self.concatAlarmDetail(df_alarmDetail, alarmDetailNo, '기타3', df_input, i, smtAssyName, 0)
                            # 긴급오더 혹은 당일착공 대상이 아닐 경우, SMT Assy 잔량을 확인 후, SMT Assy 잔량이 부족할 경우, 부족한 양만큼 착공.
                            else:
                                # 사용하는 SmtAssy가 이미 등록된 SmtAssy일 경우의 로직
                                if smtAssyName in dict_smtCnt:
                                    # 최소필요착공량보다 SmtAssy 수량이 여유 있는 경우, 그대로 착공
                                    if dict_smtCnt[smtAssyName] >= df_input[instCol][i]:
                                        # 사용하는 SmtAssy가 다수 일 경우를 고려하여 최소수량 확인
                                        if minCnt > df_input[instCol][i]:
                                            minCnt = df_input[instCol][i]
                                    # SmtAssy 수량의 여유가 없는 경우
                                    else:
                                        # 최소수량과 SmtAssy수량을 다시 비교
                                        if dict_smtCnt[smtAssyName] > 0:
                                            if minCnt > dict_smtCnt[smtAssyName]:
                                                minCnt = dict_smtCnt[smtAssyName]
                                        # SmtAssy수량이 0개 인 경우, 최소수량을 0으로 전환
                                        else:
                                            minCnt = 0
                                        # 최소착공필요량 전체에 비해 SmtAssy수량이 부족한 경우, 알람을 출력.
                                        if not isRemain:
                                            if dict_smtCnt[smtAssyName] > 0:
                                                df_alarmDetail, alarmDetailNo = self.concatAlarmDetail(df_alarmDetail,
                                                                                                        alarmDetailNo,
                                                                                                        '1',
                                                                                                        df_input,
                                                                                                        i,
                                                                                                        smtAssyName,
                                                                                                        df_input[instCol][i] - dict_smtCnt[smtAssyName])
                                # SMT Assy가 DB에 등록되지 않은 경우, 기타3 알람을 출력.
                                else:
                                    minCnt = 0
                                    df_alarmDetail, alarmDetailNo = self.concatAlarmDetail(df_alarmDetail, alarmDetailNo, '기타3', df_input, i, smtAssyName, 0)
                # MS Code와 연결된 SMT Assy가 등록되지 않았을 경우, 기타1 알람을 출력.
                else:
                    minCnt = 0
                    df_alarmDetail, alarmDetailNo = self.concatAlarmDetail(df_alarmDetail, alarmDetailNo, '기타1', df_input, i, '미등록', 0)
            # 최소 수량을 1번이라도 갱신한 경우, 결과컬럼의 값을 minCnt로 대체
            if minCnt != 9999:
                df_input[resultCol][i] = minCnt
            # 갱신하지 않았을 경우, 기존 입력값을 그대로 출력
            else:
                df_input[resultCol][i] = df_input[instCol][i]
            # 사용되는 각 Smt Assy 수량에서 결과값을 빼기위한 로직
            for j in range(1, rowCnt + 1):
                if (smtAssyName != '' and smtAssyName != 'None' and smtAssyName != 'nan'):
                    smtAssyName = str(df_input[f'ROW{str(j)}'][i])
                    if smtAssyName in dict_smtCnt:
                        dict_smtCnt[smtAssyName] -= df_input[resultCol][i]
        return [df_input, dict_smtCnt, alarmDetailNo, df_alarmDetail]

    def grMaxCntReflect(self, df_input, isRemain, dict_categoryCnt, dict_firstGrCnt, dict_secGrCnt, alarmDetailNo, df_alarmDetail, limitCtCnt):
        """
        Args:
            df_input(DataFrame)         : 입력 DataFrame
            isRemain(Bool)              : 잔여착공 여부 Flag
            dict_categoryCnt(Dict)      : 모듈/비모듈 별 잔여량 Dict
            dict_firstGrCnt(Dict)       : 1차 Max Gr 잔여량 Dict
            dict_secGrCnt(Dict)         : 2차 Max Gr 잔여량 Dict
            alarmDetailNo(int)          : 알람 번호
            df_alarmDetail(DataFrame)   : 알람 상세 기록용 DataFrame
            limitCtCnt(int)             : CT제한대수
        Return:
            return(List)
                df_input(DataFrame)         : 입력 DataFrame (갱신 후)
                dict_categoryCnt(Dict)      : 모듈/비모듈 별 잔여량 Dict(갱신 후)
                dict_firstGrCnt(Dict)       : 1차 Max Gr 잔여량 Dict(갱신 후)
                dict_secGrCnt(Dict)         : 2차 Max Gr 잔여량 Dict(갱신 후)
                alarmDetailNo(int)          : 알람 번호
                df_alarmDetail(DataFrame)   : 알람 상세 기록용 DataFrame (갱신 후)
                limitCtCnt(int)             : CT제한대수
        """
        instCol = 'SMT반영_착공량'
        resultCol = '설비능력반영_착공량'
        if isRemain:
            instCol = 'SMT반영_착공량_잔여'
            resultCol = '설비능력반영_착공량_잔여'
        for i in df_input.index:
            # 긴급오더 일 경우, 모든 조건을 무시하고 착공
            if (df_input['긴급오더'][i] == '대상' or df_input['당일착공'][i] == '대상'):
                # 모듈구분 잔여 착공량이 부족한 경우, 기타2 알람 기록
                if dict_categoryCnt[df_input['모듈 구분'][i]] < df_input[instCol][i] * df_input['공수'][i]:
                    df_alarmDetail, alarmDetailNo = self.concatAlarmDetail(df_alarmDetail, alarmDetailNo, '기타2', df_input, i, '-', df_input[instCol][i] * df_input['공수'][i] - dict_categoryCnt[df_input['모듈 구분'][i]])
                if df_input['2차_MAX_그룹'][i] != '-':
                    # 2차 MAX그룹의 잔여량이 부족한 경우, 분류2 알람 기록
                    if dict_secGrCnt[df_input['2차_MAX_그룹'][i]] < df_input[instCol][i]:
                        df_alarmDetail, alarmDetailNo = self.concatAlarmDetail(df_alarmDetail, alarmDetailNo, '2', df_input, i, df_input['2차_MAX_그룹'][i], df_input[instCol][i] - dict_firstGrCnt[df_input['2차_MAX_그룹'][i]])
                    # 딕셔너리에서 차감
                    dict_secGrCnt[df_input['2차_MAX_그룹'][i]] -= df_input[instCol][i]
                if df_input['1차_MAX_그룹'][i] != '-':
                    # 1차 MAX그룹의 잔여량이 부족한 경우, 분류2 알람 기록
                    if dict_firstGrCnt[df_input['1차_MAX_그룹'][i]] < df_input[instCol][i]:
                        df_alarmDetail, alarmDetailNo = self.concatAlarmDetail(df_alarmDetail, alarmDetailNo, '2', df_input, i, df_input['1차_MAX_그룹'][i], df_input[instCol][i] - dict_firstGrCnt[df_input['1차_MAX_그룹'][i]])
                    # 딕셔너리에서 차감
                    dict_firstGrCnt[df_input['1차_MAX_그룹'][i]] -= df_input[instCol][i]
                if '/CT' in df_input['MS Code'][i]:
                    # CT사양인 경우, CT잔여량이 부족하면 기타4 알람 기록
                    if limitCtCnt < df_input[instCol][i]:
                        df_alarmDetail, alarmDetailNo = self.concatAlarmDetail(df_alarmDetail, alarmDetailNo, '기타4', df_input, i, '-', df_input[instCol][i] - limitCtCnt)
                    limitCtCnt -= df_input[instCol][i]
                df_input[resultCol][i] = df_input[instCol][i]
                dict_categoryCnt[df_input['모듈 구분'][i]] -= df_input[instCol][i] * df_input['공수'][i]
                # # 각 조건별 딕셔너리가 0 미만이라면 0으로 재정의
                # if dict_categoryCnt[df_input['모듈 구분'][i]] < 0:
                #     dict_categoryCnt[df_input['모듈 구분'][i]] = 0
                # if dict_secGrCnt[df_input['2차_MAX_그룹'][i]] < 0:
                #     dict_secGrCnt[df_input['2차_MAX_그룹'][i]] = 0
                # if dict_firstGrCnt[df_input['1차_MAX_그룹'][i]] < 0:
                #     dict_firstGrCnt[df_input['1차_MAX_그룹'][i]] = 0
            # 긴급오더 외의 대상일 경우의 로직
            else:
                # 리스트에 [SMT반영 착공량], [모듈구분 잔여 착공량 / 공수] 을 입력
                compareList = [df_input[instCol][i], (dict_categoryCnt[df_input['모듈 구분'][i]] / df_input['공수'][i])]
                # 1차 MAX 그룹이 있다면 리스트에 입력
                if df_input['1차_MAX_그룹'][i] != '-':
                    compareList.append(dict_firstGrCnt[df_input['1차_MAX_그룹'][i]])
                # 2차 MAX 그룹이 있다면 리스트에 입력
                if df_input['2차_MAX_그룹'][i] != '-':
                    compareList.append(dict_secGrCnt[df_input['2차_MAX_그룹'][i]])
                # CT사양일 경우, [CT제한대수]를 리스트에 입력
                if '/CT' in df_input['MS Code'][i]:
                    compareList.append(limitCtCnt)
                # 리스트 중, 최소값을 착공결과로 입력
                df_input[resultCol][i] = min(compareList)
                # 최소필요착공량이며 SMT반영 착공량을 착공할 수 없는 상황일 경우, 상황에 따른 알람을 기록
                if not isRemain and df_input[instCol][i] > 0 and (df_input[instCol][i] != df_input[resultCol][i]):
                    if df_input['1차_MAX_그룹'][i] != '-' and df_input[instCol][i] > dict_firstGrCnt[df_input['1차_MAX_그룹'][i]]:
                        df_alarmDetail, alarmDetailNo = self.concatAlarmDetail(df_alarmDetail, alarmDetailNo, '2', df_input, i, df_input['1차_MAX_그룹'][i], df_input[instCol][i] - df_input[resultCol][i])
                    if df_input['2차_MAX_그룹'][i] != '-' and df_input[instCol][i] > dict_secGrCnt[df_input['2차_MAX_그룹'][i]]:
                        df_alarmDetail, alarmDetailNo = self.concatAlarmDetail(df_alarmDetail, alarmDetailNo, '2', df_input, i, df_input['2차_MAX_그룹'][i], df_input[instCol][i] - df_input[resultCol][i])
                    if '/CT' in df_input['MS Code'][i] and df_input[instCol][i] > limitCtCnt:
                        df_alarmDetail, alarmDetailNo = self.concatAlarmDetail(df_alarmDetail, alarmDetailNo, '기타4', df_input, i, '-', df_input[instCol][i] - df_input[resultCol][i])
                # 조건에 따라 각 딕셔너리에서 착공량을 차감
                dict_categoryCnt[df_input['모듈 구분'][i]] -= df_input[resultCol][i] * df_input['공수'][i]
                if df_input['1차_MAX_그룹'][i] != '-':
                    dict_firstGrCnt[df_input['1차_MAX_그룹'][i]] -= df_input[resultCol][i]
                if df_input['2차_MAX_그룹'][i] != '-':
                    dict_secGrCnt[df_input['2차_MAX_그룹'][i]] -= df_input[resultCol][i]
                if '/CT' in df_input['MS Code'][i]:
                    limitCtCnt -= df_input[resultCol][i]
            # 각 조건별 딕셔너리가 0 미만인 경우, 0으로 재정의
            if df_input['1차_MAX_그룹'][i] != '-' and dict_firstGrCnt[df_input['1차_MAX_그룹'][i]] < 0:
                dict_firstGrCnt[df_input['1차_MAX_그룹'][i]] = 0
            if df_input['2차_MAX_그룹'][i] != '-' and dict_secGrCnt[df_input['2차_MAX_그룹'][i]] < 0:
                dict_secGrCnt[df_input['2차_MAX_그룹'][i]] = 0
            if dict_categoryCnt[df_input['모듈 구분'][i]] < 0:
                dict_categoryCnt[df_input['모듈 구분'][i]] = 0
            if '/CT' in df_input['MS Code'][i] and limitCtCnt < 0:
                limitCtCnt = 0
        return [df_input, dict_categoryCnt, dict_firstGrCnt, dict_secGrCnt, alarmDetailNo, df_alarmDetail, limitCtCnt]

    def run(self):
        # pandas 경고없애기 옵션 적용
        pd.set_option('mode.chained_assignment', None)
        try:
            if self.isDebug:
                debugpy.debug_this_thread()
            maxPb = 200
            self.spReturnMaxPb.emit(maxPb)
            if self.moduleMaxCnt > 0:
                progress = 0
                self.spReturnPb.emit(progress)
                # 긴급오더, 홀딩오더 불러오기
                # 사용자 입력값 불러오기, self.max_cnt
                emgLinkage = self.emgHoldList[0]
                emgmscode = self.emgHoldList[1]
                holdLinkage = self.emgHoldList[2]
                holdmscode = self.emgHoldList[3]
                # 긴급오더, 홀딩오더 데이터프레임화
                df_emgLinkage = pd.DataFrame({'Linkage Number': emgLinkage})
                df_emgmscode = pd.DataFrame({'MS Code': emgmscode})
                df_holdLinkage = pd.DataFrame({'Linkage Number': holdLinkage})
                df_holdmscode = pd.DataFrame({'MS Code': holdmscode})
                # 각 Linkage Number 컬럼의 타입을 일치시킴
                df_emgLinkage['Linkage Number'] = df_emgLinkage['Linkage Number'].astype(str)
                df_holdLinkage['Linkage Number'] = df_holdLinkage['Linkage Number'].astype(str)
                # 긴급오더, 홍딩오더 Join 전 컬럼 추가
                df_emgLinkage['긴급오더'] = '대상'
                df_emgmscode['긴급오더'] = '대상'
                df_holdLinkage['홀딩오더'] = '대상'
                df_holdmscode['홀딩오더'] = '대상'
                # 레벨링 리스트 불러오기
                df_levelingSp = pd.read_excel(self.list_masterFile[2])
                # 레벨링 리스트의 착공 당일의 마지막 No를 가져오기 위한 처리
                df_constDateSp = df_levelingSp[df_levelingSp['Scheduled Start Date (*)'] == self.constDate]
                df_constDateSp = df_constDateSp[df_constDateSp['Sequence No'].notnull()]
                if len(df_constDateSp) > 0:
                    df_constDateSp = df_constDateSp[df_constDateSp['Sequence No'].str.contains('D0')]
                if len(df_constDateSp) > 0:
                    maxNoModule = df_constDateSp['No (*)'].max()
                else:
                    maxNoModule = 0
                # 미착공 대상만 추출(특수_모듈)
                df_levelingSpDropSeq = df_levelingSp[df_levelingSp['Sequence No'].isnull()]
                df_levelingSpUndepSeq = df_levelingSp[df_levelingSp['Sequence No'] == 'Undep']
                df_levelingSpUncorSeq = df_levelingSp[df_levelingSp['Sequence No'] == 'Uncor']
                df_levelingSp = pd.concat([df_levelingSpDropSeq, df_levelingSpUndepSeq, df_levelingSpUncorSeq])
                df_levelingSp['모듈 구분'] = '모듈'
                df_levelingSp['Linkage Number'] = df_levelingSp['Linkage Number'].astype(str)
                df_levelingSp = df_levelingSp.reset_index(drop=True)
                df_levelingSp['미착공수주잔'] = df_levelingSp.groupby('Linkage Number')['Linkage Number'].transform('size')
                df_condition = pd.read_excel(self.list_masterFile[7])
                df_condition['No'] = df_condition['No'].fillna(method='ffill')
                df_condition['1차_MAX_그룹'] = df_condition['1차_MAX_그룹'].fillna(method='ffill')
                df_condition['2차_MAX_그룹'] = df_condition['2차_MAX_그룹'].fillna(method='ffill')
                df_condition['1차_MAX'] = df_condition['1차_MAX'].fillna(method='ffill')
                df_condition['2차_MAX'] = df_condition['2차_MAX'].fillna(method='ffill')
                # 비모듈 레벨링 리스트 불러오기 - 경로에 파일이 있으면 불러올것
                if self.cb_round == '2차':
                    if Path(self.list_masterFile[9]).is_file():
                        df_levelingBL = pd.read_excel(self.list_masterFile[9])
                        df_constDateBL = df_levelingBL[df_levelingBL['Scheduled Start Date (*)'] == self.constDate]
                        df_constDateBL = df_constDateBL[df_constDateBL['Sequence No'].notnull()]
                        if len(df_constDateBL) > 0:
                            df_constDateBL = df_constDateBL[df_constDateBL['Sequence No'].str.contains('D0')]
                        if len(df_constDateBL) > 0:
                            maxNoBL = df_constDateBL['No (*)'].max()
                        else:
                            maxNoBL = 0
                        df_levelingBLDropSeq = df_levelingBL[df_levelingBL['Sequence No'].isnull()]
                        df_levelingBLUndepSeq = df_levelingBL[df_levelingBL['Sequence No'] == 'Undep']
                        df_levelingBLUncorSeq = df_levelingBL[df_levelingBL['Sequence No'] == 'Uncor']
                        df_levelingBL = pd.concat([df_levelingBLDropSeq, df_levelingBLUndepSeq, df_levelingBLUncorSeq])
                        df_levelingBL['모듈 구분'] = df_condition[df_condition['상세구분'] == 'BL=Case']['구분'].values[0]
                        df_levelingBL['Linkage Number'] = df_levelingBL['Linkage Number'].astype(str)
                        df_levelingBL = df_levelingBL.reset_index(drop=True)
                        df_levelingBL['미착공수주잔'] = df_levelingBL.groupby('Linkage Number')['Linkage Number'].transform('size')
                        df_levelingSp = pd.concat([df_levelingSp, df_levelingBL])
                    if Path(self.list_masterFile[10]).is_file():
                        df_levelingTerminal = pd.read_excel(self.list_masterFile[10])
                        df_constDateTerminal = df_levelingTerminal[df_levelingTerminal['Scheduled Start Date (*)'] == self.constDate]
                        df_constDateTerminal = df_constDateTerminal[df_constDateTerminal['Sequence No'].notnull()]
                        if len(df_constDateTerminal) > 0:
                            df_constDateTerminal = df_constDateTerminal[df_constDateTerminal['Sequence No'].str.contains('D0')]
                        if len(df_constDateTerminal) > 0:
                            maxNoTerminal = df_constDateTerminal['No (*)'].max()
                        else:
                            maxNoTerminal = 0
                        df_levelingTerminalDropSeq = df_levelingTerminal[df_levelingTerminal['Sequence No'].isnull()]
                        df_levelingTerminalUndepSeq = df_levelingTerminal[df_levelingTerminal['Sequence No'] == 'Undep']
                        df_levelingTerminalUncorSeq = df_levelingTerminal[df_levelingTerminal['Sequence No'] == 'Uncor']
                        df_levelingTerminal = pd.concat([df_levelingTerminalDropSeq, df_levelingTerminalUndepSeq, df_levelingTerminalUncorSeq])
                        df_levelingTerminal['모듈 구분'] = df_condition[df_condition['상세구분'] == 'Terminal']['구분'].values[0]
                        df_levelingTerminal['Linkage Number'] = df_levelingTerminal['Linkage Number'].astype(str)
                        df_levelingTerminal = df_levelingTerminal.reset_index(drop=True)
                        df_levelingTerminal['미착공수주잔'] = df_levelingTerminal.groupby('Linkage Number')['Linkage Number'].transform('size')
                        df_levelingSp = pd.concat([df_levelingSp, df_levelingTerminal])
                elif self.cb_round == '1차':
                    if Path(self.list_masterFile[11]).is_file():
                        df_levelingSlave = pd.read_excel(self.list_masterFile[11])
                        df_constDateSlave = df_levelingSlave[df_levelingSlave['Scheduled Start Date (*)'] == self.constDate]
                        df_constDateSlave = df_constDateSlave[df_constDateSlave['Sequence No'].notnull()]
                        if len(df_constDateSlave) > 0:
                            df_constDateSlave = df_constDateSlave[df_constDateSlave['Sequence No'].str.contains('D0')]
                        if len(df_constDateSlave) > 0:
                            maxNoSlave = df_constDateSlave['No (*)'].max()
                        else:
                            maxNoSlave = 0
                        df_levelingSlaveDropSeq = df_levelingSlave[df_levelingSlave['Sequence No'].isnull()]
                        df_levelingSlaveUndepSeq = df_levelingSlave[df_levelingSlave['Sequence No'] == 'Undep']
                        df_levelingSlaveUncorSeq = df_levelingSlave[df_levelingSlave['Sequence No'] == 'Uncor']
                        df_levelingSlave = pd.concat([df_levelingSlaveDropSeq, df_levelingSlaveUndepSeq, df_levelingSlaveUncorSeq])
                        df_levelingSlave['모듈 구분'] = df_condition[df_condition['상세구분'] == 'Slave']['구분'].values[0]
                        df_levelingSlave['Linkage Number'] = df_levelingSlave['Linkage Number'].astype(str)
                        df_levelingSlave = df_levelingSlave.reset_index(drop=True)
                        df_levelingSlave['미착공수주잔'] = df_levelingSlave.groupby('Linkage Number')['Linkage Number'].transform('size')
                        df_levelingSp = pd.concat([df_levelingSp, df_levelingSlave])
                progress += round(maxPb / 20)
                self.spReturnPb.emit(progress)
                if self.isDebug:
                    df_levelingSp.to_excel('.\\debug\\Sp\\flow1.xlsx')
                df_sosFile = pd.read_excel(self.list_masterFile[0])
                df_sosFile['Linkage Number'] = df_sosFile['Linkage Number'].astype(str)
                df_levelingSp['Linkage Number'] = df_levelingSp['Linkage Number'].astype(str)
                progress += round(maxPb / 20)
                self.spReturnPb.emit(progress)
                # if self.isDebug:
                #     df_sosFile.to_excel('.\\debug\\Sp\\flow2.xlsx')
                # 착공 대상 외 모델 삭제
                df_sosFile = df_sosFile.drop(df_sosFile[df_sosFile['MS Code'].str.contains('ZOTHER')].index)
                df_sosFile = df_sosFile.drop(df_sosFile[df_sosFile['MS Code'].str.contains('YZ')].index)
                df_sosFile = df_sosFile.drop(df_sosFile[df_sosFile['MS Code'].str.contains('SF')].index)
                df_sosFile = df_sosFile.drop(df_sosFile[df_sosFile['MS Code'].str.contains('KM')].index)
                df_sosFile = df_sosFile.drop(df_sosFile[df_sosFile['MS Code'].str.contains('TA80')].index)
                if self.cb_round != '1차':
                    df_sosFile = df_sosFile.drop(df_sosFile[df_sosFile['MS Code'].str.contains('CT')].index)
                progress += round(maxPb / 20)
                self.spReturnPb.emit(progress)
                if self.isDebug:
                    df_sosFile.to_excel('.\\debug\\Sp\\flow3.xlsx')
                # 워킹데이 캘린더 불러오기
                dfCalendar = pd.read_excel(self.list_masterFile[4])
                today = datetime.datetime.today().strftime('%Y%m%d')
                if self.isDebug:
                    today = self.date
                # 진척 파일 - SOS2파일 Join
                df_sosFileMerge = pd.merge(df_sosFile, df_levelingSp).drop_duplicates(['Linkage Number'])
                df_sosFileMerge = df_sosFileMerge[['Linkage Number', 'MS Code', 'Planned Prod. Completion date', 'Order Quantity', '미착공수주잔', '모듈 구분']]
                df_sosFileMerge = df_sosFileMerge[df_sosFileMerge['미착공수주잔'] != 0]
                # 위 파일을 완성지정일 기준 오름차순 정렬 및 인덱스 재설정
                df_sosFileMerge = df_sosFileMerge.sort_values(by=['Planned Prod. Completion date'], ascending=[True])
                df_sosFileMerge = df_sosFileMerge.reset_index(drop=True)
                # 대표모델 Column 생성
                df_sosFileMerge['대표모델'] = df_sosFileMerge['MS Code'].str[:9]
                # 남은 워킹데이 Column 생성
                df_sosFileMerge['남은 워킹데이'] = 0
                # 긴급오더, 홀딩오더 Linkage Number Column 타입 일치
                df_emgLinkage['Linkage Number'] = df_emgLinkage['Linkage Number'].astype(str)
                df_holdLinkage['Linkage Number'] = df_holdLinkage['Linkage Number'].astype(str)
                # 긴급오더, 홀딩오더와 위 Sos파일을 Join
                df_MergeLink = pd.merge(df_sosFileMerge, df_emgLinkage, on='Linkage Number', how='left')
                dfMergemscode = pd.merge(df_sosFileMerge, df_emgmscode, on='MS Code', how='left')
                df_MergeLink = pd.merge(df_MergeLink, df_holdLinkage, on='Linkage Number', how='left')
                dfMergemscode = pd.merge(dfMergemscode, df_holdmscode, on='MS Code', how='left')
                if self.isDebug:
                    dfMergemscode.to_excel('.\\debug\\Sp\\test.xlsx')
                df_MergeLink['긴급오더'] = df_MergeLink['긴급오더'].combine_first(dfMergemscode['긴급오더'])
                df_MergeLink['홀딩오더'] = df_MergeLink['홀딩오더'].combine_first(dfMergemscode['홀딩오더'])
                df_MergeLink['당일착공'] = ''
                df_MergeLink['완성지정일_원본'] = df_MergeLink['Planned Prod. Completion date']
                # CT사양은 기존 완성지정일보다 4일 더 빠르게 착공내려야 하기 때문에 보정처리
                df_MergeLink.loc[df_MergeLink['MS Code'].str.contains('/CT'), 'Planned Prod. Completion date'] = df_MergeLink['완성지정일_원본'] - datetime.timedelta(days=4)
                df_MergeLink = df_MergeLink.sort_values(by=['Planned Prod. Completion date'], ascending=[True])
                df_MergeLink = df_MergeLink.reset_index(drop=True)
                # 한달 내, S9307UF의 수주가 있을 경우, 알람 메시지 출력
                oneMonthLater = (datetime.datetime.now() + datetime.timedelta(days=30)).strftime('%Y-%m-%d %H:%M:%S')
                oneMonthLater = datetime.datetime.strptime(oneMonthLater, '%Y-%m-%d %H:%M:%S')
                df_switch = df_sosFile[df_sosFile['MS Code'].str.contains('S9307UF')]
                df_switch = df_switch.reset_index(drop=True)
                for i in df_switch.index:
                    if oneMonthLater >= df_switch['Planned Prod. Completion date'][i]:
                        self.spReturnWarning.emit(f"Linkage Number:[{str(df_switch['Linkage Number'][i])}], SWITCH(S9307UF)의 수주잔이 확인되었습니다. 완성지정일: [{str(df_switch['Planned Prod. Completion date'][i])}]")
                # 남은 워킹데이 체크 및 컬럼 추가
                for i in df_MergeLink.index:
                    df_MergeLink['남은 워킹데이'][i] = self.checkWorkDay(dfCalendar, today, df_MergeLink['Planned Prod. Completion date'][i])
                    if df_MergeLink['남은 워킹데이'][i] < 1:
                        df_MergeLink['긴급오더'][i] = '대상'
                    elif df_MergeLink['남은 워킹데이'][i] == 1:
                        df_MergeLink['당일착공'][i] = '대상'
                df_MergeLink['Linkage Number'] = df_MergeLink['Linkage Number'].astype(str)
                df_MergeLink['MODEL'] = df_MergeLink['MS Code'].str[:7]
                df_MergeLink['MODEL'] = df_MergeLink['MODEL'].astype(str).apply(self.delHypen)
                # 홀딩오더는 제외
                df_MergeLink = df_MergeLink[df_MergeLink['홀딩오더'].isnull()]
                progress += round(maxPb / 20)
                self.spReturnPb.emit(progress)
                if self.isDebug:
                    df_MergeLink.to_excel('.\\debug\\Sp\\flow4.xlsx')
                # 프로그램 기동날짜의 전일을 계산 (Debug시에는 디버그용 LineEdit에 기록된 날짜를 사용)
                yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y%m%d')
                if self.isDebug:
                    yesterday = (datetime.datetime.strptime(self.date, '%Y%m%d') - datetime.timedelta(days=1)).strftime('%Y%m%d')
                # 설정파일 불러오기
                parser = ConfigParser()
                parser.read(self.list_masterFile[16], encoding='euc-kr')
                smtAssyDbHost = parser.get('SMT Assy DB정보', 'Host')
                smtAssyDbPort = parser.getint('SMT Assy DB정보', 'Port')
                smtAssyDbSID = parser.get('SMT Assy DB정보', 'SID')
                smtAssyDbUser = parser.get('SMT Assy DB정보', 'Username')
                smtAssyDbPw = parser.get('SMT Assy DB정보', 'Password')
                # 해당 날짜의 Smt Assy 남은 대수 확인
                df_SmtAssyInven = self.readDB(smtAssyDbHost,
                                                smtAssyDbPort,
                                                smtAssyDbSID,
                                                smtAssyDbUser,
                                                smtAssyDbPw,
                                                "SELECT INV_D, PARTS_NO, CURRENT_INV_QTY FROM pdsg0040 where INV_D = TO_DATE(" + str(yesterday) + ",'YYYYMMDD')")
                df_SmtAssyInven['현재수량'] = 0
                # 2차 메인피킹 리스트 불러오기 및 Smt Assy 재고량 Df와 Join
                if Path(self.list_masterFile[5]).is_file() or Path(self.list_masterFile[14]).is_file() or Path(self.list_masterFile[15]).is_file():
                    df_secOrderList = pd.DataFrame(columns=['ASSY NO', '대수', 'SMT STORE ADDRESS'])
                    if Path(self.list_masterFile[5]).is_file():
                        df_secOrderMainList = pd.read_excel(self.list_masterFile[5], skiprows=5)
                        df_secOrderList = pd.concat([df_secOrderList, df_secOrderMainList])
                    if Path(self.list_masterFile[14]).is_file():
                        df_secOrderPowerList = pd.read_excel(self.list_masterFile[14], skiprows=5)
                        df_secOrderList = pd.concat([df_secOrderList, df_secOrderPowerList])
                    if Path(self.list_masterFile[15]).is_file():
                        df_secOrderSpList = pd.read_excel(self.list_masterFile[15], skiprows=5)
                        df_secOrderList = pd.concat([df_secOrderList, df_secOrderSpList])
                    df_joinSmt = pd.merge(df_secOrderList, df_SmtAssyInven, how='right', left_on='ASSY NO', right_on='PARTS_NO')
                    df_joinSmt['대수'] = df_joinSmt['대수'].fillna(0)
                    # Smt Assy 현재 재고량에서 사용량 차감
                    df_joinSmt['현재수량'] = df_joinSmt['CURRENT_INV_QTY'] - df_joinSmt['대수']
                else:
                    df_joinSmt = df_SmtAssyInven.copy()
                    df_joinSmt['현재수량'] = df_joinSmt['CURRENT_INV_QTY']
                progress += round(maxPb / 20)
                self.spReturnPb.emit(progress)
                dict_smtCnt = {}
                # Smt Assy 재고량을 PARTS_NO를 Key로 Dict화
                for i in df_joinSmt.index:
                    if df_joinSmt['현재수량'][i] < 0:
                        df_joinSmt['현재수량'][i] = 0
                    dict_smtCnt[df_joinSmt['PARTS_NO'][i]] = df_joinSmt['현재수량'][i]
                if self.isDebug:
                    df_joinSmt.to_excel('.\\debug\\Sp\\flow5.xlsx')
                # PB01: S9221DS, TA40: S9091BU 재고량 미확인 모델 dict_smtCnt 추가
                df_smtUnCheck = pd.read_excel(self.list_masterFile[8])
                list_nonManageSmt = df_smtUnCheck['SMT ASSY'].tolist()
                pdbsDbHost = parser.get('MSCODE별 SMT Assy DB정보', 'Host')
                pdbsDbPort = parser.getint('MSCODE별 SMT Assy DB정보', 'Port')
                pdbsDbSID = parser.get('MSCODE별 SMT Assy DB정보', 'SID')
                pdbsDbUser = parser.get('MSCODE별 SMT Assy DB정보', 'Username')
                pdbsDbPw = parser.get('MSCODE별 SMT Assy DB정보', 'Password')

                # DB로부터 메인라인의 MSCode별 사용 Smt Assy 가져옴
                df_pdbs = self.readDB(pdbsDbHost,
                                        pdbsDbPort,
                                        pdbsDbSID,
                                        pdbsDbUser,
                                        pdbsDbPw,
                                        "SELECT SMT_MS_CODE, SMT_SMT_ASSY, SMT_CRP_GR_NO FROM sap.pdbs0010 WHERE SMT_CRP_GR_NO = '100L1304' or SMT_CRP_GR_NO = '100L1318' or SMT_CRP_GR_NO = '100L1331' or SMT_CRP_GR_NO = '100L1312' or SMT_CRP_GR_NO = '100L1303'")
                # 불필요한 데이터 삭제
                df_pdbs = df_pdbs[~df_pdbs['SMT_MS_CODE'].str.contains('AST')]
                df_pdbs = df_pdbs[~df_pdbs['SMT_MS_CODE'].str.contains('BMS')]
                df_pdbs = df_pdbs[~df_pdbs['SMT_MS_CODE'].str.contains('WEB')]
                progress += round(maxPb / 20)
                self.spReturnPb.emit(progress)
                if self.isDebug:
                    df_pdbs.to_excel('.\\debug\\Sp\\flow6.xlsx')
                # 사용 Smt Assy를 병렬화
                gb = df_pdbs.groupby('SMT_MS_CODE')
                df_temp = pd.DataFrame([df_pdbs.loc[gb.groups[n], 'SMT_SMT_ASSY'].values for n in gb.groups], index=gb.groups.keys())
                df_temp.columns = ['ROW' + str(i + 1) for i in df_temp.columns]
                rowNo = len(df_temp.columns)
                df_temp = df_temp.reset_index()
                df_temp.rename(columns={'index': 'MS Code'}, inplace=True)
                progress += round(maxPb / 20)
                self.spReturnPb.emit(progress)
                if self.isDebug:
                    df_temp.to_excel('.\\debug\\Sp\\flow7.xlsx')
                # 모델별 사용 Smt Assy를 Join
                df_addSmtAssy = pd.merge(df_MergeLink, df_temp, on='MS Code', how='left')
                df_addSmtAssy = df_addSmtAssy.reset_index(drop=True)
                progress += round(maxPb / 20)
                self.spReturnPb.emit(progress)
                if self.isDebug:
                    df_addSmtAssy.to_excel('.\\debug\\Sp\\flow8.xlsx')
                df_addSmtAssy = pd.merge(df_addSmtAssy, df_condition, on='MODEL', how='left')
                df_addSmtAssy['1차_MAX_그룹'] = df_addSmtAssy['1차_MAX_그룹'].fillna('-')
                df_addSmtAssy['2차_MAX_그룹'] = df_addSmtAssy['2차_MAX_그룹'].fillna('-')
                df_addSmtAssy['공수'] = df_addSmtAssy['공수'].fillna(1)
                df_addSmtAssy['대표모델별_최소착공필요량_per_일'] = 0
                dict_integCnt = {}
                dict_minContCnt = {}
                # 대표모델 별 최소 착공 필요량을 계산
                for i in df_addSmtAssy.index:
                    # 1차 MAX 그룹이 있는 경우는 대표모델을 그룹명으로 교체
                    if str(df_addSmtAssy['1차_MAX_그룹'][i]) != '' and str(df_addSmtAssy['1차_MAX_그룹'][i]) != '-' and str(df_addSmtAssy['1차_MAX_그룹'][i]) != 'nan' and str(df_addSmtAssy['우선착공'][i]) != '' and str(df_addSmtAssy['우선착공'][i]) != 'nan':
                        df_addSmtAssy['대표모델'][i] = df_addSmtAssy['1차_MAX_그룹'][i]
                    # 각 대표모델 별 적산착공량을 계산하여 딕셔너리에 저장
                    if df_addSmtAssy['대표모델'][i] in dict_integCnt:
                        dict_integCnt[df_addSmtAssy['대표모델'][i]] += int(df_addSmtAssy['미착공수주잔'][i])
                    else:
                        dict_integCnt[df_addSmtAssy['대표모델'][i]] = int(df_addSmtAssy['미착공수주잔'][i])
                    # 이미 완성지정일을 지난경우, 워킹데이 계산을 위해 워킹데이를 1로 설정
                    if df_addSmtAssy['남은 워킹데이'][i] <= 0:
                        workDay = 1
                    else:
                        workDay = df_addSmtAssy['남은 워킹데이'][i]
                    if str(df_addSmtAssy['1차_MAX_그룹'][i]) != '' and str(df_addSmtAssy['1차_MAX_그룹'][i]) != 'nan' and str(df_addSmtAssy['1차_MAX_그룹'][i]) != '-':
                        dict_minContCnt[df_addSmtAssy['대표모델'][i]] = [df_addSmtAssy['1차_MAX'][i], df_addSmtAssy['Planned Prod. Completion date'][i]]
                    # 완성지정일별 최소필요착공량 계산 후, 딕셔너리에 리스트(대표모델, 완성지정일)로 저장
                    elif len(dict_minContCnt) > 0:
                        if df_addSmtAssy['대표모델'][i] in dict_minContCnt:
                            if dict_minContCnt[df_addSmtAssy['대표모델'][i]][0] < math.ceil(dict_integCnt[df_addSmtAssy['대표모델'][i]] / workDay):
                                dict_minContCnt[df_addSmtAssy['대표모델'][i]][0] = math.ceil(dict_integCnt[df_addSmtAssy['대표모델'][i]] / workDay)
                                dict_minContCnt[df_addSmtAssy['대표모델'][i]][1] = df_addSmtAssy['Planned Prod. Completion date'][i]
                        else:
                            dict_minContCnt[df_addSmtAssy['대표모델'][i]] = [math.ceil(dict_integCnt[df_addSmtAssy['대표모델'][i]] / workDay),
                                                                            df_addSmtAssy['Planned Prod. Completion date'][i]]
                    else:
                        dict_minContCnt[df_addSmtAssy['대표모델'][i]] = [math.ceil(dict_integCnt[df_addSmtAssy['대표모델'][i]] / workDay),
                                                                        df_addSmtAssy['Planned Prod. Completion date'][i]]
                    if workDay <= 0:
                        workDay = 1
                    # 위에서 계산한 최소필요착공량을 컬럼화시켜 데이터프레임에 입력
                    df_addSmtAssy['대표모델별_최소착공필요량_per_일'][i] = dict_integCnt[df_addSmtAssy['대표모델'][i]] / workDay
                progress += round(maxPb / 20)
                self.spReturnPb.emit(progress)
                if self.isDebug:
                    df_addSmtAssy.to_excel('.\\debug\\Sp\\flow9.xlsx')
                dict_minContCopy = dict_minContCnt.copy()
                # 대표모델 별 최소착공 필요량을 기준으로 평준화 적용 착공량을 계산. 미착공수주잔에서 해당 평준화 적용 착공량을 제외한 수량은 잔여착공량으로 기재
                df_addSmtAssy['평준화_적용_착공량'] = 0
                for i in df_addSmtAssy.index:
                    if df_addSmtAssy['긴급오더'][i] == '대상':
                        df_addSmtAssy['평준화_적용_착공량'][i] = int(df_addSmtAssy['미착공수주잔'][i])
                        if df_addSmtAssy['대표모델'][i] in dict_minContCopy:
                            if dict_minContCopy[df_addSmtAssy['대표모델'][i]][0] >= int(df_addSmtAssy['미착공수주잔'][i]):
                                dict_minContCopy[df_addSmtAssy['대표모델'][i]][0] -= int(df_addSmtAssy['미착공수주잔'][i])
                            else:
                                dict_minContCopy[df_addSmtAssy['대표모델'][i]][0] = 0
                    elif df_addSmtAssy['대표모델'][i] in dict_minContCopy:
                        if dict_minContCopy[df_addSmtAssy['대표모델'][i]][0] >= int(df_addSmtAssy['미착공수주잔'][i]):
                            df_addSmtAssy['평준화_적용_착공량'][i] = int(df_addSmtAssy['미착공수주잔'][i])
                            dict_minContCopy[df_addSmtAssy['대표모델'][i]][0] -= int(df_addSmtAssy['미착공수주잔'][i])
                        else:
                            df_addSmtAssy['평준화_적용_착공량'][i] = dict_minContCopy[df_addSmtAssy['대표모델'][i]][0]
                            dict_minContCopy[df_addSmtAssy['대표모델'][i]][0] = 0
                df_addSmtAssy['잔여_착공량'] = df_addSmtAssy['미착공수주잔'] - df_addSmtAssy['평준화_적용_착공량']
                df_addSmtAssy = df_addSmtAssy.sort_values(by=['긴급오더', '당일착공', 'Planned Prod. Completion date', '평준화_적용_착공량'], ascending=[False, False, True, False])
                df_addSmtAssy = df_addSmtAssy.reset_index(drop=True)
                progress += round(maxPb / 20)
                self.spReturnPb.emit(progress)
                if self.isDebug:
                    df_addSmtAssy.to_excel('.\\debug\\Sp\\flow10.xlsx')
                # SMT 잔여수량 적용
                df_addSmtAssy['SMT반영_착공량'] = 0
                pattern = '|'.join(list_nonManageSmt)
                for j in range(1, rowNo):
                    df_addSmtAssy[f'SMT비관리대상{str(j)}'] = df_addSmtAssy[f'ROW{str(j)}'].str.contains(pattern, case=False)
                if self.isDebug:
                    df_addSmtAssy.to_excel('.\\debug\\Sp\\flow10-1.xlsx')
                # 알람 상세 DataFrame 생성
                df_alarmDetail = pd.DataFrame(columns=["No.", "분류", "L/N", "MS CODE", "SMT ASSY", "수주수량", "부족수량", "검사호기(그룹)", "대상 검사시간(초)", "필요시간(초)", "완성예정일"])
                alarmDetailNo = 1
                # 최소착공량에 대해 Smt적용 착공량 계산
                df_addSmtAssy, dict_smtCnt, alarmDetailNo, df_alarmDetail = self.smtReflectInst(df_addSmtAssy, False, dict_smtCnt, alarmDetailNo, df_alarmDetail, rowNo)
                # 잔여 착공량에 대해 Smt적용 착공량 계산
                df_addSmtAssy['SMT반영_착공량_잔여'] = 0
                df_addSmtAssy, dict_smtCnt, alarmDetailNo, df_alarmDetail = self.smtReflectInst(df_addSmtAssy, True, dict_smtCnt, alarmDetailNo, df_alarmDetail, rowNo)
                progress += round(maxPb / 20)
                self.spReturnPb.emit(progress)
                if self.isDebug:
                    df_addSmtAssy.to_excel('.\\debug\\Sp\\flow11.xlsx')
                # 특수 기종분류표 반영 착공 로직 start
                progress += round(maxPb / 20)
                self.spReturnPb.emit(progress)
                if self.isDebug:
                    df_addSmtAssy.to_excel('.\\debug\\Sp\\flow12.xlsx')
                dict_firstGrCnt = {}
                dict_secGrCnt = {}
                dict_categoryCnt = {'모듈': self.moduleMaxCnt, '비모듈': self.nonModuleMaxCnt}
                # 특수 조건표 불러오기
                for i in df_condition.index:
                    if (str(df_condition['2차_MAX_그룹'][i]) != '-' and str(df_condition['2차_MAX_그룹'][i]) != '' and (df_condition['2차_MAX_그룹'][i]) != 'nan'):
                        dict_firstGrCnt[df_condition['1차_MAX_그룹'][i]] = df_condition['1차_MAX'][i]
                        dict_secGrCnt[df_condition['2차_MAX_그룹'][i]] = df_condition['2차_MAX'][i]
                    elif str(df_condition['1차_MAX_그룹'][i]) != '-' and str(df_condition['1차_MAX_그룹'][i]) != '' and str(df_condition['1차_MAX_그룹'][i]) != 'nan':
                        dict_firstGrCnt[df_condition['1차_MAX_그룹'][i]] = df_condition['1차_MAX'][i]
                df_addSmtAssy = df_addSmtAssy[df_addSmtAssy['검사호기'] != 'P']
                # 메인 클래스로부터 전달받은 P호기 검사대상을 가져와서 병합처리 (중복되었으나 불필요한 컬럼은 삭제)
                if len(self.df_receiveMain) > 0:
                    df_receiveMain = self.df_receiveMain
                    df_receiveMain['MODEL'] = df_receiveMain['MS Code'].str[:6]
                    df_receiveMain['공수'] = 1
                    df_receiveMain['모듈 구분'] = '모듈'
                    df_addSmtAssy = pd.concat([df_addSmtAssy, df_receiveMain])
                    del df_addSmtAssy['구분']
                    del df_addSmtAssy['No']
                    del df_addSmtAssy['상세구분']
                    del df_addSmtAssy['검사호기']
                    del df_addSmtAssy['1차_MAX_그룹']
                    del df_addSmtAssy['2차_MAX_그룹']
                    del df_addSmtAssy['1차_MAX']
                    del df_addSmtAssy['2차_MAX']
                    del df_addSmtAssy['공수']
                    del df_addSmtAssy['우선착공']
                    df_addSmtAssy = pd.merge(df_addSmtAssy, df_condition, on='MODEL', how='left')
                    df_addSmtAssy['1차_MAX_그룹'] = df_addSmtAssy['1차_MAX_그룹'].fillna('-')
                    df_addSmtAssy['2차_MAX_그룹'] = df_addSmtAssy['2차_MAX_그룹'].fillna('-')
                    df_addSmtAssy['공수'] = df_addSmtAssy['공수'].fillna(1)
                df_addSmtAssy = df_addSmtAssy.sort_values(by=['우선착공', 'Planned Prod. Completion date'], ascending=[False, True])
                df_addSmtAssy = df_addSmtAssy.reset_index(drop=True)
                progress += round(maxPb / 20)
                self.spReturnPb.emit(progress)
                if self.isDebug:
                    df_addSmtAssy.to_excel('.\\debug\\Sp\\flow12-1.xlsx')
                df_addSmtAssy['설비능력반영_착공량'] = 0
                # CT 조건표 불러오기
                df_limitCtCond = pd.read_excel(self.list_masterFile[13])
                limitCtCnt = df_limitCtCond[df_limitCtCond['상세구분'] == 'OTHER']['허용수량'].values[0]
                # 조건표의 제한대수를 적용하여 착공 (최소필요착공량)
                df_addSmtAssy, dict_categoryCnt, dict_firstGrCnt, dict_secGrCnt, alarmDetailNo, df_alarmDetail, limitCtCnt = self.grMaxCntReflect(df_addSmtAssy,
                                                                                                                                                False,
                                                                                                                                                dict_categoryCnt,
                                                                                                                                                dict_firstGrCnt,
                                                                                                                                                dict_secGrCnt,
                                                                                                                                                alarmDetailNo,
                                                                                                                                                df_alarmDetail,
                                                                                                                                                limitCtCnt)
                df_addSmtAssy['설비능력반영_착공량_잔여'] = 0
                # 조건표의 제한대수를 적용하여 착공 (여유분)
                df_addSmtAssy, dict_categoryCnt, dict_firstGrCnt, dict_secGrCnt, alarmDetailNo, df_alarmDetail, limitCtCnt = self.grMaxCntReflect(df_addSmtAssy,
                                                                                                                                                True,
                                                                                                                                                dict_categoryCnt,
                                                                                                                                                dict_firstGrCnt,
                                                                                                                                                dict_secGrCnt,
                                                                                                                                                alarmDetailNo,
                                                                                                                                                df_alarmDetail,
                                                                                                                                                limitCtCnt)
                if self.isDebug:
                    df_alarmDetail.to_excel('.\\debug\\Sp\\df_alarmDetail.xlsx')
                df_addSmtAssy = df_addSmtAssy.reset_index(drop=True)
                progress += round(maxPb / 20)
                self.spReturnPb.emit(progress)
                if self.isDebug:
                    df_addSmtAssy.to_excel('.\\debug\\Sp\\flow13.xlsx')
                # 알람 상세 결과에서 각 항목별로 요약
                # 분류1 요약
                if len(df_alarmDetail) > 0:
                    df_firstAlarm = df_alarmDetail[df_alarmDetail['분류'] == '1']
                    df_firstAlarmSummary = df_firstAlarm.groupby("SMT ASSY")['부족수량'].sum()
                    df_firstAlarmSummary = df_firstAlarmSummary.reset_index()
                    df_firstAlarmSummary['분류'] = '1'
                    df_firstAlarmSummary['MS CODE'] = '-'
                    df_firstAlarmSummary['검사호기(그룹)'] = '-'
                    df_firstAlarmSummary['부족 시간'] = '-'
                    df_firstAlarmSummary['Message'] = '[SMT ASSY : ' + df_firstAlarmSummary["SMT ASSY"] + ']가 부족합니다. SMT ASSY 제작을 지시해주세요.'
                    df_secAlarm = df_alarmDetail[df_alarmDetail['분류'] == '2']
                    df_secAlarmSummary = df_secAlarm.groupby("MS CODE")['부족수량'].max()
                    df_secAlarmSummary = pd.merge(df_secAlarmSummary, df_alarmDetail[['MS CODE', '검사호기(그룹)']], how='left', on='MS CODE').drop_duplicates('MS CODE')
                    df_secAlarmSummary = df_secAlarmSummary.reset_index()
                    df_secAlarmSummary['부족 시간'] = '-'
                    df_secAlarmSummary['분류'] = '2'
                    df_secAlarmSummary['SMT ASSY'] = '-'
                    df_secAlarmSummary['Message'] = '당일 최대 착공 제한 대수가 부족합니다. 설정 데이터를 확인해 주세요.'
                    # 분류 기타2 요약
                    df_etc2Alarm = df_alarmDetail[df_alarmDetail['분류'] == '기타2']
                    df_etc2AlarmSummary = df_etc2Alarm.groupby('MS CODE')['부족수량'].sum()
                    df_etc2AlarmSummary = df_etc2AlarmSummary.reset_index()
                    df_etc2AlarmSummary['수량'] = df_etc2AlarmSummary['부족수량']
                    df_etc2AlarmSummary['분류'] = '기타2'
                    df_etc2AlarmSummary['SMT ASSY'] = '-'
                    df_etc2AlarmSummary['검사호기'] = '-'
                    df_etc2AlarmSummary['부족 시간'] = '-'
                    df_etc2AlarmSummary['Message'] = '긴급오더 및 당일착공 대상의 총 착공량이 입력한 최대착공량보다 큽니다. 최대착공량을 확인해주세요.'
                    del df_etc2AlarmSummary['부족수량']
                    # 분류 기타4 요약
                    df_etc4Alarm = df_alarmDetail[df_alarmDetail['분류'] == '기타4']
                    df_etc4AlarmSummary = df_etc4Alarm.groupby('MS CODE')['부족수량'].sum()
                    df_etc4AlarmSummary = df_etc4AlarmSummary.reset_index()
                    df_etc4AlarmSummary['수량'] = df_etc4AlarmSummary['부족수량']
                    df_etc4AlarmSummary['분류'] = '기타4'
                    df_etc4AlarmSummary['SMT ASSY'] = '-'
                    df_etc4AlarmSummary['검사호기'] = '-'
                    df_etc4AlarmSummary['부족 시간'] = '-'
                    df_etc4AlarmSummary['Message'] = '설정된 CT 제한대수보다 최소 착공 필요량이 많습니다. 설정된 CT 제한대수를 확인해주세요.'
                    del df_etc4AlarmSummary['부족수량']
                    # 위 알람을 병합
                    df_alarmSummary = pd.concat([df_firstAlarmSummary, df_secAlarmSummary, df_etc2AlarmSummary, df_etc4AlarmSummary])
                    # 기타 알람에 대한 추가
                    df_etcList = df_alarmDetail[(df_alarmDetail['분류'] == '기타1') | (df_alarmDetail['분류'] == '기타3')]
                    df_etcList = df_etcList.drop_duplicates(['MS CODE', '분류'])
                    df_etcList = df_etcList.reset_index()
                    for i in df_etcList.index:
                        if df_etcList['분류'][i] == '기타1':
                            df_alarmSummary = pd.concat([df_alarmSummary,
                                                        pd.DataFrame.from_records([{"분류": df_etcList['분류'][i],
                                                                                    "MS CODE": df_etcList['MS CODE'][i],
                                                                                    "SMT ASSY": '-',
                                                                                    "부족수량": 0,
                                                                                    "검사호기(그룹)": '-',
                                                                                    "부족 시간": 0,
                                                                                    "Message": '해당 MS CODE에서 사용되는 SMT ASSY가 등록되지 않았습니다. 등록 후 다시 실행해주세요.'}])])
                        elif df_etcList['분류'][i] == '기타3':
                            df_alarmSummary = pd.concat([df_alarmSummary,
                                                        pd.DataFrame.from_records([{"분류": df_etcList['분류'][i],
                                                                                    "MS CODE": df_etcList['MS CODE'][i],
                                                                                    "SMT ASSY": df_etcList['SMT ASSY'][i],
                                                                                    "수량": 0,
                                                                                    "검사호기(그룹)": '-',
                                                                                    "부족 시간": 0,
                                                                                    "Message": 'SMT ASSY 정보가 등록되지 않아 재고를 확인할 수 없습니다. 등록 후 다시 실행해주세요.'}])])
                    df_alarmSummary = df_alarmSummary.reset_index(drop=True)
                    df_alarmSummary = df_alarmSummary[['분류', 'MS CODE', 'SMT ASSY', '부족수량', '검사호기(그룹)', '부족 시간', 'Message']]
                    if self.isDebug:
                        df_alarmSummary.to_excel('.\\debug\\Sp\\df_alarmSummary.xlsx')
                    if not os.path.exists(f'.\\Output\\Alarm\\{str(today)}\\{self.cb_round}'):
                        os.makedirs(f'.\\Output\\Alarm\\{str(today)}\\{self.cb_round}')
                    df_alarmExplain = pd.DataFrame({'분류': ['1', '2', '기타1', '기타2', '기타3', '기타4'],
                                                        '분류별 상황': ['DB상의 Smt Assy가 부족하여 해당 MS-Code를 착공 내릴 수 없는 경우',
                                                        '당일 착공분(or 긴급착공분)에 대해 MAX 대수가 부족할 경우',
                                                        'MS-Code와 일치하는 Smt Assy가 마스터 파일에 없는 경우',
                                                        '긴급오더 대상 착공시 최대착공량(사용자입력공수)이 부족할 경우',
                                                        'SMT ASSY 정보가 DB에 미등록된 경우',
                                                        '당일 최소 착공필요량 > CT제한 대수인 경우']})
                    # 파일 한개로 출력
                    with pd.ExcelWriter(f'.\\Output\\Alarm\\{str(today)}\\{self.cb_round}\\FAM3_AlarmList_{today}_Sp.xlsx') as writer:
                        df_alarmSummary.to_excel(writer, sheet_name='정리', index=True)
                        df_alarmDetail.to_excel(writer, sheet_name='상세', index=True)
                        df_alarmExplain.to_excel(writer, sheet_name='설명', index=False)
                # 총착공량 컬럼으로 병합
                df_addSmtAssy['총착공량'] = df_addSmtAssy['설비능력반영_착공량'] + df_addSmtAssy['설비능력반영_착공량_잔여']
                df_addSmtAssy = df_addSmtAssy[df_addSmtAssy['총착공량'] != 0]
                # 홀딩리스트 불러오기
                df_holdingList = pd.read_excel(self.list_masterFile[17])
                # 홀딩리스트와 비교하여 조건에 해당하는 경우, 알람 메시지 출력
                for i in df_holdingList.index:
                    message = ""
                    if len(df_addSmtAssy[df_addSmtAssy['MODEL'] == df_holdingList['MODEL'][i]]['총착공량'].values) > 0:
                        totalCnt = 0
                        for cnt in df_addSmtAssy[df_addSmtAssy['MODEL'] == df_holdingList['MODEL'][i]]['총착공량'].values:
                            totalCnt += cnt
                        message = f"{df_holdingList['MODEL'][i]} {int(totalCnt)}대 {df_holdingList['REMARK'][i]}"
                    if len(df_addSmtAssy[df_addSmtAssy['Linkage Number'] == df_holdingList['LINKAGENO'][i]]['총착공량'].values) > 0:
                        message = f"{df_holdingList['LINKAGENO'][i]} {int(df_addSmtAssy[df_addSmtAssy['Linkage Number'] == df_holdingList['LINKAGENO'][i]]['총착공량'].values[0])}대 {df_holdingList['REMARK'][i]}"
                    if len(df_addSmtAssy[df_addSmtAssy['MS Code'] == df_holdingList['MS-CODE'][i]]['총착공량'].values) > 0:
                        totalCnt = 0
                        for cnt in df_addSmtAssy[df_addSmtAssy['MS Code'] == df_holdingList['MS-CODE'][i]]['총착공량'].values:
                            totalCnt += cnt
                        message = f"{df_holdingList['MS-CODE'][i]} {int(totalCnt)}대 {df_holdingList['REMARK'][i]}"
                    if len(message) > 0:
                        self.spReturnWarning.emit(message)
                # 최대착공량만큼 착공 못했을 경우, 메시지 출력
                if math.floor(dict_categoryCnt['모듈']) > 0:
                    self.spReturnWarning.emit(f'아직 착공하지 못한 특수(모듈)이 [{math.floor(dict_categoryCnt["모듈"])}대] 남았습니다. 최대 생산대수 설정을 확인해주세요.')
                if math.floor(dict_categoryCnt['비모듈']) > 0:
                    self.spReturnWarning.emit(f'아직 착공하지 못한 특수(비모듈)이 [{math.floor(dict_categoryCnt["비모듈"])}대] 남았습니다. 레벨링 리스트 파일 혹은 최대 생산대수 설정을 확인해주세요.')
                # 레벨링 리스트와 병합
                df_addSmtAssy = df_addSmtAssy.astype({'Linkage Number': 'str'})
                df_levelingSp = df_levelingSp.astype({'Linkage Number': 'str'})
                df_mergeOrder = pd.merge(df_addSmtAssy, df_levelingSp, on='Linkage Number', how='left')
                progress += round(maxPb / 20)
                self.spReturnPb.emit(progress)
                if self.isDebug:
                    df_mergeOrder.to_excel('.\\debug\\Sp\\flow14.xlsx')
                df_mergeOrderResult = pd.DataFrame().reindex_like(df_mergeOrder)
                df_mergeOrderResult = df_mergeOrderResult[0:0]
                # 총착공량 만큼 개별화
                for i in df_addSmtAssy.index:
                    for j in df_mergeOrder.index:
                        if df_addSmtAssy['Linkage Number'][i] == df_mergeOrder['Linkage Number'][j]:
                            if j > 0:
                                if df_mergeOrder['Linkage Number'][j] != df_mergeOrder['Linkage Number'][j - 1]:
                                    orderCnt = int(df_addSmtAssy['총착공량'][i])
                            else:
                                orderCnt = int(df_addSmtAssy['총착공량'][i])
                            if orderCnt > 0:
                                df_mergeOrderResult = df_mergeOrderResult.append(df_mergeOrder.iloc[j])
                                orderCnt -= 1
                # 사이클링을 위해 검사설비별로 정리
                df_mergeOrderResult = df_mergeOrderResult.sort_values(by=['대표모델'], ascending=[False])
                df_mergeOrderResult = df_mergeOrderResult.reset_index(drop=True)
                progress += round(maxPb / 20)
                self.spReturnPb.emit(progress)
                if self.isDebug:
                    df_mergeOrderResult.to_excel('.\\debug\\Sp\\flow15.xlsx')
                # 긴급오더 제외하고 사이클 대상만 식별하여 검사장치별로 갯수 체크
                if len(df_mergeOrderResult) > 0:
                    df_unCt = df_mergeOrderResult[df_mergeOrderResult['MS Code'].str.contains('/CT')]
                    df_mergeOrderResult = df_mergeOrderResult[~df_mergeOrderResult['MS Code'].str.contains('/CT')]
                    df_cycleCopy = df_mergeOrderResult[df_mergeOrderResult['긴급오더'].isnull()]
                    df_cycleCopy['대표모델'] = df_cycleCopy['MS Code'].str[:9]
                    df_cycleCopy['대표모델Cnt'] = df_cycleCopy.groupby('대표모델')['대표모델'].transform('size')
                    df_cycleCopy = df_cycleCopy.sort_values(by=['대표모델Cnt'], ascending=[False])
                    df_cycleCopy = df_cycleCopy.reset_index(drop=True)
                    # 긴급오더 포함한 Df와 병합
                    df_mergeOrderResult = pd.merge(df_mergeOrderResult, df_cycleCopy[['Planned Order', '대표모델Cnt']], on='Planned Order', how='left')
                    df_mergeOrderResult = df_mergeOrderResult.sort_values(by=['대표모델Cnt'], ascending=[False])
                    df_mergeOrderResult = df_mergeOrderResult.reset_index(drop=True)
                    progress += round(maxPb / 20)
                    self.spReturnPb.emit(progress)
                    if self.isDebug:
                        df_mergeOrderResult.to_excel('.\\debug\\Sp\\flow15-1.xlsx')
                    df_module = df_mergeOrderResult[df_mergeOrderResult['모듈 구분_x'] == '모듈']
                    df_module = df_module[~df_module['MODEL'].str.contains('TAH')]
                    df_module = df_module.reset_index(drop=True)
                    if self.cb_round == '1차':
                        df_slave = df_mergeOrderResult[df_mergeOrderResult['MODEL'].str.contains('TAH')]
                        df_slave = df_slave.reset_index(drop=True)
                    if self.cb_round == '2차':
                        df_BL = df_mergeOrderResult[df_mergeOrderResult['MODEL'].str.contains('F3BL00')]
                        df_BL = df_BL.reset_index(drop=True)
                        df_terminal = df_mergeOrderResult[df_mergeOrderResult['MODEL'].str.contains('RK|TA40')]
                        df_terminal = df_terminal.reset_index(drop=True)
                    # 최대 사이클 번호 체크
                    maxCycle = float(df_cycleCopy['대표모델Cnt'][0])
                    cycleGr = 1.0
                    df_module['사이클그룹'] = 0
                    # 각 검사장치별로 사이클 그룹을 작성하고, 최대 사이클과 비교하여 각 사이클그룹에서 배수처리
                    for i in df_module.index:
                        if df_module['긴급오더'][i] != '대상':
                            multiCnt = maxCycle / df_module['대표모델Cnt'][i]
                            if i == 0:
                                df_module['사이클그룹'][i] = cycleGr
                            else:
                                if df_module['대표모델'][i] != df_module['대표모델'][i - 1]:
                                    if i == 1:
                                        cycleGr = 2.0
                                    else:
                                        cycleGr = 1.0
                                df_module['사이클그룹'][i] = cycleGr * multiCnt
                            cycleGr += 1.0
                        if cycleGr >= maxCycle:
                            cycleGr = 1.0
                    # 배정된 사이클 그룹 순으로 정렬
                    df_module = df_module.sort_values(by=['사이클그룹'], ascending=[True])
                    df_module = df_module.reset_index(drop=True)
                    progress += round(maxPb / 20)
                    self.spReturnPb.emit(progress)
                    if self.isDebug:
                        df_module.to_excel('.\\debug\\Sp\\flow16.xlsx')
                    df_module = df_module.reset_index()
                    for i in df_module.index:
                        if df_module['긴급오더'][i] != '대상':
                            if (i != 0 and (df_module['대표모델'][i] == df_module['대표모델'][i - 1])):
                                for j in df_module.index:
                                    if df_module['긴급오더'][j] != '대상':
                                        if ((j != 0 and j < len(df_module) - 1) and (df_module['대표모델'][i] != df_module['대표모델'][j + 1]) and (df_module['대표모델'][i] != df_module['대표모델'][j])):
                                            df_module['index'][i] = ((float(df_module['index'][j]) + float(df_module['index'][j + 1])) / 2)
                                            df_module = df_module.sort_values(by=['index'], ascending=[True])
                                            df_module = df_module.reset_index(drop=True)
                                            break
                    if self.isDebug:
                        df_module.to_excel('.\\debug\\Sp\\flow16-1.xlsx')
                    df_unCt['index'] = 0
                    df_unCt['사이클그룹'] = 0
                    # CT사양과 병합
                    df_module = pd.concat([df_unCt, df_module])
                    df_module = df_module.reset_index(drop=True)
                    df_module['No (*)'] = int(maxNoModule) + (df_module.index.astype(int) + 1) * 10
                    # 1차 착공일 경우, 슬레이브에 대한 결과를 만들고 병합
                    if self.cb_round == '1차':
                        if len(df_slave) > 0:
                            df_slave = df_slave.reset_index(drop=True)
                            df_slave['No (*)'] = int(maxNoSlave) + (df_slave.index.astype(int) + 1) * 10
                            if self.isDebug:
                                df_slave.to_excel('.\\debug\\Sp\\df_slave.xlsx')
                        df_mergeOrderResult = pd.concat([df_module, df_slave])
                    # 2차 착공일 경우, 베이스와 터미널에 대한 결과를 만들고 병합
                    if self.cb_round == '2차':
                        if len(df_BL) > 0:
                            df_BL = df_BL.reset_index(drop=True)
                            df_BL['No (*)'] = int(maxNoBL) + (df_BL.index.astype(int) + 1) * 10
                        if len(df_terminal) > 0:
                            df_terminal = df_terminal.sort_values(by=['MODEL'], ascending=[True])
                            df_terminal = df_terminal.reset_index(drop=True)
                            df_terminal['No (*)'] = int(maxNoTerminal) + (df_terminal.index.astype(int) + 1) * 10
                        if self.isDebug:
                            df_BL.to_excel('.\\debug\\Sp\\df_BL.xlsx')
                            df_terminal.to_excel('.\\debug\\Sp\\df_terminal.xlsx')
                        df_mergeOrderResult = pd.concat([df_module, df_BL, df_terminal])
                    df_mergeOrderResult = df_mergeOrderResult.reset_index(drop=True)
                    progress += round(maxPb / 20)
                    self.spReturnPb.emit(progress)
                    if self.isDebug:
                        df_mergeOrderResult.to_excel('.\\debug\\Sp\\flow17.xlsx')
                    # df_mergeOrderResult['No (*)'] = (df_mergeOrderResult.index.astype(int) + 1) * 10
                    df_mergeOrderResult['Scheduled Start Date (*)'] = self.constDate
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
                    dict_emgLinkage = {}
                    dict_emgMscode = {}
                    for i in df_emgLinkage.index:
                        if len(df_mergeOrderResult[df_mergeOrderResult['Linkage Number'] == df_emgLinkage['Linkage Number'][i]]['Linkage Number'].values) > 0:
                            dict_emgLinkage[df_emgLinkage['Linkage Number'][i]] = True
                        else:
                            dict_emgLinkage[df_emgLinkage['Linkage Number'][i]] = False
                    for i in df_emgmscode.index:
                        if len(df_mergeOrderResult[df_mergeOrderResult['MS Code'] == df_emgmscode['MS-CODE'][i]]['MS Code'].values) > 0:
                            dict_emgMscode[df_emgLinkage['MS-CODE'][i]] = True
                        else:
                            dict_emgMscode[df_emgLinkage['MS-CODE'][i]] = False

                    self.spReturnEmgLinkage.emit(dict_emgLinkage)
                    self.spReturnEmgMscode.emit(dict_emgMscode)
                    progress += round(maxPb / 20)
                    self.spReturnPb.emit(progress)
                    if not os.path.exists(f'.\\Output\\Result\\{str(today)}'):
                        os.makedirs(f'.\\Output\\Result\\{str(today)}')
                    if not os.path.exists(f'.\\Output\\Result\\{str(today)}\\{self.cb_round}'):
                        os.makedirs(f'.\\Output\\Result\\{str(today)}\\{self.cb_round}')
                    outputFile = f'.\\Output\\Result\\{str(today)}\\{self.cb_round}\\{str(today)}_Sp.xlsx'
                    df_mergeOrderResult.to_excel(outputFile, index=False)
                else:
                    self.spReturnPb.emit(maxPb)
                self.spReturnEnd.emit(True)
                return
            else:
                self.spReturnPb.emit(maxPb)
                self.spReturnEnd.emit(True)
                return
        except Exception as e:
            self.spReturnError.emit(e)
            return


class CustomFormatter(logging.Formatter):
    FORMATS = {logging.ERROR: ('[%(asctime)s] %(levelname)s:%(message)s', 'yellow'),
                logging.DEBUG: ('[%(asctime)s] %(levelname)s:%(message)s', 'white'),
                logging.INFO: ('[%(asctime)s] %(levelname)s:%(message)s', 'white'),
                logging.WARNING: ('[%(asctime)s] %(levelname)s:%(message)s', 'yellow')}

    def format(self, record):
        last_fmt = self._style._fmt
        opt = CustomFormatter.FORMATS.get(record.levelno)
        if opt:
            fmt, color = opt
            self._style._fmt = "<font color=\"{}\">{}</font>".format(QtGui.QColor(color).name(), fmt)
        res = logging.Formatter.format(self, record)
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
        sizePolicy = QSizePolicy(QSizePolicy.Ignored, QSizePolicy.Fixed)
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
        self.setGeometry(500, 500, 500, 400)
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

    def __init__(self, linkageList, msCodeList):
        super().__init__()
        self.linkageList = linkageList
        self.msCodeList = msCodeList
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
        self.linkageInputBtn.setMinimumSize(QSize(0, 25))
        self.gridLayout3.addWidget(self.linkageInputBtn, 0, 4, 1, 2)
        self.linkageAddExcelBtn = QPushButton(self.groupBox)
        self.linkageAddExcelBtn.setMinimumSize(QSize(0, 25))
        self.gridLayout3.addWidget(self.linkageAddExcelBtn, 0, 6, 1, 2)
        self.mscodeInput = QLineEdit(self.groupBox)
        self.mscodeInput.setMinimumSize(QSize(0, 25))
        self.mscodeInput.setObjectName('mscodeInput')
        self.mscodeInputBtn = QPushButton(self.groupBox)
        self.mscodeInputBtn.setMinimumSize(QSize(0, 25))
        self.gridLayout3.addWidget(self.mscodeInput, 1, 1, 1, 3)
        self.gridLayout3.addWidget(self.mscodeInputBtn, 1, 4, 1, 2)
        self.mscodeAddExcelBtn = QPushButton(self.groupBox)
        self.mscodeAddExcelBtn.setMinimumSize(QSize(0, 25))
        self.gridLayout3.addWidget(self.mscodeAddExcelBtn, 1, 6, 1, 2)
        sizePolicy = QSizePolicy(QSizePolicy.Ignored, QSizePolicy.Fixed)
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
        self.label.setAlignment(Qt.AlignRight | Qt.AlignTrailing | Qt.AlignVCenter)
        self.label.setObjectName('label')
        self.gridLayout3.addWidget(self.label, 0, 0, 1, 1)
        self.label2 = QLabel(self.groupBox)
        self.label2.setAlignment(Qt.AlignRight | Qt.AlignTrailing | Qt.AlignVCenter)
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
        self.label3.setAlignment(Qt.AlignLeft | Qt.AlignTrailing | Qt.AlignVCenter)
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
        self.label4.setAlignment(Qt.AlignLeft | Qt.AlignTrailing | Qt.AlignVCenter)
        self.label4.setObjectName('label4')
        self.gridLayout5.addWidget(self.label4, 0, 2, 1, 1)
        self.label5 = QLabel(self.groupBox2)
        self.label5.setAlignment(Qt.AlignLeft | Qt.AlignTrailing | Qt.AlignVCenter)
        self.label5.setObjectName('label5')
        self.gridLayout5.addWidget(self.label5, 0, 3, 1, 1)
        self.linkageDelBtn = QPushButton(self.groupBox2)
        self.linkageDelBtn.setMinimumSize(QSize(0, 25))
        self.gridLayout5.addWidget(self.linkageDelBtn, 2, 0, 1, 1)
        self.mscodeDelBtn = QPushButton(self.groupBox2)
        self.mscodeDelBtn.setMinimumSize(QSize(0, 25))
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
        self.submitBtn.setText(_translate('SubWindow', '추가 완료'))
        self.label3.setText(_translate('SubWindow', 'Linkage No List'))
        self.label4.setText(_translate('SubWindow', 'MS-Code List'))
        self.linkageDelBtn.setText(_translate('SubWindow', '삭제'))
        self.mscodeDelBtn.setText(_translate('SubWindow', '삭제'))
        self.linkageAddExcelBtn.setText(_translate('SubWindow', '엑셀 입력'))
        self.mscodeAddExcelBtn.setText(_translate('SubWindow', '엑셀 입력'))
        if len(self.linkageList) > 0:
            self.loadLinkage(self.linkageList)
        if len(self.msCodeList) > 0:
            self.loadMsCode(self.msCodeList)

    # Linkage리스트를 불러오는 함수
    def loadLinkage(self, linkageList):
        for linkageNo in linkageList:
            if linkageNo.isdigit():
                model = self.listViewLinkage.model()
                linkageItem = QStandardItem()
                linkageItemModel = QStandardItemModel()
                dupFlag = False
                for i in range(model.rowCount()):
                    index = model.index(i, 0)
                    item = model.data(index)
                    if item == linkageNo:
                        dupFlag = True
                    linkageItem = QStandardItem(item)
                    linkageItemModel.appendRow(linkageItem)
                if not dupFlag:
                    linkageItem = QStandardItem(linkageNo)
                    linkageItemModel.appendRow(linkageItem)
                    self.listViewLinkage.setModel(linkageItemModel)

    # MsCode리스트를 불러오는 함수
    def loadMsCode(self, msCodeList):
        for mscode in msCodeList:
            if len(mscode) > 0:
                model = self.listViewmscode.model()
                mscodeItem = QStandardItem()
                mscodeItemModel = QStandardItemModel()
                dupFlag = False
                for i in range(model.rowCount()):
                    index = model.index(i, 0)
                    item = model.data(index)
                    if item == mscode:
                        dupFlag = True
                    mscodeItem = QStandardItem(item)
                    mscodeItemModel.appendRow(mscodeItem)
                if not dupFlag:
                    mscodeItem = QStandardItem(mscode)
                    mscodeItemModel.appendRow(mscodeItem)
                    self.listViewmscode.setModel(mscodeItemModel)

    # 리스트뷰에 LinakgeNo를 추가하는 함수
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
                    index = model.index(i, 0)
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

    # 리스트뷰에서 LinakgeNo를 삭제하는 함수
    @pyqtSlot()
    def delLinkage(self):
        model = self.listViewLinkage.model()
        linkageItem = QStandardItem()
        linkageItemModel = QStandardItemModel()
        for index in self.listViewLinkage.selectedIndexes():
            selected_item = self.listViewLinkage.model().data(index)
            for i in range(model.rowCount()):
                index = model.index(i, 0)
                item = model.data(index)
                linkageItem = QStandardItem(item)
                if selected_item != item:
                    linkageItemModel.appendRow(linkageItem)
            self.listViewLinkage.setModel(linkageItemModel)

    # 리스트뷰에 MSCode를 추가하는 함수
    @pyqtSlot()
    def addmscode(self):
        mscode = self.mscodeInput.text().replace(' ', '')
        if len(mscode) > 0:
            model = self.listViewmscode.model()
            mscodeItem = QStandardItem()
            mscodeItemModel = QStandardItemModel()
            dupFlag = False
            for i in range(model.rowCount()):
                index = model.index(i, 0)
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

    # 리스트뷰에서 MSCode를 삭제하는 함수
    @pyqtSlot()
    def delmscode(self):
        model = self.listViewmscode.model()
        mscodeItem = QStandardItem()
        mscodeItemModel = QStandardItemModel()
        for index in self.listViewmscode.selectedIndexes():
            selected_item = self.listViewmscode.model().data(index)
            for i in range(model.rowCount()):
                index = model.index(i, 0)
                item = model.data(index)
                mscodeItem = QStandardItem(item)
                if selected_item != item:
                    mscodeItemModel.appendRow(mscodeItem)
            self.listViewmscode.setModel(mscodeItemModel)

    # 리스트뷰에 LinakgeNo를 엑셀파일 형식으로 추가하는 함수
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
                                index = model.index(i, 0)
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

    # 리스트뷰에 MSCode를 엑셀파일 형식으로 추가하는 함수
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
                            index = model.index(i, 0)
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

    # 메인윈도우에 리스트뷰 전달
    @pyqtSlot()
    def confirm(self):
        self.submitClicked.emit([self.listViewLinkage.model(), self.listViewmscode.model()])
        self.close()


class Ui_MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setupUi()

    def setupUi(self):
        rfh = RotatingFileHandler(filename='./Log.log', mode='a', maxBytes=5 * 1024 * 1024, backupCount=2, encoding=None, delay=0)
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s:%(levelname)s:%(message)s', datefmt='%m/%d/%Y %H:%M:%S', handlers=[rfh])
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
        self.label_round = QLabel(self.groupBox)
        self.label_round.setAlignment(Qt.AlignRight | Qt.AlignTrailing | Qt.AlignVCenter)
        self.label_round.setObjectName('label4')
        self.gridLayout3.addWidget(self.label_round, 0, 0, 1, 1)
        self.mainOrderinput = QLineEdit(self.groupBox)
        self.mainOrderinput.setMinimumSize(QSize(0, 25))
        self.mainOrderinput.setObjectName('mainOrderinput')
        self.mainOrderinput.setValidator(QIntValidator(self))
        self.gridLayout3.addWidget(self.mainOrderinput, 1, 1, 1, 1)
        self.spModuleOrderinput = QLineEdit(self.groupBox)
        self.spModuleOrderinput.setMinimumSize(QSize(0, 25))
        self.spModuleOrderinput.setObjectName('spModuleOrderinput')
        self.spModuleOrderinput.setValidator(QIntValidator(self))
        self.gridLayout3.addWidget(self.spModuleOrderinput, 2, 1, 1, 1)
        self.spNonModuleOrderinput = QLineEdit(self.groupBox)
        self.spNonModuleOrderinput.setMinimumSize(QSize(0, 25))
        self.spNonModuleOrderinput.setObjectName('spModuleOrderinput')
        self.spNonModuleOrderinput.setValidator(QIntValidator(self))
        self.gridLayout3.addWidget(self.spNonModuleOrderinput, 3, 1, 1, 1)
        self.powerOrderinput = QLineEdit(self.groupBox)
        self.powerOrderinput.setMinimumSize(QSize(0, 25))
        self.powerOrderinput.setObjectName('powerOrderinput')
        self.powerOrderinput.setValidator(QIntValidator(self))
        self.gridLayout3.addWidget(self.powerOrderinput, 4, 1, 1, 1)
        self.dateBtn = QToolButton(self.groupBox)
        self.dateBtn.setMinimumSize(QSize(0, 25))
        self.dateBtn.setObjectName('dateBtn')
        self.gridLayout3.addWidget(self.dateBtn, 5, 1, 1, 1)
        self.emgFileInputBtn = QPushButton(self.groupBox)
        self.emgFileInputBtn.setMinimumSize(QSize(0, 25))
        self.gridLayout3.addWidget(self.emgFileInputBtn, 6, 1, 1, 1)
        self.holdFileInputBtn = QPushButton(self.groupBox)
        self.holdFileInputBtn.setMinimumSize(QSize(0, 25))
        self.gridLayout3.addWidget(self.holdFileInputBtn, 9, 1, 1, 1)
        self.label4 = QLabel(self.groupBox)
        self.label4.setAlignment(Qt.AlignRight | Qt.AlignTrailing | Qt.AlignVCenter)
        self.label4.setObjectName('label4')
        self.gridLayout3.addWidget(self.label4, 7, 1, 1, 1)
        self.label5 = QLabel(self.groupBox)
        self.label5.setAlignment(Qt.AlignRight | Qt.AlignTrailing | Qt.AlignVCenter)
        self.label5.setObjectName('label5')
        self.gridLayout3.addWidget(self.label5, 7, 2, 1, 1)
        self.label6 = QLabel(self.groupBox)
        self.label6.setAlignment(Qt.AlignRight | Qt.AlignTrailing | Qt.AlignVCenter)
        self.label6.setObjectName('label6')
        self.gridLayout3.addWidget(self.label6, 10, 1, 1, 1)
        self.label7 = QLabel(self.groupBox)
        self.label7.setAlignment(Qt.AlignRight | Qt.AlignTrailing | Qt.AlignVCenter)
        self.label7.setObjectName('label7')
        self.gridLayout3.addWidget(self.label7, 10, 2, 1, 1)
        listViewModelEmgLinkage = QStandardItemModel()
        self.listViewEmgLinkage = QListView(self.groupBox)
        self.listViewEmgLinkage.setModel(listViewModelEmgLinkage)
        self.gridLayout3.addWidget(self.listViewEmgLinkage, 8, 1, 1, 1)
        listViewModelEmgmscode = QStandardItemModel()
        self.listViewEmgmscode = QListView(self.groupBox)
        self.listViewEmgmscode.setModel(listViewModelEmgmscode)
        self.gridLayout3.addWidget(self.listViewEmgmscode, 8, 2, 1, 1)
        listViewModelHoldLinkage = QStandardItemModel()
        self.listViewHoldLinkage = QListView(self.groupBox)
        self.listViewHoldLinkage.setModel(listViewModelHoldLinkage)
        self.gridLayout3.addWidget(self.listViewHoldLinkage, 11, 1, 1, 1)
        listViewModelHoldmscode = QStandardItemModel()
        self.listViewHoldmscode = QListView(self.groupBox)
        self.listViewHoldmscode.setModel(listViewModelHoldmscode)
        self.gridLayout3.addWidget(self.listViewHoldmscode, 11, 2, 1, 1)
        self.labelBlank = QLabel(self.groupBox)
        self.labelBlank.setObjectName('labelBlank')
        self.gridLayout3.addWidget(self.labelBlank, 4, 4, 1, 1)
        self.progressbar_main = QProgressBar(self.groupBox)
        self.progressbar_main.setObjectName('progressbar_main')
        self.progressbar_main.setAlignment(Qt.AlignVCenter)
        self.progressbar_main.setFormat('메인라인 진행률')
        self.gridLayout3.addWidget(self.progressbar_main, 12, 1, 1, 2)
        self.progressbar_sp = QProgressBar(self.groupBox)
        self.progressbar_sp.setObjectName('progressbar_sp')
        self.progressbar_sp.setAlignment(Qt.AlignVCenter)
        self.progressbar_sp.setFormat('특수라인 진행률')
        self.gridLayout3.addWidget(self.progressbar_sp, 13, 1, 1, 2)
        self.progressbar_power = QProgressBar(self.groupBox)
        self.progressbar_power.setObjectName('progressbar_power')
        self.progressbar_power.setAlignment(Qt.AlignVCenter)
        self.progressbar_power.setFormat('전원라인 진행률')
        self.gridLayout3.addWidget(self.progressbar_power, 14, 1, 1, 2)
        self.runBtn = QToolButton(self.groupBox)
        sizePolicy = QSizePolicy(QSizePolicy.Ignored, QSizePolicy.Fixed)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(self.runBtn.sizePolicy().hasHeightForWidth())
        self.runBtn.setSizePolicy(sizePolicy)
        self.runBtn.setMinimumSize(QSize(30, 35))
        self.runBtn.setStyleSheet('background-color: rgb(63, 63, 63);\ncolor: rgb(255, 255, 255);')
        self.runBtn.setObjectName('runBtn')
        self.gridLayout3.addWidget(self.runBtn, 16, 3, 1, 2)
        self.label = QLabel(self.groupBox)
        self.label.setAlignment(Qt.AlignRight | Qt.AlignTrailing | Qt.AlignVCenter)
        self.label.setObjectName('label')
        self.gridLayout3.addWidget(self.label, 1, 0, 1, 1)
        self.label9 = QLabel(self.groupBox)
        self.label9.setAlignment(Qt.AlignRight | Qt.AlignTrailing | Qt.AlignVCenter)
        self.label9.setObjectName('label9')
        self.gridLayout3.addWidget(self.label9, 2, 0, 1, 1)
        self.label10 = QLabel(self.groupBox)
        self.label10.setAlignment(Qt.AlignRight | Qt.AlignTrailing | Qt.AlignVCenter)
        self.label10.setObjectName('label10')
        self.gridLayout3.addWidget(self.label10, 4, 0, 1, 1)
        self.label19 = QLabel(self.groupBox)
        self.label19.setAlignment(Qt.AlignRight | Qt.AlignTrailing | Qt.AlignVCenter)
        self.label19.setObjectName('label19')
        self.gridLayout3.addWidget(self.label19, 3, 0, 1, 1)
        self.label12 = QLabel(self.groupBox)
        self.label12.setAlignment(Qt.AlignRight | Qt.AlignTrailing | Qt.AlignVCenter)
        self.label12.setObjectName('label12')
        self.gridLayout3.addWidget(self.label12, 2, 2, 1, 1)
        self.label13 = QLabel(self.groupBox)
        self.label13.setAlignment(Qt.AlignRight | Qt.AlignTrailing | Qt.AlignVCenter)
        self.label13.setObjectName('label13')
        self.gridLayout3.addWidget(self.label13, 3, 2, 1, 1)
        self.label8 = QLabel(self.groupBox)
        self.label8.setAlignment(Qt.AlignRight | Qt.AlignTrailing | Qt.AlignVCenter)
        self.label8.setObjectName('label8')
        self.gridLayout3.addWidget(self.label8, 5, 0, 1, 1)
        self.labelDate = QLabel(self.groupBox)
        self.labelDate.setAlignment(Qt.AlignRight | Qt.AlignTrailing | Qt.AlignVCenter)
        self.labelDate.setObjectName('labelDate')
        self.gridLayout3.addWidget(self.labelDate, 5, 2, 1, 1)
        self.label2 = QLabel(self.groupBox)
        self.label2.setAlignment(Qt.AlignRight | Qt.AlignTrailing | Qt.AlignVCenter)
        self.label2.setObjectName('label2')
        self.gridLayout3.addWidget(self.label2, 6, 0, 1, 1)
        self.label3 = QLabel(self.groupBox)
        self.label3.setAlignment(Qt.AlignRight | Qt.AlignTrailing | Qt.AlignVCenter)
        self.label3.setObjectName('label3')
        self.gridLayout3.addWidget(self.label3, 9, 0, 1, 1)
        self.line = QFrame(self.groupBox)
        self.line.setFrameShape(QFrame.HLine)
        self.line.setFrameShadow(QFrame.Sunken)
        self.line.setObjectName('line')
        self.cb_round = QComboBox(self.groupBox)
        self.gridLayout3.addWidget(self.cb_round, 0, 1, 1, 1)
        self.gridLayout3.addWidget(self.line, 15, 0, 1, 10)
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
        # 디버그용 플래그
        self.isDebug = False
        self.isFileReady = False
        self.MaxOrderInputFilePath = r'.\\1차_착공량입력.xlsx'
        self.etcOrderInputFilePath = r'.\\2차_착공량입력.xlsx'
        self.df_etcOrderInput = self.readMaxOrderFile()
        self.cb_round.currentTextChanged.connect(self.readMaxOrderFile)
        self.dict_mainEmgLinkage = {}
        self.dict_mainEmgMsCode = {}
        self.dict_powerEmgLinkage = {}
        self.dict_powerEmgMsCode = {}
        self.dict_spEmgLinkage = {}
        self.dict_spEmgMsCode = {}
        self.isMainEnd = False
        self.isPowerEnd = False
        self.isSpEnd = False
        if self.isDebug:
            self.date = QLineEdit(self.groupBox)
            self.date.setObjectName('date')
            self.gridLayout3.addWidget(self.date, 12, 0, 1, 1)
            self.date.setPlaceholderText('디버그용 날짜입력')
        self.thread = QThread()
        self.thread.setTerminationEnabled(True)
        self.thread2 = QThread()
        self.thread2.setTerminationEnabled(True)
        self.thread3 = QThread()
        self.thread3.setTerminationEnabled(True)
        self.show()

    def retranslateUi(self, MainWindow):
        _translate = QCoreApplication.translate
        MainWindow.setWindowTitle(_translate('MainWindow', 'FA-M3 착공 평준화 자동화 프로그램 Rev0.13'))
        MainWindow.setWindowIcon(QIcon('.\\Logo\\logo.png'))
        self.label_round.setText(_translate('MainWindow', '착공 회차 선택:'))
        self.label.setText(_translate('MainWindow', '메인 생산대수:'))
        self.label9.setText(_translate('MainWindow', '특수(모듈) 생산대수:'))
        self.label19.setText(_translate('MainWindow', '특수(비모듈) 생산대수:'))
        self.label10.setText(_translate('MainWindow', '전원 생산대수:'))
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
        list_round = ['1차', '2차']
        self.cb_round.addItems(list_round)
        logging.info('프로그램이 정상 기동했습니다')

    # 최대착공량입력 파일 불러오기 함수
    def readMaxOrderFile(self):
        if self.cb_round.currentText() == "1차":
            self.MaxOrderInputFilePath = r'.\\1차_착공량입력.xlsx'
            self.etcOrderInputFilePath = r'.\\2차_착공량입력.xlsx'
        elif self.cb_round.currentText() == "2차":
            self.MaxOrderInputFilePath = r'.\\2차_착공량입력.xlsx'
            self.etcOrderInputFilePath = r'.\\1차_착공량입력.xlsx'
        if os.path.exists(self.MaxOrderInputFilePath):
            df_orderInput = pd.read_excel(self.MaxOrderInputFilePath)
            self.mainOrderinput.setText(str(df_orderInput['착공량'][0]))
            self.spModuleOrderinput.setText(str(df_orderInput['착공량'][1]))
            self.spNonModuleOrderinput.setText(str(df_orderInput['착공량'][2]))
            self.powerOrderinput.setText(str(df_orderInput['착공량'][3]))
        else:
            logging.error('%s 파일이 없습니다. 착공량을 수동으로 입력해주세요.', self.MaxOrderInputFilePath)
        if os.path.exists(self.etcOrderInputFilePath):
            df_etcOrderInput = pd.read_excel(self.etcOrderInputFilePath)
        else:
            df_etcOrderInput = pd.DataFrame()
            logging.error('%s 파일이 없습니다. 파일을 확인해주세요.', self.etcOrderInputFilePath)
        return df_etcOrderInput

    # 착공지정일 캘린더 호출
    def selectStartDate(self):
        self.w = CalendarWindow()
        self.w.submitClicked.connect(self.getStartDate)
        self.w.show()

    # 긴급오더 윈도우 호출
    @pyqtSlot()
    def emgWindow(self):
        list_emgHold = self.loadEmgHoldList()
        self.w = UISubWindow(list_emgHold[0], list_emgHold[1])
        self.w.submitClicked.connect(self.getEmgListview)
        self.w.show()

    # 홀딩오더 윈도우 호출
    @pyqtSlot()
    def holdWindow(self):
        list_emgHold = self.loadEmgHoldList()
        self.w = UISubWindow(list_emgHold[2], list_emgHold[3])
        self.w.submitClicked.connect(self.getHoldListview)
        self.w.show()

    # 긴급오더 리스트뷰 가져오기
    def getEmgListview(self, list):
        if len(list) > 0:
            self.listViewEmgLinkage.setModel(list[0])
            self.listViewEmgmscode.setModel(list[1])
            logging.info('긴급오더 리스트를 정상적으로 불러왔습니다.')
        else:
            logging.error('긴급오더 리스트가 없습니다. 다시 한번 확인해주세요')

    # 홀딩오더 리스트뷰 가져오기
    def getHoldListview(self, list):
        if len(list) > 0:
            self.listViewHoldLinkage.setModel(list[0])
            self.listViewHoldmscode.setModel(list[1])
            logging.info('홀딩오더 리스트를 정상적으로 불러왔습니다.')
        else:
            logging.error('홀딩오더 리스트가 없습니다. 다시 한번 확인해주세요')

    # 착공지정일 가져오기
    def getStartDate(self, date):
        if len(date) > 0:
            self.labelDate.setText(date)
            logging.info('착공지정일이 %s 로 정상적으로 지정되었습니다.', date)
        else:
            logging.error('착공지정일이 선택되지 않았습니다.')

    # 실행버튼 활성화
    def enableRunBtn(self):
        self.runBtn.setEnabled(True)
        self.runBtn.setText('실행')

    # 실행버튼 비활성화
    def disableRunBtn(self):
        self.runBtn.setEnabled(False)
        self.runBtn.setText('실행 중')

    # 메인라인 에러메시지 출력용 함수
    def mainShowError(self, str):
        logging.error(f'메인라인 에러 - {str}')
        self.enableRunBtn()
        self.progressbar_main.setValue(0)
        self.thread.quit()
        self.thread.wait()

    # 전원라인 에러메시지 출력용 함수
    def powerShowError(self, str):
        logging.warning(f'전원라인 에러 - {str}')
        self.enableRunBtn()
        self.progressbar_power.setValue(0)
        self.thread2.quit()
        self.thread2.wait()

    # 특수라인 에러메시지 출력용 함수
    def spShowError(self, str):
        logging.warning(f'특수라인 에러 - {str}')
        self.enableRunBtn()
        self.progressbar_sp.setValue(0)
        self.thread3.quit()
        self.thread3.wait()

    # 메인라인 경고메시지 출력용 함수
    def mainShowWarning(self, str):
        logging.warning(f'메인라인 경고 - {str}')

    # 전원라인 경고메시지 출력용 함수
    def powerShowWarning(self, str):
        logging.warning(f'전원라인 경고 - {str}')

    # 특수라인 경고메시지 출력용 함수
    def spShowWarning(self, str):
        logging.warning(f'특수라인 경고 - {str}')

    # 메인라인 쓰레드 종료용 함수
    def mainThreadEnd(self, isEnd):
        if isEnd:
            logging.info('메인라인 착공이 완료되었습니다.')
            self.thread.quit()
            self.thread.wait()
        self.isMainEnd = isEnd
        if self.isMainEnd and self.isPowerEnd and self.isSpEnd:
            self.enableRunBtn()
            for key in self.dict_mainEmgLinkage.keys():
                if not (self.dict_mainEmgLinkage[key] or self.dict_powerEmgLinkage[key] or self.dict_spEmgLinkage[key]):
                    logging.warning(f'긴급오더 대상 : {str(key)} 가 모든 라인의 결과물에 없습니다.')
            for key in self.dict_mainEmgMsCode.keys():
                if not (self.dict_mainEmgMsCode[key] or self.dict_mainEmgMsCode[key] or self.dict_mainEmgMsCode[key]):
                    logging.warning(f'긴급오더 대상 : {str(key)} 가 모든 라인의 결과물에 없습니다.')

    # 전원라인 쓰레드 종료용 함수
    def powerThreadEnd(self, isEnd):
        if isEnd:
            logging.info('전원라인 착공이 완료되었습니다.')
            self.thread2.quit()
            self.thread2.wait()
        self.isPowerEnd = isEnd
        if self.isMainEnd and self.isPowerEnd and self.isSpEnd:
            self.enableRunBtn()
            for key in self.dict_mainEmgLinkage.keys():
                if not (self.dict_mainEmgLinkage[key] or self.dict_powerEmgLinkage[key] or self.dict_spEmgLinkage[key]):
                    logging.warning(f'긴급오더 대상 : {str(key)} 가 모든 라인의 결과물에 없습니다.')
            for key in self.dict_mainEmgMsCode.keys():
                if not (self.dict_mainEmgMsCode[key] or self.dict_mainEmgMsCode[key] or self.dict_mainEmgMsCode[key]):
                    logging.warning(f'긴급오더 대상 : {str(key)} 가 모든 라인의 결과물에 없습니다.')

    # 특수라인 쓰레드 종료용 함수
    def spThreadEnd(self, isEnd):
        if isEnd:
            logging.info('특수라인 착공이 완료되었습니다.')
            self.thread3.quit()
            self.thread3.wait()
        self.isSpEnd = isEnd
        if self.isMainEnd and self.isPowerEnd and self.isSpEnd:
            self.enableRunBtn()
            for key in self.dict_mainEmgLinkage.keys():
                if not (self.dict_mainEmgLinkage[key] or self.dict_powerEmgLinkage[key] or self.dict_spEmgLinkage[key]):
                    logging.warning(f'긴급오더 대상 : {str(key)} 가 모든 라인의 결과물에 없습니다.')
            for key in self.dict_mainEmgMsCode.keys():
                if not (self.dict_mainEmgMsCode[key] or self.dict_mainEmgMsCode[key] or self.dict_mainEmgMsCode[key]):
                    logging.warning(f'긴급오더 대상 : {str(key)} 가 모든 라인의 결과물에 없습니다.')

    def setMainEmgLinkage(self, dict_input):
        self.dict_mainEmgLinkage = dict_input

    def setMainEmgMscode(self, dict_input):
        self.dict_mainEmgMsCode = dict_input

    def setPowerEmgLinkage(self, dict_input):
        self.dict_powerEmgLinkage = dict_input

    def setPowerEmgMscode(self, dict_input):
        self.dict_powerEmgMsCode = dict_input

    def setSpEmgLinkage(self, dict_input):
        self.dict_spEmgLinkage = dict_input

    def setSpEmgMscode(self, dict_input):
        self.dict_spEmgMsCode = dict_input

    # 메인라인 프로그레스바 범위 설정용 함수
    def setMainMaxPb(self, maxPb):
        self.progressbar_main.setRange(0, maxPb)

    # 전원라인 프로그레스바 범위 설정용 함수
    def setPowerMaxPb(self, maxPb):
        self.progressbar_power.setRange(0, maxPb)

    # 특수라인 프로그레스바 범위 설정용 함수
    def setSpMaxPb(self, maxPb):
        self.progressbar_sp.setRange(0, maxPb)

    # 마스터파일 불러오기 함수
    def loadMasterFile(self):
        self.isFileReady = True
        masterFileList = []
        date = datetime.datetime.today().strftime('%Y%m%d')
        if self.isDebug:
            date = self.date.text()
        roundTxt = self.cb_round.currentText()
        sosFilePath = r'.\\input\\Master_File\\' + date + '\\' + roundTxt + r'\\SOS2.xlsx'
        if float(self.mainOrderinput.text()) != 0.0:
            mainFilePath = r'.\\input\\Master_File\\' + date + '\\' + roundTxt + r'\\MAIN.xlsx'
        else:
            mainFilePath = r'.\\input\\Master_File\\' + date + '\\' + roundTxt + r'\\'
        if float(self.spModuleOrderinput.text()) != 0.0:
            spFilePath = r'.\\input\\Master_File\\' + date + '\\' + roundTxt + r'\\OTHER.xlsx'
        else:
            spFilePath = r'.\\input\\Master_File\\' + date + '\\' + roundTxt + r'\\'
        if float(self.powerOrderinput.text()) != 0.0:
            powerFilePath = r'.\\input\\Master_File\\' + date + '\\' + roundTxt + r'\\POWER.xlsx'
        else:
            powerFilePath = r'.\\input\\Master_File\\' + date + '\\' + roundTxt + r'\\'
        calendarFilePath = r'.\\Input\\Calendar_File\\FY' + date[2:4] + '_Calendar.xlsx'
        if os.path.exists(r'.\\input\\Master_File\\' + date + '\\' + roundTxt + r'\\100L1311(' + date[4:8] + ')MAIN_2차.xlsx'):
            secMainListFilePath = r'.\\input\\Master_File\\' + date + '\\' + roundTxt + r'\\100L1311(' + date[4:8] + ')MAIN_2차.xlsx'
        else:
            secMainListFilePath = r'.\\input\\Master_File\\' + date + '\\' + roundTxt + r'\\'
        powerCondFilePath = r'.\\input\\DB\\Power\\FAM3_Power_MST_Table.xlsx'
        spCondFilePath = r'.\\input\\DB\\Sp\\FAM3_Sp_MST_Table.xlsx'
        smtAssyUnCheckFilePath = r'.\\input\\DB\\SP\\SMT수량_비관리대상.xlsx'
        if float(self.spNonModuleOrderinput.text()) != 0.0:
            if os.path.exists(r'.\\input\\Master_File\\' + date + '\\' + roundTxt + r'\\BL.xlsx'):
                nonSpBLFilePath = r'.\\input\\Master_File\\' + date + '\\' + roundTxt + r'\\BL.xlsx'
            else:
                nonSpBLFilePath = r'.\\input\\Master_File\\' + date + r'\\'
        else:
            nonSpBLFilePath = r'.\\input\\Master_File\\' + date + '\\' + roundTxt + r'\\'
        if float(self.spNonModuleOrderinput.text()) != 0.0:
            if os.path.exists(r'.\\input\\Master_File\\' + date + '\\' + roundTxt + r'\\TERMINAL.xlsx'):
                nonSpTerminalFilePath = r'.\\input\\Master_File\\' + date + '\\' + roundTxt + r'\\TERMINAL.xlsx'
            else:
                nonSpTerminalFilePath = r'.\\input\\Master_File\\' + date + r'\\'
        else:
            nonSpTerminalFilePath = r'.\\input\\Master_File\\' + date + '\\' + roundTxt + r'\\'
        if float(self.spNonModuleOrderinput.text()) != 0.0:
            if os.path.exists(r'.\\input\\Master_File\\' + date + '\\' + roundTxt + r'\\SLAVE.xlsx'):
                SpSlaveFilePath = r'.\\input\\Master_File\\' + date + '\\' + roundTxt + r'\\SLAVE.xlsx'
            else:
                SpSlaveFilePath = r'.\\input\\Master_File\\' + date + r'\\'
        else:
            SpSlaveFilePath = r'.\\input\\Master_File\\' + date + '\\' + roundTxt + r'\\'
        mainAteCapaFilePath = r'.\\input\\DB\\Main\\' + roundTxt + '\\Main_ATE_Capacity_Table.xlsx'
        ctCondFilePath = r'.\\input\\DB\\CT\\FAM3_CT_MST_Table.xlsx'
        if os.path.exists(r'.\\input\\Master_File\\' + date + '\\' + roundTxt + r'\\100L1313(' + date[4:8] + ')POWER_2차.xlsx'):
            secPowerListFilePath = r'.\\input\\Master_File\\' + date + '\\' + roundTxt + r'\\100L1313(' + date[4:8] + ')POWER_2차.xlsx'
        else:
            secPowerListFilePath = r'.\\input\\Master_File\\' + date + '\\' + roundTxt + r'\\'
        if os.path.exists(r'.\\input\\Master_File\\' + date + '\\' + roundTxt + r'\\100L1304(' + date[4:8] + ')OTHER_2차.xlsx'):
            secSpListFilePath = r'.\\input\\Master_File\\' + date + '\\' + roundTxt + r'\\100L1304(' + date[4:8] + ')OTHER_2차.xlsx'
        else:
            secSpListFilePath = r'.\\input\\Master_File\\' + date + '\\' + roundTxt + r'\\'
        configFilePath = r'.\\Config.ini'
        holdingListFilePath = r'.\\input\\DB\\홀딩리스트.xlsx'
        # 위의 마스터파일 경로들을 리스트화
        pathList = [sosFilePath,
                    mainFilePath,
                    spFilePath,
                    powerFilePath,
                    calendarFilePath,
                    secMainListFilePath,
                    powerCondFilePath,
                    spCondFilePath,
                    smtAssyUnCheckFilePath,
                    nonSpBLFilePath,
                    nonSpTerminalFilePath,
                    SpSlaveFilePath,
                    mainAteCapaFilePath,
                    ctCondFilePath,
                    secPowerListFilePath,
                    secSpListFilePath,
                    configFilePath,
                    holdingListFilePath]
        # 각 경로의 파일들이 없는지 체크
        for path in pathList:
            if os.path.exists(path):
                file = glob.glob(path)[0]
                masterFileList.append(file)
            else:
                logging.error('%s 파일이 없습니다. 확인해주세요.', path)
                self.enableRunBtn()
                self.isFileReady = False
        if self.isFileReady:
            logging.info('마스터 파일 및 캘린더 파일을 정상적으로 불러왔습니다.')
        return masterFileList

    # 긴급/홀딩오더 리스트 불러오기
    def loadEmgHoldList(self):
        list_emgHold = []
        list_emgHold.append([str(self.listViewEmgLinkage.model().data(self.listViewEmgLinkage.model().index(x, 0))) for x in range(self.listViewEmgLinkage.model().rowCount())])
        list_emgHold.append([self.listViewEmgmscode.model().data(self.listViewEmgmscode.model().index(x, 0)) for x in range(self.listViewEmgmscode.model().rowCount())])
        list_emgHold.append([str(self.listViewHoldLinkage.model().data(self.listViewHoldLinkage.model().index(x, 0))) for x in range(self.listViewHoldLinkage.model().rowCount())])
        list_emgHold.append([self.listViewHoldmscode.model().data(self.listViewHoldmscode.model().index(x, 0)) for x in range(self.listViewHoldmscode.model().rowCount())])
        return list_emgHold

    # 특수라인 착공시작 함수
    def startSpLeveling(self, df):
        date = datetime.datetime.today().strftime('%Y%m%d')
        if self.isDebug:
            date = self.date.text()
        list_masterFile = self.loadMasterFile()
        list_emgHold = self.loadEmgHoldList()
        if self.isFileReady:
            if len(self.spModuleOrderinput.text()) > 0:
                if self.labelDate.text() != '미선택':
                    self.thread_sp = SpThread(self.isDebug,
                                                date,
                                                self.labelDate.text(),
                                                list_masterFile,
                                                float(self.spModuleOrderinput.text()),
                                                float(self.spNonModuleOrderinput.text()),
                                                list_emgHold,
                                                df,
                                                self.cb_round.currentText(),
                                                self.df_etcOrderInput)
                    self.thread_sp.moveToThread(self.thread3)
                    self.thread3.started.connect(self.thread_sp.run)
                    self.thread_sp.spReturnError.connect(self.spShowError)
                    self.thread_sp.spReturnEnd.connect(self.spThreadEnd)
                    self.thread_sp.spReturnWarning.connect(self.spShowWarning)
                    self.thread_sp.spReturnMaxPb.connect(self.setSpMaxPb)
                    self.thread_sp.spReturnPb.connect(self.progressbar_sp.setValue)
                    self.thread_sp.spReturnEmgLinkage.connect(self.setSpEmgLinkage)
                    self.thread_sp.spReturnEmgMscode.connect(self.setSpEmgMscode)
                    self.thread3.start()
                else:
                    self.enableRunBtn()
                    logging.info('착공지정일이 입력되지 않았습니다. 캘린더로부터 착공지정일을 선택해주세요.')
            else:
                self.enableRunBtn()
                logging.info('특수기종 착공량이 입력되지 않아 특수기종 착공은 미실시 됩니다.')

    # 메인/전원라인 착공시작 함수
    @pyqtSlot()
    def startLeveling(self):
        self.disableRunBtn()
        self.setSpMaxPb(200)
        self.progressbar_sp.setValue(0)
        date = datetime.datetime.today().strftime('%Y%m%d')
        if self.isDebug:
            date = self.date.text()
        list_masterFile = self.loadMasterFile()
        list_emgHold = self.loadEmgHoldList()
        if self.isFileReady:
            if len(self.mainOrderinput.text()) > 0:
                if self.labelDate.text() != '미선택':
                    self.thread_main = MainThread(self.isDebug,
                                                    date,
                                                    self.labelDate.text(),
                                                    list_masterFile,
                                                    float(self.mainOrderinput.text()),
                                                    list_emgHold,
                                                    self.cb_round.currentText(),
                                                    self.df_etcOrderInput)
                    self.thread_main.moveToThread(self.thread)
                    self.thread.started.connect(self.thread_main.run)
                    self.thread_main.mainReturnError.connect(self.mainShowError)
                    self.thread_main.mainReturnEnd.connect(self.mainThreadEnd)
                    self.thread_main.mainReturnWarning.connect(self.mainShowWarning)
                    self.thread_main.mainReturnDf.connect(self.startSpLeveling)
                    self.thread_main.mainReturnMaxPb.connect(self.setMainMaxPb)
                    self.thread_main.mainReturnPb.connect(self.progressbar_main.setValue)
                    self.thread_main.mainReturnEmgLinkage.connect(self.setMainEmgLinkage)
                    self.thread_main.mainReturnEmgMscode.connect(self.setMainEmgMscode)
                    self.thread.start()
                else:
                    self.enableRunBtn()
                    logging.info('착공지정일이 입력되지 않았습니다. 캘린더로부터 착공지정일을 선택해주세요.')
            else:
                logging.info('메인기종 착공량이 입력되지 않아 메인기종 착공은 미실시 됩니다.')
            if len(self.powerOrderinput.text()) > 0:
                if self.labelDate.text() != '미선택':
                    self.thread_power = PowerThread(self.isDebug,
                                                    date,
                                                    self.labelDate.text(),
                                                    list_masterFile,
                                                    float(self.powerOrderinput.text()),
                                                    list_emgHold,
                                                    self.cb_round.currentText(),
                                                    self.df_etcOrderInput)
                    self.thread_power.moveToThread(self.thread2)
                    self.thread2.started.connect(self.thread_power.run)
                    self.thread_power.powerReturnError.connect(self.powerShowError)
                    self.thread_power.powerReturnEnd.connect(self.powerThreadEnd)
                    self.thread_power.powerReturnWarning.connect(self.powerShowWarning)
                    self.thread_power.powerReturnMaxPb.connect(self.setPowerMaxPb)
                    self.thread_power.powerReturnPb.connect(self.progressbar_power.setValue)
                    self.thread_power.powerReturnEmgLinkage.connect(self.setPowerEmgLinkage)
                    self.thread_power.powerReturnEmgMscode.connect(self.setPowerEmgMscode)
                    self.thread2.start()
                else:
                    self.enableRunBtn()
                    logging.info('착공지정일이 입력되지 않았습니다. 캘린더로부터 착공지정일을 선택해주세요.')
            else:
                logging.info('전원기종 착공량이 입력되지 않아 전원기종 착공은 미실시 됩니다.')
        else:
            self.enableRunBtn()
            logging.warning('필수 파일이 없어 더 이상 진행할 수 없습니다.')


if __name__ == '__main__':
    import sys
    app = QtWidgets.QApplication(sys.argv)
    ui = Ui_MainWindow()
    sys.exit(app.exec_())
