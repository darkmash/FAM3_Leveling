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
from PyQt5 import QtCore, QtWidgets
from PyQt5.QtCore import Qt, QCoreApplication
from PyQt5.QtGui import QDoubleValidator, QStandardItemModel, QIcon, QStandardItem, QIntValidator, QFont
from PyQt5.QtWidgets import QMainWindow, QMessageBox, QProgressBar, QPlainTextEdit, QWidget, QGridLayout, QGroupBox, QLineEdit, QSizePolicy, QToolButton, QLabel, QFrame, QListView, QMenuBar, QStatusBar, QPushButton, QApplication, QCalendarWidget, QVBoxLayout, QFileDialog, QCheckBox
from PyQt5.QtCore import pyqtSlot, pyqtSignal, QObject, QThread, QRect, QSize, QDate
import pandas as pd
import cx_Oracle

class QTextEditLogger(logging.Handler):
    def __init__(self, parent):
        super().__init__()
        self.widget = QPlainTextEdit(parent)
        self.widget.setGeometry(QRect(10, 260, 661, 161))
        self.widget.setReadOnly(True)
        self.widget.setPlainText('')
        self.widget.setStyleSheet('background-color: rgb(53, 53, 53);\ncolor: rgb(255, 255, 255);')
        self.widget.setObjectName('logBrowser')
        font = QFont()
        font.setFamily('Nanum Gothic')
        font.setBold(False)
        font.setPointSize(9)
        self.widget.setFont(font)

    def emit(self, record):
        msg = self.format(record)
        self.widget.appendPlainText(msg)

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
        self.resize(613, 800)
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
        self.logBrowser.setFormatter(
                                    logging.Formatter('[%(asctime)s] %(levelname)s:%(message)s', 
                                                        datefmt='%Y-%m-%d %H:%M:%S')
                                    )
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
        if self.isDebug:
            self.debugDate = QLineEdit(self.groupBox)
            self.debugDate.setObjectName('debugDate')
            self.gridLayout3.addWidget(self.debugDate, 10, 0, 1, 1)
            self.debugDate.setPlaceholderText('디버그용 날짜입력')
        self.show()

    def retranslateUi(self, MainWindow):
        _translate = QCoreApplication.translate
        MainWindow.setWindowTitle(_translate('MainWindow', 'FA-M3 착공량 평준화 프로그램 Rev0.00'))
        MainWindow.setWindowIcon(QIcon('.\\Logo\\logo.png'))
        self.label.setText(_translate('MainWindow', '메인 생산대수:'))
        self.label9.setText(_translate('MainWindow', '특수 생산대수:'))
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

        try:
            self.df_productTime = self.loadProductTimeDb()
            # self.df_productTime.to_excel(r'.\result.xlsx')
        except Exception as e:
            logging.error('검사시간DB 불러오기에 실패했습니다. 관리자에게 문의해주세요.')
            logging.exception(e, exc_info=True)      
        try:
            self.df_smt = self.loadSmtDb
        except Exception as e:
            logging.error('SMT Assy 재고량 DB 불러오기에 실패했습니다. 관리자에게 문의해주세요.')
            logging.exception(e, exc_info=True)   

        logging.info('프로그램이 정상 기동했습니다')

    def loadProductTimeDb(self):
        location = r'.\\instantclient_21_6'
        os.environ["PATH"] = location + ";" + os.environ["PATH"]
        dsn = cx_Oracle.makedsn("ymzn-bdv19az029-rds.cgbtxsdj6fjy.ap-northeast-1.rds.amazonaws.com", 1521, "tprod")
        db = cx_Oracle.connect("TEST_SCM","test_scm", dsn)

        cursor= db.cursor()
        cursor.execute("SELECT MODEL, COMPONENT_SET, MAEDZUKE, MAUNT, LEAD_CUTTING, VISUAL_EXAMINATION, PICKUP, ASSAMBLY, M_FUNCTION_CHECK, A_FUNCTION_CHECK, PERSON_EXAMINE, INSPECTION_EQUIPMENT FROM FAM3_PRODUCT_TIME_TB")
        out_data = cursor.fetchall()
        df_productTime = pd.DataFrame(out_data)
        df_productTime.columns = ["MODEL", "COMPONENT_SET", "MAEDZUKE", "MAUNT", "LEAD_CUTTING", "VISUAL_EXAMINATION", "PICKUP", "ASSAMBLY", "M_FUNCTION_CHECK", "A_FUNCTION_CHECK", "PERSON_EXAMINE", "INSPECTION_EQUIPMENT"]
        return df_productTime

    def loadSmtDb(self):
        location = r'.\\instantclient_21_6'
        os.environ["PATH"] = location + ";" + os.environ["PATH"]
        dsn = cx_Oracle.makedsn("10.36.15.42", 1521, "NEURON")
        db = cx_Oracle.connect("ymi_user","ymi123!", dsn)

        cursor= db.cursor()
        cursor.execute("SELECT INV_D, PARTS_NO, CURRENT_INV_QTY FROM pdsg0040 where INV_D = TO_DATE(TO_CHAR(SYSDATE-1,'YYYYMMDD'),'YYYYMMDD')")
        out_data = cursor.fetchall()
        df_smt = pd.DataFrame(out_data)
        df_smt.columns = ["출력일", "PARTS NO", "TOTAL 재고"]
        return df_smt

    def selectStartDate(self):
        self.w = CalendarWindow()
        self.w.submitClicked.connect(self.getStartDate)
        self.w.show()
    
    @pyqtSlot()
    def emgWindow(self):
        self.w = UISubWindow()
        self.w.submitClicked.connect(self.getEmgListview)
        self.w.show()

    @pyqtSlot()
    def holdWindow(self):
        self.w = UISubWindow()
        self.w.submitClicked.connect(self.getHoldListview)
        self.w.show()

    def getEmgListview(self, list):
        if len(list) > 0 :
            self.listViewEmgLinkage.setModel(list[0])
            self.listViewEmgmscode.setModel(list[1])
            logging.info('긴급오더 리스트를 정상적으로 불러왔습니다.')
        else:
            logging.error('긴급오더 리스트가 없습니다. 다시 한번 확인해주세요')
    
    def getHoldListview(self, list):
        if len(list) > 0 :
            self.listViewHoldLinkage.setModel(list[0])
            self.listViewHoldmscode.setModel(list[1])
            logging.info('홀딩오더 리스트를 정상적으로 불러왔습니다.')
        else:
            logging.error('긴급오더 리스트가 없습니다. 다시 한번 확인해주세요')
    def updateProgressbar(self, val):
        self.progressbar.setValue(val)

    def selectStartDate(self):
        self.w = CalendarWindow()
        self.w.submitClicked.connect(self.getStartDate)
        self.w.show()
    
    def getStartDate(self, date):
        if len(date) > 0 :
            self.labelDate.setText(date)
            logging.info('착공지정일이 %s 로 정상적으로 지정되었습니다.', date)
        else:
            logging.error('착공지정일이 선택되지 않았습니다.')

    @pyqtSlot()
    def startLeveling(self):
        def loadMasterFile():
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

            if os.path.exists(secMainListFilePath):
                # if os.path.exists(usedSmtAssyFilePath):
                    if os.path.exists(smtAssyFilePath):
                        if os.path.exists(calendarFilePath):
                            if os.path.exists(powerFilePath):
                                if os.path.exists(spFilePath):
                                    if os.path.exists(mainFilePath):
                                        if os.path.exists(progressFilePath):
                                            if os.path.exists(sosFilePath):
                                                sosFile = glob.glob(sosFilePath)[0]
                                                progressFile = glob.glob(progressFilePath)[0]
                                                mainFile = glob.glob(mainFilePath)[0]
                                                spFile = glob.glob(spFilePath)[0]
                                                powerFile = glob.glob(powerFilePath)[0]
                                                calendarFile = glob.glob(calendarFilePath)[0]
                                                smtAssyFile = glob.glob(smtAssyFilePath)[0]
                                                # usedSmtAssyFile = glob.glob(usedSmtAssyFilePath)[0]
                                                secMainListFile = glob.glob(secMainListFilePath)[0]
                                                masterFileList.append(sosFile)
                                                masterFileList.append(progressFile)
                                                masterFileList.append(mainFile)
                                                masterFileList.append(spFile)
                                                masterFileList.append(powerFile)
                                                masterFileList.append(calendarFile)
                                                masterFileList.append(smtAssyFile)
                                                # masterFileList.append(usedSmtAssyFile)
                                                masterFileList.append(secMainListFile)

                                                logging.info('마스터 파일 및 캘린더 파일을 정상적으로 불러왔습니다.')
                                            else:
                                                logging.error('%s 파일이 없습니다. 확인해주세요.', sosFilePath)
                                                self.runBtn.setEnabled(True)
                                        else:
                                            logging.error('%s 파일이 없습니다. 확인해주세요.', progressFilePath)
                                            self.runBtn.setEnabled(True)                               
                                    else:
                                        logging.error('%s 파일이 없습니다. 확인해주세요.', mainFilePath)
                                        self.runBtn.setEnabled(True)
                                else:
                                    logging.error('%s 파일이 없습니다. 확인해주세요.', spFilePath)
                                    self.runBtn.setEnabled(True)
                            else:
                                logging.error('%s 파일이 없습니다. 확인해주세요.', powerFilePath)
                                self.runBtn.setEnabled(True)
                        else:
                            logging.error('%s 파일이 없습니다. 확인해주세요.', calendarFilePath)
                            self.runBtn.setEnabled(True)
                    else:
                        logging.error('%s 파일이 없습니다. 확인해주세요.', smtAssyFilePath)
                        self.runBtn.setEnabled(True)                
                # else:
                #     logging.error('%s 파일이 없습니다. 확인해주세요.', usedSmtAssyFilePath)
                #     self.runBtn.setEnabled(True)       
            else:
                logging.error('%s 파일이 없습니다. 확인해주세요.', secMainListFilePath)
                self.runBtn.setEnabled(True)                          
            return masterFileList
        
        def checkWorkDay(df, today, compDate):
            dtToday = pd.to_datetime(datetime.datetime.strptime(today, '%Y%m%d'))
            dtComp = pd.to_datetime(compDate, unit='s')
            workDay = 0
            for i in df.index:
                dt = pd.to_datetime(df['Date'][i], unit='s')
                if dtToday < dt and dt <= dtComp:
                    if df['WorkingDay'][i] == 1:
                        workDay += 1
            return workDay

        def delComma(value):
            return str(value).split('.')[0]

        def readDB(ip, port, sid, userName, password, sql):
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
        def getSec(time_str):
            time_str = re.sub(r'[^0-9:]', '', str(time_str))
            if len(time_str) > 0:
                h, m, s = time_str.split(':')
                return int(h) * 3600 + int(m) * 60 + int(s)
            else:
                return 0
        self.runBtn.setEnabled(False)   
        pd.set_option('mode.chained_assignment', None)

        try:
            list_masterFile = loadMasterFile()

            # mainOrderCnt = 0.0
            # spOrderCnt = 0.0
            # powerOrderCnt = 0.0

            # if len(self.mainOrderinput.text()) <= 0:
            #     logging.info('메인기종 착공량이 입력되지 않아 메인기종 착공은 미실시 됩니다.')
            # else:
            #     mainOrderCnt = float(self.mainOrderinput.text())
            # if len(self.spOrderinput.text()) <= 0:
            #     logging.info('특수기종 착공량이 입력되지 않아 특수기종 착공은 미실시 됩니다.')
            # else:
            #     spOrderCnt = float(self.spOrderinput.text())
            # if len(self.powerOrderinput.text()) <= 0:
            #     logging.info('전원기종 착공량이 입력되지 않아 전원기종 착공은 미실시 됩니다.')            
            # else:
            #     powerOrderCnt = float(self.powerOrderinput.text())

            # emgLinkage = [str(self.listViewEmgLinkage.model().data(self.listViewEmgLinkage.model().index(x,0))) for x in range(self.listViewEmgLinkage.model().rowCount())]
            # emgmscode = [self.listViewEmgmscode.model().data(self.listViewEmgmscode.model().index(x,0)) for x in range(self.listViewEmgmscode.model().rowCount())]
            # holdLinkage = [str(self.listViewHoldLinkage.model().data(self.listViewHoldLinkage.model().index(x,0))) for x in range(self.listViewHoldLinkage.model().rowCount())]
            # holdmscode = [self.listViewHoldmscode.model().data(self.listViewHoldmscode.model().index(x,0)) for x in range(self.listViewHoldmscode.model().rowCount())]        

            # df_emgLinkage = pd.DataFrame({'Linkage Number':emgLinkage})
            # df_emgmscode = pd.DataFrame({'MS Code':emgmscode})
            # df_holdLinkage = pd.DataFrame({'Linkage Number':holdLinkage})
            # df_holdmscode = pd.DataFrame({'MS Code':holdmscode})

            # df_emgLinkage['Linkage Number'] = df_emgLinkage['Linkage Number'].astype(np.int64)
            # df_holdLinkage['Linkage Number'] = df_holdLinkage['Linkage Number'].astype(np.int64)
            # df_emgLinkage['긴급오더'] = '대상'
            # df_emgmscode['긴급오더'] = '대상'
            # df_holdLinkage['홀딩오더'] = '대상'
            # df_holdmscode['홀딩오더'] = '대상'

            # df_levelingMain = pd.read_excel(list_masterFile[2])
            # df_levelingSp = pd.read_excel(list_masterFile[3])
            # df_levelingPower = pd.read_excel(list_masterFile[4])

            # df_levelingMainDropSEQ = df_levelingMain[df_levelingMain['Sequence No'].isnull()]
            # df_levelingMainUndepSeq = df_levelingMain[df_levelingMain['Sequence No']=='Undep']
            # df_levelingMainUncorSeq = df_levelingMain[df_levelingMain['Sequence No']=='Uncor']
            # df_levelingMain = pd.concat([df_levelingMainDropSEQ, df_levelingMainUndepSeq, df_levelingMainUncorSeq])
            # df_levelingMain = df_levelingMain.reset_index(drop=True)
            # df_levelingMain['미착공수량'] = df_levelingMain.groupby('Linkage Number')['Linkage Number'].transform('size')

            # df_levelingSpDropSEQ = df_levelingSp[df_levelingSp['Sequence No'].isnull()]
            # df_levelingSpUndepSeq = df_levelingSp[df_levelingSp['Sequence No']=='Undep']
            # df_levelingSpUncorSeq = df_levelingSp[df_levelingSp['Sequence No']=='Uncor']
            # df_levelingSp = pd.concat([df_levelingSpDropSEQ, df_levelingSpUndepSeq, df_levelingSpUncorSeq])
            # df_levelingSp = df_levelingSp.reset_index(drop=True)
            # df_levelingSp['미착공수량'] = df_levelingSp.groupby('Linkage Number')['Linkage Number'].transform('size')

            # df_levelingPowerDropSEQ = df_levelingPower[df_levelingPower['Sequence No'].isnull()]
            # df_levelingPowerUndepSeq = df_levelingPower[df_levelingPower['Sequence No']=='Undep']
            # df_levelingPowerUncorSeq = df_levelingPower[df_levelingPower['Sequence No']=='Uncor']
            # df_levelingPower = pd.concat([df_levelingPowerDropSEQ, df_levelingPowerUndepSeq, df_levelingPowerUncorSeq])
            # df_levelingPower = df_levelingPower.reset_index(drop=True)
            # df_levelingPower['미착공수량'] = df_levelingPower.groupby('Linkage Number')['Linkage Number'].transform('size')

            # # if self.isDebug:
            # #     df_levelingMain.to_excel('.\\debug\\flow1_main.xlsx')
            # #     df_levelingSp.to_excel('.\\debug\\flow1_sp.xlsx')
            # #     df_levelingPower.to_excel('.\\debug\\flow1_power.xlsx')

            # df_progressFile = pd.read_excel(list_masterFile[1], skiprows=3)
            
            # df_progressFile = df_progressFile.drop(df_progressFile.index[len(df_progressFile.index) - 2:])
            # df_progressFile['미착공수주잔'] = df_progressFile['수주\n수량'] - df_progressFile['생산\n지시\n수량']
            # df_progressFile['LINKAGE NO'] = df_progressFile['LINKAGE NO'].astype(str).apply(delComma)
            # # if self.isDebug:
            # #     df_progressFile.to_excel('.\\debug\\flow1.xlsx')

            # df_sosFile = pd.read_excel(list_masterFile[0])
            # df_sosFile['Linkage Number'] = df_sosFile['Linkage Number'].astype(str)
            # # if self.isDebug:
            #     # df_sosFile.to_excel('.\\debug\\flow2.xlsx')

            # df_sosFile = df_sosFile.drop(df_sosFile[df_sosFile['MS Code'].str.contains('ZOTHER')].index)
            # df_sosFile = df_sosFile.drop(df_sosFile[df_sosFile['MS Code'].str.contains('YZ')].index)
            # df_sosFile = df_sosFile.drop(df_sosFile[df_sosFile['MS Code'].str.contains('SF')].index)
            # df_sosFile = df_sosFile.drop(df_sosFile[df_sosFile['MS Code'].str.contains('KM')].index)
            # df_sosFile = df_sosFile.drop(df_sosFile[df_sosFile['MS Code'].str.contains('TA80')].index)

            # # if self.isDebug:
            #     # df_sosFile.to_excel('.\\debug\\flow3.xlsx')

            # df_sosFileMerge = pd.merge(df_sosFile, df_progressFile, left_on='Linkage Number', right_on='LINKAGE NO', how='left').drop_duplicates(['Linkage Number'])

            # df_sosFileMerge = df_sosFileMerge.sort_values(by=['Planned Prod. Completion date'],
            #                                                 ascending=[True])
            # df_sosFileMerge = df_sosFileMerge.reset_index(drop=True)
            
            # # if self.isDebug:
            #     # df_sosFileMerge.to_excel('.\\debug\\flow4.xlsx')

            yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y%m%d')
            if self.isDebug:
                yesterday = (datetime.datetime.strptime(self.debugDate.text(),'%Y%m%d') - datetime.timedelta(days=1)).strftime('%Y%m%d')

            df_SmtAssyInven = readDB('10.36.15.42',
                                    1521,
                                    'NEURON',
                                    'ymi_user',
                                    'ymi123!',
                                    "SELECT INV_D, PARTS_NO, CURRENT_INV_QTY FROM pdsg0040 where INV_D = TO_DATE("+ str(yesterday) +",'YYYYMMDD')")
            # df_SmtAssyInven.columns = ['INV_D','PARTS_NO','CURRENT_INV_QTY']
            df_SmtAssyInven['현재수량'] = 0
            print(df_SmtAssyInven)
            # if self.isDebug:
            #     df_SmtAssyInven.to_excel('.\\debug\\flow5.xlsx')

            df_secOrderMainList = pd.read_excel(list_masterFile[7], skiprows=5)
            # print(df_secOrderMainList)
            df_joinSmt = pd.merge(df_secOrderMainList, df_SmtAssyInven, how = 'right', left_on='ASSY NO', right_on='PARTS_NO')
            df_joinSmt['대수'] = df_joinSmt['대수'].fillna(0)
            df_joinSmt['현재수량'] = df_joinSmt['CURRENT_INV_QTY'] - df_joinSmt['대수']
            df_joinSmt.to_excel('.\\debug\\flow6.xlsx')

            df_productTime = readDB('ymzn-bdv19az029-rds.cgbtxsdj6fjy.ap-northeast-1.rds.amazonaws.com',
                                    1521,
                                    'TPROD',
                                    'TEST_SCM',
                                    'test_scm',
                                    'SELECT * FROM FAM3_PRODUCT_TIME_TB')
            df_productTime['TotalTime'] = df_productTime['COMPONENT_SET'].apply(getSec) + df_productTime['MAEDZUKE'].apply(getSec) + df_productTime['MAUNT'].apply(getSec) + df_productTime['LEAD_CUTTING'].apply(getSec) + df_productTime['VISUAL_EXAMINATION'].apply(getSec) + df_productTime['PICKUP'].apply(getSec) + df_productTime['ASSAMBLY'].apply(getSec) + df_productTime['M_FUNCTION_CHECK'].apply(getSec) + df_productTime['A_FUNCTION_CHECK'].apply(getSec) + df_productTime['PERSON_EXAMINE'].apply(getSec)
            
            df_productTime.to_excel('.\\debug\\flow7.xlsx')
            # print(df_productTime.columns)
        except Exception as e:
            logging.exception(e, exc_info=True)                     
            self.runBtn.setEnabled(True)
if __name__ == '__main__':
    import sys
    app = QtWidgets.QApplication(sys.argv)
    ui = Ui_MainWindow()
    sys.exit(app.exec_())