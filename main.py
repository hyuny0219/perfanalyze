import sys
import os
import json
import hashlib
from datetime import datetime
from PyQt5.QtWidgets import QGridLayout, QLabel, QLineEdit, QPushButton, QComboBox, QGroupBox, QMessageBox, \
    QApplication, QWidget, QRadioButton, QCheckBox, QFileDialog
from PyQt5.QtGui import QIntValidator, QIcon, QPixmap
from validate_ip import validateIP
from analyze import execAnalyze, execAnalyzeRAC
from conndb import connectDBINFO, isRAC, isSingle

try:
    os.chdir(sys._MEIPASS)
    print(sys._MEIPASS)
except:
    os.chdir(os.getcwd())

class PerfAz(QWidget):

    def __init__(self):

        super().__init__()
        self.initUI()

    def initUI(self):
        passwd = 'redpepper'
        today = datetime.now()
        today = today.strftime("%Y%m%d")
        today = datetime.strptime(today, '%Y%m%d')

        fname = QFileDialog.getOpenFileName(self)
        fname = fname[0]
        if fname=='':
            QMessageBox.warning(self, "경고", "파일을 추가하세요")
            sys.exit(0)
        else:
            pass

        with open(fname, encoding='utf-8') as f:
            license = json.load(f)

        enddate = license['content']['expiresOn']
        passwd = (passwd + enddate).encode()
        result = hashlib.sha256(passwd).hexdigest()
        licensekey = license['r1Sig']
        enddate = datetime.strptime(enddate, '%Y%m%d')
        remain = str((enddate - today).days)
        if result == licensekey:
            pass
        else:
            QMessageBox.critical(self, '라이선스', '라이선스가 만료되었습니다.')
            sys.exit(0)

        if today == enddate:
            QMessageBox.critical(self, '라이선스', '라이선스가 만료되되었습니다.')
            sys.exit(0)
        else:
            QMessageBox.information(self, '라이선스', '라이선스 만료일이 ' + remain + "일 남았습니다.")
            pass

        grid = QGridLayout()

        ## Connection 그룹 위젯 설정
        self.label_ip = QLabel('IP ADDRESS')
        self.label_port = QLabel('PORT')
        self.label_dbname = QLabel('DBNAME')
        self.label_id = QLabel('ID')
        self.label_pwd = QLabel('PASSWORD')

        self.input_ip = QLineEdit()
        self.input_port = QLineEdit()
        self.input_dbname = QLineEdit()
        self.input_id = QLineEdit()
        self.input_pwd = QLineEdit()

        self.input_ip.setText('192.168.200.76')
        self.input_port.setText('1523')
        self.input_dbname.setText('AZW')
        self.input_id.setText('SYSTEM')
        self.input_pwd.setText('welcome1')
        #
        # self.input_ip.setText('192.168.150.144')
        # self.input_port.setText('1521')
        # self.input_dbname.setText('ORCL')
        # self.input_id.setText('SYSTEM')
        # self.input_pwd.setText('Orclazwell1')

        # DB Type 설정
        self.single = QRadioButton('Single')
        self.rac = QRadioButton('RAC')
        self.rac1 = QCheckBox('RAC 지표')
        self.instance = QComboBox()
        self.rac1.setEnabled(False)
        self.instance.setEnabled(False)

        # 접속 및 해제 버튼 설정
        self.connbtn = QPushButton()
        self.disconnbtn = QPushButton()
        self.connbtn.setText('DB 접속')
        self.disconnbtn.setText('접속 해제')
        self.connbtn.setEnabled(False)
        self.disconnbtn.setEnabled(False)

        # IP, Port Validate 및 Password Masking 처리
        self.input_ip.setValidator(validateIP())
        self.input_port.setValidator(QIntValidator(1000, 9999))
        self.input_pwd.setEchoMode(QLineEdit.Password)

        ## Snapshot 그룹 위젯 설정
        self.showlabel1 = QLabel('Start Snapshot :')
        self.showlabel2 = QLabel('End Snapshot :')
        self.showlabel3 = QLabel()
        self.showlabel4 = QLabel()

        self.begin_snapshot = QComboBox()
        self.end_snapshot = QComboBox()
        self.begin_snapshot.setEnabled(False)
        self.end_snapshot.setEnabled(False)

        self.start_analyze = QPushButton()
        self.start_analyze.setText('분석 시작')
        self.start_analyze.setEnabled(False)

        ## DB 정보 위젯 설정
        self.dbid = QLabel('DBID : ')
        self.insnb = QLabel('INSTANCE_NUMBER : ')
        self.insnm = QLabel('INSTANCE_NAME : ')
        self.hn = QLabel('HOST_NAME : ')
        self.ver = QLabel('VERSION: ')
        self.pn = QLabel('PLATFORM_NAME: ')
        self.output_dbid = QLineEdit()
        self.output_insnb = QLineEdit()
        self.output_insnm = QLineEdit()
        self.output_hn = QLineEdit()
        self.output_ver = QLineEdit()
        self.output_pn = QLineEdit()

        self.output_dbid.setReadOnly(True)
        self.output_insnb.setReadOnly(True)
        self.output_insnm.setReadOnly(True)
        self.output_hn.setReadOnly(True)
        self.output_ver.setReadOnly(True)
        self.output_pn.setReadOnly(True)

        # AZWELL PLUS Label
        self.azwell = QLabel()
        self.pixmap = QLabel()
        self.pixmap.setPixmap(QPixmap("image/azwellplus.png"))
        self.azwell.setText('Copyrightⓒ2023 AZWELLPLUS Co.Ltd. All RESERVED')
        self.azwell.setStyleSheet("font: 8pt;")

        grid.addWidget(self.connectGroup(), 0, 0)
        grid.addWidget(self.snapGroup(), 1, 0)
        grid.addWidget(self.dbinfoGroup(), 0, 1)
        grid.addWidget(self.copyrightgroup(), 1, 1)
        self.setLayout(grid)

        # 접속 및 접속해제 이벤트
        self.connbtn.clicked.connect(self.clickconnbtn)
        self.disconnbtn.clicked.connect(self.clickdisconnbtn)

        # DB Type 설정 이벤트
        self.single.clicked.connect(self.clickSingle)
        self.rac.clicked.connect(self.clickRAC)

        # 스냅샷 설정 이벤트
        self.begin_snapshot.currentTextChanged.connect(self.select_combobox)
        self.end_snapshot.currentTextChanged.connect(self.select_combobox)

        # 성능분석 시작 이벤트
        self.start_analyze.clicked.connect(self.analyzebtn)

        self.setWindowTitle('Oracle Performance Analyzer')
        self.setWindowIcon(QIcon('image/에즈웰로고.ico'))
        self.show()

    def connectGroup(self):
        groupbox = QGroupBox('접속정보')
        connect_layout = QGridLayout()
        connect_layout.addWidget(self.label_ip, 0, 0)
        connect_layout.addWidget(self.label_port, 1, 0)
        connect_layout.addWidget(self.label_dbname, 2, 0)
        connect_layout.addWidget(self.label_id, 3, 0)
        connect_layout.addWidget(self.label_pwd, 4, 0)

        connect_layout.addWidget(self.input_ip, 0, 1)
        connect_layout.addWidget(self.input_port, 1, 1)
        connect_layout.addWidget(self.input_dbname, 2, 1)
        connect_layout.addWidget(self.input_id, 3, 1)
        connect_layout.addWidget(self.input_pwd, 4, 1)

        connect_layout.addWidget(self.connbtn, 5, 0)
        connect_layout.addWidget(self.disconnbtn, 5, 1)
        connect_layout.addWidget(self.single, 6, 0)
        connect_layout.addWidget(self.rac, 6, 1)
        connect_layout.addWidget(self.rac1, 6, 2)
        connect_layout.addWidget(self.instance, 6, 3)

        groupbox.setLayout(connect_layout)

        return groupbox
    def snapGroup(self):
        groupbox = QGroupBox('스냅샷 정보')

        snap_layout = QGridLayout()

        snap_layout.addWidget(self.begin_snapshot, 1, 0)
        snap_layout.addWidget(self.end_snapshot, 1, 1)
        snap_layout.addWidget(self.showlabel1, 0, 0)
        snap_layout.addWidget(self.showlabel2, 0, 1)
        snap_layout.addWidget(self.showlabel3, 2, 0)
        snap_layout.addWidget(self.showlabel4, 2, 1)
        snap_layout.addWidget(self.start_analyze, 3, 0)

        groupbox.setLayout(snap_layout)

        return groupbox

    def dbinfoGroup(self):
        groupbox = QGroupBox('DB 정보')

        dbinfo_layout = QGridLayout()
        dbinfo_layout.addWidget(self.dbid, 0, 0)
        dbinfo_layout.addWidget(self.insnb, 1, 0)
        dbinfo_layout.addWidget(self.insnm, 2, 0)
        dbinfo_layout.addWidget(self.hn, 3, 0)
        dbinfo_layout.addWidget(self.ver, 4, 0)
        dbinfo_layout.addWidget(self.pn, 5, 0)

        dbinfo_layout.addWidget(self.output_dbid, 0, 1)
        dbinfo_layout.addWidget(self.output_insnb, 1, 1)
        dbinfo_layout.addWidget(self.output_insnm, 2, 1)
        dbinfo_layout.addWidget(self.output_hn, 3, 1)
        dbinfo_layout.addWidget(self.output_ver, 4, 1)
        dbinfo_layout.addWidget(self.output_pn, 5, 1)

        groupbox.setLayout(dbinfo_layout)
        return groupbox

    def copyrightgroup(self):
        groupbox = QGroupBox()

        copy_layout = QGridLayout()
        copy_layout.addWidget(self.pixmap, 0, 0)
        copy_layout.addWidget(self.azwell, 1, 0)

        groupbox.setLayout(copy_layout)
        return groupbox

    def clickconnbtn(self):
        self.begin_snapshot.setEnabled(True)
        self.end_snapshot.setEnabled(True)

        ip = self.input_ip.text()
        port = self.input_port.text()
        dbname = self.input_dbname.text()
        schema = self.input_id.text()
        pwd = self.input_pwd.text()
        instid = self.instance.currentText()
        connstring = ip+":"+port+"/"+dbname

        conn = connectDBINFO(connstring, schema, pwd, instid)

        if str(type(conn)) == "<class 'str'>":
            QMessageBox.about(self, 'Connection', conn)
        else:
            QMessageBox.about(self, 'Connection', '접속 성공')

        dbid = str(conn[0][0])
        inst_no = str(conn[0][1])
        inst_nm = str(conn[0][2])
        host_nm = str(conn[0][3])
        version = str(conn[0][4])
        plat = str(conn[0][5])

        # DB Information 출력
        self.output_dbid.setText(dbid)
        self.output_insnb.setText(inst_no)
        self.output_insnm.setText(inst_nm)
        self.output_hn.setText(host_nm)
        self.output_ver.setText(version)
        self.output_pn.setText(plat)

        # Snapshot Information 출력
        self.listsnap = []
        for a,b,c,d,e,f,h,i,j in conn:
            begin = '{} | {}'.format(h,i)
            end = '{} | {}'.format(h,j)
            self.listsnap.append(begin)
            self.begin_snapshot.addItem(begin)
        currentsnap = self.begin_snapshot.currentText()
        ind_currentsnap = self.listsnap.index(currentsnap)
        end_list = self.listsnap[ind_currentsnap+1:]
        for i in end_list:
            self.end_snapshot.addItem(i)
        self.begin_snapshot.currentTextChanged.connect(self.changedSnap)

        self.single.setEnabled(False)
        self.rac.setEnabled(False)
        self.instance.setEnabled(False)
        self.connbtn.setEnabled(False)
        self.connbtn.setText('접속중')
        self.start_analyze.setEnabled(True)
        self.disconnbtn.setEnabled(True)

    def changedSnap(self):
        if self.begin_snapshot.currentText() == '':
            pass
        else:
            begin_snap = self.begin_snapshot.currentText().split('|')[0].strip()
            end_snap = self.end_snapshot.currentText().split('|')[0].strip()
            if begin_snap >= end_snap or not self.end_snapshot.currentText():
                self.end_snapshot.clear()
                currentsnap = self.begin_snapshot.currentText()
                ind_currentsnap = self.listsnap.index(currentsnap)

                end_list = self.listsnap[ind_currentsnap+1:]
                for i in end_list:
                    self.end_snapshot.addItem(i)
            else:
                pass

    def clickdisconnbtn(self):
        QMessageBox.about(self, 'Connection', '접속 해제')
        self.initial()

    def select_combobox(self):
        self.showlabel3.setText(self.begin_snapshot.currentText())
        self.showlabel4.setText(self.end_snapshot.currentText())

    def clickSingle(self):
        if not self.input_ip.text() or not self.input_port.text() or not self.input_dbname.text() or not self.input_id.text() or not self.input_pwd.text():
            QMessageBox.warning(self, '확인', '접속정보를 확인하세요')
            self.single.setAutoExclusive(False)
            self.single.setChecked(False)
            self.single.setAutoExclusive(True)
        else:
            self.instance.clear()
            ip = self.input_ip.text()
            port = self.input_port.text()
            dbname = self.input_dbname.text()
            schema = self.input_id.text()
            pwd = self.input_pwd.text()
            connstring = ip+":"+port+"/"+dbname
            instno = isSingle(connstring, schema, pwd)

            if str(type(instno)) == "<class 'str'>":
                QMessageBox.about(self, 'Connection', instno)
                self.initial()
            else:
                self.instance.addItem(str(instno))
                self.instance.setEnabled(True)
                self.connbtn.setEnabled(True)

    def clickRAC(self):
        if not self.input_ip.text() or not self.input_port.text() or not self.input_dbname.text() or not self.input_id.text() or not self.input_pwd.text():
            QMessageBox.warning(self, '확인', '접속정보를 확인하세요')
            self.rac.setAutoExclusive(False)
            self.rac.setChecked(False)
            self.rac.setAutoExclusive(True)
        else:
            self.instance.clear()
            ip = self.input_ip.text()
            port = self.input_port.text()
            dbname = self.input_dbname.text()
            schema = self.input_id.text()
            pwd = self.input_pwd.text()
            connstring = ip+":"+port+"/"+dbname
            conn = isRAC(connstring, schema, pwd)

            # RAC Instance 정보 출력
            if str(type(conn)) == "<class 'str'>":
                QMessageBox.about(self, 'Connection', conn)
                self.initial()
            else:
                for a in conn:
                    rac = '{} '.format(a)[1]
                    self.instance.addItem(str(rac))
                self.instance.setEnabled(True)
                self.connbtn.setEnabled(True)
                self.rac1.setEnabled(True)

    def analyzebtn(self):
        self.start_analyze.setEnabled(False)
        self.start_analyze.setText('분석중')
        self.begin_snapshot.setEnabled(False)
        self.end_snapshot.setEnabled(False)
        self.disconnbtn.setEnabled(False)
        self.instance.setEnabled(False)
        self.rac1.setEnabled(False)

        today = datetime.now()
        today = today.strftime("%Y%m%d")

        desktop = os.path.expanduser('~/Desktop')
        os.chdir(desktop)
        directory = str("Report" +"\\"+ today)


        try:
            if not os.path.exists(directory):
                os.makedirs(directory)
        except OSError:
            print('Error: Creating directory. ' + directory)

        # 변수 선언
        ip = self.input_ip.text()
        port = self.input_port.text()
        dbname = self.input_dbname.text()
        schema = self.input_id.text()
        pwd = self.input_pwd.text()
        connstring = ip+":"+port+"/"+dbname
        instnm = self.output_insnm.text()
        path = os.path.expanduser("~\\Desktop\\Report\\" + today)

        vsnap_start = str(self.begin_snapshot.currentText().split('|')[0])
        vsnap_end = str(self.end_snapshot.currentText().split('|')[0])
        vdbid = str(self.output_dbid.text())
        vinstno = str(self.output_insnb.text())

        if self.rac.isChecked():
            if self.rac1.isChecked():
                analrac = execAnalyzeRAC(connstring, schema, pwd, vsnap_start, vsnap_end, vdbid, vinstno, instnm, path)
                QMessageBox.information(self, 'Analyze', analrac)
            else:
                pass
            anal = execAnalyze(connstring, schema, pwd, vsnap_start, vsnap_end, vdbid, vinstno, instnm, path)
        else:
            anal = execAnalyze(connstring, schema, pwd, vsnap_start, vsnap_end, vdbid, vinstno, instnm, path)

        QMessageBox.information(self, 'Analyze', anal)

        os.startfile(path)
        self.initial()

    def initial(self):
        self.connbtn.setText('DB 접속')
        self.start_analyze.setText('분석 시작')
        self.disconnbtn.setEnabled(False)
        self.connbtn.setEnabled(False)
        self.single.setAutoExclusive(False)
        self.single.setChecked(False)
        self.single.setAutoExclusive(True)
        self.single.setEnabled(True)
        self.rac.setAutoExclusive(False)
        self.rac.setChecked(False)
        self.rac1.setChecked(False)
        self.rac.setAutoExclusive(True)
        self.rac.setEnabled(True)
        self.rac1.setEnabled(False)
        self.instance.clear()
        self.start_analyze.setEnabled(False)
        self.begin_snapshot.clear()
        self.end_snapshot.clear()
        self.begin_snapshot.setEnabled(False)
        self.end_snapshot.setEnabled(False)
        self.output_dbid.clear()
        self.output_insnb.clear()
        self.output_insnm.clear()
        self.output_hn.clear()
        self.output_ver.clear()
        self.output_pn.clear()

    def my_exception_hook(exctype, value, traceback):
        # Print the error and traceback
        print(exctype, value, traceback)
        # Call the normal Exception hook after
        sys._excepthook(exctype, value, traceback)
        # sys.exit(1)

    sys._excepthook = sys.excepthook
    sys.excepthook = my_exception_hook

if __name__ == '__main__':
    app = QApplication(sys.argv)
    pa = PerfAz()
    sys.exit(app.exec_())
    os.system('pause')