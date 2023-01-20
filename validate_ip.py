from PyQt5.QtGui import QRegExpValidator
from PyQt5.QtCore import QRegExp

def validateIP():
    # IP Validate 함수
    ipRange = "(?:[0-1]?[0-9]?[0-9]|2[0-4][0-9]|25[0-5])"  # Part of the regular expression

    # Regulare expression
    ipRegex = QRegExp("^" + ipRange + "\\." + ipRange + "\\." + ipRange + "\\." + ipRange + "$")
    ipValidator = QRegExpValidator(ipRegex)

    return ipValidator