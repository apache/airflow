import xml.etree.ElementTree as ET
import csv

tree = ET.parse('report_test.xml')
root = tree.getroot()




for child in root:
    for subchild in child:
        testclass=subchild.attrib['classname']
        testduration=subchild.attrib['time']
        testresult = 'S'
        for subsubchild in subchild:
            testresult='F'
        with open('reporttest.csv', 'a', newline='') as csvfile:
            spamwriter = csv.writer(csvfile, delimiter=' ', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            spamwriter.writerow([testclass, testresult, testduration])