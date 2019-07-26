#!/usr/bin/python3
import os
import csv
import happybase
import hashlib
import sys

def program(f, c):
    csvfile = open('%s/datasets/SET-dec-2013.csv' % os.getcwd(), 'rb')
    elements = csv.reader(csvfile, delimiter=',', quotechar='|')
    family = {'SensorData': {}}
    string_families = "SensorData"
    for cont in range(c):
        string_families += "|Measure%s" % str(cont+1)
        family.update({"Measure%s" % str(cont+1): {}})
    #Estableciendo conexion
    connect = happybase.Connection(host="master.krejcmat.com", port=9090)
    connect.open()
    tables = connect.tables()
    if "Sensores" in tables:
        table = connect.table('Sensores')
    else:
        connect.create_table('Sensores', family)
        table = connect.table('Sensores')
    print ("Se crearon las Familias %s" % string_families)
    batch = table.batch(batch_size=1000)
    contf = 1
    while contf <= f:
        contf += 1
        for element in elements:
            key = hashlib.md5(element[0] + element[1].split(' ')[0]).hexdigest()
            SensorData = {'SensorData:Sensor': str(f) + element[0],
                          'SensorData:Date': element[1].split(' ')[0]}
            for contc in range(c):
                SensorData.update({"Measure%d:%s" % (contc+1, element[1].split(' ')[1]): element[2]})
            batch.put(key, SensorData)
            print ("Insertando Key:%s y Valores:%s" % (key, str(SensorData)))
            batch.send()
    #Finalizando conexion
    connect.close()

if __name__ == "__main__":
    f = int(sys.argv[1])
    c = int(sys.argv[2])
    program(f, c)
