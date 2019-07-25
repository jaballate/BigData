import os
import csv
import happybase
import hashlib
import sys

def process(f, c):
    csvfile = open('%s/datasets/SET-dec-2013.csv' % os.getcwd(), 'rb')
    elements = csv.reader(csvfile, delimiter=',', quotechar='|')
    families = {'SensorData': {}}
    string_families = "SensorData"
    cont = 1
    while cont <= c:
        string_families += "|Measure%d" % cont
        families.update({"Measure%d" % cont: {}})
        cont += 1
    #Estableciendo conexion
    connect = happybase.Connection(host="master.krejcmat.com", port=9090)
    connect.open()
    tables = connect.tables()
    if "Sensores" in tables:
        table = connect.table('Sensores')
    else:
        connect.create_table('Sensores', families)
        table = connect.table('Sensores')
    print ("Se crearon las Familias %s" % string_families)
    batch = table.batch(batch_size=1000)
    cont = 1
    while cont <= f:
        cont += 1
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
    #arg = sys.argv
    f = int(sys.argv[1])
    c = int(sys.argv[2])
    print ("Se crearan %s filas y %s columnas" % (f,c))
    process(f, c)
