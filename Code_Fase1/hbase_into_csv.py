import os
import happybase
from datetime import datetime
from dateutil.relativedelta import relativedelta
import csv
import sys

def program(f, c):
    with open('%s/salida.csv' % os.getcwd(), 'wb') as csvfile:
        #Estableciendo conexion
        connect = happybase.Connection(host="master.krejcmat.com", port=9090)
        connect.open()
        table = connect.table('Sensores')
        print ("Conexion realizada a hbase")
        values = {}
        headder_write = False
        filter = b"SingleColumnValueFilter ('SensorData', 'Sensor',=, 'regexstring:^" + str(f) + "DG')"
        families = ['SensorData', "Measure%d" % c]
        headder = ["Sensor", "Date"]
        for key, data in table.scan(filter = filter, columns = ['SensorData', 'Measure' + str(c)], sorted_columns=True):
            values = data
            if not headder_write:
                first_date = datetime.datetime.strptime(values.get("%s:%s" %(families[0], headder[1])), "%Y-%m-%d")
                minutes_lis = [first_date.time().strftime("%H:%M")]
                next_date = first_date
                while next_date.date() == first_date.date():
                    next_date += relativedelta(minutes =+ 10)
                    if next_date.time().strftime("%H:%M") in minutes_lis:
                        break
                    minutes_lis.append(next_date.time().strftime("%H:%M"))
            writer = csv.DictWriter(csvfile, fieldnames = headder + minutes_lis)
            if not headder_write:
                writer.writeheader()
                headder_write = True
                print ("Estableciendo cabeceras en el csv")
            write_dict = {x: values.get("%s:%s" % (families[0], x)) for x in headder}
            write_dict.update({x: values.get("%s:%s" % (families[1], x)) or -99999999 for x in minutes_lis})
            print ("Estableciendo valores:%s en el csv" % str(write_dict))
            writer.writerow(write_dict)
        #Finalizando conexion
        connect.close()

if __name__ == "__main__":
    arg = sys.argv
    f = int(sys.argv[1])
    c = int(sys.argv[2])
    print ("Medidor %s de la columna %c" % (f,c))
    program(f, c)
