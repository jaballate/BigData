#!/bin/bash

# run N slave containers

if [ $# != 2  ]
then
	echo "Debe introducir los parametros para extraer los datos, el primero (F) son las filas, el segundo (C) las columnas"
	exit 1
fi

python Code_Fase1/get_from_hbase.py $1 $2
