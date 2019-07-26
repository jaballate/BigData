#!/bin/bash

# run N slave containers

if [ $# != 2  ]
then
	echo "Debe introducir los parametro para replicar, el primero (F) son las filas, el segundo (C) las columnas"
	exit 1
fi

python Code_Fase1/set_into_hbase.py $1 $2
