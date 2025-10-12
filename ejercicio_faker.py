"""
    * Generar un script de python con Faker para poblar una base de datos con un millon de registros.
    
    pip install Faker
    pip install mysql-connector-python
    
    j0t4InC0de
"""
import mysql.connector
from faker import Faker
import random
from datetime import datetime, timedelta
import time

#? HACER CONEXCION CON LA BASE DE DATOS
db_config = {
    'host': 'localhost',
    'user': 'admin',
    'password': 'admin',
    'database': 'bd_instituto'
}
print("Conectando a la base de datos...\n", db_config)

#? CONFIGURACION DE LA GENERACIÓN DE DATOS

#? FUNCIONES AUXILIARES

#? FUNCIONES DE GENERACIÓN E INSERCIÓN DE DATOS

#* SCRIPT PRINCIPAL