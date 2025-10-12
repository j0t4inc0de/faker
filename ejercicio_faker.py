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

#? HACER CONEXIÓN CON LA BASE DE DATOS
db_config = {
    'host': 'localhost',
    'user': 'admin',
    'password': 'admin',
    'database': 'bd_instituto'
}
print("Conectando a la base de datos...\n", db_config)

#? CONFIGURACION DE LA GENERACIÓN DE DATOS
NUM_INSTITUTOS = 10
NUM_CARRERAS = 50
NUM_DOCENTES = 200
NUM_ASIGNATURAS = 500
NUM_ESTUDIANTES = 10000
NUM_CURSOS = 1000
NUM_MATRICULAS = 100000
NUM_EVALUACIONES = 1000000

# Inicializar Faker
fake = Faker('es_CL')

#? FUNCIONES AUXILIARES

# Calcular digito verificador del RUT chileno
def calcular_dv(rut_sin_dv):
    reverso = str(rut_sin_dv)[::-1]
    multiplicador = 2
    suma = 0
    for digito in reverso:
        suma += int(digito) * multiplicador
        multiplicador += 1
        if multiplicador == 8:
            multiplicador = 2
    resto = suma % 11
    dv = 11 - resto
    if dv == 11:
        return '0'
    if dv == 10:
        return 'K'
    return str(dv)
    
# Genera rut validos y formateados
def generar_rut():
    rut_base = random.randint(5000000, 25000000)
    verificador = calcular_dv(rut_base)
    return  f"{rut_base}-{verificador}"

#? FUNCIONES DE GENERACIÓN E INSERCIÓN DE DATOS


#* SCRIPT PRINCIPAL