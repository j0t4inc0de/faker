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

def poblar_institutos(cursor):
    print("Poblando tabla 'instituto'...")
    institutos = []
    for i in range(1, NUM_INSTITUTOS + 1):
        nombre = f"Instituto Profesional {fake.company_suffix()} {fake.city()}"
        institutos.append((i, nombre))
    
    query = "INSERT INTO instituto (id_instituto, nombre_instituto) VALUES (%s, %s)"
    cursor.executemany(query, institutos)
    print(f"-> {cursor.rowcount} registros insertados en 'instituto'.")
    return [i[0] for i in institutos]

def poblar_carreras(cursor, instituto_ids):
    print("Poblando tabla 'carrera'...")
    carreras = []
    for i in range(1, NUM_CARRERAS + 1):
        nombre = f"Ingeniería en {fake.job()}"
        duracion = random.randint(3, 6)
        id_instituto = random.choice(instituto_ids)
        carreras.append((i, nombre, duracion, id_instituto))
        
    query = "INSERT INTO carrera (id_carrera, nombre_carrera, duracion_anios, id_instituto) VALUES (%s, %s, %s, %s)"
    cursor.executemany(query, carreras)
    print(f"-> {cursor.rowcount} registros insertados en 'carrera'.")
    return [c[0] for c in carreras]

def poblar_docentes(cursor):
    print("Poblando tabla 'docente'...")
    docentes = []
    ruts_usados = set()
    for i in range(1, NUM_DOCENTES + 1):
        while True:
            rut = generar_rut()
            if rut not in ruts_usados:
                ruts_usados.add(rut)
                break
        nombre = fake.first_name()
        apellido = fake.last_name()
        correo = f"{nombre.lower()}.{apellido.lower()}{random.randint(1,99)}@instituto.local"
        docentes.append((i, rut, nombre, apellido, fake.job(), correo, fake.phone_number()))
        
    query = "INSERT INTO docente (id_docente, rut, nombre, apellido, especialidad, correo_institucional, telefono) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    cursor.executemany(query, docentes)
    print(f"-> {cursor.rowcount} registros insertados en 'docente'.")
    return [d[0] for d in docentes]

def poblar_asignaturas(cursor, carrera_ids):
    print("Poblando tabla 'asignatura'...")
    asignaturas = []
    for i in range(1, NUM_ASIGNATURAS + 1):
        nombre = f"Fundamentos de {fake.word().capitalize()}"
        id_carrera = random.choice(carrera_ids)
        asignaturas.append((i, nombre, id_carrera))
        
    query = "INSERT INTO asignatura (id_asignatura, nombre_asignatura, id_carrera) VALUES (%s, %s, %s)"
    cursor.executemany(query, asignaturas)
    print(f"-> {cursor.rowcount} registros insertados en 'asignatura'.")
    return [a[0] for a in asignaturas]

def poblar_estudiantes(cursor, carrera_ids):
    print("Poblando tabla 'estudiante'...")
    estudiantes = []
    ruts_usados = set()
    for i in range(1, NUM_ESTUDIANTES + 1):
        while True:
            rut = generar_rut()
            if rut not in ruts_usados:
                ruts_usados.add(rut)
                break
        estudiantes.append((
            i, rut, fake.first_name(), fake.last_name(), 
            fake.date_of_birth(minimum_age=18, maximum_age=40),
            random.choice(['Masculino', 'Femenino', 'Otro']),
            fake.address(), fake.phone_number(), random.choice(carrera_ids)
        ))
        
    query = "INSERT INTO estudiante (id_estudiante, rut, nombre, apellido, fecha_nacimiento, genero, direccion, telefono, id_carrera) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    cursor.executemany(query, estudiantes)
    print(f"-> {cursor.rowcount} registros insertados en 'estudiante'.")
    return [e[0] for e in estudiantes]

def poblar_cursos(cursor, asignatura_ids, docente_ids):
    print("Poblando tabla 'curso'...")
    cursos = []
    for i in range(1, NUM_CURSOS + 1):
        id_asignatura = random.choice(asignatura_ids)
        id_docente = random.choice(docente_ids)
        semestre = random.choice(['1', '2'])
        anio = random.randint(2022, 2025)
        cursos.append((i, id_asignatura, id_docente, semestre, anio))
        
    query = "INSERT INTO curso (id_curso, id_asignatura, id_docente, semestre, anio) VALUES (%s, %s, %s, %s, %s)"
    cursor.executemany(query, cursos)
    print(f"-> {cursor.rowcount} registros insertados en 'curso'.")
    return [c[0] for c in cursos]

def poblar_matriculas(cursor, estudiante_ids, curso_ids):
    print("Poblando tabla 'matricula'...")
    matriculas = []
    for i in range(1, NUM_MATRICULAS + 1):
        id_estudiante = random.choice(estudiante_ids)
        id_curso = random.choice(curso_ids)
        fecha = fake.date_between(start_date='-2y', end_date='today')
        estado = random.choice(['Activo', 'Aprobado', 'Reprobado', 'Retirado'])
        matriculas.append((i, id_estudiante, id_curso, fecha, estado))
        
    query = "INSERT INTO matricula (id_matricula, id_estudiante, id_curso, fecha_matricula, estado) VALUES (%s, %s, %s, %s, %s)"
    cursor.executemany(query, matriculas)
    print(f"-> {cursor.rowcount} registros insertados en 'matricula'.")
    return [m[0] for m in matriculas]

def poblar_evaluaciones(cursor, matricula_ids):
    print(f"Poblando tabla 'evaluacion' con {NUM_EVALUACIONES} registros...")
    evaluaciones = []
    nombres_evaluacion = ['Prueba 1', 'Prueba 2', 'Examen', 'Informe', 'Presentación']
    for i in range(1, NUM_EVALUACIONES + 1):
        id_matricula = random.choice(matricula_ids)
        nota = round(random.uniform(1.0, 7.0), 1)
        fecha = fake.date_between(start_date='-1y', end_date='today')
        evaluaciones.append((i, id_matricula, random.choice(nombres_evaluacion), nota, fecha))
        
    query = "INSERT INTO evaluacion (id_evaluacion, id_matricula, nombre_evaluacion, nota, fecha_evaluacion) VALUES (%s, %s, %s, %s, %s)"
    cursor.executemany(query, evaluaciones)
    print(f"-> {cursor.rowcount} registros insertados en 'evaluacion'.")
    
#* SCRIPT PRINCIPAL

def main():
    start_time = time.time()
    try:
        print("Conectando a la base de datos...")
        cnx = mysql.connector.connect(**db_config)
        cursor = cnx.cursor()
        print("Conexión exitosa.")

        # Desactivar temporalmente las claves foráneas para acelerar la inserción
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
        
        # --- Ejecutar funciones en orden de dependencia ---
        instituto_ids = poblar_institutos(cursor)
        carrera_ids = poblar_carreras(cursor, instituto_ids)
        docente_ids = poblar_docentes(cursor)
        asignatura_ids = poblar_asignaturas(cursor, carrera_ids)
        estudiante_ids = poblar_estudiantes(cursor, carrera_ids)
        curso_ids = poblar_cursos(cursor, asignatura_ids, docente_ids)
        matricula_ids = poblar_matriculas(cursor, estudiante_ids, curso_ids)
        poblar_evaluaciones(cursor, matricula_ids)
        
        # Reactivar claves foráneas
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
        
        print("\nConfirmando transacción (commit)...")
        cnx.commit()
        print("¡Proceso completado con éxito!")

    except mysql.connector.Error as err:
        print(f"Error de base de datos: {err}")
        if 'cnx' in locals() and cnx.is_connected():
            print("Realizando rollback...")
            cnx.rollback()
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'cnx' in locals() and cnx.is_connected():
            cnx.close()
            print("Conexión cerrada.")
            
    end_time = time.time()
    print(f"\nTiempo total de ejecución: {end_time - start_time:.2f} segundos.")
    
main()