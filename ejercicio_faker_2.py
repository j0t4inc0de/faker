import mysql.connector
from faker import Faker
import random
from datetime import datetime, timedelta
import time
from tqdm import tqdm

#? HACER CONEXIÓN CON LA BASE DE DATOS
db_config = {
    'host': 'localhost',
    'user': 'root',      # Cambia esto si tu usuario es diferente
    'password': '',      # Cambia esto por tu contraseña
    'database': 'smartlend_bd'
}
print("Conectando a la base de datos SmartLend...\n", db_config)

#? CONFIGURACION DE LA GENERACIÓN DE DATOS
NUM_CARRERAS = 20
NUM_USUARIOS = 5000
NUM_LOGS_FACIAL = 8000
NUM_CATEGORIAS = 10
NUM_TIPOS_HERRAMIENTA = 100
NUM_HERRAMIENTAS_INDIVIDUALES = 2000
NUM_PRESTAMOS = 10000
NUM_RESERVAS = 3000

# Inicializar Faker
fake = Faker('es_CL')

#? FUNCIONES AUXILIARES (RUT CHILENO)

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
    
def generar_rut():
    rut_base = random.randint(5000000, 25000000)
    verificador = calcular_dv(rut_base)
    return  f"{rut_base}-{verificador}"

#? FUNCIONES DE GENERACIÓN E INSERCIÓN DE DATOS

def poblar_roles(cursor):
    print("Poblando tabla 'rol_usuario'...")
    # Roles fijos lógicos para el sistema
    roles_data = [
        (1, 'Administrador', 'Acceso total al sistema', 'ALL_PRIVILEGES'),
        (2, 'Pañolero', 'Encargado de entregar y recibir herramientas', 'MANAGE_INVENTORY, MANAGE_LOANS'),
        (3, 'Docente', 'Puede solicitar para clases y validar alumnos', 'REQUEST_LOAN, APPROVE_LOAN'),
        (4, 'Alumno', 'Puede reservar y solicitar herramientas', 'REQUEST_LOAN')
    ]
    
    query = "INSERT INTO rol_usuario (id_rol, nombre, descripcion, permisos) VALUES (%s, %s, %s, %s)"
    cursor.executemany(query, roles_data)
    print(f"-> {cursor.rowcount} registros insertados en 'rol_usuario'.\n")
    return [r[0] for r in roles_data]

def poblar_carreras(cursor):
    print("Poblando tabla 'carrera'...")
    carreras = []
    # Generamos nombres de carreras técnicas/ingenierías
    areas = ['Informática', 'Mecánica', 'Electricidad', 'Construcción', 'Minería', 'Agronomía']
    
    for i in range(1, NUM_CARRERAS + 1):
        area = random.choice(areas)
        nombre = f"Ingeniería en {area} mención {fake.word().capitalize()}"
        carreras.append((i, nombre))
        
    query = "INSERT INTO carrera (id_carrera, nombre) VALUES (%s, %s)"
    cursor.executemany(query, carreras)
    print(f"-> {cursor.rowcount} registros insertados en 'carrera'.\n")
    return [c[0] for c in carreras]

def poblar_usuarios(cursor, rol_ids, carrera_ids):
    usuarios_ids = []
    ruts_usados = set()
    query = "INSERT INTO usuario (id_usuario, rut, embedding, nombres, apellidos, correo, id_rol, id_carrera) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    
    lote_size = 1000
    total_insertados = 0
    
    with tqdm(total=NUM_USUARIOS, desc="Poblando 'usuario'") as pbar:
        for i in range(1, NUM_USUARIOS + 1, lote_size):
            usuarios_lote = []
            for j in range(i, min(i + lote_size, NUM_USUARIOS + 1)):
                while True:
                    rut = generar_rut()
                    if rut not in ruts_usados:
                        ruts_usados.add(rut)
                        break
                
                nombre = fake.first_name()
                apellido = fake.last_name()
                # 80% prob de ser Alumno (ID 4), 10% Docente (ID 3), resto staff
                rol_elegido = random.choices([4, 3, 2, 1], weights=[80, 10, 5, 5], k=1)[0]
                
                # Embedding simulado (vector de texto largo)
                embedding_fake = str([random.random() for _ in range(10)]) 
                
                usuarios_lote.append((
                    j, rut, embedding_fake, nombre, apellido,
                    f"{nombre.lower()}.{apellido.lower()}{random.randint(1,999)}@smartlend.cl",
                    rol_elegido,
                    random.choice(carrera_ids) if rol_elegido in [3, 4] else None
                ))
                usuarios_ids.append(j)
            
            cursor.executemany(query, usuarios_lote)
            total_insertados += cursor.rowcount
            pbar.update(len(usuarios_lote))

    print(f"-> {total_insertados} registros insertados en 'usuario'.\n")
    return usuarios_ids

def poblar_autenticacion_facial(cursor, usuario_ids):
    print("Poblando tabla 'autenticacion_facial'...")
    logs = []
    for i in range(1, NUM_LOGS_FACIAL + 1):
        exito = random.choice([True, True, True, False]) # 75% éxito
        confianza = round(random.uniform(0.7, 0.99), 2) if exito else round(random.uniform(0.1, 0.6), 2)
        fecha = fake.date_time_between(start_date='-6m', end_date='now')
        
        logs.append((
            i, exito, confianza, fecha, random.choice(usuario_ids)
        ))
        
    query = "INSERT INTO autenticacion_facial (id_autenticacion, exito, confianza, fecha_hora, id_usuario) VALUES (%s, %s, %s, %s, %s)"
    cursor.executemany(query, logs)
    print(f"-> {cursor.rowcount} registros insertados en 'autenticacion_facial'.\n")

def poblar_categorias(cursor):
    print("Poblando tabla 'categoria_herramienta'...")
    nombres_cat = ['Herramientas Manuales', 'Herramientas Eléctricas', 'Insumos', 'Seguridad (EPP)', 'Medición', 'Corte', 'Soldadura', 'Automotriz', 'Jardinería', 'Otros']
    categorias = []
    for i, nombre in enumerate(nombres_cat, 1):
        categorias.append((i, nombre))
    
    query = "INSERT INTO categoria_herramienta (id_categoria, nombre) VALUES (%s, %s)"
    cursor.executemany(query, categorias)
    print(f"-> {cursor.rowcount} registros insertados en 'categoria_herramienta'.\n")
    return [c[0] for c in categorias]

def poblar_tipos_herramienta(cursor, categoria_ids):
    print("Poblando tabla 'tipo_herramienta'...")
    tipos = []
    for i in range(1, NUM_TIPOS_HERRAMIENTA + 1):
        nombre_base = fake.word().capitalize()
        nombre = f"{nombre_base} {fake.color_name()} Pro"
        descripcion = fake.sentence(nb_words=10)
        imagen = f"https://fakeimg.pl/300x200/?text={nombre_base}"
        
        tipos.append((
            i, nombre, descripcion, imagen, random.choice(categoria_ids)
        ))
        
    query = "INSERT INTO tipo_herramienta (id_tipo_herramienta, nombre, descripcion, imagen_url, id_categoria) VALUES (%s, %s, %s, %s, %s)"
    cursor.executemany(query, tipos)
    print(f"-> {cursor.rowcount} registros insertados en 'tipo_herramienta'.\n")
    return [t[0] for t in tipos]

def poblar_herramientas_individuales(cursor, tipo_ids):
    herramientas_ids = []
    codigos_usados = set()
    query = "INSERT INTO herramienta_individual (id_herramienta, codigo_barra, estado_herramienta, fecha_adquisicion, id_tipo_herramienta) VALUES (%s, %s, %s, %s, %s)"
    
    lote_size = 1000
    total_insertados = 0
    
    with tqdm(total=NUM_HERRAMIENTAS_INDIVIDUALES, desc="Poblando 'herramienta_individual'") as pbar:
        for i in range(1, NUM_HERRAMIENTAS_INDIVIDUALES + 1, lote_size):
            lote = []
            for j in range(i, min(i + lote_size, NUM_HERRAMIENTAS_INDIVIDUALES + 1)):
                while True:
                    codigo = fake.ean13()
                    if codigo not in codigos_usados:
                        codigos_usados.add(codigo)
                        break
                
                estado = random.choices(['disponible', 'prestada', 'mantenimiento', 'baja'], weights=[60, 20, 10, 10], k=1)[0]
                fecha = fake.date_between(start_date='-2y', end_date='today')
                
                lote.append((
                    j, codigo, estado, fecha, random.choice(tipo_ids)
                ))
                herramientas_ids.append(j)
                
            cursor.executemany(query, lote)
            total_insertados += cursor.rowcount
            pbar.update(len(lote))
            
    print(f"-> {total_insertados} registros insertados en 'herramienta_individual'.\n")
    return herramientas_ids

def poblar_prestamos(cursor, usuario_ids, tipo_ids, herramienta_individual_ids):
    query = "INSERT INTO prestamo (id_prestamo, fecha_prestamo, fecha_devolucion_esperada, fecha_devolucion_real, estado_prestamo, estado_devolucion, observaciones, id_usuario, id_tipo_herramienta, id_herramienta_individual) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    
    lote_size = 2000
    total_insertados = 0
    
    with tqdm(total=NUM_PRESTAMOS, desc="Poblando 'prestamo'") as pbar:
        for i in range(1, NUM_PRESTAMOS + 1, lote_size):
            lote = []
            for j in range(i, min(i + lote_size, NUM_PRESTAMOS + 1)):
                estado = random.choice(['pendiente', 'activo', 'devuelto', 'vencido'])
                fecha_inicio = fake.date_time_between(start_date='-1y', end_date='now')
                fecha_esperada = fecha_inicio + timedelta(days=3)
                
                fecha_real = None
                id_individual = None
                estado_devolucion = None
                
                # Logica para asignar ID individual y fechas
                if estado == 'pendiente':
                    id_individual = None # Aún no se asigna la herramienta fisica
                else:
                    id_individual = random.choice(herramienta_individual_ids) # Ya se asignó
                
                if estado in ['devuelto', 'vencido']:
                    fecha_real = fecha_esperada + timedelta(days=random.randint(-1, 5))
                    estado_devolucion = random.choice(['bueno', 'bueno', 'malo'])
                
                lote.append((
                    j, fecha_inicio, fecha_esperada, fecha_real,
                    estado, estado_devolucion, fake.sentence() if random.random() > 0.8 else None,
                    random.choice(usuario_ids),
                    random.choice(tipo_ids),
                    id_individual
                ))
            
            cursor.executemany(query, lote)
            total_insertados += cursor.rowcount
            pbar.update(len(lote))

    print(f"-> {total_insertados} registros insertados en 'prestamo'.\n")

def poblar_reservas(cursor, usuario_ids, tipo_ids):
    print("Poblando tabla 'reserva'...")
    reservas = []
    for i in range(1, NUM_RESERVAS + 1):
        fecha_reserva = fake.date_time_between(start_date='-3m', end_date='now')
        fecha_inicio = fecha_reserva + timedelta(days=random.randint(1, 7))
        fecha_fin = fecha_inicio + timedelta(days=2)
        
        reservas.append((
            i, fecha_reserva, fecha_inicio, fecha_fin,
            random.choice(['activa', 'completada', 'cancelada']),
            random.choice(usuario_ids),
            random.choice(tipo_ids)
        ))
        
    query = "INSERT INTO reserva (id_reserva, fecha_reserva, fecha_inicio_reserva, fecha_fin_reserva, estado_reserva, id_usuario, id_tipo_herramienta) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    cursor.executemany(query, reservas)
    print(f"-> {cursor.rowcount} registros insertados en 'reserva'.\n")

#* SCRIPT PRINCIPAL

def main():
    start_time = time.time()
    try:
        print("Iniciando generación de datos...")
        cnx = mysql.connector.connect(**db_config)
        cursor = cnx.cursor()
        print("Conexión exitosa.\n")

        cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
        
        # Secuencia de inserción
        rol_ids = poblar_roles(cursor)
        carrera_ids = poblar_carreras(cursor)
        usuario_ids = poblar_usuarios(cursor, rol_ids, carrera_ids)
        poblar_autenticacion_facial(cursor, usuario_ids)
        
        categoria_ids = poblar_categorias(cursor)
        tipo_ids = poblar_tipos_herramienta(cursor, categoria_ids)
        herramienta_individual_ids = poblar_herramientas_individuales(cursor, tipo_ids)
        
        poblar_prestamos(cursor, usuario_ids, tipo_ids, herramienta_individual_ids)
        poblar_reservas(cursor, usuario_ids, tipo_ids)
        
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
        
        print("\nConfirmando transacción (commit)...")
        cnx.commit()
        print("¡Base de datos SmartLend poblada con éxito!\n")

    except mysql.connector.Error as err:
        print(f"Error de base de datos: {err}")
        if 'cnx' in locals() and cnx.is_connected():
            cnx.rollback()
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'cnx' in locals() and cnx.is_connected():
            cnx.close()
            print("Conexión cerrada.")
            
    end_time = time.time()
    print(f"\nTiempo total de ejecución: {end_time - start_time:.2f} segundos.")
    
if __name__ == "__main__":
    main()