#--------------------------------------------------------------
#                                                             |
#             Esto es una prueba                              |
#              Seguismo hacienod pruebas                      |
#                                                             |
#--------------------------------------------------------------


import cx_Oracle
import json
import argparse
import dpath.util
import time

matriz = []
campos_insert = []
query = ''
con = ''
json_file = ''
datosmatiz = []

# ---------------Inserta la información a una tabla ........................


def insertOracle():
    try:
        cur = con.cursor()
        cur.executemany(query, datosmatiz)
        con.commit()
    except Exception as e:
        print("Error al insertar la informacion: " + str(e))
    cur.close()

# ---------------Crea el query para insertar la informacion -----------------


def makeQuery(entity):
    global query
    global campos_insert
    i = 0
    x = 0
    cabecera = "insert into  " + entity
    campos = ''
    valores = ''
    while (i < len(matriz)):
        if matriz[i][6] == 'attributes':
            campos_insert.append([])
            campos_insert[x].append(matriz[i][4])
            campos = campos + "," + matriz[i][5]
            valores = valores + ":" + str(i + 1) + ", "
            x += 1
        i += 1
    campos = campos.lstrip(",")
    campos = campos + ")"
    valores = valores.rstrip(", ")
    valores = valores + ")"
    query = cabecera + " (" + campos + " values (" + valores

# ------------------Mapeo del archivo Json---------------------------------


def mapeojson():
    global datosmatiz
    j = 0
    k = 0
    x = 0
    for l in json_file:
        datosmatiz.append([])
        j = 0
        for h in campos_insert:
            try:
                datosmatiz[x].append(str(
                    dpath.util.get(json_file['data'][k], campos_insert[j][0])))
            except:
                datosmatiz[x].append('')
            j += 1
        k += 1
        x += 1

#  --------------------Lectura del archivo json-----------------------------


def cargarjson(file):
    global json_file
    try:
        json_file = json.loads(open(file).read())
    except Exception as e:
        print("OS error: {0}".format(e))

# --------------------Conector a la base de datos--------------------------


def conectar_database(username, password, database, server):
    global con
    try:
        con = cx_Oracle.connect(
            usuario + '/' + password + '@' + server + '/' + database)
    except Exception as e:
        print("OS error: {0}".format(e))

# --------------------Carga de datos Parametricos--------------------------


def carga_parametro(entidad):
    global matriz
    cur = con.cursor()
    cur.execute("select * from bss_api where entidad = '" + entidad + "'")
    k = 0

    for result in cur:
        matriz.append([])
        matriz[k].append(result[0])  # <==== dominios
        matriz[k].append(result[1])  # <==== entidad
        matriz[k].append(result[2])  # <==== atributos
        matriz[k].append(result[3])  # <==== tabla
        matriz[k].append(result[4])  # <==== campojson
        matriz[k].append(result[5])  # <==== campo_tabla_destino
        matriz[k].append(result[6])  # <==== tipo_atribuno
        matriz[k].append(result[7])  # <==== descripcion
        k = k + 1

    cur.close()

# --------------------------mmain-----------------------------------------


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-u", "--username", help="Usuario Oracle")
    parser.add_argument("-p", "--password", help="Password Oracle")
    parser.add_argument("-s", "--server", help="IP del servidor Oracle")
    parser.add_argument("-d", "--database", help="Nombre de la base de datos")
    parser.add_argument(
        "-f", "--file", help="Dirección del archivo Json a procesa")
    parser.add_argument("-e", "--entity", help="Nombre de la entidad ")
    args = parser.parse_args()
    # Aquí procesamos lo que se tiene que hacer con cada argumento
    if args.username:
        usuario = args.username
    if args.password:
        password = args.password
    if args.server:
        server = args.server
    if args.database:
        database = args.database
    if args.file:
        file = args.file
    if args.entity:
        entity = args.entity
    start = time.time()
    # conexión a la base de datos.
    conectar_database(usuario, password, database, server)
    elapsed = (time.time() - start)
    print(elapsed, " : conexion")
    # carga de los datos parametricos a la matris.
    carga_parametro(entity)
    elapsed = (time.time() - start)
    print(elapsed, " carga de parametros")
    # crea la consulta inser oracle sql
    makeQuery(entity)
    elapsed = (time.time() - start)
    print(elapsed, " armado del query")
    # Lee el archivo json
    cargarjson(file)
    elapsed = (time.time() - start)
    print(elapsed, " lectura del archivo json_file")
    # generacion del mapeo json
    mapeojson()
    elapsed = (time.time() - start)
    print(elapsed, " Mapeo del archivo json_file")
    # inserta el json a una base de datos Oracle
    insertOracle()
    elapsed = (time.time() - start)
    print(elapsed, " Insercion de la base de datos")
