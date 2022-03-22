import requests
import os
import psycopg2
import logging
import pandas as pd
from sqlalchemy import create_engine
from decouple import config
from datetime import datetime
from function import Concat
from conectar import create_table, update_DB

if __name__ == "__main__":
    #Config file log
    logging.basicConfig(filename="FileLog.log", level="DEBUG", filemode="w")

    #Download date is current
    now = datetime.now()
    now = now.strftime("%Y %m %d")
    months = {
        "01":'enero',
        "02":'febrero',
        "03":'marzo',
        "04":'abril',
        "05":'mayo',
        "06":'junio',
        "07":'julio',
        "08":'agosto',
        "09":'septiembre',
        "10":'octubre',
        "11":'noviembre',
        "12":'diciembre'
    }
    #Create directories
    directory = ["./src/museos/"+ now[0:4]+"-"+months[now[5:7]],
                "./src/salas_cine/"+ now[0:4]+"-"+months[now[5:7]],
                "./src/bibliotecas_populares/"+ now[0:4]+"-"+months[now[5:7]]]

    for dir in directory:
        os.makedirs(dir,
                    exist_ok=True)

    #Create csv files where the data is stored
    files = [directory[0]+"/museos-"+now[8:10]+"-"+now[5:7]+"-"+now[0:4]+".csv",
            directory[1]+"/salas_cine-"+now[8:10]+"-"+now[5:7]+"-"+now[0:4]+".csv",
            directory[2]+"/bibliotecas_populares-"+now[8:10]+"-"+now[5:7]+"-"+now[0:4]+".csv"]

    #URLs for downloading files
    URLs = ['https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/4207def0-2ff7-41d5-9095-d42ae8207a5d/download/museo.csv',
            'https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/392ce1a8-ef11-4776-b280-6f1c7fae16ae/download/cine.csv',
            'https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/01c6c048-dbeb-44e0-8efa-6944f73715d7/download/biblioteca_popular.csv']

    #Save file data to local
    for i in range(3):
        resp = requests.get(URLs[i])
        with open(files[i], 'wb')as file:
            file.write(resp.content)

    #name of the columns of the general table
    column = ["cod_localidad","id_provincia","id_departamento","categoría","provincia","localidad","nombre","domicilio","código_postal","número_teléfono","mail","web"]

    #Column names as found in each file
    columnMuseo = ["cod_loc","idprovincia","iddepartamento","categoria","provincia","localidad","nombre","direccion","cod_area","telefono","mail","web"]
    columnSala = ["Cod_Loc","IdProvincia","IdDepartamento","Categoría","Provincia","Localidad","Nombre","Dirección","cod_area","Teléfono","Mail","Web"]
    columnBiblioteca = ["Cod_Loc","IdProvincia","IdDepartamento","Categoría","Provincia","Localidad","Nombre","Domicilio","Cod_tel","Teléfono","Mail","Web"]

    #Converting to dataframe makes it easy to normalize
    museo = pd.read_csv(files[0])
    museo_data = museo[columnMuseo]
    #Rename the column 'fuente' of the file "museo" to concatenate with 'sala' and 'biblioteca'
    museo = museo.rename(columns={'fuente':'Fuente'})

    sala = pd.read_csv(files[1])
    sala_data = sala[columnSala]

    biblioteca = pd.read_csv(files[2])
    biblioteca_data = biblioteca[columnBiblioteca]

    #Rename columns
    museo_data.columns = column
    sala_data.columns = column
    biblioteca_data.columns = column

    #Concatenate the Dataframes with the function Concat
    data = Concat(museo_data,
                    sala_data,
                    biblioteca_data
    )
    #Remove the index for the data for the first Table
    data.set_index(data.columns[0])

    #Extract the Fuente columns, for the number of records per Fuente
    Fuentes_data = Concat(pd.DataFrame(museo["Fuente"]),
                            pd.DataFrame(sala["Fuente"]),
                            pd.DataFrame(biblioteca["Fuente"])
    )

    #Create dataframe to Table cantidad_registros
    registros_categoria= data["categoría"].value_counts().to_frame().reset_index()
    registros_fuente = Fuentes_data.value_counts()
    registros_cat_pro= data[["categoría", "provincia"]].value_counts().to_frame().reset_index()

    #Rename the columns
    registros_categoria.columns = ["Tipo","Cantidad"]
    registros_fuente.columns = ["Tipo","Cantidad"]
    registros_cat_pro.columns = ["Tipo","Provincia","Cantidad"]

    #Concatenate the dataframes with the function concatenate
    registros = Concat(registros_categoria, registros_fuente, registros_cat_pro)
    registros.set_index(registros.columns[0])

    #Process information from the third table, info_cine
    info_cine = sala[["Provincia", "Pantallas", "Butacas", "espacio_INCAA"]]
    info_cine = info_cine.set_index(info_cine.columns[0])

    #Add upload date column
    data["Fecha_carga"] = now
    registros["Fecha_carga"] = now
    info_cine["Fecha_carga"] = now

    #Database connection

    #Establish connections
    conn_string = "postgresql://"+config('DB_USER')+":"+config('DB_PASSWORD')+"@"+config('DB_HOST')+"/"+config('DB_NAME')

    db = create_engine(conn_string)
    conn = db.connect()

    conn1 = psycopg2.connect(
            host = config('DB_HOST'),
            database = config('DB_NAME'),
            user =  config('DB_USER'),
            password = config('DB_PASSWORD')
    )
    conn1.autocommit = True
    cursor = conn1.cursor()


    DB = ["./src/DB_general.sql","./src/DB_infoCine.sql", "./src/DB_registros.sql"]
    create_table(DB, cursor)

    update_DB(data,"tabla_general",conn)
    update_DB(registros,"registros",conn)
    update_DB(info_cine,"infoCine",conn)

    conn1.commit()

    #Close the connection
    conn1.close()