Challenge Data Analytics - Python 🚀
En este proyecto se encuentra el desarrollo del reto Data Analytics con python para Alkemy

Requerimientos

Es requerido instalar y configurar el entorno virtual en donde se ejecutará las respectivas librerias

virtualenv -p python venv

Acivar el entorno virtual

.\venv\Scripts\activate

Ejecución

Para la correcta ejecución, son necesarios todos los archivos que se encuentran en el directorio src, ademas de la instalación de las librerias que se encuentran en el archivo 'requirements.txt'. Para esto, puede ejecutar el siguiente comando:

 sudo pip3 install -r requirements.txt

El archivo principal que se debe ejecutar es el "main.py" que hace uso de los demas archivos que se encuentran dentro de la carpeta src, entre ellos los correspondientes a los de extensión SQL que contienen el lenguaje de consulta para la creación de las tablas.

El proyecto cuenta con los debidos comentarios para entender el funcionamiento y desarrollo, las credenciales de la base de datos se encuentran en el archivo .env que por seguridad esta almacenado en el .gitignore.

Los loggings ejecutados se encuentran en el FileLog.log

Para comenzar se debe ubicar en el entorno virtual, y ejecutar el comando:

python .\src\main.py


Realizado por: Tannia Lucía Hernández Rojas
