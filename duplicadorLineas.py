# Rutinas del SiB Colombia para la validación y limpieza de datos primarios 
# de Biodiversidad en OPEN REFINE
# https://github.com/SIB-Colombia/Data_Quality

# Nombre: Duplicador de líneas para archivos
# Languaje: python
# Creado:2021-07-28
# Última Actualización:2022-04-19
# Autores: Esteban Marentes, Ricardo Ortiz, Camila Plata


# Propósito y uso: Script para duplicar líneas de un conjunto de datos utilizando la información presente en el individualCount.
# DISCLAIMER: Antes de utilizar el script, modifique el archivo original para los registros de ausencia y pongales un "1", si está como 0 o vacio se eliminaran las líneas
# 1) Coloque ambos archivos en la misma ubicación del computador
# 2) Nombre sus datos como "datosConvertir.txt"
# 3) Modifique la línea 38 y pongo la dirección donde va a exportar el resultado (Usar la ruta absoluta)
# 4) Corra este script en Spyder, verá el resultado como "Datos_TodasLineas.txt"
# 5) Revise el resultado del script y edite el campo individualCount para todos los registros de present como "1", para los registros de absent vuelva a poner "0"

#__________________________________________________________


# Importar paquetes
import math
import csv
import pandas as pd

# DATOS INICIALES___________________

datos_originales = pd.read_csv('/Users/estebanmarentes/Desktop/EstebanMH/Recursos AC/datosConvertir.csv', encoding = "utf8", sep="\t")

Resultado = datos_originales

Resultado =  datos_originales.reindex(datos_originales.index.repeat(datos_originales.individualCount))

x="Datos_TodasLineas.txt"

#EXPORTAR RESULTADOS

Resultado.to_csv('/Users/estebanmarentes/Desktop/EstebanMH/Recursos AC/' + x, sep="\t", encoding = "utf8")