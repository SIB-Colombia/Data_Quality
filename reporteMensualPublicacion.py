#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Rutinas del SiB Colombia para la generacion de reportes mensuales
# Nombre: Gneracion de reporte mensual de publicacion a través del SiB Colombia
# Languaje: python
# Creado:2022-05-04
# Ultima Actualizacion:2022-06-06
# Autores: Esteban Marentes, Ricardo Ortiz, Camila Plata
# Proposito y uso: Script para sacar las cifras de publicacion mensuales de un país en GBIF
#__________________________________________________________


# Importar paquetes
import pandas as pd
import requests
import json
import time
import xlrd
from pandas.io.json import json_normalize
from datetime import date

#### Variables de trabajo
codigoPaisGbif = '7e865cba-7c46-417b-ade5-97f2cf5b7be0'

#### Importar archivos que se van a utilizar
# Archivo de organizaciones en Excel. 
# Antes de descargar el archivo, eliminar las dos filas del comienzo

OrganizacionesRegistradas_AAAAMMDD = pd.read_excel('/Users/estebanmarentes/Desktop/EstebanMH/Recursos_AC/reporteMensual/OrganizacionesRegistradas_AAAAMMDD.xlsx')
# Eliminar las columnas inncesarias para dejar únicamente las que se necesitan: Nivel 1, GBIF ID, Logo Github, Nombre corto y URL Portal de Datos SiB
OrganizacionesRegistradas_AAAAMMDD = OrganizacionesRegistradas_AAAAMMDD[["GBIF ID", "Nivel 1", "Logo Github","Nombre corto", "URL Portal de Datos SiB"]]


OrganizacionesRegistradas_AAAAMMDD.rename(columns = {'GBIF ID': 'publishingOrganizationKey', 'Nivel 1': 'typeOrg', 'Logo Github': 'Logo','Nombre corto': 'NombreCorto_Org', 'URL Portal de Datos SiB': 'URLSocio'}, inplace = True)


# Archivo del mes anterior
datasetCO_Anterior_AAAAMMDD = pd.read_csv('/Users/estebanmarentes/Desktop/EstebanMH/Recursos_AC/reporteMensual/datasetCO_Anterior_AAAAMMDD.txt', encoding = "utf8", sep="\t")


#### 1. Llamada de datos iniciales en el API de GBIF para todos los conjuntos de datos del nodo Colombia a un dataFrame Pandas

response1000_JSON = pd.read_json("http://api.gbif.org/v1/node/" +codigoPaisGbif+ "/dataset?limit=1000") #Llamada al API desde el dato 1 al 1000
response2000_JSON = pd.read_json("http://api.gbif.org/v1/node/" +codigoPaisGbif+ "/dataset?limit=1000&offset=1000") #Llamada al API desde el dato 1000 al 2000, si hya más de 2000 recursos publicados, toca hacer uno llamado extra
response3000_JSON = pd.read_json("http://api.gbif.org/v1/node/7e865cba-7c46-417b-ade5-97f2cf5b7be0/dataset?limit=1000&offset=2000") #Llamada al API desde el dato 2000 al 3000

#### 2. Convertir el archivo JSON original para extraer los datos que estaban anidados en el JSON

response1000 = pd.json_normalize(response1000_JSON['results']) # Extraer los resultados de la llamada original del API con pandas.normalize
response2000 = pd.json_normalize(response2000_JSON['results']) # Extraer los resultados de la llamada original del API con pandas.normalize
response3000 = pd.json_normalize(response3000_JSON['results']) # Extraer los resultados de la llamada original del API con pandas.normalize

# Concatenar el resultado de los dos llamados del API
resultadosAPI = pd.concat([response1000, response2000, response3000])

#### 3. Ejecutar pasos para extraer las columnas necesarias para el reporte, renombre las columnas y crear los campos adicionales necesarios para el reporte en cada archivo

# Escoger solamente las columnas que se van ausar
resultadosAPI = resultadosAPI[["key", "doi", "type","title", "created", "modified","version", "publishingOrganizationKey"]]

# Separar la informacion de la columna created en año, mes día
resultadosAPI[['created1','created2']] = resultadosAPI.created.str.split("T",expand=True,) # Separa la fecha de la hora usando T como separador
del resultadosAPI['created2']# Elimina la columna que quedo con la hora
resultadosAPI[['year','month','day']] = resultadosAPI.created1.str.split("-",expand=True,) # Separa la fecha en año, mes día y crea las columnas
del resultadosAPI['created']# Elimina la columna original
resultadosAPI.rename(columns = {'created1':'created'}, inplace = True) # cambia nombre a created de la columna corregida


# Separar la informacion de la columna modified en año, mes día
resultadosAPI[['modified1','modified2']] = resultadosAPI.modified.str.split("T",expand=True,) # Separa la fecha de la hora usando T como separador
del resultadosAPI['modified2']# Elimina la columna que quedo con la hora
resultadosAPI[['year-mod','month-mod','day-mod']] = resultadosAPI.modified1.str.split("-",expand=True,)# Separa la fecha en año, mes día y crea las columnas
del resultadosAPI['modified']# Elimina la columna original
resultadosAPI.rename(columns = {'modified1':'modified'}, inplace = True) # cambia nombre a created de la columna corregida

#Modificar la columna doi para agregar el prefijo y cambiar el nombre
resultadosAPI[['doi']] = 'http://doi.org/' + resultadosAPI[['doi']].astype(str)
resultadosAPI.rename(columns = {'doi':'DOI_URL'}, inplace = True)


# Modificar el nombre de los type y pasarlo a español, n este paso toca crear un nuevo DataFrame para guardar el resultado del remplazo
resultadosAPI['type']=resultadosAPI['type'].replace('OCCURRENCE', 'Registros biologicos').replace('CHECKLIST', 'Listas de especies').replace('METADATA', 'Metadatos').replace('SAMPLING_EVENT', 'Eventos de muestreo')

# Llamado al API de GBIF API para extraer la informacion del nombre de la organizacion y ponerla en una nueva columna organization
api_organization=resultadosAPI[["key","publishingOrganizationKey"]] # subset de los datos con las columnas a llamar
dfo=api_organization.drop_duplicates('publishingOrganizationKey',inplace=False) # quitar los duplicados para que se más rápido el llamado
#Define the gbif API call for publishingOrganizationKey
def call_gbif_title(row):
    try:
        url = "http://api.gbif.org/v1/organization/"+ str(row['publishingOrganizationKey'])      
        response = (requests.get(url).text)
        response_json = json.loads(response)
        time.sleep(0.005)
        return response_json
    except Exception as e:
        raise e

dfo['API_response'] = dfo.apply(call_gbif_title,axis=1)
norm_ok = json_normalize(dfo['API_response'])
ok = norm_ok[['key','title']]
ok.rename(columns={'title': 'organization', 'key': 'publishingOrganizationKey'}, inplace=True)

#merge API call's to dataset
resultadosAPI=pd.merge(resultadosAPI,ok,how='left',on='publishingOrganizationKey')


# Llamado al API de GBIF API para extraer la informacion endpoints

api_dataset=resultadosAPI[["key"]] # subset de los datos con las columnas a llamar
dfdk=api_dataset.drop_duplicates('key',inplace=False)

#Define the gbif API call for publishingOrganizationKey
def call_gbif_endpoints(row):
    try:
        url = "http://api.gbif.org/v1/dataset/"+ str(row['key'])      
        response = (requests.get(url).text)
        response_json = json.loads(response)
        time.sleep(0.005)
        return response_json
    except Exception as e:
        raise e

dfdk['API_response'] = dfdk.apply(call_gbif_endpoints,axis=1)
norm_dk = json_normalize(dfdk['API_response'])
dk = norm_dk[['key','endpoints']]

# Paso intermedio para guardar toda la informacion de dk en un nuevo dataframe
dfEndpoints = dk
# Separar el llamado de los varios diccionarios a 4 columnas, aquí está la magia https://stackoverflow.com/questions/64037243/pythonhow-to-split-column-into-multiple-columns-in-a-dataframe-and-with-dynamic
d = [pd.DataFrame(dfEndpoints[col].tolist()).add_prefix(col) for col in dfEndpoints.columns]
dfEndpoints = pd.concat(d, axis=1)


# Extraer el json dentro del dataframe
dfEndpoints[['keyInterna','type','url','createdBy','modifiedBy','created','modified','machineTags']] = json_normalize(dfEndpoints['endpoints0'])
# Cambiar el nombre de la columna llave-key
dfEndpoints.rename(columns={'key0': 'key'}, inplace=True)


# Separar la URL para extraer el IPT y el nombrecorto

dfEndpoints[['PrevioIPT','nombrecorto']] = dfEndpoints.url.str.split("=",expand=True,) # Separa la URL usando = para dejar solo el nombre corto
dfEndpoints[['IPTtemporal','desctarteIPT']] = dfEndpoints.PrevioIPT.str.split("/archive",expand=True,) # Separa la URL del IPT quitando el /archive
dfEndpoints['IPTtemporal']=dfEndpoints.IPTtemporal.str.replace('//', '-') # Reemplazar solamente una parte del string para quitar el // y poder separarlos en el siguiente paso
dfEndpoints[['IPTdescartado','IPT']] = dfEndpoints.IPTtemporal.str.split("/",expand=True,)

# Dejar solamente el resultado deseado
dfEndpoints = dfEndpoints[["key", "nombrecorto", "IPT"]]

#merge resultado de nombre corto e IPT

resultadosAPI=pd.merge(resultadosAPI,dfEndpoints,how='left',on='key')

#### 5 Obtener número de registros por conjunto de datos

### 5.1 a 5.3

## URL para el llamado a los conjuntos de datos
ApiNumeroRegistros = 'http://api.gbif.org/v1/occurrence/search?publishingCountry=CO&limit=0&facet=datasetKey&facetLimit=3000'
# download the data https://stackoverflow.com/questions/50531308/valueerror-arrays-must-all-be-same-length

# Realizar el llamado a la URL y guardarlos
llamadoApiNumeroRegistros = requests.get(url=ApiNumeroRegistros)
# Convertir el llamado a un JSON
JSON_NumeroRegistros = json.loads(llamadoApiNumeroRegistros.content)
# Extrare del JSON la parte correspondiente al diccionario de facets
JSON_Normalizado = pd.DataFrame.from_dict(JSON_NumeroRegistros['facets'][0])
# Normalizar el JSON y dejar el archivo organizado
numberOfRecords_AAAAMMDD = pd.json_normalize(JSON_Normalizado['counts'])

### 5.4 Modificar el nombre de las columnas

numberOfRecords_AAAAMMDD.rename(columns = {'name':'key', 'count':'numberOfRecords'}, inplace = True)

### 5.5 Extraer el número de registros por dataset y anexarlo al recurso a exportar

resultadosAPI=pd.merge(resultadosAPI,numberOfRecords_AAAAMMDD,how='left',on='key')


#### 6 Asignar el tipo de organizacion publicadora a cada conjunto de datos
# Antes de realizar este paso asegurese de haber cargado los datos al comienzo del archivo y haber seleccionado las columnas a importar

resultadosAPI=pd.merge(resultadosAPI,OrganizacionesRegistradas_AAAAMMDD,how='left',on='publishingOrganizationKey')# Cruzar a informacion desde el archivo de organizaciones


#### 7 Rastrear los Recursos_ACtualizados durante el mes

datasetCO_Anterior_AAAAMMDD = datasetCO_Anterior_AAAAMMDD[['key','numberOfRecords']] #Quedarse solo con la columna del número de registros
datasetCO_Anterior_AAAAMMDD.rename(columns={'numberOfRecords': 'Indexacion_MesAnterior'}, inplace=True) # modificar el nombre de la columna numberOfRecors
#Unir los datos del mes anterior con el dataset general
resultadosAPI=pd.merge(resultadosAPI,datasetCO_Anterior_AAAAMMDD,how='left',on='key')


# Evaluar los datos del presente mes con los datos del mes anterior
resultadosAPI.loc[resultadosAPI['numberOfRecords'] == resultadosAPI['Indexacion_MesAnterior'], 'Cambios_Datos'] = '1' # Crear la nueva columna evaluando si son iguales y poniendo 0
resultadosAPI.loc[resultadosAPI['numberOfRecords'] != resultadosAPI['Indexacion_MesAnterior'], 'Cambios_Datos'] = '0' # Modificar la columna para poner 0 sino son iguales

# Poner como 1 el valor de la actualizacion para las listas que al no tener número de registros dan 0
resultadosAPI.loc[resultadosAPI['type'] == 'Listas de especies', 'Cambios_Datos'] = '1'

# Crear la nueva columna indicando que son actualizaciones
resultadosAPI.loc[resultadosAPI['Cambios_Datos'] == '0', 'Actualizaciones'] = 'Actualizacion'

# Crear la nueva columna Cambios_Datos

resultadosAPI['Incremento_Actualizacion'] = resultadosAPI['numberOfRecords'] - resultadosAPI['Indexacion_MesAnterior']


#### 8 llamar el número de citas

api_cites=resultadosAPI[["publishingOrganizationKey"]] # subset de los datos por la llave de organizacion
apiCallOrg1=api_cites.drop_duplicates('publishingOrganizationKey',inplace=False) # quitar los duplicados para que se más rápido el llamado
#Define the gbif API call for publishingOrganizationKey
def call_gbif_cites(row):
    try:
        url = 'https://www.gbif.org/api/resource/search?contentType=literature&publishingOrganizationKey=' + str(row['publishingOrganizationKey'])      
        response = (requests.get(url).text)
        response_json = json.loads(response)
        time.sleep(0.005)
        return response_json
    except Exception as e:
        raise e

apiCallOrg1['API_response'] = apiCallOrg1.apply(call_gbif_cites,axis=1) # Llamada del API al interior del datafram
apiCallOrg1[['offset','limit','endOfRecords','count','results','final']] = json_normalize(apiCallOrg1['API_response']) # Normalizar la llamada y ponerla en las 6 columnas apropiadas

# Separar el llamado de los varios diccionarios a 8 columnas

apiCallOrg1 = apiCallOrg1[['publishingOrganizationKey','API_response']] 
apiCallOrg2 = [pd.DataFrame(apiCallOrg1[col].tolist()).add_prefix(col) for col in apiCallOrg1.columns]
apiCallOrg3 = pd.concat(apiCallOrg2, axis=1)
apiCallOrg3 = apiCallOrg3[['publishingOrganizationKey0','API_responsecount']]
apiCallOrg3.rename(columns={'publishingOrganizationKey0': 'publishingOrganizationKey'}, inplace=True)


apiCallOrg3['URL_Citaciones'] = 'https://www.gbif.org/resource/search?contentType=literature&publishingOrganizationKey='+apiCallOrg3['publishingOrganizationKey'] # Crear la columna con el enlace
apiCallOrg = apiCallOrg3[['publishingOrganizationKey','API_responsecount','URL_Citaciones']] # Modificar el dataset para conservar solo las columnas que se van a unir
apiCallOrg.rename(columns={'API_responsecount': 'total_citesOrg'}, inplace=True) # modificar el nombre de la columna count

#Unir la llamada del API Cites con el dataset general
resultadosAPI=pd.merge(resultadosAPI,apiCallOrg,how='left',on='publishingOrganizationKey')


#### 8.5 Eliminar los datos del mes si se hace el llamado posteriormente



#### 9 Exportar el resultado final completo
# Organizacion del resultado del archivo final SIN REALIZAR DE AQUÍ PARA ABAJO
resultadosFinales = resultadosAPI[[ "key", "total_citesOrg", "URL_Citaciones", "Actualizaciones", "Incremento_Actualizacion", "Cambios_Datos", "Indexacion_MesAnterior", "IPT", "numberOfRecords", "nombrecorto", "type", "organization", "title", "DOI_URL", "created", "year", "month", "day", "modified", "year-mod", "month-mod", "day-mod", "version", "NombreCorto_Org", "Logo", "typeOrg", "publishingOrganizationKey", "URLSocio"]]
# Exportar resultado hasta el momento como datasetCO_AAAAMMDD
#(MM: mes del reporte, DD:  el último día del mes de reporte,  así el proceso se esté realizando en días posteriores)

today = date.today() # Llamar la fecha del día en el que estamos para que el resultado final salga con el nombre correcto
x='datasetCO_' + str(today)+'.csv' # Darle el nombre al archivo
resultadosFinales.to_csv('/Users/estebanmarentes/Desktop/EstebanMH/Recursos_AC/reporteMensual/resultadoReporte/' + x, sep="\t", encoding = "utf8")
y='datasetCO_' + str(today)+'.xlsx'
resultadosFinales.to_excel('/Users/estebanmarentes/Desktop/EstebanMH/Recursos_AC/reporteMensual/resultadoReporte/' + y)

#### 10 Exportar los datos parciales que toca pegar en el dataStudio


# Tipo de publicador + Logo

resultadosTipoPublicador = resultadosFinales[[ "organization","typeOrg", "URLSocio", "publishingOrganizationKey"]]

x='datasetCO_20220531TipoPublicador.xlsx'
resultadosTipoPublicador.to_excel('/Users/estebanmarentes/Desktop/EstebanMH/Recursos_AC/reporteMensual/resultadoReporte/' + x)


# Todos los publicadores/Tipo de publicador / N. Registros / Total-Tipo

resultadosNumeroRegistros = resultadosFinales[[ "organization","typeOrg", "numberOfRecords", "type"]]
resultadosNumeroRegistros.sort_values('organization')

x='datasetCO_20220531NumeroRegistros.xlsx'
resultadosNumeroRegistros.to_excel('/Users/estebanmarentes/Desktop/EstebanMH/Recursos_AC/reporteMensual/resultadoReporte/' + x)


# Cifras uso de datos (citaciones)
resultadosCitaciones = resultadosFinales[[ "organization","typeOrg", "total_citesOrg", "URL_Citaciones"]]


resultadosCitaciones=resultadosCitaciones.drop_duplicates('organization',inplace=False)
x='datasetCO_20220531Citaciones.xlsx'
resultadosCitaciones.to_excel('/Users/estebanmarentes/Desktop/EstebanMH/Recursos_AC/reporteMensual/resultadoReporte/' + x)



#### Ha finalizado el proceso del reporte mensual en este script de python
#### vaya al documento Procedimiento Reporte Mensual(https://docs.google.com/document/d/1CLz-BB5RDktcbEkTkP19PcsuhRyGOEprKR2stbQn9o8/edit#heading=h.hl6go1ovc7z8) para continuar con el proceso



## Cosas que faltan por hacer: 
# Colocar unas variables para seleccionar el año y mes del reporte y elegir las filas que son de este mes
# con eso creamos una nueva columna y tambien le agregamos las actualizaciones

# borrar el recurso del CIAT

# Mejorar la exportación de los datos de la segunda parte del reporte, para que exporte solo lo que este realmente

# Hay unos datos que toca sacer en excel (suma total registros publicados, registros publicados por socio en este periodo, número de tipo de acompañamiento por socio en este periodo)
# Crear diferentes filas del escript para esto para que salga todo desde este script y no toque abrir el excel para casi nada

# Yo veo bastante complicado crear la hoja de excel del mes por la estructura que tiene desde este script, pero se puede evaluar en un futuro lejano
