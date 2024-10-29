# Capital Sendas
#### Se procesan los reportes de DGH "facturacion_total.xlsx", "facturacion_rips.xlsx" y "bases_norte.xlsx" generando como resultado el archivo "Capital_sendas.xlsx"

# %% Cargar archivos

# Convertir en df

# Cargar librerias
import pandas as pd
import numpy as np
import requests
import duckdb
import os

print('Cargando archivos')
# Conectar a DuckDB y cargar los xlsx a df
con = duckdb.connect()
con.sql("INSTALL spatial; LOAD spatial;")

# Función descargaExcel para descargar los excel compartidos en OneDrive 365
def descargaExcel(url):
    # Reemplazar la parte después del ? con download=1
    url = url.split('?')[0] + '?download=1'

    # Descargar el Excel
    data = requests.get(url)

    # Verificar si la descarga fue exitosa
    if data.status_code == 200:
        # Guardar el contenido en un archivo temporal
        with open("temp.xlsx", "wb") as file:
            file.write(data.content)

        # Leer el archivo temporal con pandas
        df = con.query("SELECT * FROM st_read('temp.xlsx')").df()

        # Eliminar el archivo temporal (opcional)
        os.remove("temp.xlsx")
        return df
    else:
        print(f"Error al descargar el archivo: {data.status_code}")
        return False

# Cargar Codigos consultas de OneDrive
print('-Códigos')
dfCodigos = descargaExcel("https://subredeintenorte-my.sharepoint.com/:x:/g/personal/mercadeo_subrednorte_gov_co/EcLsPJKWhwxIoljSAm24vB8BouYTCUF1__tXxPVwDn44aA?e=WNkZxt")

# Cargar Anexos Capital Salud unificados 2023 de de OneDrive
print('-Anexos')
dfAnexos = descargaExcel("https://subredeintenorte-my.sharepoint.com/:x:/g/personal/mercadeo_subrednorte_gov_co/EdjY3dEvXXFHod9G_nNByAYBiNlxWKem41zHWM1y2vM3Cw")

# Cargar Codigo tipologia de de OneDrive
print('-Tipologia')
dfTipologia = descargaExcel("https://subredeintenorte-my.sharepoint.com/:x:/g/personal/mercadeo_subrednorte_gov_co/EcJnfLQcpo1IhICDndY709kBtCTVQQ5t2bkRyw4PPA3U9w")

# Cargar Facturacion rips
print('-facturacion_rips.xlsx')
dfFacRips = con.query("SELECT * FROM st_read('facturacion_rips.xlsx')").df()
# Cargar de Facturación total solo las columnas y los valores únicos necesarios
print('-facturacion_total.xlsx')
dfFacTotal = con.query("SELECT DISTINCT FACTURA as NumeroFactura, TIPO_DOC, GENERO, EDAD, CUMS FROM st_read('facturacion_total.xlsx')").df()
# Cargar Bases norte
print('-bases_norte.xlsx')
dfBases = con.query("SELECT * FROM st_read('bases_norte.xlsx')").df()

# %% Procesar datos
print('Procesando datos')

# Crear dfCapital_sendas cruzando dfFacRips y dfFacTotal con 'NumeroFactura' y seleccionando solo la primera aparición
dfCapital_sendas = pd.merge(dfFacRips, dfFacTotal.drop_duplicates(subset='NumeroFactura', keep='first'), on=['NumeroFactura'], how='left')

# Repetir columnas de dfCapital_sendas

# Agregar otras columnas a dfCapital_sendas
dfCapital_sendas['AMBITO'] = dfCapital_sendas['IngresoPor']
dfCapital_sendas['FechaEgreso_'] = dfCapital_sendas['FechaEgreso']
dfCapital_sendas['TIPO_DOC_'] = dfCapital_sendas['TIPO_DOC']
dfCapital_sendas['DOC_PACIENTE_'] = dfCapital_sendas['PacienteNit']
dfCapital_sendas['NOMBRE_PACIENTE'] = dfCapital_sendas['PacienteNombre']
dfCapital_sendas['FECHA_NACIMIENTO'] = dfCapital_sendas['PacienteFechaNac']
dfCapital_sendas['FEC_SERVICIO'] = dfCapital_sendas['FechaServicio']
dfCapital_sendas['SERVICIO'] = dfCapital_sendas['ServicioCodigo']
dfCapital_sendas['NOMBRE_SERVICIO'] = dfCapital_sendas['ServicioNombre']

# Agregar columnas de dfCodigos a dfCapital_sendas

# Agregar columnas 'CONCEPTO' y 'GRUPO QX' de dfCodigos a dfCapital_sendas cruzando con 'SERVICIO'
dfCapital_sendas = pd.merge(dfCapital_sendas, dfCodigos[['SERVICIO', 'CONCEPTO', 'GRUPO QX']].drop_duplicates(), on=['SERVICIO'], how='left')

# Calcular columnas de dfCapital_sendas

# Agregar otras columnas a dfCapital_sendas
dfCapital_sendas['EDAD 1'] = (dfCapital_sendas['FEC_SERVICIO'] - dfCapital_sendas['FECHA_NACIMIENTO']).apply(lambda x: x.days // 365 if x.days >= 365 else (x.days // 30 if x.days >= 30 else x.days))
dfCapital_sendas['EDAD 2'] = (dfCapital_sendas['FEC_SERVICIO'] - dfCapital_sendas['FECHA_NACIMIENTO']).apply(lambda x: 'Años' if x.days >= 365 else ('Meses' if x.days >= 30 else 'Días'))

# Agregar columna de dfTipologia a dfCapital_sendas

# Agregar columnas 'tipologia' de dfTipologia a dfCapital_sendas cruzando con 'SERVICIO'
dfCapital_sendas = pd.merge(dfCapital_sendas, dfTipologia[['SERVICIO', 'tipologia']].drop_duplicates(subset='SERVICIO', keep='first'), on=['SERVICIO'], how='left')

# Agregar columnas de dfAnexos a dfCapital_sendas

# Crea dfTemporal cruzando dfCapital_sendas y dfAnexos
dfTemporal = pd.merge(dfCapital_sendas[['GENERO', 'EDAD', 'SERVICIO']].drop_duplicates(), dfAnexos[['CUPS', 'TIPOLOGIA NOMBRE']].drop_duplicates(), left_on=['SERVICIO'], right_on=['CUPS'], how='left').drop(columns=['CUPS'])

# Asegúrate de que no haya NaN en 'TIPOLOGIA NOMBRE'
dfTemporal['TIPOLOGIA NOMBRE'] = dfTemporal['TIPOLOGIA NOMBRE'].fillna('')

# Agregar la columna 'Contiene' con el valor 1 si 'TIPOLOGIA NOMBRE' contiene 'PEDIATRIA' o 'GINECOLOGIA', de lo contrario 1
dfTemporal['Contiene'] = np.where(dfTemporal['TIPOLOGIA NOMBRE'].str.contains('PEDIATRIA|GINECOLOGIA', case=False, na=False), 1, 2)

# Ordenar por 'SERVICIO', 'GENERO', 'EDAD' y 'Contiene'
dfTemporal = dfTemporal.sort_values(by=['SERVICIO', 'GENERO', 'EDAD', 'Contiene'])

# Función asignar_tipologia para asignar 'TIPOLOGIA NOMBRE' según las reglas especificadas
# revisar si hay una fila cuyo valor de 'TIPOLOGIA NOMBRE' contenga la palabra 'PEDIATRIA'
# y si 'EDAD' < 14 entonces asignar ese valor de 'TIPOLOGIA NOMBRE', 
# si no entonces revisar si hay una fila cuyo valor de 'TIPOLOGIA NOMBRE' contenga la palabra 'GINECOLOGIA'
# y si 'GENERO' = F entonces asignar ese valor de 'TIPOLOGIA NOMBRE',
# si no entonces identifica la primera fila cuyo valor de 'TIPOLOGIA NOMBRE' no contenga las palabras 'PEDIATRIA' o 'GINECOLOGIA'
# y asignar ese valor de 'TIPOLOGIA NOMBRE' de lo contrario asignar ''
def asignar_tipologia(row):
    # Filtrar por 'PEDIATRIA' y 'EDAD' < 14
    if 'PEDIATRIA' in row['TIPOLOGIA NOMBRE'] and row['EDAD'] < 14:
        return row['TIPOLOGIA NOMBRE']
    
    # Filtrar por 'GINECOLOGIA' y 'GENERO' = 'F'
    if 'GINECOLOGIA' in row['TIPOLOGIA NOMBRE'] and row['GENERO'] == 'F':
        return row['TIPOLOGIA NOMBRE']
    
    # Filtrar por 'TIPOLOGIA NOMBRE' que no contenga 'PEDIATRIA' o 'GINECOLOGIA'
    if not 'PEDIATRIA' in row['TIPOLOGIA NOMBRE'] and not 'GINECOLOGIA' in row['TIPOLOGIA NOMBRE']:
        return row['TIPOLOGIA NOMBRE']
    
    return ''

# Aplicar la función asignar_tipologia para crear la columna 'Valida'
dfTemporal['Valida'] = dfTemporal.apply(asignar_tipologia, axis=1)

# Eliminar loa 'Valida' = ''
# Aplicar la función asignar_tipologia para crear la columna 'Valida'
dfTemporal = dfTemporal[dfTemporal['Valida'] != '']

# Dejar solo la primera fila para cada grupo 'GENERO', 'EDAD', 'SERVICIO'
dfTemporal = dfTemporal.groupby(['GENERO', 'EDAD', 'SERVICIO']).first().reset_index()

# Agregar columna 'TIPOLOGIA' de dfTemporal a dfCapital_sendas cruzando con 'SERVICIO' y 'CUPS'
dfCapital_sendas = pd.merge(dfCapital_sendas, dfTemporal[['GENERO', 'EDAD', 'SERVICIO', 'TIPOLOGIA NOMBRE']], on=['GENERO', 'EDAD', 'SERVICIO'], how='left')

# Agregar columnas de dfBases a dfCapital_sendas

# Agregar columna 'ips' de dfBases a dfCapital_sendas cruzando con 'SERVICIO' y 'CUPS' y seleccionando solo la primera aparición
dfCapital_sendas = pd.merge(dfCapital_sendas, dfBases.drop_duplicates(subset='documento', keep='first'), left_on=['PacienteNit'], right_on=['documento'], how='left').drop(columns=['documento'])

# Agregar columna 'PacienteNit' de dfCapital_sendas a dfComprobar cuando no tiene 'ips'
dfComprobar = dfCapital_sendas[dfCapital_sendas['ips'].isna()][['PacienteNit', 'UsuarioNombre']].drop_duplicates()

# Funcion para separar nombres y apellidos
def separar_nombres(nombre_completo):
    # Separa los nombres en partes
    partes_ini = nombre_completo.split()
    partes = []
    parte = ''

    # Unifica los nombres compuestos
    for nombre in partes_ini:
        if nombre in ['DE', 'DEL', 'LA', 'LOS']:
            parte = parte + nombre + ' '
        else:
            parte = parte + nombre
            partes.append(parte)
            parte = ''
    
    # Decide las posiciones de los nombres
    if len(partes) == 4:
        return partes[0], partes[1], partes[2], partes[3]
    elif len(partes) > 4:
        return partes[0], ' '.join(partes[1:-2]), partes[-2], partes[-1]  # Ultimos 2 como apellidos, el resto como apellidos
    elif len(partes) == 3:
        return partes[0], '', partes[1], partes[2]  # Si falta un nombre
    elif len(partes) == 2:
        return partes[0], '', partes[1], ''  # Solo un nombre y un apellido
    else:
        return partes[0], '', '', ''  # Caso de un solo nombre

# Aplicar la función para separar nombres y apellidos
dfComprobar[['nombre1', 'nombre2', 'apellido1', 'apellido2']] = dfComprobar['UsuarioNombre'].apply(separar_nombres).apply(pd.Series)

# Eliminar la columna 'UsuarioNombre'
dfComprobar = dfComprobar.drop(columns=['UsuarioNombre'])

# %% Reglas

# Crear columna 'validacion' con valor 0
dfCapital_sendas['validacion'] = 0

# Regla Quirófano

# De dfCapital_sendas filtrar por 'GRUPO QX' que comience por 'Grupo 'y seleccionar las columnas 'NumeroFactura', 'FechaServicio', 'GRUPO QX' y crear dfTemporal
dfTemporal = dfCapital_sendas[dfCapital_sendas['GRUPO QX'].fillna('').str.startswith('Grupo ')][['NumeroFactura', 'FechaServicio', 'GRUPO QX', 'validacion']]

# De 'FechaServicio' extraer solo la fecha sin la hora
dfTemporal['FechaServicio'] = dfTemporal['FechaServicio'].dt.date

# Ordenar dfTemporal por 'NumeroFactura', 'FechaServicio' ascendentes y por 'GRUPO QX' descendente
dfTemporal = dfTemporal.sort_values(by=['NumeroFactura', 'FechaServicio', 'GRUPO QX'], ascending=[True, True, False])

# Función validacion_Qx
#  ≤ 3 registros en la misma 'NumeroFactura', 'FechaServicio', colocar 'validacion' = 1
#  > 3 registros en la misma 'NumeroFactura', 'FechaServicio', colocar 'validacion' = 1 para los 2 registros del mayor 'GRUPO QX' y 1 del siguiente mayor 'GRUPO QX'
def validacion_Qx(grupo):
    # Si hay más de 3 registros        
    # Inicializa contadores
    actualizados = 0
    actualizados_grupo = 0
    grupo_qx = ''
    # Valida cada registro
    for indice, fila in grupo.iterrows():
        # Valida que no se asignen más de 3 registros
        if actualizados < 3:
            # Valida que se sigue en el mismo 'GRUPO QX'
            if fila['GRUPO QX'] == grupo_qx:
                # Valida que no se asignen más de 2 registros por 'GRUPO QX'
                if actualizados_grupo < 2:
                    grupo.at[indice, 'validacion'] = 1
                    actualizados += 1
                    actualizados_grupo += 1
            else:
                # Por ser un nuevo grupo se asiga validación y se actualizan contadores
                grupo.at[indice, 'validacion'] = 1
                actualizados += 1
                actualizados_grupo = 1
                grupo_qx = fila['GRUPO QX']                
    return grupo

# Aplicar la función validacion_Qx a cada grupo 'NumeroFactura', 'FechaServicio' de dfTemporal
dfTemporal = dfTemporal.groupby(['NumeroFactura', 'FechaServicio']).apply(validacion_Qx).reset_index(level= ['NumeroFactura', 'FechaServicio'], drop=True)

# Actualizar los valores de 'validacion' de dfCapital_sendas a partir de dfTemporal
dfCapital_sendas.update(dfTemporal[['validacion']])

# Regla Egreso

# De dfCapital_sendas filtrar por 'GRUPO QX' que comience por 'Grupo 'y seleccionar las columnas 'NumeroFactura', 'FechaServicio', 'GRUPO QX' y crear dfTemporal
dfTemporal = dfCapital_sendas[dfCapital_sendas['CONCEPTO'].fillna('').str.startswith(('UCI ', 'HOSPITALIZACION GENERAL', 'U.SALUD MENTAL'))][['NumeroFactura', 'CONCEPTO', 'validacion']]

# Eliminar duplicados de 'NumeroFactura' y 'CONCEPTO'
dfTemporal = dfTemporal.drop_duplicates(subset=['NumeroFactura', 'CONCEPTO'], keep='first')

# Actualizar 'validacion' a 1
dfTemporal['validacion'] = 1

# Actualizar los valores de 'validacion' de dfCapital_sendas a partir de dfTemporal
dfCapital_sendas.update(dfTemporal[['validacion']])

# %% Descargar los archivos
print('Descargando archivos')

# Convertir las columnas tipo fecha/hora a solo fecha en texto
dfCapital_sendas['FechaFactura'] = dfCapital_sendas['FechaFactura'].dt.strftime('%Y/%m/%d')
dfCapital_sendas['PacienteFechaNac'] = dfCapital_sendas['PacienteFechaNac'].dt.strftime('%Y/%m/%d')
dfCapital_sendas['FechaServicio'] = dfCapital_sendas['FechaServicio'].dt.strftime('%Y/%m/%d')
dfCapital_sendas['FechaIngreso'] = dfCapital_sendas['FechaIngreso'].dt.strftime('%Y/%m/%d')
dfCapital_sendas['FechaEgreso'] = dfCapital_sendas['FechaEgreso'].dt.strftime('%Y/%m/%d')
dfCapital_sendas['FechaEgreso_'] = dfCapital_sendas['FechaEgreso_'].dt.strftime('%Y/%m/%d')

# Convertir los df a xlsx
print('-capital_sendas.xlsx')
con.execute("COPY (SELECT * FROM dfCapital_sendas) TO 'capital_sendas.xlsx' WITH (FORMAT GDAL, DRIVER 'xlsx');")
con.execute("COPY (SELECT * FROM dfComprobar) TO 'comprobar.csv' WITH (HEADER, DELIMITER ',');")

# %%
