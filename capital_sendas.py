"""
Capital Sendas

Este script procesa los reportes de DGH "produccion_AAAAMMDD_AAAAMMDD.xlsx", "bases_norte.xlsx" para generar el archivo "Capital_sendas.xlsx".

Pasos del proceso:
1. Carga de archivos.
2. Procesamiento de datos.
3. Aplicación de reglas de validación.
4. Descarga de archivos resultantes.
"""

# %% Cargar archivos

# Convertir en df

# Cargar librerias
import pandas as pd
import numpy as np
import requests
import duckdb
import os

print('Cargando archivos...')
# Conectar a DuckDB y cargar los xlsx a df
con = duckdb.connect()
con.sql("INSTALL spatial; LOAD spatial;")

# Función descargaExcel para descargar los excel compartidos en OneDrive 365
def descargaExcel(url):
    """
    Descarga un archivo Excel desde una URL de OneDrive 365 y lo carga en un DataFrame.
    
    Args:
    - url (str): URL del archivo Excel en OneDrive 365.
    
    Returns:
    - df (DataFrame): DataFrame con los datos del archivo Excel descargado.
    """

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
print('- Cargando Códigos')
dfCodigos = descargaExcel("https://subredeintenorte-my.sharepoint.com/:x:/g/personal/mercadeo_subrednorte_gov_co/EcLsPJKWhwxIoljSAm24vB8BouYTCUF1__tXxPVwDn44aA?e=WNkZxt")

# Cargar Anexos Capital Salud unificados 2023 de OneDrive
print('- Cargando Anexos')
dfAnexos = descargaExcel("https://subredeintenorte-my.sharepoint.com/:x:/g/personal/mercadeo_subrednorte_gov_co/EdjY3dEvXXFHod9G_nNByAYBiNlxWKem41zHWM1y2vM3Cw")

# Cargar Codigo tipologia de de OneDrive
print('- Cargando Tipologia')
dfTipologia = descargaExcel("https://subredeintenorte-my.sharepoint.com/:x:/g/personal/mercadeo_subrednorte_gov_co/EcJnfLQcpo1IhICDndY709kBtCTVQQ5t2bkRyw4PPA3U9w")

# Cargar Bases norte
print('- Cargando bases_norte')
dfBases = descargaExcel("https://subredeintenorte-my.sharepoint.com/:x:/g/personal/mercadeo_subrednorte_gov_co/EXBZ0Ym5E1lJpRDK6W48trMBFbExvI0oO7Us7EhWa2ph4g?e=EZcGQn")

# listar los archivos en el directorio actual que comiencen con "produccion" y terminen con ".xlsx"
archivos = [f for f in os.listdir('.') if f.startswith('produccion') and f.endswith('.xlsx')]
# Convertir la lista de archivos en un dataframe
dfArchivos = pd.DataFrame(archivos, columns=['Archivo'])
# Separar la columna 'Archivo' usando '_' como separador, y solo guardar la columna 2 en 'AnoMes'
dfArchivos['AnoMes'] = dfArchivos['Archivo'].str.split('_').str[1].str[:6]
# Filtrar el dataframe para quedarse solo con la fila que tiene la el valor máximo en la columna 'AnoMes'
dfArchivos = dfArchivos[dfArchivos['AnoMes'] == dfArchivos['AnoMes'].max()]
# Seleccionar los ultimos 2 digitos de 'AnoMes' y guardarlos en la variable 'Mes' como entero   
Mes = int(dfArchivos['AnoMes'].str[-2:].max())

# Cagar los archivos excel de la columna 'Archivo' en dfCapital_sendas
# Si hay más de un archivo, concatenarlos en un solo dataframe
# La primera fila de cada archivo es el encabezado
print('- Cargando Producción')
dfCapital_sendas = pd.DataFrame()
for archivo in dfArchivos['Archivo']:
    print('- -', archivo)
    dfTemp = con.query(f"SELECT * FROM st_read('{archivo}')").df()
    # la primera fila es el encabezado
    dfTemp.columns = dfTemp.iloc[0]
    dfTemp = dfTemp[1:]
    # Seleccionar las columnas necesarias
    dfTemp = dfTemp[['SEDE_NOMBRE','FACTURA','FECHA_FACT','DOC_PACIENTE','NOMBRE_PACIENTE','FEC_NACIMIENTO','GENERO','EDAD',
                     'SERVICIO','NOM_SERVICIO_PRODUCTO','FEC_SERVICIO','CANT_SERVICIO',
                     'COD_PLAN','NOM_PLAN','COD_ENTIDAD1','NOM_ENTIDAD1','AMBITO',
                     'DX_PRINCIPAL.0','DX_PRINCIPAL.1']]
    # Seleccionar las filas donde 'NOM_PLAN' contiene 'PGP'
    dfTemp = dfTemp[dfTemp['NOM_PLAN'].str.contains('PGP', na=False)]
    # seleecionar las filas donde 'FACTURA' no comienza por 'NS'
    dfTemp = dfTemp[~dfTemp['FACTURA'].str.startswith('SN', na=False)]
    # Concatenar los dataframes
    dfCapital_sendas = pd.concat([dfCapital_sendas, dfTemp], ignore_index=True)

# seleccionar las filas unicas
dfCapital_sendas = dfCapital_sendas.drop_duplicates()

# Cargar Facturacion rips
#print('- Cargando facturacion_rips.xlsx')
#dfFacRips = con.query("SELECT * FROM st_read('facturacion_rips.xlsx')").df()
# Cargar de Facturación total solo las columnas y los valores únicos necesarios
#print('- Cargando facturacion_total.xlsx')
#dfFacTotal = con.query("SELECT DISTINCT FACTURA as FACTURA, TIPO_DOC, GENERO, EDAD, CUMS FROM st_read('facturacion_total.xlsx')").df()
# Cargar Bases norte
#print('- Cargando bases_norte.xlsx')
#dfBases = con.query("SELECT * FROM st_read('bases_norte.xlsx')").df()

# %% Procesar datos
print('Procesando datos...')

# Crear dfCapital_sendas cruzando dfFacRips y dfFacTotal con 'FACTURA' y seleccionando solo la primera aparición
#dfCapital_sendas = pd.merge(
#    dfFacRips,
#    dfFacTotal.drop_duplicates(subset='FACTURA', keep='first'),
#    on=['FACTURA'], how='left')

# Convertir las columnas 'FEC_NACIMIENTO', 'FEC_SERVICIO' y 'FECHA_FACT' a tipo fecha
dfCapital_sendas['FEC_NACIMIENTO'] = pd.to_datetime(dfCapital_sendas['FEC_NACIMIENTO'].str.slice(0, 15), format='%a %b %d %Y')
dfCapital_sendas['FEC_SERVICIO'] = pd.to_datetime(dfCapital_sendas['FEC_SERVICIO'].str.slice(0, 15), format='%a %b %d %Y')
dfCapital_sendas['FECHA_FACT'] = pd.to_datetime(dfCapital_sendas['FECHA_FACT'].str.slice(0, 15), format='%a %b %d %Y')

# Seleccionar el mes de 'FECHA_FACT' igual a la variable 'Mes'
dfCapital_sendas = dfCapital_sendas[dfCapital_sendas['FECHA_FACT'].dt.month == Mes]

# Convertir 'EDAD' y 'CANT_SERVICIO' a entero
dfCapital_sendas['EDAD'] = dfCapital_sendas['EDAD'].astype(int)
dfCapital_sendas['CANT_SERVICIO'] = pd.to_numeric(dfCapital_sendas['CANT_SERVICIO'], errors='coerce').fillna(0).astype(int)

# Agregar columnas de dfCodigos a dfCapital_sendas

# Agregar columnas 'CONCEPTO' y 'GRUPO QX' de dfCodigos a dfCapital_sendas cruzando con 'SERVICIO'
dfCapital_sendas = pd.merge(
    dfCapital_sendas,
    dfCodigos[['SERVICIO', 'CONCEPTO', 'GRUPO QX']].drop_duplicates(),
    on=['SERVICIO'], how='left')

# Calcular columnas de dfCapital_sendas

# Agregar otras columnas a dfCapital_sendas
dfCapital_sendas['EDAD 1'] = (dfCapital_sendas['FEC_SERVICIO'] - dfCapital_sendas['FEC_NACIMIENTO']).apply(
    lambda x: x.days // 365 if x.days >= 365 else (x.days // 30 if x.days >= 30 else x.days))
dfCapital_sendas['EDAD 2'] = (dfCapital_sendas['FEC_SERVICIO'] - dfCapital_sendas['FEC_NACIMIENTO']).apply(
    lambda x: 'Años' if x.days >= 365 else ('Meses' if x.days >= 30 else 'Días'))

# Agregar columna de dfTipologia a dfCapital_sendas

# Agregar columnas 'tipologia' de dfTipologia a dfCapital_sendas cruzando con 'SERVICIO'
dfCapital_sendas = pd.merge(
    dfCapital_sendas,
    dfTipologia[['SERVICIO', 'tipologia']].drop_duplicates(subset='SERVICIO', keep='first'),
    on=['SERVICIO'], how='left')

# Agregar columnas de dfAnexos a dfCapital_sendas

# Crea dfTemporal cruzando dfCapital_sendas y dfAnexos
dfTemporal = pd.merge(
    dfCapital_sendas[['GENERO', 'EDAD', 'SERVICIO']].drop_duplicates(),
    dfAnexos[['CUPS', 'TIPOLOGIA NOMBRE']].drop_duplicates(),
    left_on=['SERVICIO'], right_on=['CUPS'], how='left').drop(columns=['CUPS'])

# Asegúrate de que no haya NaN en 'TIPOLOGIA NOMBRE'
dfTemporal['TIPOLOGIA NOMBRE'] = dfTemporal['TIPOLOGIA NOMBRE'].fillna('')

# Agregar la columna 'Contiene' con el valor 1 si 'TIPOLOGIA NOMBRE' contiene 'PEDIATRIA' o 'GINECOLOGIA', de lo contrario 2
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
    """
    Asigna la tipología según las reglas especificadas.
    
    Args:
    - row (Series): Fila del DataFrame dfTemporal.
    
    Returns:
    - tipologia (str): Valor de la tipología asignada.
    """

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
dfTemporal = dfTemporal.groupby(['GENERO', 'EDAD', 'SERVICIO'], dropna=False).first().reset_index()

# Agregar columna 'TIPOLOGIA' de dfTemporal a dfCapital_sendas cruzando con 'SERVICIO' y 'CUPS'
dfCapital_sendas = pd.merge(
    dfCapital_sendas,
    dfTemporal[['GENERO', 'EDAD', 'SERVICIO', 'TIPOLOGIA NOMBRE']],
    on=['GENERO', 'EDAD', 'SERVICIO'], how='left')

# Agregar columnas de dfBases a dfCapital_sendas

# Agregar columna 'ips' de dfBases a dfCapital_sendas cruzando con 'SERVICIO' y 'CUPS' y seleccionando solo la primera aparición
dfCapital_sendas = pd.merge(
    dfCapital_sendas,
    dfBases.drop_duplicates(subset='documento', keep='first'), left_on=['DOC_PACIENTE'],
    right_on=['documento'], how='left').drop(columns=['documento'])

# Agregar columna 'DOC_PACIENTE' de dfCapital_sendas a dfComprobar cuando no tiene 'ips'
dfComprobar = dfCapital_sendas[dfCapital_sendas['ips'].isna()][['DOC_PACIENTE', 'NOMBRE_PACIENTE']].drop_duplicates()

# Funcion para separar nombres y apellidos
def separar_nombres(nombre_completo):
    """
    Separa un nombre completo en nombres y apellidos.
    
    Parámetros:
    - nombre_completo (str): Nombre completo a separar.
    
    Retorna:
    - tuple: Tupla con los nombres y apellidos separados.
    """

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
dfComprobar[['nombre1', 'nombre2', 'apellido1', 'apellido2']] = dfComprobar['NOMBRE_PACIENTE'].astype(str).apply(separar_nombres).apply(pd.Series)

# Eliminar la columna 'NOMBRE_PACIENTE'
dfComprobar = dfComprobar.drop(columns=['NOMBRE_PACIENTE'])

# %% Reglas

# Crear columna 'validacion' con valor 0
dfCapital_sendas['validacion'] = 0

# Regla Quirófano

# De dfCapital_sendas filtrar por 'GRUPO QX' que comience por 'Grupo 'y seleccionar las columnas 'FACTURA', 'FEC_SERVICIO', 'GRUPO QX' y crear dfTemporal
dfTemporal = dfCapital_sendas[
    dfCapital_sendas['GRUPO QX'].fillna('').str.startswith('Grupo ')][[
        'FACTURA', 'FEC_SERVICIO', 'GRUPO QX', 'validacion']]

# De 'FEC_SERVICIO' extraer solo la fecha sin la hora
dfTemporal['FEC_SERVICIO'] = dfTemporal['FEC_SERVICIO'].dt.date

# Ordenar dfTemporal por 'FACTURA', 'FEC_SERVICIO' ascendentes y por 'GRUPO QX' descendente
dfTemporal = dfTemporal.sort_values(by=['FACTURA', 'FEC_SERVICIO', 'GRUPO QX'], ascending=[True, True, False])

# Función validacion_Qx
#  ≤ 3 registros en la misma 'FACTURA', 'FEC_SERVICIO', colocar 'validacion' = 1
#  > 3 registros en la misma 'FACTURA', 'FEC_SERVICIO', colocar 'validacion' = 1 para los 2 registros del mayor 'GRUPO QX' y 1 del siguiente mayor 'GRUPO QX'
def validacion_Qx(grupo):
    """
    Aplica la regla de validación para quirófanos.
    
    Args:
    - grupo (DataFrame): Grupo de registros con la misma 'FACTURA' y 'FEC_SERVICIO'.
    
    Returns:
    - grupo (DataFrame): Grupo de registros con la columna 'validacion' actualizada.
    """

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

# Aplicar la función validacion_Qx a cada grupo 'FACTURA', 'FEC_SERVICIO' de dfTemporal
dfTemporal = dfTemporal.groupby(['FACTURA', 'FEC_SERVICIO']).apply(validacion_Qx, include_groups=False).reset_index(
    level = ['FACTURA', 'FEC_SERVICIO'], drop=False)

# Actualizar los valores de 'validacion' de dfCapital_sendas a partir de dfTemporal
dfCapital_sendas.update(dfTemporal[['validacion']])

# Regla Egreso

# De dfCapital_sendas filtrar por 'GRUPO QX' que comience por 'Grupo 'y seleccionar las columnas 'FACTURA', 'FEC_SERVICIO', 'GRUPO QX' y crear dfTemporal
dfTemporal = dfCapital_sendas[
    dfCapital_sendas['CONCEPTO'].fillna('').str.startswith(('UCI ', 'HOSPITALIZACION GENERAL', 'U.SALUD MENTAL'))][[
        'FACTURA', 'CONCEPTO', 'validacion']]

# Eliminar duplicados de 'FACTURA' y 'CONCEPTO'
dfTemporal = dfTemporal.drop_duplicates(subset=['FACTURA', 'CONCEPTO'], keep='first')

# Actualizar 'validacion' a 1
dfTemporal['validacion'] = 1

# Actualizar los valores de 'validacion' de dfCapital_sendas a partir de dfTemporal
dfCapital_sendas.update(dfTemporal[['validacion']])

# Regla Ambulatorio

# Para tipologia C1
# De dfCapital_sendas filtrar por 'COD_PLAN' los que comiencen por PGP y seleccionar las columnas 'DOC_PACIENTE', 'FEC_SERVICIO' y crear dfTemporal
dfTemporal = dfCapital_sendas[
    (dfCapital_sendas['tipologia'] == 'C1') & 
    (dfCapital_sendas['COD_PLAN'].fillna('').str.startswith('PGP'))][[
        'DOC_PACIENTE', 'FEC_SERVICIO']]

# Eliminar duplicados de 'DOC_PACIENTE' y 'FEC_SERVICIO'
dfTemporal = dfTemporal.drop_duplicates(subset=['DOC_PACIENTE', 'FEC_SERVICIO'], keep='first')

# Actualizar 'validacion' a 1
dfTemporal['validacion'] = 1

# Actualizar los valores de 'validacion' de dfCapital_sendas a partir de dfTemporal
dfCapital_sendas.update(dfTemporal[['validacion']])

# Para tipologia C4
# De dfCapital_sendas 'validacion' es 1 para todos excepto para 'SERVICIO' con valor 890502
dfCapital_sendas.loc[
    (dfCapital_sendas['tipologia'] == 'C4') & 
    (dfCapital_sendas['SERVICIO'] != '890502'), 'validacion'] = 1

# De dfCapital_sendas para 'SERVICIO' con valor 890502, 'validacion' es igual al valor de 'CANT_SERVICIO'
dfCapital_sendas.loc[
    (dfCapital_sendas['tipologia'] == 'C4') & 
    (dfCapital_sendas['SERVICIO'] == '890502'), 'validacion'] = dfCapital_sendas['CANT_SERVICIO']

# Para tipologia C7
# De dfCapital_sendas 'validacion' es igual al valor de 'CANT_SERVICIO'
dfCapital_sendas.loc[
    (dfCapital_sendas['tipologia'] == 'C7'), 'validacion'] = dfCapital_sendas['CANT_SERVICIO']

# Para tipologia C8
# De dfCapital_sendas 'validacion' es igual al valor de 'CANT_SERVICIO'
dfCapital_sendas.loc[
    (dfCapital_sendas['tipologia'] == 'C8'), 'validacion'] = dfCapital_sendas['CANT_SERVICIO']

# %% Descargar los archivos
print('Descargando archivos...')

# Convertir las columnas tipo fecha/hora a solo fecha en texto
dfCapital_sendas['FECHA_FACT'] = dfCapital_sendas['FECHA_FACT'].dt.strftime('%Y/%m/%d')
dfCapital_sendas['FEC_SERVICIO'] = dfCapital_sendas['FEC_SERVICIO'].dt.strftime('%Y/%m/%d')

# Columnas
Columnas = ['SEDE','SEDE_NOMBRE','FACTURA','FECHA_FACT','TIPO_FACTURA','INGRESO','FEC_INGRESO','COD_USU_FACTURADOR','NOM_FACTURADOR','DOC_PACIENTE',
            'PACTIPDOC','TIPO_DOC','ESTADO_PAC','NOMBRE_PACIENTE','COD_PACIENTE','FEC_NACIMIENTO','GENERO','EDAD','ESTRATO','NOM_ESTRATO',
            'VALOR_ENTIDAD','VALOR_PACIENTE','SERVICIO','CUMS','PRODUCTO','NOM_SERVICIO_PRODUCTO','FEC_SERVICIO','CANT_SERVICIO','VALOR_UNITARIO','VALOR_TOTAL',
            'COD_PLAN','NOM_PLAN','COD_MEDICO','NOM_MEDICO','CENTRO_DE_COSTO','NOM_CENTROCOS','SIPCODCUP','COD_ENTIDAD1','NOM_ENTIDAD1','NUM_EGRESO',
            'CODIGO_CUMS','GMETIPMED','SFATIPDOC','PRODUCTO_SERVICIO','AMBITO','DX_PRINCIPAL.0','DX_PRINCIPAL.1','CONCEPTO','GRUPO QX','EDAD 1',
            'EDAD 2','tipologia','TIPOLOGIA NOMBRE','ips','validacion']
# Columnas a publicar
Columnas = ['SEDE_NOMBRE','FACTURA','FECHA_FACT',
            'GENERO',
            'SERVICIO','NOM_SERVICIO_PRODUCTO','FEC_SERVICIO','CANT_SERVICIO','COD_PLAN',
            'NOM_PLAN','COD_ENTIDAD1','NOM_ENTIDAD1',
            'AMBITO','DX_PRINCIPAL.0','DX_PRINCIPAL.1','CONCEPTO','GRUPO QX','EDAD 1','EDAD 2',
            'tipologia','TIPOLOGIA NOMBRE','ips','validacion']

# Seleccionar las columnas
dfCapital_sendas = dfCapital_sendas[Columnas]

# Convertir los df a xlsx
print('-capital_sendas.xlsx')
con.execute("COPY (SELECT * FROM dfCapital_sendas) TO 'capital_sendas.xlsx' WITH (FORMAT GDAL, DRIVER 'xlsx');")
#con.execute("COPY (SELECT * FROM dfCapital_sendas) TO 'capital_sendas.csv' (HEADER, DELIMITER '|');")
print('-comprobar.csv')
con.execute("COPY (SELECT * FROM dfComprobar) TO 'comprobar.csv' WITH (HEADER, DELIMITER ',');")

# %%
