# Capital Sendas
#### Se procesan los reportes de DGH "facturacion_total.xlsx", "facturacion_rips.xlsx" y "bases_norte.xlsx" generando como resultado el archivo "Capital_sendas.xlsx"

# %% Cargar archivos

# Convertir en df

# Cargar librerias
import pandas as pd
import requests
import io
import duckdb

print('Cargando archivos')
# Cargar Codigos consultas de Google Sheet
print('-Códigos')
url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTev0VnRGqk48QqiFXpkzYbHlgkiqdzcPLmbBclTAs8oHnWc_ldYB-5PB9wfv_RH5cmbYMLaxJHcXnc/pub?gid=376848632&single=true&output=csv"
data = requests.get(url).content
dfCodigos = pd.read_csv(io.StringIO(data.decode("utf-8")), dtype=str)

# Cargar Anexos Capital Salud unificados 2023 de Google Sheet
print('-Anexos')
url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vQKgHVUjz81og_o-HBHr8VgVxiOyRQMpo36zoX_Ckpf31dQMR2ocCRFUyU0BBPqfPT5Wemrd-lQH7Qf/pub?gid=663714319&single=true&output=csv"
data = requests.get(url).content
dfAnexos = pd.read_csv(io.StringIO(data.decode("utf-8")), dtype=str)

# Conectar a DuckDB y cargar los xlsx a df
con = duckdb.connect()
con.sql("INSTALL spatial; LOAD spatial;")
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
dfCapital_sendas['NOMBRE_SERVICIO'] = dfCapital_sendas['ServicioNombre']

# Agregar columnas de dfCodigos a dfCapital_sendas

# Agregar columnas 'CONCEPTO' y 'GRUPO QX' de dfCodigos a dfCapital_sendas cruzando con 'SERVICIO'
dfCapital_sendas = pd.merge(dfCapital_sendas, dfCodigos[['SERVICIO', 'CONCEPTO', 'GRUPO QX']].drop_duplicates(), on=['SERVICIO'], how='left')

# Calcular columnas de dfCapital_sendas

# Agregar otras columnas a dfCapital_sendas
dfCapital_sendas['EDAD 1'] = (dfCapital_sendas['FEC_SERVICIO'] - dfCapital_sendas['FECHA_NACIMIENTO']).apply(lambda x: x.days // 365 if x.days >= 365 else (x.days // 30 if x.days >= 30 else x.days))
dfCapital_sendas['EDAD 2'] = (dfCapital_sendas['FEC_SERVICIO'] - dfCapital_sendas['FECHA_NACIMIENTO']).apply(lambda x: 'Años' if x.days >= 365 else ('Meses' if x.days >= 30 else 'Días'))

# Agregar columnas de dfAnexos a dfCapital_sendas

# Agregar columna 'TIPOLOGIA' de dfAnexos a dfCapital_sendas cruzando con 'SERVICIO' y 'CUPS' y seleccionando solo la primera aparición
dfCapital_sendas = pd.merge(dfCapital_sendas, dfAnexos[['CUPS', 'TIPOLOGIA']].drop_duplicates(subset='CUPS', keep='first'), left_on=['SERVICIO'], right_on=['CUPS'], how='left').drop(columns=['CUPS'])

# Agregar columnas de dfBases a dfCapital_sendas

# Agregar columna 'ips' de dfBases a dfCapital_sendas cruzando con 'SERVICIO' y 'CUPS' y seleccionando solo la primera aparición
dfCapital_sendas = pd.merge(dfCapital_sendas, dfBases.drop_duplicates(subset='documento', keep='first'), left_on=['PacienteNit'], right_on=['documento'], how='left').drop(columns=['documento'])

# Agregar PacienteNit de dfCapital_sendas a dfComprobar

# Agregar columna 'PacienteNit' de dfCapital_sendas a dfComprobar cuando no tiene 'ips'
dfComprobar = dfCapital_sendas[dfCapital_sendas['ips'].isna()][['PacienteNit']].drop_duplicates()

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