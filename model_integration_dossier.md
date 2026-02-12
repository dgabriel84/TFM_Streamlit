# Dossier de Integración: Modelo de Predicción de Cancelaciones (LightGBM)

Este documento detalla técnicamente cómo se ha conectado el modelo de predicción `lightgbm.joblib` con la aplicación web (Streamlit). Está diseñado para servir como guía para replicar esta integración en cualquier otra plataforma web (por ejemplo, una desarrollada en Django, Flask, FastAPI o Node.js con un microservicio en Python).

## 1. Requisitos Previos

El entorno debe tener instaladas las siguientes librerías de Python, que son estrictamente necesarias para cargar el modelo y procesar los datos de la misma forma que se hizo durante el entrenamiento:

*   **`lightgbm`**: El framework del modelo.
*   **`joblib`**: Para serializar/deserializar el archivo del modelo.
*   **`pandas`**: Para la manipulación de datos y creación del DataFrame de entrada.
*   **`numpy`**: Para cálculos matemáticos en la ingeniería de características (seno/coseno).

```bash
pip install lightgbm joblib pandas numpy
```

## 2. Carga del Modelo

El modelo se carga directamente desde el sistema de archivos.

```python
import joblib
import os

# Ruta absoluta o relativa al archivo .joblib
MODEL_PATH = 'ruta/hacia/04_models/lightgbm_FINAL20260206_142533/lightgbm.joblib'

def load_model():
    if not os.path.exists(MODEL_PATH):
        raise FileNotFoundError(f"No se encuentra el modelo en: {MODEL_PATH}")
    # Se utiliza joblib para cargar el objeto del modelo
    model = joblib.load(MODEL_PATH)
    return model
```

## 3. Ingeniería de Características (El "Puente")

El modelo **NO** acepta los datos crudos del formulario web (fechas, strings simples). Requiere una transformación específica para generar las columnas matemáticas (senos, cosenos, ratios) que aprendió.

Esta lógica debe replicarse exactamente igual.

### 3.1 Entradas Requeridas
La función de transformación recibe estos parámetros crudos desde la web:
*   `LLEGADA` (datetime/string fecha)
*   `NOCHES` (int)
*   `PAX` (int)
*   `ADULTOS` (int)
*   `CLIENTE` (str)
*   `FECHA_TOMA` (datetime/string fecha)
*   `FIDELIDAD` (str/bool/None)
*   `PAIS` (str)
*   `SEGMENTO` (str)
*   `FUENTE_NEGOCIO` (str)
*   `NOMBRE_HOTEL` (str)
*   `NOMBRE_HABITACION` (str)
*   `VALOR_RESERVA` (float)

### 3.2 Transformaciones Clave
El siguiente código muestra cómo transformar esas entradas en un DataFrame listo para el modelo (`df_reservas_in`).

```python
import pandas as pd
import numpy as np

def get_features(LLEGADA, NOCHES, PAX, ADULTOS, CLIENTE, FECHA_TOMA, FIDELIDAD, PAIS, SEGMENTO, FUENTE_NEGOCIO, NOMBRE_HOTEL, NOMBRE_HABITACION, VALOR_RESERVA):
    
    # 1. Crear DataFrame inicial con datos crudos
    data = {
        'LLEGADA': [LLEGADA],
        'NOCHES': [NOCHES],
        'PAX': [PAX],
        'ADULTOS': [ADULTOS],
        'CLIENTE': [CLIENTE],
        'ID_MULTIPLE': 0, # Asumimos reserva individual por defecto
        'FECHA_TOMA': [FECHA_TOMA],
        'FIDELIDAD': [FIDELIDAD],
        'PAIS': [PAIS],
        'SEGMENTO': [SEGMENTO],
        'FUENTE_NEGOCIO': [FUENTE_NEGOCIO],
        'NOMBRE_HOTEL': [NOMBRE_HOTEL],
        'NOMBRE_HABITACION': [NOMBRE_HABITACION],
        'VALOR_RESERVA': [VALOR_RESERVA]
    }
    df = pd.DataFrame(data)

    # 2. Conversión de Tipos
    df['LLEGADA'] = pd.to_datetime(df['LLEGADA'])
    df['FECHA_TOMA'] = pd.to_datetime(df['FECHA_TOMA'])
    # Manejo de None/NaN en Fidelidad
    df['FIDELIDAD'] = df['FIDELIDAD'].replace({None: np.nan})

    # 3. Generación de Variables (Feature Engineering)
    
    # Ratios Económicos
    df['ADR'] = df['VALOR_RESERVA'] / df['NOCHES']
    df['REV_PAX'] = df['VALOR_RESERVA'] / df['PAX']
    
    # Tiempos (Lead Time)
    # Días entre reserva y llegada (normalizando a medianoche para evitar decimales por horas)
    df['LEAD_TIME'] = (df['LLEGADA'].dt.normalize() - df['FECHA_TOMA'].dt.normalize()).dt.days
    
    # Variables Cíclicas (Hora de Toma)
    df['HORA_TOMA'] = df['FECHA_TOMA'].dt.hour
    df['HORA_TOMA_SIN'] = np.sin(2 * np.pi * df['HORA_TOMA'] / 24)
    df['HORA_TOMA_COS'] = np.cos(2 * np.pi * df['HORA_TOMA'] / 24)
    
    # Variables Cíclicas (Mes de Llegada)
    df['MES_LLEGADA'] = df['LLEGADA'].dt.month
    df['MES_LLEGADA_SIN'] = np.sin(2 * np.pi * df['MES_LLEGADA'] / 12)
    df['MES_LLEGADA_COS'] = np.cos(2 * np.pi * df['MES_LLEGADA'] / 12)
    
    # Lógica de Negocio Específica
    # PAIS_TOP_200 (En este modelo parece ser directo, en otros podría requerir mapeo)
    df['PAIS_TOP_200'] = df['PAIS'] 
    
    # TIPO_VIAJERO
    condiciones_pax = [(df['PAX'] == 1), (df['PAX'] == 2), (df['PAX'] >= 3)]
    etiquetas_pax = ['Single', 'Parejas', 'Familias']
    df['TIPO_VIAJERO'] = np.select(condiciones_pax, etiquetas_pax, default='Otro')
    
    # Fidelidad
    df['TIENE_FIDELIDAD'] = np.where(df['FIDELIDAD'].isna(), 0, 1)
    df['PALLADIUM_REWARDS'] = np.where(df['FIDELIDAD'] == 'Palladium Rewards', 1, 0)
    
    # Grupos
    df['ES_GRUPO'] = np.where(df['ID_MULTIPLE'] != 0, 1, 0)
    
    # Cadenas Compuestas (Manejo de strings complejos para Hotel/Habitación)
    # NOMBRE_COMPLEJO
    condiciones_hotel = [
        df['NOMBRE_HOTEL'] == 'Complejo Riviera Maya',
        df['NOMBRE_HOTEL'] == 'Complejo Punta Cana',
        df['NOMBRE_HOTEL'] == 'Complejo Costa Mujeres'
    ]
    df['NOMBRE_COMPLEJO'] = np.select(condiciones_hotel, ['MAYA', 'CANA', 'MUJE'], default='OTRO')
    
    # PREFIJO_HAB y HOTEL_COMPLEJO
    # (Lógica simplificada para brevedad, ver utils.py para mapeo completo de prefijos como BAV, TRS, etc.)
    # Es vital extraer el prefijo de la habitación para determinar el sub-hotel dentro del complejo.
    df['PREFIJO_HAB'] = [x.split(' ')[0] for x in df['NOMBRE_HABITACION']]
    # ... lógica de mapeo HOTEL_COMPLEJO ...
    
    # Variables Concatenadas (Interacciones)
    # Estas son críticas porque el modelo aprendió comportamientos específicos de estas combinaciones
    df['COMPLEJO_RESERVA'] = df['HOTEL_COMPLEJO'] + ' - ' + df['NOMBRE_HABITACION']
    df['HOTEL_HABITACION_TOP'] = df['HOTEL_COMPLEJO'] + ' - ' + df['HABITACION_TOP'] # Nota: HABITACION_TOP requiere lógica de frecuencia
    df['FUENTE_NEGOCIO_SEGMENTO_CLIENTE'] = df['FUENTE_NEGOCIO'] + '_' + df['SEGMENTO'] + '_' + df['CLIENTE']

    return df
```

### 3.3 Manejo de Categóricos (Punto Crítico)
LightGBM en Python requiere que las columnas categóricas tengan el tipo de dato `category`. Si se pasan como `object` (string), el modelo fallará o dará resultados incorrectos.

```python
# Lista de columnas que el modelo espera como categóricas
categorical_cols = [
    'FUENTE_NEGOCIO_SEGMENTO_CLIENTE',
    'PAIS_TOP_200',
    'COMPLEJO_RESERVA',
    'TIPO_VIAJERO',
    'HOTEL_HABITACION_TOP'
]

# Conversión explicita antes de predecir
for col in categorical_cols:
    if col in df.columns:
        df[col] = df[col].astype('category')
```

## 4. Ejecución de la Predicción

Una vez se tiene el DataFrame enriquecido (`df_processed`) y con los tipos correctos:

```python
# El modelo espera solo ciertas columnas. 
# Si el DF tiene extras, LightGBM suele ignorarlas, pero es mejor filtrar si es posible.
# Se usa predict_proba para obtener la probabilidad (0 a 1).

try:
    # Retorna array numpy: [[prob_clase_0, prob_clase_1]]
    probs = model.predict_proba(df_processed)
    
    # La probabilidad de cancelación es la clase 1
    probabilidad_cancelacion = probs[0][1]
    
    print(f"Probabilidad de cancelación: {probabilidad_cancelacion:.2%}")

except Exception as e:
    print(f"Error en predicción: {e}")
```

## Resumen para el Desarrollador Web

Para integrar este modelo en tu web existente:

1.  Asegúrate de tener un **backend en Python** (o un microservicio) que pueda ejecutar el código anterior.
2.  Recoge los datos del formulario web y envíalos a este servicio.
3.  Ejecuta la función `get_features` para transformar los datos crudos en variables matemáticas.
4.  Convierte las columnas categóricas a `dtype="category"`.
5.  Pasa el DataFrame resultante a `model.predict_proba()`.
6.  Devuelve el resultado al frontend.
