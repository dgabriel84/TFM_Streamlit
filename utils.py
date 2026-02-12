import pandas as pd
import numpy as np
import joblib
import os
import streamlit as st
import warnings

try:
    from sklearn.exceptions import InconsistentVersionWarning
    warnings.filterwarnings("ignore", category=InconsistentVersionWarning)
except Exception:
    pass

# Model path as identified in the analysis
# Cargar modelo con ruta relativa o absoluta
try:
    # Intenta cargar desde carpeta local 'models' (para despliegue)
    MODEL_PATH = os.path.join(os.path.dirname(__file__), 'models', 'lightgbm.joblib')
    if not os.path.exists(MODEL_PATH):
        # Fallback a ruta absoluta original (desarrollo local)
        MODEL_PATH = '/Users/gabi/Library/CloudStorage/GoogleDrive-dgabriel84@gmail.com/.shortcut-targets-by-id/1MHQ8E-8B1PFz3EgbmBRY7WLo5AhTV1mF/TFM_Palladium_Team/04_models/lightgbm_FINAL20260206_142533/lightgbm.joblib'
except Exception:
    MODEL_PATH = 'models/lightgbm.joblib'

@st.cache_resource
def load_model():
    """Loads the pre-trained LightGBM model."""
    if not os.path.exists(MODEL_PATH):
        st.error(f"Modelo no encontrado en: {MODEL_PATH}")
        return None
    return joblib.load(MODEL_PATH)

def get_ADR(df_reservas):
  df_reservas['ADR'] = df_reservas['VALOR_RESERVA'] / df_reservas['NOCHES']
  return df_reservas

def get_REV_PAX(df_reservas):
  df_reservas['REV_PAX'] = df_reservas['VALOR_RESERVA'] / df_reservas['PAX']
  return df_reservas

def get_LEAD_TIME(df_reservas):
  # Ensure date columns are datetime objects within the function for robustness
  df_reservas['LLEGADA'] = pd.to_datetime(df_reservas['LLEGADA'])
  df_reservas['FECHA_TOMA'] = pd.to_datetime(df_reservas['FECHA_TOMA'])
  df_reservas['LEAD_TIME'] = (df_reservas['LLEGADA'].dt.normalize() - df_reservas['FECHA_TOMA'].dt.normalize()).dt.days
  return df_reservas

def get_PAIS_TOP_200 (df_reservas):
  df_reservas['PAIS_TOP_200'] = df_reservas['PAIS'] # Simplified based on original code
  return df_reservas

def get_HORA_TOMA(df_reservas):
  df_reservas['HORA_TOMA'] = df_reservas['FECHA_TOMA'].dt.hour
  df_reservas['HORA_TOMA_SIN'] = np.sin(2 * np.pi * df_reservas['HORA_TOMA'] / 24)
  df_reservas['HORA_TOMA_COS'] = np.cos(2 * np.pi * df_reservas['HORA_TOMA'] / 24)
  return df_reservas

def get_TIPO_VIAJERO(df_reservas):
  condiciones_pax = [
      (df_reservas['PAX'] == 1),
      (df_reservas['PAX'] == 2),
      (df_reservas['PAX'] >= 3)
  ]
  etiquetas_pax = ['Single', 'Parejas', 'Familias']
  df_reservas['TIPO_VIAJERO'] = np.select(condiciones_pax, etiquetas_pax, default='Otro')
  return df_reservas

def get_PALLADIUM_REWARDS_y_TIENE_FIDELIDAD(df_reservas):
  # Handle None/NaN properly
  df_reservas['FIDELIDAD'] = df_reservas['FIDELIDAD'].replace({None: np.nan})
  df_reservas['TIENE_FIDELIDAD'] = np.where(df_reservas['FIDELIDAD'].isna(), 0, 1)
  df_reservas['PALLADIUM_REWARDS'] = np.where(df_reservas['FIDELIDAD'] == 'Palladium Rewards', 1, 0)
  return df_reservas

def get_ES_GRUPO(df_reservas):
  df_reservas['ES_GRUPO'] = np.where(df_reservas['ID_MULTIPLE'] != 0, 1, 0)
  return df_reservas

def get_MES_LLEGADA(df_reservas):
  # Ensure LLEGADA is a datetime object within the function for robustness
  df_reservas['LLEGADA'] = pd.to_datetime(df_reservas['LLEGADA'])
  df_reservas['MES_LLEGADA'] = df_reservas['LLEGADA'].dt.month
  df_reservas['MES_LLEGADA_SIN'] = np.sin(2 * np.pi * df_reservas['MES_LLEGADA'] / 12)
  df_reservas['MES_LLEGADA_COS'] = np.cos(2 * np.pi * df_reservas['MES_LLEGADA'] / 12)
  return df_reservas

def get_NOMBRE_COMPLEJO(df_reservas):
  condiciones = [
      df_reservas['NOMBRE_HOTEL'] == 'Complejo Riviera Maya',
      df_reservas['NOMBRE_HOTEL'] == 'Complejo Punta Cana',
      df_reservas['NOMBRE_HOTEL'] == 'Complejo Costa Mujeres'
  ]
  opciones = ['MAYA', 'CANA', 'MUJE']
  df_reservas['NOMBRE_COMPLEJO'] = np.select(condiciones, opciones, default='OTRO')
  return df_reservas

def get_PREFIJO_HAB(df_reservas):
  df_reservas['PREFIJO_HAB'] = [nombre.split(' ')[0] for nombre in df_reservas['NOMBRE_HABITACION']]
  filtro_cmu_fs = df_reservas['NOMBRE_HABITACION'].str.startswith('CMU FS')
  df_reservas.loc[filtro_cmu_fs, 'PREFIJO_HAB'] = 'CMU FS'
  return df_reservas

def get_HOTEL_COMPLEJO(df_reservas):
  condiciones = [
      (df_reservas['NOMBRE_HOTEL'] == 'Complejo Riviera Maya') & (df_reservas['PREFIJO_HAB'].str.startswith('COL')),
      (df_reservas['NOMBRE_HOTEL'] == 'Complejo Riviera Maya') & (df_reservas['PREFIJO_HAB'].str.startswith('KAN')),
      (df_reservas['NOMBRE_HOTEL'] == 'Complejo Riviera Maya') & (df_reservas['PREFIJO_HAB'].str.startswith('TRS')),
      (df_reservas['NOMBRE_HOTEL'] == 'Complejo Riviera Maya') & (df_reservas['PREFIJO_HAB'].str.startswith('WS')),
      (df_reservas['NOMBRE_HOTEL'] == 'Complejo Punta Cana') & (df_reservas['PREFIJO_HAB'].str.startswith('BAV')),
      (df_reservas['NOMBRE_HOTEL'] == 'Complejo Punta Cana') & (df_reservas['PREFIJO_HAB'].str.startswith('PAL')),
      (df_reservas['NOMBRE_HOTEL'] == 'Complejo Punta Cana') & (df_reservas['PREFIJO_HAB'].str.startswith('PC')),
      (df_reservas['NOMBRE_HOTEL'] == 'Complejo Punta Cana') & (df_reservas['PREFIJO_HAB'].str.startswith('TRS')),
      (df_reservas['NOMBRE_HOTEL'] == 'Complejo Costa Mujeres') & (df_reservas['PREFIJO_HAB'].str.startswith('CMU FS')),
      (df_reservas['NOMBRE_HOTEL'] == 'Complejo Costa Mujeres') & (df_reservas['PREFIJO_HAB'].str.startswith('CMU')),
      (df_reservas['NOMBRE_HOTEL'] == 'Complejo Costa Mujeres') & (df_reservas['PREFIJO_HAB'].str.startswith('TRSC')),
      (df_reservas['NOMBRE_HOTEL'] == 'Complejo Costa Mujeres') & (df_reservas['PREFIJO_HAB'].str.startswith('TRS'))
  ]
  opciones = [
      'MAYA_COL', 'MAYA_KAN', 'MAYA_TRS', 'MAYA_WS',
      'CANA_BAV', 'CANA_PAL', 'CANA_PC', 'CANA_TRS',
      'MUJE_CMU_FS', 'MUJE_CMU', 'MUJE_TRSC', 'MUJE_TRS'
  ]
  df_reservas['HOTEL_COMPLEJO'] = np.select(condiciones, opciones, default='OTRO')
  return df_reservas

def get_COMPLEJO_RESERVA(df_reservas):
  df_reservas['COMPLEJO_RESERVA'] = df_reservas['HOTEL_COMPLEJO'] + ' - ' + df_reservas['NOMBRE_HABITACION']
  return df_reservas

def get_HABITACION_TOP(df_reservas):
  # In a real production environment, this list should be loaded from training artifacts.
  # For now, we reuse the logic but since we are processing a single row/small df, 
  # we can't dynamically determine "top" based on frequency of the input.
  # We should just pass the value as is, or handle provided 'OTHERS' logic if specific list available.
  # Based on the original code, it calculates frequency on the *input* df. 
  # If input is 1 row, that room is 100% freq. This logic might need adjustment if it relies on training data distribution.
  # However, `prediccion_reserva.ipynb` simply calls `get_features` which calls this.
  # If we look at `funcion_producion.py`, `get_HABITACION_TOP` calculates `top_habitaciones` from `df_reservas`.
  # If `df_reservas` is just the single prediction row, `top_habitaciones` will contain that single room.
  # So it effectively does nothing for single prediction (returns the room name).
  # We will keep it as is to mirror the logic, acknowledging this limitation.
  
  ESCOGER_PONCENTAJE = 0.003
  counts = df_reservas['NOMBRE_HABITACION'].value_counts()
  percentage_threshold = ESCOGER_PONCENTAJE
  top_habitaciones = counts[counts / len(df_reservas) > percentage_threshold].index.tolist()
  df_reservas['HABITACION_TOP'] = df_reservas['NOMBRE_HABITACION'].apply(lambda x: x if x in top_habitaciones else 'OTROS')
  return df_reservas

def get_HOTEL_HABITACION_TOP (df_reservas):
  df_reservas['HOTEL_HABITACION_TOP'] = df_reservas['HOTEL_COMPLEJO'] + ' - ' + df_reservas['HABITACION_TOP']
  return df_reservas

def get_FUENTE_NEGOCIO_SEGMENTO_CLIENTE (df_reservas):
  df_reservas['FUENTE_NEGOCIO_SEGMENTO_CLIENTE'] = df_reservas['FUENTE_NEGOCIO'] + '_' + df_reservas['SEGMENTO'] + '_' + df_reservas['CLIENTE']
  return df_reservas


def get_features(LLEGADA, NOCHES, PAX, ADULTOS, CLIENTE, FECHA_TOMA, FIDELIDAD, PAIS, SEGMENTO, FUENTE_NEGOCIO, NOMBRE_HOTEL, NOMBRE_HABITACION, VALOR_RESERVA):
  data = {
      'LLEGADA': [LLEGADA],
      'NOCHES': [NOCHES],
      'PAX': [PAX],
      'ADULTOS': [ADULTOS],
      'CLIENTE': [CLIENTE],
      'ID_MULTIPLE': 0, # Default to 0 for single predictions
      'FECHA_TOMA': [FECHA_TOMA],
      'FIDELIDAD': [FIDELIDAD],
      'PAIS': [PAIS],
      'SEGMENTO': [SEGMENTO],
      'FUENTE_NEGOCIO': [FUENTE_NEGOCIO],
      'NOMBRE_HOTEL': [NOMBRE_HOTEL],
      'NOMBRE_HABITACION': [NOMBRE_HABITACION],
      'VALOR_RESERVA': [VALOR_RESERVA]
  }

  df_reservas_in = pd.DataFrame(data)

  # Convert date columns to datetime objects
  df_reservas_in['LLEGADA'] = pd.to_datetime(df_reservas_in['LLEGADA'])
  df_reservas_in['FECHA_TOMA'] = pd.to_datetime(df_reservas_in['FECHA_TOMA'])

  df_reservas_in = get_ADR(df_reservas_in)
  df_reservas_in = get_REV_PAX(df_reservas_in)
  df_reservas_in = get_LEAD_TIME(df_reservas_in)
  df_reservas_in = get_PAIS_TOP_200 (df_reservas_in)
  df_reservas_in = get_HORA_TOMA(df_reservas_in)
  df_reservas_in = get_TIPO_VIAJERO(df_reservas_in)
  df_reservas_in = get_PALLADIUM_REWARDS_y_TIENE_FIDELIDAD(df_reservas_in)
  df_reservas_in = get_ES_GRUPO(df_reservas_in)
  df_reservas_in = get_MES_LLEGADA(df_reservas_in)
  df_reservas_in = get_NOMBRE_COMPLEJO(df_reservas_in)
  df_reservas_in = get_PREFIJO_HAB(df_reservas_in)
  df_reservas_in = get_HOTEL_COMPLEJO(df_reservas_in)
  df_reservas_in = get_COMPLEJO_RESERVA(df_reservas_in)
  df_reservas_in = get_HABITACION_TOP(df_reservas_in)
  df_reservas_in = get_HOTEL_HABITACION_TOP (df_reservas_in)
  df_reservas_in = get_FUENTE_NEGOCIO_SEGMENTO_CLIENTE (df_reservas_in)

  # Conversión a category requerida por el modelo LightGBM
  categorical_features = [
      'FUENTE_NEGOCIO_SEGMENTO_CLIENTE',
      'PAIS_TOP_200',
      'COMPLEJO_RESERVA',
      'TIPO_VIAJERO',
      'HOTEL_HABITACION_TOP'
  ]
  
  for col in categorical_features:
      if col in df_reservas_in.columns:
          df_reservas_in[col] = df_reservas_in[col].astype('category')

  return df_reservas_in
