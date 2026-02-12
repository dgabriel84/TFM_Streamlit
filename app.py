"""
Palladium Hotel Group - Sistema de Prediccion de Cancelaciones
Aplicacion unificada con navegacion superior y wizard de reserva.

Estructura Cliente (Wizard 4 pasos):
1. Destino (Cancun / Punta Cana)
2. Datos del cliente
3. Hotel, habitacion y condiciones
4. Confirmacion

Estructura Intranet:
- Busqueda por codigo de reserva
- Panel de prediccion manual

Autor: Grupo 4 - TFM Palladium
Fecha: Febrero 2026
"""

import streamlit as st
import pandas as pd
import datetime
import base64
import requests
import json
import textwrap
from io import BytesIO
import sys
import os
import numpy as np
import random

# -----------------------------------------------------------------------------
# CONFIGURACION DE RUTAS
# -----------------------------------------------------------------------------
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils import load_model, get_features
from precios_data import PRECIOS_ADR
import agent_v2

# -----------------------------------------------------------------------------
# CONFIGURACION DE PAGINA
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="Palladium Hotels",
    page_icon="P",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# -----------------------------------------------------------------------------
# PALETA DE COLORES CORPORATIVOS
# -----------------------------------------------------------------------------
COLOR_DORADO_OSCURO = "#6E550C"
COLOR_DORADO = "#BB9719"
COLOR_BEIGE = "#CCC3A5"
COLOR_VERDE_OSCURO = "#50685C"
COLOR_NEGRO = "#292929"
COLOR_GRIS = "#757171"
COLOR_OLIVA = "#AEA780"
COLOR_CREMA = "#FCF4E4"
COLOR_CREMA_CLARO = "#F0EDE6"
COLOR_BEIGE_CLARO = "#E3D7C1"
COLOR_BLANCO = "#FFFFFF"
COLOR_RIESGO_ALTO = "#C0392B"
COLOR_RIESGO_MEDIO = "#D68910"
COLOR_RIESGO_BAJO = "#1E8449"

# -----------------------------------------------------------------------------
# ESTILOS CSS
# -----------------------------------------------------------------------------
CUSTOM_CSS = """
<style>
    @import url('https://fonts.googleapis.com/css2?family=Cinzel:wght@400;500;600&family=Inter:wght@300;400;500&display=swap');
    
    [data-testid="stSidebarNav"] {{ display: none; }}
    .main {{ background-color: {COLOR_CREMA_CLARO}; }}
    
    /* Wizard steps */
    .wizard-container {{
        display: flex;
        justify-content: center;
        gap: 2rem;
        margin: 1.5rem 0 2rem 0;
        padding: 1rem;
        background: {COLOR_BLANCO};
        border-radius: 4px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.05);
    }}
    .wizard-step {{
        display: flex;
        align-items: center;
        gap: 0.5rem;
        color: {COLOR_GRIS};
    }}
    .wizard-step.active {{
        color: {COLOR_DORADO_OSCURO};
        font-weight: 600;
    }}
    .wizard-step.completed {{
        color: {COLOR_VERDE_OSCURO};
    }}
    .step-number {{
        width: 32px;
        height: 32px;
        border-radius: 50%;
        display: flex;
        align-items: center;
        justify-content: center;
        background: {COLOR_BEIGE};
        color: {COLOR_NEGRO};
        font-size: 0.9rem;
        font-weight: 600;
    }}
    .wizard-step.active .step-number {{
        background: {COLOR_DORADO_OSCURO};
        color: {COLOR_BLANCO};
    }}
    .wizard-step.completed .step-number {{
        background: {COLOR_VERDE_OSCURO};
        color: {COLOR_BLANCO};
    }}

    /* UNIFIED BUTTONS */
    div.stButton > button {{
        width: 100% !important;
        background-color: white !important;
        color: {COLOR_DORADO_OSCURO} !important;
        border: 1px solid {COLOR_OLIVA} !important;
        border-radius: 0px !important;
        padding: 0.6rem 1.5rem !important;
        font-family: 'Cinzel', serif !important;
        letter-spacing: 2px !important;
        text-transform: uppercase !important;
        font-size: 0.8rem !important;
        transition: all 0.3s ease !important;
        font-weight: 500 !important;
    }}

    div.stButton > button:hover {{
        background-color: {COLOR_DORADO_OSCURO} !important;
        color: white !important;
        border-color: {COLOR_DORADO_OSCURO} !important;
        box-shadow: 0 4px 12px rgba(110, 85, 12, 0.2) !important;
    }}

    div.stButton > button[kind="primary"] {{
        background-color: {COLOR_DORADO} !important;
        color: white !important;
        border: 1px solid {COLOR_DORADO} !important;
    }}

    div.stButton > button[kind="primary"]:hover {{
        background-color: {COLOR_DORADO_OSCURO} !important;
        border-color: {COLOR_DORADO_OSCURO} !important;
    }}

    /* Inputs Uniformity */
    .stTextInput > div > div > input, 
    .stNumberInput > div > div > input, 
    .stDateInput > div > div > input,
    .stSelectbox > div > div {{
        background-color: {COLOR_CREMA} !important;
        border-radius: 0px !important;
        border: 1px solid {COLOR_OLIVA} !important;
        color: {COLOR_NEGRO} !important;
        height: 45px !important;
    }}

    /* Destino & Zone Cards */
    .zone-selection-card {{
        border: 1px solid {COLOR_OLIVA};
        padding: 0px;
        overflow: hidden;
        transition: all 0.3s ease;
        background: white;
        margin-bottom: 20px;
    }}
    .zone-selection-card:hover {{
        border-color: {COLOR_DORADO};
        box-shadow: 0 10px 25px rgba(0,0,0,0.1);
    }}
    .zone-image-container {{
        width: 100%;
        height: 150px;
        background-size: cover;
        background-position: center;
    }}

    /* Success Box Custom Celebratory */
    .success-box-palladium {{
        background-color: {COLOR_VERDE_OSCURO};
        color: white !important;
        padding: 3rem;
        text-align: center;
        border-radius: 0px;
        border-left: 5px solid {COLOR_DORADO};
        box-shadow: 0 15px 45px rgba(0,0,0,0.15);
        margin: 2rem 0;
        animation: fadeIn 1s ease-out;
    }}
    @keyframes fadeIn {{
        from {{ opacity: 0; transform: translateY(20px); }}
        to {{ opacity: 1; transform: translateY(0); }}
    }}

    /* Titles & Headings */
    h1, h2, h3 {{
        font-family: 'Cinzel', serif !important;
        color: {COLOR_DORADO_OSCURO};
        font-weight: 400 !important;
    }}
    
    /* Ensure white text in corporate green boxes */
    .success-box-palladium h1, 
    .success-box-palladium h2, 
    .success-box-palladium h3,
    .rewards-box h1,
    .rewards-box h2,
    .rewards-box h3 {{
        color: white !important;
    }}

    /* Info Bar (Fechas/Pueblo) */
    div[data-testid="stNotification"] {{
        background-color: {COLOR_CREMA} !important;
        color: {COLOR_DORADO_OSCURO} !important;
        border: 1px solid {COLOR_DORADO} !important;
        border-radius: 0px !important;
    }}
    div[data-testid="stNotification"] svg {{ display: none !important; }} /* No icons */
    
    /* Expander Styling (Fotos/Plano) */
    .stExpander {{ 
        border: 1px solid {COLOR_OLIVA} !important;
        border-radius: 0px !important;
        background: white !important;
    }}
    .stExpander > details > summary {{
        font-family: 'Cinzel', serif !important;
        text-transform: uppercase !important;
        letter-spacing: 1px !important;
        font-size: 0.8rem !important;
        color: {COLOR_DORADO_OSCURO} !important;
    }}
    .stExpander > details > summary:hover {{
        background-color: {COLOR_CREMA} !important;
    }}

    /* Caja Rewards (Verde Oscuro) */
    .rewards-box {{
        background-color: {COLOR_VERDE_OSCURO};
        color: white !important;
        padding: 20px;
        border-radius: 0px;
        margin-top: 20px;
        margin-bottom: 20px;
        text-align: center;
        border-left: 5px solid {COLOR_DORADO};
    }}
    .rewards-box * {{ color: white !important; }}
</style>""".format(
    COLOR_CREMA_CLARO=COLOR_CREMA_CLARO,
    COLOR_BLANCO=COLOR_BLANCO,
    COLOR_GRIS=COLOR_GRIS,
    COLOR_DORADO_OSCURO=COLOR_DORADO_OSCURO,
    COLOR_VERDE_OSCURO=COLOR_VERDE_OSCURO,
    COLOR_BEIGE=COLOR_BEIGE,
    COLOR_NEGRO=COLOR_NEGRO,
    COLOR_OLIVA=COLOR_OLIVA,
    COLOR_DORADO=COLOR_DORADO,
    COLOR_CREMA=COLOR_CREMA
)

st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# DATOS MAESTROS
# Estructura jerarquica: Destino > Complejo > Hotel > Habitaciones
# El modelo recibe NOMBRE_HOTEL = Complejo, pero el usuario selecciona Hotel
# -----------------------------------------------------------------------------

CODIGO_HOTEL = {
    "Complejo Costa Mujeres": "09601",
    "Complejo Riviera Maya": "10601",
    "Complejo Punta Cana": "10701"
}

# Estructura completa de hoteles por complejo
HOTELES_POR_COMPLEJO = {
    "Complejo Costa Mujeres": {
        "Grand Palladium Select Costa Mujeres": {
            "descripcion": "Resort de lujo frente al mar Caribe con playa privada y servicios premium",
            "prefijo": "CMU",
            "habitaciones": [
                "CMU JUNIOR SUITE GV",
                "CMU JUNIOR SUITE PS", 
                "CMU JUNIOR SUITE PS OV",
                "CMU FAMILY SUITE"
            ]
        },
        "Family Selection Costa Mujeres": {
            "descripcion": "Experiencia familiar exclusiva con servicios especiales para familias con niños",
            "prefijo": "CMU FS",
            "habitaciones": [
                "CMU FS JUNIOR SUITE BS",
                "CMU FS JUNIOR SUITE BS OV"
            ]
        },
        "TRS Coral Hotel": {
            "descripcion": "Hotel solo adultos con experiencia premium y servicios exclusivos",
            "prefijo": "TRSC",
            "habitaciones": [
                "TRSC JUNIOR SUITE GV",
                "TRSC JUNIOR SUITE SW",
                "TRSC JUNIOR SUITE BS OV",
                "TRS JUNIOR SUITE GV",
                "TRS JUNIOR SUITE OV",
                "TRS JUNIOR SUITE SW"
            ]
        }
    },
    "Complejo Riviera Maya": {
        "Grand Palladium Colonial Resort & Spa": {
            "descripcion": "Resort con arquitectura colonial junto a la playa y cenotes naturales",
            "prefijo": "COL",
            "habitaciones": [
                "COL JUNIOR SUITE GV",
                "COL JUNIOR SUITE PS",
                "COL DELUXE GARDEN VIEW",
                "COL ROMANCE VILLA SUI PS"
            ]
        },
        "Grand Palladium Kantenah Resort & Spa": {
            "descripcion": "Resort frente al mar con acceso a cenote privado y actividades acuáticas",
            "prefijo": "KAN",
            "habitaciones": [
                "KAN JUNIOR SUITE GV",
                "KAN JUNIOR SUITE OV",
                "KAN DELUXE GARDEN VIEW"
            ]
        },
        "TRS Yucatan Hotel": {
            "descripcion": "Hotel solo adultos con experiencia gourmet y spa de lujo",
            "prefijo": "TRS",
            "habitaciones": [
                "TRS JUNIOR SUITE GV",
                "TRS JUNIOR SUITE PS",
                "TRS JUNIOR SUITE PS OV",
                "TRS JS JACUZZI TERR PS",
                "TRS SUITE GARDEN VIEW",
                "TRS SUITE PP PS",
                "TRS AMBASSADOR SUITE POV",
                "TRS ROMANCE BW BYTHE LAKE"
            ]
        },
        "Grand Palladium Select White Sand": {
            "descripcion": "Resort premium con playa de arena blanca y servicios exclusivos",
            "prefijo": "WS",
            "habitaciones": [
                "WS JUNIOR SUITE GV",
                "WS JUNIOR SUITE BS",
                "WS SUITE GARDEN VIEW"
            ]
        }
    },
    "Complejo Punta Cana": {
        "Grand Palladium Select Bavaro": {
            "descripcion": "Resort premium en playa Bávaro con servicios Select exclusivos",
            "prefijo": "BAV",
            "habitaciones": [
                "BAV JUNIOR SUITE GV",
                "BAV SUPERIOR JS GV",
                "BAV SUPERIOR JS BS",
                "BAV PREMIUM JS GV",
                "BAV ROOFTOP JT ST"
            ]
        },
        "Grand Palladium Palace Resort & Spa": {
            "descripcion": "Resort emblemático con arquitectura elegante y spa de clase mundial",
            "prefijo": "PAL",
            "habitaciones": [
                "PAL JUNIOR SUITE GV",
                "PAL JUNIOR SUITE BS",
                "PAL JUNIOR SUITE SW BS",
                "PAL JUNIOR SUITE BS OV",
                "PAL DELUXE BEACHSIDE",
                "PAL DELUXE GARDEN VIEW",
                "PAL DELUXE BS POV",
                "PAL LOFT SUITE GV"
            ]
        },
        "Grand Palladium Punta Cana Resort & Spa": {
            "descripcion": "Resort familiar con amplia oferta de actividades y entretenimiento",
            "prefijo": "PC",
            "habitaciones": [
                "PC JUNIOR SUITE GV",
                "PC JUNIOR SUITE POOLSIDE",
                "PC DELUXE GARDEN VIEW",
                "PC DELUXE POOLSIDE",
                "PC FAMILY SUITE"
            ]
        },
        "TRS Turquesa Hotel": {
            "descripcion": "Hotel solo adultos con playa privada y experiencia gastronómica exclusiva",
            "prefijo": "TRS",
            "habitaciones": [
                "TRS JUNIOR SUITE GV/PV",
                "TRS JUNIOR SUITE PS",
                "TRS JUNIOR SUITE SWIM UP",
                "TRS ROMANCE SUITE PS"
            ]
        }
    }
}

# Mapeo de destinos a complejos
DESTINOS = {
    "MEXICO": {
        "nombre": "Cancun - Riviera Maya",
        "descripcion": "Costa Mujeres y Riviera Maya",
        "complejos": ["Complejo Costa Mujeres", "Complejo Riviera Maya"]
    },
    "REPUBLICA DOMINICANA": {
        "nombre": "Punta Cana",
        "descripcion": "Playa Bávaro y Palladium Resort",
        "complejos": ["Complejo Punta Cana"]
    }
}

PAISES = [
    "ESPAÑA", "ESTADOS UNIDOS", "CANADA", "MEXICO", "ALEMANIA", "REINO UNIDO",
    "FRANCIA", "ITALIA", "ARGENTINA", "BRASIL", "COLOMBIA", "CHILE", "PERU",
    "URUGUAY", "REPUBLICA DOMINICANA", "SUECIA", "POLONIA", "SIN PAIS"
]

SEGMENTOS = ["BAR", "Fixed Rates", "Loyalty", "Package", "Group Leisure", "Weddings"]
FUENTES_NEGOCIO = ["DIRECT SALES", "T.O. / T.A.", "E-COMMERCE", "CORPORATE"]
CLIENTES = [
    "ROIBACK (GLOBAL OBI S.L.)", "CALL CENTER", "PALLADIUM TRAVEL CLUB_SOCIOS",
    "EXPEDIA", "BOOKING EUROPE BV", "AGODA COMPANY PTE LTD", "HOTELBEDS USA INC.",
    "DESPEGAR.COM MEXICO S.A. DE C.V.", "FUNJET VACATIONS ALG", "CVC BRASIL", "OTROS"
]
PROGRAMAS_FIDELIDAD = ["Sin programa", "Palladium Rewards", "WyndHam Rewards", "Palladium Connect"]

# Mapeo de nombres de app.py a codigos de precios_data.py
MAPEO_HOTELES_PRECIOS = {
    "Grand Palladium Select Costa Mujeres": "MUJE_CMU",
    "Grand Palladium Costa Mujeres": "MUJE_CMU", # Alias
    "Family Selection Costa Mujeres": "MUJE_CMU_FS",
    "TRS Coral Hotel": "MUJE_TRS",
    "Grand Palladium Colonial Resort & Spa": "MAYA_COL",
    "Grand Palladium Colonial": "MAYA_COL", # Alias
    "Grand Palladium Kantenah Resort & Spa": "MAYA_KAN",
    "Grand Palladium Kantenah": "MAYA_KAN", # Alias
    "TRS Yucatan Hotel": "MAYA_TRS",
    "TRS Yucatan": "MAYA_TRS", # Alias
    "Grand Palladium Select White Sand": "MAYA_WS",
    "Grand Palladium White Sand": "MAYA_WS", # Alias
    "Grand Palladium Select Bavaro": "CANA_BAV",
    "Grand Palladium Bavaro": "CANA_BAV", # Alias
    "Grand Palladium Palace Resort & Spa": "CANA_PAL",
    "Grand Palladium Palace": "CANA_PAL", # Alias
    "Grand Palladium Punta Cana Resort & Spa": "CANA_PC",
    "Grand Palladium Punta Cana": "CANA_PC", # Alias
    "TRS Turquesa Hotel": "CANA_TRS",
    "TRS Turquesa": "CANA_TRS" # Alias
}

# -----------------------------------------------------------------------------
# FUNCIONES AUXILIARES
# -----------------------------------------------------------------------------
def get_base64_image(image_path):
    """Convierte una imagen local a base64 para incrustarla en HTML."""
    if image_path and os.path.exists(image_path):
        with open(image_path, "rb") as img_file:
            return base64.b64encode(img_file.read()).decode()
    return ""


@st.cache_data(show_spinner=False)
def get_base64_image_thumbnail(image_path, size=88):
    """
    Convierte una imagen local a thumbnail base64 (mucho más ligero para sidebar).
    """
    if not image_path or not os.path.exists(image_path):
        return ""
    try:
        from PIL import Image, ImageOps

        with Image.open(image_path) as img:
            img = ImageOps.exif_transpose(img).convert("RGBA")
            img = ImageOps.fit(img, (size, size), Image.Resampling.LANCZOS)
            buf = BytesIO()
            img.save(buf, format="PNG", optimize=True)
            return base64.b64encode(buf.getvalue()).decode()
    except Exception:
        return get_base64_image(image_path)


def render_team_collage_sidebar(base_dir):
    """
    Renderiza un collage horizontal de miembros para cabecera del agente.
    Excluye al tutor (Gustavo) y solapa ligeramente las imágenes.
    """
    miembros = ["Alex.png", "David.png", "Francisco.png", "Gabi.png", "Jose.png"]
    folder = os.path.join(base_dir, "media", "Miembros")
    items = []
    for idx, nombre_archivo in enumerate(miembros):
        img_path = os.path.join(folder, nombre_archivo)
        b64 = get_base64_image_thumbnail(img_path, size=88)
        if b64:
            items.append((idx, b64))

    if not items:
        fallback_logo = os.path.join(base_dir, "media", "general", "Logo.jpg")
        if os.path.exists(fallback_logo):
            st.image(fallback_logo, width="stretch")
        return

    avatar_size = 88
    step = 60
    width = step * (len(items) - 1) + avatar_size
    imgs_html = []
    for idx, b64 in items:
        left = idx * step
        imgs_html.append(
            f"<img src='data:image/png;base64,{b64}' "
            f"style='position:absolute;left:{left}px;top:0;width:{avatar_size}px;height:{avatar_size}px;"
            f"object-fit:cover;border-radius:50%;z-index:{100-idx};"
            f"filter:drop-shadow(0 4px 8px rgba(0,0,0,0.15));'/>"
        )

    st.markdown(
        f"""
        <div style="display:flex; justify-content:center; margin: 0.2rem 0 0.8rem 0;">
            <div style="position:relative; width:{width}px; height:{avatar_size}px;">
                {''.join(imgs_html)}
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

def get_iso_week_key(date_obj):
    """Devuelve la clave AAAA-WW para el diccionario de precios."""
    year, week, _ = date_obj.isocalendar()
    # Ajuste para semanas de cambio de año si es necesario, 
    # pero precios_data tiene claves como '2022-52', '2023-01'.
    # isocalendar maneja bien esto (ej: 1 enero 2023 puede ser semana 52 de 2022)
    # Sin embargo, el dict usa keys strings. Asumimos ISO standard.
    return f"{year}-{week:02d}"

def get_precio_medio_2026(hotel_nombre, habitacion, pax):
    """
    Calcula el precio medio para 2026 para mostrar 'Desde X€'.
    """
    codigo_hotel = MAPEO_HOTELES_PRECIOS.get(hotel_nombre)
    if not codigo_hotel: return 0
    
    # Datos del hotel
    datos_hotel = PRECIOS_ADR.get(codigo_hotel, {})
    # Datos de la habitacion
    datos_hab = datos_hotel.get(habitacion, {})
    if not datos_hab: return 0
    
    # Datos del pax (convertir a str)
    pax_str = str(pax)
    datos_pax = datos_hab.get(pax_str)
    
    # Fallback pax: si no hay para 4 pax, probar 3, 2...
    if not datos_pax:
        for p in range(pax, 0, -1):
            if str(p) in datos_hab:
                datos_pax = datos_hab[str(p)]
                break
    
    if not datos_pax: return 0
    
    # Filtrar solo precios de 2026
    precios_2026 = [v for k, v in datos_pax.items() if k.startswith("2026")]
    
    if not precios_2026: return 0
    
    return sum(precios_2026) / len(precios_2026)

def calcular_coste_estancia(hotel_nombre, habitacion, llegada, noches, pax):
    """
    Calcula el coste exacto sumando precios por semana/noche.
    Precios en PRECIOS_ADR son por SEMANA (Key YYYY-WW).
    Asumimos que el precio almacenado es el ADR (Average Daily Rate) para esa semana.
    """
    # Precios base aproximados por noche para fallback (si no hay datos reales en CSV/Pickle)
    PRECIOS_BASE = {
        "Grand Palladium Select Costa Mujeres": 450,
        "Family Selection Costa Mujeres": 650,
        "TRS Coral Hotel": 700,
        "Grand Palladium Colonial Resort & Spa": 350,
        "Grand Palladium Kantenah Resort & Spa": 380,
        "TRS Yucatan Hotel": 550,
        "Grand Palladium Select White Sand": 420,
        "Grand Palladium Select Bavaro": 400,
        "Grand Palladium Palace Resort & Spa": 380,
        "Grand Palladium Punta Cana Resort & Spa": 360,
        "TRS Turquesa Hotel": 580
    }

    codigo_hotel = MAPEO_HOTELES_PRECIOS.get(hotel_nombre)
    # Si no hay codigo, usar el base del nombre, o default 300
    if not codigo_hotel: 
        return PRECIOS_BASE.get(hotel_nombre, 300) * noches
    
    datos_hotel = PRECIOS_ADR.get(codigo_hotel, {})
    
    # DEBUG: Fallback search si no encuentra el hotel o la habitación
    if not datos_hotel or habitacion not in datos_hotel:
         print(f"DEBUG PRICE: Hotel {codigo_hotel} or Room {habitacion} not found initially.")
         for h_key, h_data in PRECIOS_ADR.items():
              if habitacion in h_data:
                   datos_hotel = h_data
                   print(f"DEBUG PRICE: Found room {habitacion} under hotel {h_key} instead of {codigo_hotel}")
                   break
    
    datos_hab = datos_hotel.get(habitacion, {})
    
    # NUEVO: Fuzzy search si no encuentra la habitacion exacta
    if not datos_hab:
         print(f"DEBUG PRICE: Room '{habitacion}' exact match failed. Trying fuzzy search in {list(datos_hotel.keys())}")
         best_match = None
         for key in datos_hotel.keys():
              # Si la habitacion pedida esta contenida en la key (ej: "Junior Suite" in "CMU JUNIOR SUITE GV")
              if habitacion.lower() in key.lower() or key.lower() in habitacion.lower():
                   best_match = key
                   break
         
         if best_match:
              print(f"DEBUG PRICE: Fuzzy match found: '{habitacion}' -> '{best_match}'")
              datos_hab = datos_hotel[best_match]
    
    # Selección de PAX (Fallback downward)
    pax_str = str(pax)
    if pax_str not in datos_hab:
        found = False
        for p in range(pax, 0, -1):
             if str(p) in datos_hab:
                 pax_str = str(p)
                 found = True
                 break
        if not found and "2" in datos_hab: pax_str = "2" # Default ultimo recurso
        
    datos_precios = datos_hab.get(pax_str, {})
    
    # Si no hay datos de precios reales, usar FALLBACK DINAMICO
    if not datos_precios: 
        base = PRECIOS_BASE.get(hotel_nombre, 300)
        # Ajuste por habitación (simple heurística si contiene palabras clave)
        if "Suite" in habitacion: base += 50
        if "Jacuzzi" in habitacion: base += 80
        if "Ocean" in habitacion: base += 100
        if "Swim Up" in habitacion: base += 120
        return base * noches
    
    total = 0.0
    # Iterar cada noche
    for i in range(noches):
        dia = llegada + datetime.timedelta(days=i)
        key_semana = get_iso_week_key(dia)
        
        precio_noche = datos_precios.get(key_semana)
        
        # Si no encontramos precio para esa semana (ej: fuera de rango 2022-2027), usar promedio
        if not precio_noche:
             precio_noche = PRECIOS_BASE.get(hotel_nombre, 300)
             
        total += precio_noche
        
    return total


# Mapeo de imagenes de hoteles
IMAGENES_HOTELES = {
    "Grand Palladium Select Costa Mujeres": "hoteles/costa_mujeres/gp_select_costa_mujeres.jpg",
    "Family Selection Costa Mujeres": "hoteles/costa_mujeres/family_selection_costa_mujeres.jpg",
    "TRS Coral Hotel": "hoteles/costa_mujeres/trs_coral.jpg",
    "Grand Palladium Colonial Resort & Spa": "hoteles/riviera_maya/gp_colonial.jpg",
    "Grand Palladium Kantenah Resort & Spa": "hoteles/riviera_maya/gp_kantenah.jpg",
    "TRS Yucatan Hotel": "hoteles/riviera_maya/trs_yucatan.jpg",
    "Grand Palladium Select White Sand": "hoteles/riviera_maya/gp_white_sand.jpg",
    "Grand Palladium Select Bavaro": "hoteles/punta_cana/gp_select_bavaro.jpg",
    "Grand Palladium Palace Resort & Spa": "hoteles/punta_cana/gp_palace.jpg",
    "Grand Palladium Punta Cana Resort & Spa": "hoteles/punta_cana/gp_punta_cana.jpg",
    "TRS Turquesa Hotel": "hoteles/punta_cana/trs_turquesa.jpg"
}

# Mapeo de prefijos de habitacion a carpetas de imagenes
CARPETAS_HABITACIONES = {
    "CMU": "habitaciones/MUJE_CMU",
    "CMU FS": "habitaciones/MUJE_CMU_FS",
    "TRSC": "habitaciones/MUJE_TRSC",
    "TRS": "habitaciones/MUJE_TRS",  # Default TRS para Costa Mujeres
    "COL": "habitaciones/MAYA_COL",
    "KAN": "habitaciones/MAYA_KAN",
    "WS": "habitaciones/MAYA_WS",
    "BAV": "habitaciones/CANA_BAV",
    "PAL": "habitaciones/CANA_PAL",
    "PC": "habitaciones/CANA_PC"
}

# Mapeo especifico para TRS segun complejo
CARPETAS_TRS = {
    "Complejo Costa Mujeres": "habitaciones/MUJE_TRS",
    "Complejo Riviera Maya": "habitaciones/MAYA_TRS",
    "Complejo Punta Cana": "habitaciones/CANA_TRS"
}
# Descripciones de tipos de habitacion con servicios y precios
DESCRIPCIONES_HABITACIONES = {
    # Costa Mujeres
    "CMU JUNIOR SUITE GV": {
        "nombre": "Junior Suite Garden View",
        "descripcion": "Espaciosa suite con vistas al jardín tropical",
        "servicios": ["45 m²", "Cama King o 2 dobles", "Balcón privado", "Minibar", "WiFi gratis", "Aire acondicionado"],
        "patron_busqueda": ["junior-suite-garden", "JUNIOR-SUITE", "garden-lagoon"],
        "precio_noche": 180
    },
    "CMU JUNIOR SUITE PS": {
        "nombre": "Junior Suite Poolside",
        "descripcion": "Suite con acceso directo a la piscina",
        "servicios": ["50 m²", "Cama King", "Terraza con acceso piscina", "Minibar premium", "WiFi gratis", "Servicio 24h"],
        "patron_busqueda": ["poolside", "pool-side"],
        "precio_noche": 220
    },
    "CMU JUNIOR SUITE PS OV": {
        "nombre": "Junior Suite Poolside Ocean View",
        "descripcion": "Suite junto a piscina con vistas al mar",
        "servicios": ["50 m²", "Cama King", "Vistas al océano", "Acceso piscina", "Minibar premium", "Servicio 24h"],
        "patron_busqueda": ["poolside-ocean", "ocean-view"],
        "precio_noche": 260
    },
    "CMU LOFT SUITE JT": {
        "nombre": "Loft Suite Jacuzzi Terrace",
        "descripcion": "Suite dúplex con jacuzzi privado en terraza",
        "servicios": ["70 m²", "Jacuzzi privado", "2 niveles", "Sala de estar", "Minibar premium", "Vistas panorámicas"],
        "patron_busqueda": ["LOFT-SUITE", "Loft-Suite-Jacuzzi"],
        "precio_noche": 350
    },
    "CMU FAMILY SUITE": {
        "nombre": "Family Suite",
        "descripcion": "Suite familiar con espacio extra para niños",
        "servicios": ["65 m²", "2 habitaciones", "Sala de estar", "Minibar", "WiFi gratis", "Kids amenities"],
        "patron_busqueda": ["family", "FAMILY"],
        "precio_noche": 280
    },
    # TRS (Solo adultos)
    "TRS JUNIOR SUITE GV": {
        "nombre": "TRS Junior Suite Garden View",
        "descripcion": "Suite premium solo adultos con jardín privado",
        "servicios": ["55 m²", "Cama King", "Solo adultos", "Servicio butler", "Minibar premium", "Acceso exclusivo"],
        "patron_busqueda": ["Junior-Suite-Private-Pool-Garden", "JS-6726"],
        "precio_noche": 320
    },
    "TRS JUNIOR SUITE PS": {
        "nombre": "TRS Junior Suite Poolside",
        "descripcion": "Suite premium junto a la piscina infinita",
        "servicios": ["55 m²", "Jacuzzi privado", "Solo adultos", "Butler 24h", "Terraza privada", "Acceso spa"],
        "patron_busqueda": ["Poolside", "Jacuzzi-Terrace"],
        "precio_noche": 380
    },
    # Riviera Maya
    "COL JUNIOR SUITE GV": {
        "nombre": "Colonial Junior Suite Garden View",
        "descripcion": "Suite de estilo colonial con vistas al jardín",
        "servicios": ["48 m²", "Estilo colonial", "Balcón", "Minibar", "WiFi gratis", "Aire acondicionado"],
        "patron_busqueda": ["junior-suite", "garden"],
        "precio_noche": 170
    },
    "KAN JUNIOR SUITE GV": {
        "nombre": "Kantenah Junior Suite Garden View",
        "descripcion": "Suite frente a los cenotes naturales",
        "servicios": ["48 m²", "Vista cenote", "Balcón privado", "Minibar", "WiFi gratis", "Acceso cenote"],
        "patron_busqueda": ["junior-suite", "garden"],
        "precio_noche": 175
    },
    # Punta Cana
    "PAL JUNIOR SUITE GV": {
        "nombre": "Palace Junior Suite Garden View",
        "descripcion": "Suite elegante con vistas a jardines tropicales",
        "servicios": ["52 m²", "Cama King", "Balcón", "Minibar", "WiFi gratis", "Room service 24h"],
        "patron_busqueda": ["junior-suite-garden"],
        "precio_noche": 190
    },
    "PAL JUNIOR SUITE BS": {
        "nombre": "Palace Junior Suite Beachside",
        "descripcion": "Suite junto a la playa con vistas al océano",
        "servicios": ["52 m²", "Vista al mar", "Acceso playa", "Minibar premium", "WiFi gratis", "Servicio playa"],
        "patron_busqueda": ["junior-suite-beachside", "beachside"],
        "precio_noche": 240
    },
    "PAL DELUXE GV": {
        "nombre": "Palace Deluxe Garden View",
        "descripcion": "Habitación deluxe con vistas al jardín",
        "servicios": ["40 m²", "Cama King", "Terraza", "Minibar", "WiFi gratis", "Aire acondicionado"],
        "patron_busqueda": ["deluxe-garden", "DELUXE"],
        "precio_noche": 150
    },
    "PAL DELUXE BS": {
        "nombre": "Palace Deluxe Beachside",
        "descripcion": "Habitación deluxe con acceso directo a playa",
        "servicios": ["40 m²", "Frente al mar", "Terraza", "Minibar", "WiFi gratis", "Sombrilla incluida"],
        "patron_busqueda": ["deluxe-beachside", "DELUXE"],
        "precio_noche": 200
    },
    "BAV JUNIOR SUITE GV": {
        "nombre": "Bavaro Junior Suite Garden View",
        "descripcion": "Suite premium en playa Bávaro",
        "servicios": ["50 m²", "Vistas jardín", "Balcón", "Minibar premium", "WiFi gratis", "Servicio Select"],
        "patron_busqueda": ["junior-suite-garden"],
        "precio_noche": 210
    },
    "TRS JUNIOR SUITE GV/PV": {
        "nombre": "TRS Junior Suite Garden/Pool View",
        "descripcion": "Suite solo adultos con vistas a jardín o piscina",
        "servicios": ["55 m²", "Solo adultos", "Butler 24h", "Piscina infinita", "Premium all-inclusive", "Spa access"],
        "patron_busqueda": ["Junior-Suite", "Pool"],
        "precio_noche": 340
    }
}


def obtener_complejo_de_hotel(nombre_hotel: str) -> str:
    """
    Dado un nombre de hotel individual, devuelve el Complejo al que pertenece.
    Esto es necesario porque el modelo usa NOMBRE_HOTEL = Complejo.
    """
    resuelto = resolver_hotel_y_complejo(nombre_hotel)
    if resuelto:
        return resuelto[0]
    for complejo, hoteles in HOTELES_POR_COMPLEJO.items():
        if nombre_hotel in hoteles:
            return complejo
    return "Complejo Riviera Maya"  # Default


def resolver_hotel_y_complejo(nombre_hotel: str):
    """
    Normaliza nombres que vienen del agente/usuario (p.ej. 'TRS Coral' -> 'TRS Coral Hotel').
    Devuelve (complejo, hotel_canon) o None si no encuentra match.
    """
    if not nombre_hotel:
        return None

    s = str(nombre_hotel).strip().lower()
    if not s:
        return None

    # 1) Match exacto case-insensitive
    for complejo, hoteles in HOTELES_POR_COMPLEJO.items():
        for hotel_key in hoteles.keys():
            if s == hotel_key.strip().lower():
                return (complejo, hotel_key)

    # 2) Match parcial (subcadena) - escogemos el más largo (más específico)
    candidatos = []
    for complejo, hoteles in HOTELES_POR_COMPLEJO.items():
        for hotel_key in hoteles.keys():
            key = hotel_key.strip().lower()
            if s in key or key in s:
                candidatos.append((len(key), complejo, hotel_key))

    if candidatos:
        candidatos.sort(reverse=True)
        _, complejo, hotel_key = candidatos[0]
        return (complejo, hotel_key)

    return None


def generar_id_reserva(hotel: str) -> str:
    """
    Genera un ID de reserva con la misma estructura del dataset.
    Formato: XXXXXXXXXYYYYY donde YYYYY es el codigo de hotel.
    """
    codigo_hotel = CODIGO_HOTEL.get(hotel, "10601")
    # Generamos un numero secuencial basado en timestamp + random
    timestamp_part = int(datetime.datetime.now().timestamp()) % 100000000
    random_part = random.randint(0, 9)
    id_base = str(timestamp_part) + str(random_part)
    return id_base[:7] + codigo_hotel


# Ruta del archivo de historial de reservas (Dataset Maestro Unificado)
# Rutas de archivos
HISTORIAL_RESERVAS_PATH = os.path.join(os.path.dirname(__file__), "reservas_2026_full.csv")
RESERVAS_WEB_PATH = os.path.join(os.path.dirname(__file__), "reservas_web_2026.csv")


# Definición de columnas fijas para evitar corrupción de CSV
COLUMNAS_WEB_FIJAS = [
    'ID_RESERVA', 'LLEGADA', 'SALIDA', 'NOCHES', 'PAX', 'VALOR_RESERVA',
    'NOMBRE_HABITACION', 'CANAL', 'MERCADO', 'AGENCIA', 'NOMBRE_HOTEL_REAL',
    'COMPLEJO_REAL', 'PROBABILIDAD_CANCELACION', 'HOTEL_COMPLEJO',
    'CLIENTE_NOMBRE', 'CLIENTE_EMAIL', 'SEGMENTO', 'FIDELIDAD', 'FUENTE_NEGOCIO',
    'FECHA_CREACION', 'ESTADO'
]

def guardar_reserva_csv(reserva: dict) -> bool:
    """
    Guarda una reserva nueva en el archivo dedicado de reservas web (reservas_web_2026.csv).
    Preserva todos los datos del cliente y estructura enriquecida.
    """
    try:
        # Calcular fechas
        llegada_dt = pd.to_datetime(reserva.get('llegada'))
        # Asegurar noches int
        try:
            noches = int(reserva.get('noches', 0))
        except:
            noches = 1
        salida_dt = llegada_dt + datetime.timedelta(days=noches)
        
        # Mapear a columnas Estándar (Compatible con Maestro + Campos Nuevos)
        row_data = {
            'ID_RESERVA': str(reserva.get('id', '')), # ID como string para evitar líos
            'LLEGADA': llegada_dt.strftime('%Y-%m-%d'),
            'SALIDA': salida_dt.strftime('%Y-%m-%d'),
            'NOCHES': noches,
            'PAX': reserva.get('pax', 0),
            'VALOR_RESERVA': reserva.get('valor', 0),
            'NOMBRE_HABITACION': reserva.get('habitacion', ''),
            'CANAL': 'WEBPROPIA',
            'MERCADO': reserva.get('pais', 'Directo'),
            'AGENCIA': 'Cliente Directo',
            'NOMBRE_HOTEL_REAL': reserva.get('hotel', ''),
            'COMPLEJO_REAL': reserva.get('complejo', ''),
            'PROBABILIDAD_CANCELACION': reserva.get('cancel_prob', 0),
            'HOTEL_COMPLEJO': 'WEB_DIRECT', 
            # Campos NUEVOS ricos (Solo en archivo web)
            'CLIENTE_NOMBRE': reserva.get('nombre', ''),
            'CLIENTE_EMAIL': reserva.get('email', ''),
            'SEGMENTO': reserva.get('segmento', 'BAR'),
            'FIDELIDAD': reserva.get('fidelidad', 'None'),
            'FUENTE_NEGOCIO': reserva.get('fuente_negocio', 'DIRECT SALES'),
            'FECHA_CREACION': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'ESTADO': 'Confirmada'
        }
        
        df_new = pd.DataFrame([row_data])
        
        # Asegurar columnas exactas
        for col in COLUMNAS_WEB_FIJAS:
            if col not in df_new.columns:
                 df_new[col] = ''
        df_new = df_new[COLUMNAS_WEB_FIJAS]
        
        # Guardar en archivo Web (Append o Create)
        if os.path.exists(RESERVAS_WEB_PATH):
            # Append sin header
            df_new.to_csv(RESERVAS_WEB_PATH, mode='a', header=False, index=False)
        else:
            # Create con header
            df_new.to_csv(RESERVAS_WEB_PATH, mode='w', header=True, index=False)
            
        return True
    except Exception as e:
        st.error(f"Error guardando reserva web: {e}")
        return False


def cargar_reservas_csv() -> pd.DataFrame:
    """
    Carga y fusiona reservas del Histórico (Solo lectura) y Web (Lectura/Escritura).
    Devuelve DataFrame unificado.
    """
    dfs = []
    
    # 1. Cargar Reservas Web (Prioridad Alta, Datos Ricos)
    if os.path.exists(RESERVAS_WEB_PATH):
        try:
            # Tolerancia a fallos: Saltamos líneas corruptas automáticamente
            df_web = pd.read_csv(RESERVAS_WEB_PATH, on_bad_lines='skip')
            # Limpieza básica web
            df_web.columns = df_web.columns.astype(str).str.strip()
            df_web['ID_RESERVA'] = df_web['ID_RESERVA'].astype(str).str.strip()
            # Convertir fechas web
            for col in ['LLEGADA', 'SALIDA']:
                if col in df_web.columns:
                    df_web[col] = pd.to_datetime(df_web[col], errors='coerce')
            dfs.append(df_web)
        except Exception as e:
            st.error(f"Error leyendo reservas web (Archivo corrupto, se ignorará): {e}")

    # 2. Cargar Histórico (Prioridad Baja, Datos Anónimos)
    if os.path.exists(HISTORIAL_RESERVAS_PATH):
        try:
            # Optimización: Leer tipos específicos si es posible, pero fallan los floats en ID.
            # Leemos todo y convertimos después.
            df_hist = pd.read_csv(HISTORIAL_RESERVAS_PATH)
            df_hist.columns = df_hist.columns.astype(str).str.strip()
            
            # Limpieza ID Histórico
            if 'ID_RESERVA' in df_hist.columns:
                df_hist.dropna(subset=['ID_RESERVA'], inplace=True)
                # Convertir float IDs (123.0) a string "123"
                df_hist['ID_RESERVA'] = df_hist['ID_RESERVA'].apply(
                    lambda x: str(int(x)) if isinstance(x, float) and x.is_integer() else str(x)
                ).str.strip()
            
            # Fechas Histórico
            for col in ['LLEGADA', 'SALIDA']:
                if col in df_hist.columns:
                    df_hist[col] = pd.to_datetime(df_hist[col], errors='coerce')
            
            dfs.append(df_hist)
        except Exception as e:
            st.error(f"Error cargando histórico: {e}")

    if not dfs:
        return pd.DataFrame()
        
    # Fusionar (Web va primero -> Prioridad en drop_duplicates si hiciera falta)
    # concat alinea columnas automáticamente. Las columnas de Web (Cliente) serán NaN en Histórico.
    df_full = pd.concat(dfs, ignore_index=True)
    
    return df_full


def buscar_reserva_por_id(id_reserva: str) -> dict:
    """
    Busca una reserva por su código.
    Devuelve un diccionario con formato estandarizado para la UI.
    """
    try:
        df = cargar_reservas_csv()
        if df.empty:
            return {}
            
        # Detectar columnas
        cols = df.columns.tolist()
        
        # Mapping de columnas (CSV Real -> UI Esperada)
        # CSV Real visto: ID_RESERVA, LLEGADA, SALIDA, NOCHES, PAX, NOMBRE_HABITACION, 
        # VALOR_RESERVA, HOTEL_COMPLEJO, NOMBRE_HOTEL_REAL, COMPLEJO_REAL, PROBABILIDAD_CANCELACION
        
        col_id = 'ID_RESERVA' if 'ID_RESERVA' in cols else 'id_reserva'
        
        # Buscar
        if col_id in cols:
            id_buscado = str(id_reserva).strip()
            if id_buscado.endswith(".0"): id_buscado = id_buscado[:-2]
            
            # Filtrado seguro
            df[col_id] = df[col_id].astype(str).str.strip()
            match = df[df[col_id] == id_buscado]
            
            if not match.empty:
                row = match.iloc[0]
                
                # Extracción directa usando nombres confirmados del CSV
                # Nota: Usamos .get() con valores default por seguridad
                
                # Cliente (No existe en CSV, simulamos)
                cliente_nombre = row.get('CLIENTE_NOMBRE', f"Huésped {id_buscado[-4:]}")
                if pd.isna(cliente_nombre) or str(cliente_nombre) == 'nan':
                    cliente_nombre = f"Huésped {id_buscado[-4:]}" # Ej: Huésped 0601
                
                # Probabilidad (Ajuste escala 0-1 -> 0-100)
                prob_raw = row.get('PROBABILIDAD_CANCELACION', 0.0)
                if pd.isna(prob_raw): prob_raw = 0.0
                if 0.0 < prob_raw <= 1.0:
                    prob_pct = prob_raw * 100.0  # Convertir 0.21 -> 21.0
                else:
                    prob_pct = prob_raw # Asumimos ya es % o es 0
                    
                # Valor
                valor = row.get('VALOR_RESERVA', 0.0)
                if pd.isna(valor): valor = 0.0
                
                return {
                    'id': str(row.get(col_id)),
                    'llegada': row.get('LLEGADA', row.get('llegada')),
                    'salida': row.get('SALIDA', row.get('salida')),
                    'noches': row.get('NOCHES', 1),
                    'pax': row.get('PAX', 2),
                    'valor': float(valor),
                    'habitacion': row.get('NOMBRE_HABITACION', 'Estándar'),
                    'hotel': row.get('NOMBRE_HOTEL_REAL', row.get('hotel', 'Hotel Desconocido')),
                    'complejo': row.get('COMPLEJO_REAL', 'Palladium Complex'),
                    'cancel_prob': float(prob_pct),
                    'nombre': cliente_nombre,
                    'email': row.get('CLIENTE_EMAIL', 'cliente@example.com'),
                    'estado': row.get('ESTADO', 'Confirmada'),
                    'raw_data': row.to_dict() # Debug info for UI
                }
    except Exception as e:
        st.error(f"Error buscando reserva: {e}")
        return {}
        
    return {}


def actualizar_reserva_csv(id_reserva: str, campo: str, valor) -> bool:
    """
    Actualiza un campo específico de una reserva.
    """
    if os.path.exists(HISTORIAL_RESERVAS_PATH):
        try:
            df = pd.read_csv(HISTORIAL_RESERVAS_PATH)
            
            # Detectar columna ID
            col_id = 'ID_RESERVA' if 'ID_RESERVA' in df.columns else 'id_reserva'
            
            if col_id in df.columns:
                mask = df[col_id].astype(str).str.strip() == str(id_reserva).strip()
                if mask.any():
                    # Mapear campos UI -> Maestro si es necesario
                    # (Por ahora asumimos que 'campo' viene correcto o lo dejamos dinámico)
                    # Si el campo es 'estado' y el CSV usa 'ESTADO', adaptar
                    if campo == 'estado' and 'ESTADO' in df.columns:
                        campo_real = 'ESTADO'
                    else:
                        campo_real = campo
                        
                    df.loc[mask, campo_real] = valor
                    df.to_csv(HISTORIAL_RESERVAS_PATH, index=False)
                    return True
        except Exception as e:
            st.error(f"Error actualizando reserva: {e}")
    return False


def obtener_imagen_hotel(nombre_hotel: str) -> str:
    """
    Devuelve la ruta completa de la imagen del hotel.
    """
    ruta_relativa = IMAGENES_HOTELES.get(nombre_hotel, "")
    if ruta_relativa:
        return os.path.join(os.path.dirname(__file__), "media", ruta_relativa)
    return ""


def obtener_imagenes_habitacion(codigo_habitacion: str, complejo: str, max_imagenes: int = 4) -> list:
    """
    Devuelve una lista de rutas de imágenes de la habitación.
    Usa patrones de búsqueda inteligentes basados en el tipo de habitación.
    """
    imagenes = []
    
    # Determinar prefijo
    prefijo = codigo_habitacion.split()[0] if codigo_habitacion else ""
    
    # Caso especial para TRS (depende del complejo)
    if prefijo == "TRS":
        carpeta = CARPETAS_TRS.get(complejo, "habitaciones/MAYA_TRS")
    else:
        carpeta = CARPETAS_HABITACIONES.get(prefijo, "")
    
    if carpeta:
        carpeta_completa = os.path.join(os.path.dirname(__file__), "media", carpeta)
        if os.path.exists(carpeta_completa):
            # Obtener patrones de búsqueda
            info_hab = DESCRIPCIONES_HABITACIONES.get(codigo_habitacion, {})
            patrones = info_hab.get("patron_busqueda", [])
            
            # Buscar imágenes que coincidan con patrones (excluyendo planos)
            archivos_encontrados = []
            for archivo in os.listdir(carpeta_completa):
                if archivo.endswith(('.jpg', '.webp', '.png')):
                    # Excluir planos (formato GP-HOTEL_TIPO con guiones bajos)
                    if archivo.startswith('GP-') and '_' in archivo and archivo.split('_')[0].isupper():
                        continue  # Es un plano, lo saltamos aquí
                    
                    # Priorizar por patrón
                    prioridad = 100
                    for i, patron in enumerate(patrones):
                        if patron.lower() in archivo.lower():
                            prioridad = i
                            break
                    archivos_encontrados.append((prioridad, archivo))
            
            # Ordenar por prioridad y tomar las primeras max_imagenes
            archivos_encontrados.sort(key=lambda x: x[0])
            for _, archivo in archivos_encontrados[:max_imagenes]:
                imagenes.append(os.path.join(carpeta_completa, archivo))
    
    return imagenes


def obtener_plano_habitacion(codigo_habitacion: str, complejo: str) -> str:
    """
    Devuelve la ruta del plano de planta de la habitación si existe.
    Los planos tienen formato: GP-HOTEL_TIPO.webp
    """
    # Determinar prefijo
    prefijo = codigo_habitacion.split()[0] if codigo_habitacion else ""
    
    # Caso especial para TRS (depende del complejo)
    if prefijo == "TRS":
        carpeta = CARPETAS_TRS.get(complejo, "habitaciones/MAYA_TRS")
    else:
        carpeta = CARPETAS_HABITACIONES.get(prefijo, "")
    
    if carpeta:
        carpeta_completa = os.path.join(os.path.dirname(__file__), "media", carpeta)
        if os.path.exists(carpeta_completa):
            # Obtener patrones de búsqueda para el plano
            info_hab = DESCRIPCIONES_HABITACIONES.get(codigo_habitacion, {})
            patrones = info_hab.get("patron_busqueda", [])
            
            # Buscar archivos que parezcan planos
            for archivo in os.listdir(carpeta_completa):
                if archivo.endswith(('.jpg', '.webp', '.png')):
                    # Identificar planos: formato GP-HOTEL_TIPO (mayúsculas con guiones bajos)
                    if archivo.startswith('GP-') and '_' in archivo:
                        # Verificar que coincide con algún patrón de la habitación
                        for patron in patrones:
                            if patron.upper().replace('-', '_').replace(' ', '_') in archivo.upper():
                                return os.path.join(carpeta_completa, archivo)
                        # Si no hay patrón específico, buscar por nombre de habitación
                        nombre_hab_upper = codigo_habitacion.upper().replace(' ', '-')
                        if nombre_hab_upper in archivo.upper() or any(p in archivo.upper() for p in nombre_hab_upper.split('-') if len(p) > 3):
                            return os.path.join(carpeta_completa, archivo)
    
    return ""


def obtener_imagen_habitacion(codigo_habitacion: str, complejo: str) -> str:
    """
    Devuelve la ruta de una imagen representativa de la habitación (backwards compatibility).
    """
    imagenes = obtener_imagenes_habitacion(codigo_habitacion, complejo, max_imagenes=1)
    return imagenes[0] if imagenes else ""


def obtener_info_habitacion(codigo_habitacion: str) -> dict:
    """
    Devuelve la información descriptiva de una habitación.
    """
    info = DESCRIPCIONES_HABITACIONES.get(codigo_habitacion, {})
    if not info:
        # Generar información por defecto
        return {
            "nombre": codigo_habitacion,
            "descripcion": "Habitación confortable con todas las comodidades",
            "servicios": ["Aire acondicionado", "WiFi gratis", "Minibar", "Caja fuerte", "TV pantalla plana"],
            "precio_noche": 160
        }
    return info


def render_wizard_steps(current_step: int):
    """
    Renderiza los pasos del wizard con un estilo unificado y profesional.
    """
    steps = ["Destino", "Buscador", "Selección", "Reserva"]
    
    st.markdown('<div class="wizard-container">', unsafe_allow_html=True)
    cols = st.columns(len(steps))
    
    for i, (col, step_name) in enumerate(zip(cols, steps), 1):
        with col:
            is_active = i == current_step
            is_completed = i < current_step
            
            status_class = "active" if is_active else ("completed" if is_completed else "")
            icon = "✓" if is_completed else str(i)
            
            st.markdown(f"""
<div class="wizard-step {status_class}">
<div class="step-number">{icon}</div>
<div style="font-family: 'Cinzel', serif; letter-spacing: 1px; font-size: 0.8rem; text-transform: uppercase;">{step_name}</div>
</div>
""", unsafe_allow_html=True)
    
    st.markdown('</div>', unsafe_allow_html=True)


# =============================================================================
# VISTA CLIENTE - WIZARD
# =============================================================================
def render_vista_cliente():
    """
    Renderiza el wizard de reserva en 4 pasos REFACTORIZADO.
    Paso 1: Buscador (Destino, Fechas, Pax)
    Paso 2: Selección de Hotel
    Paso 3: Selección de Habitación
    Paso 4: Datos y Confirmación
    """
    
    # Inicializamos estados
    if 'wizard_step' not in st.session_state:
        st.session_state.wizard_step = 1
    if 'reservations' not in st.session_state:
        st.session_state.reservations = []
    
    # Renderizamos steps
    render_wizard_steps(st.session_state.wizard_step)
    
    # CSS Personalizado Simplificado y Ajustado
    st.markdown(f"""
<style>
/* Estilo base para botones secundarios (los normales) */
div.stButton > button {{
background-color: white;
color: {COLOR_GRIS};
border: 1px solid {COLOR_BEIGE};
}}
/* Hover marrón para TODOS los botones */
div.stButton > button:hover {{
background-color: {COLOR_DORADO_OSCURO} !important;
border-color: {COLOR_DORADO_OSCURO} !important;
color: white !important;
}}
/* Boton primario (Confirmar) */
div.stButton > button[kind="primary"] {{
background-color: {COLOR_DORADO};
color: white !important;
border: none;
font-weight: 600;
}}
/* Ajuste para el primer hijo (streamlitez) */
div.stButton > button:first-child:active {{
background-color: {COLOR_DORADO_OSCURO};
color: white;
}}
/* Estilo para Inputs (Cajas de texto) - Tono Marrón Clarito */
.stTextInput > div > div > input, .stNumberInput > div > div > input, .stDateInput > div > div > input, .stSelectbox > div > div > div {{
background-color: {COLOR_CREMA} !important;
border-color: {COLOR_OLIVA} !important;
color: {COLOR_NEGRO} !important;
}}
/* Caja Rewards (Verde Oscuro) */
.rewards-box {{
background-color: {COLOR_VERDE_OSCURO};
color: white;
padding: 20px;
border-radius: 5px;
margin-top: 20px;
margin-bottom: 20px;
text-align: center;
}}
.rewards-title {{
font-family: 'Cinzel', serif;
letter-spacing: 2px;
font-size: 1.2rem;
margin-bottom: 5px;
}}
.rewards-subtitle {{
font-size: 0.8rem;
letter-spacing: 4px;
text-transform: uppercase;
margin-bottom: 15px;
}}
</style>
""", unsafe_allow_html=True)
    
    # -------------------------------------------------------------------------
    # PASO 1: BUSCADOR (DESTINO, FECHAS, PAX)
    # -------------------------------------------------------------------------
    if st.session_state.wizard_step == 1:
        st.markdown("<h2 style='text-align:center;'>Empiece su viaje</h2>", unsafe_allow_html=True)
        
        # 1. Selección de Destino (- Seleccionado es PRIMARY, no seleccionado es SECONDARY)
        col_dest1, col_dest2 = st.columns(2)
        with col_dest1:
            if st.button("MEXICO\n\nCancun - Riviera Maya", key="dest_mexico", width="stretch", type="primary" if st.session_state.get('destino_seleccionado') == 'MEXICO' else "secondary"):
                st.session_state.destino_seleccionado = "MEXICO"
                st.rerun()
        with col_dest2:
            if st.button("REPUBLICA DOMINICANA\n\nPunta Cana", key="dest_rd", width="stretch", type="primary" if st.session_state.get('destino_seleccionado') == 'REPUBLICA DOMINICANA' else "secondary"):
                st.session_state.destino_seleccionado = "REPUBLICA DOMINICANA"
                st.rerun()

        st.markdown("---")
        
        # 2. Fechas y Huéspedes
        st.subheader("Fechas y Huéspedes")
        col_f1, col_f2, col_f3, col_f4, col_f5 = st.columns(5)
        
        with col_f1:
            fecha_llegada = st.date_input(
                "Llegada",
                value=datetime.date.today() + datetime.timedelta(days=30),
                min_value=datetime.date.today(),
                key="w_llegada"
            )
        
        with col_f2:
            noches = st.number_input("Noches", min_value=1, max_value=30, value=7, key="w_noches")
            
        with col_f3:
            adultos = st.number_input("Adultos", min_value=1, max_value=6, value=2, key="w_adultos")
            
        with col_f4:
            ninos = st.number_input("Niños", min_value=0, max_value=4, value=0, key="w_ninos")
            
        with col_f5:
            cunas = st.number_input("Cunas", min_value=0, max_value=2, value=0, key="w_cunas")

        st.markdown("---")
        
        # Botón Buscar - PRIMARY (Dorado)
        if st.button("BUSCAR HOTELES", key="btn_buscar_hoteles", type="primary", width="stretch"):
            if not st.session_state.get('destino_seleccionado'):
                st.error("Por favor, seleccione un destino primero.")
            else:
                # Guardar datos
                st.session_state.reserva_llegada = fecha_llegada
                st.session_state.reserva_noches = noches
                st.session_state.reserva_adultos = adultos
                st.session_state.reserva_ninos = ninos
                st.session_state.reserva_cunas = cunas
                st.session_state.reserva_pax = adultos + ninos
                # Avanzar
                st.session_state.wizard_step = 2
                st.rerun()

    # -------------------------------------------------------------------------
    # PASO 2: SELECCIÓN DE HOTEL
    # -------------------------------------------------------------------------
    elif st.session_state.wizard_step == 2:
        destino = st.session_state.get('destino_seleccionado', 'MEXICO')
        complejos_disponibles = DESTINOS[destino]['complejos']
        destino_nombre = DESTINOS[destino]['nombre']
        
        st.markdown(textwrap.dedent(f"""
            <h2 style="text-align: center; font-weight: 300; margin-bottom: 0.5rem;">Hoteles en {destino_nombre}</h2>
        """), unsafe_allow_html=True)
        
        st.markdown("<h3 style='text-align: center; margin-top: 2rem;'>Seleccione su Zona de Destino</h3>", unsafe_allow_html=True)
        
        col_z1, col_z2 = st.columns(2)
        
        with col_z1:
            img_costa = obtener_imagen_hotel("Grand Palladium Select Costa Mujeres")
            b64_costa = get_base64_image(img_costa)
            st.markdown(f'''
<div class="zone-selection-card">
<div class="zone-image-container" style="background-image: url('data:image/jpeg;base64,{b64_costa}');"></div>
<div style="padding: 1rem; text-align: center;">
<h4 style="margin: 0; color: {COLOR_DORADO_OSCURO}; font-family: 'Cinzel', serif;">Costa Mujeres</h4>
</div>
</div>
''', unsafe_allow_html=True)
            if st.button("SELECCIONAR COSTA MUJERES", key="sel_zone_costa", type="primary" if st.session_state.get('reserva_complejo') == "Complejo Costa Mujeres" else "secondary"):
                st.session_state.reserva_complejo = "Complejo Costa Mujeres"
                st.rerun()
                
        with col_z2:
            img_riviera = obtener_imagen_hotel("Grand Palladium Colonial Resort & Spa")
            b64_riviera = get_base64_image(img_riviera)
            st.markdown(f'''
<div class="zone-selection-card">
<div class="zone-image-container" style="background-image: url('data:image/jpeg;base64,{b64_riviera}');"></div>
<div style="padding: 1rem; text-align: center;">
<h4 style="margin: 0; color: {COLOR_DORADO_OSCURO}; font-family: 'Cinzel', serif;">Riviera Maya</h4>
</div>
</div>
''', unsafe_allow_html=True)
            if st.button("SELECCIONAR RIVIERA MAYA", key="sel_zone_riviera", type="primary" if st.session_state.get('reserva_complejo') == "Complejo Riviera Maya" else "secondary"):
                st.session_state.reserva_complejo = "Complejo Riviera Maya"
                st.rerun()

        complejo = st.session_state.get('reserva_complejo', complejos_disponibles[0])
        if complejo not in complejos_disponibles: complejo = complejos_disponibles[0]
        st.session_state.reserva_complejo = complejo
        
        # Listar hoteles
        hoteles_del_complejo = HOTELES_POR_COMPLEJO.get(complejo, {})
        
        # Inicializar seleccion de hotel
        if 'hotel_seleccionado_paso3' not in st.session_state:
             st.session_state.hotel_seleccionado_paso3 = None
             
        for nombre_hotel, info_hotel in hoteles_del_complejo.items():
            imagen_hotel = obtener_imagen_hotel(nombre_hotel)
            descripcion = info_hotel.get('descripcion', '')
            
            col_img, col_info = st.columns([1, 2])
            with col_img:
                if imagen_hotel and os.path.exists(imagen_hotel):
                     st.image(imagen_hotel, width="stretch")
            with col_info:
                st.markdown(f"### {nombre_hotel}")
                st.markdown(f"{descripcion}")
                # BOTON VER HABITACIONES - SECONDARY
                if st.button("VER HABITACIONES", key=f"btn_sel_{nombre_hotel}", type="secondary"):
                    st.session_state.hotel_seleccionado_paso3 = nombre_hotel
                    st.session_state.wizard_step = 3
                    st.rerun()
            st.markdown("---")
            
        # Navegación
        if st.button("Atrás", key="back_3_no_hotel", type="secondary"):
            st.session_state.wizard_step = 1
            st.rerun()

    # -------------------------------------------------------------------------
    # PASO 3: SELECCIÓN DE HABITACIÓN
    # -------------------------------------------------------------------------
    elif st.session_state.wizard_step == 3:
        hotel_seleccionado = st.session_state.hotel_seleccionado_paso3
        
        # Lógica robusta para encontrar el complejo real del hotel seleccionado
        complejo_real = None
        
        # 1. Intentar con función helper
        complejo_candidato = obtener_complejo_de_hotel(hotel_seleccionado)
        if complejo_candidato in HOTELES_POR_COMPLEJO and hotel_seleccionado in HOTELES_POR_COMPLEJO[complejo_candidato]:
            complejo_real = complejo_candidato
        
        # 2. Si falla, buscar manualmente en todos los complejos
        if not complejo_real:
            for c_key, c_val in HOTELES_POR_COMPLEJO.items():
                if hotel_seleccionado in c_val:
                    complejo_real = c_key
                    break
        
        # Si aún no tenemos complejo (no debería pasar), usar fallback suave
        if not complejo_real:
             st.error(f"No se encontraron datos para el hotel: {hotel_seleccionado}")
             if st.button("Volver", type="secondary"):
                 st.session_state.wizard_step = 2
                 st.rerun()
             return

        st.markdown(f"## Habitaciones en {hotel_seleccionado}")
        
        # Datos de fechas (Contexto)
        fecha_llegada = st.session_state.get('reserva_llegada', datetime.date.today())
        noches = st.session_state.get('reserva_noches', 7)
        fecha_salida = fecha_llegada + datetime.timedelta(days=noches)
        pax = st.session_state.get('reserva_pax', 2)
        
        st.info(f"{fecha_llegada.strftime('%d %b')} - {fecha_salida.strftime('%d %b %Y')} ({noches} noches) | {pax} Huéspedes")

        # Obtener habitaciones del hotel usando el complejo VALIDADO
        hoteles_del_complejo = HOTELES_POR_COMPLEJO.get(complejo_real, {})
        info_hotel_data = hoteles_del_complejo.get(hotel_seleccionado, {})
        habitaciones_hotel = info_hotel_data.get('habitaciones', [])
        
        if not habitaciones_hotel:
            st.warning("No hay habitaciones disponibles para este hotel.")
        
        # Listar habitaciones con DETALLE COMPLETO
        for codigo_hab in habitaciones_hotel:
            info_hab = obtener_info_habitacion(codigo_hab)
            nombre_hab = info_hab.get('nombre', codigo_hab)
            
            # CALCULO DE PRECIO MEDIO 2026
            avg_price = get_precio_medio_2026(hotel_seleccionado, codigo_hab, pax)
            if avg_price > 0:
                precio_display = f"Desde {avg_price:,.0f}€"
            else:
                precio_display = f"{info_hab.get('precio_noche', 160)}€"
            
            desc_hab = info_hab.get('descripcion', '')
            servicios = info_hab.get('servicios', [])
            
            # Imágenes (Carousel) y Plano
            imgs = obtener_imagenes_habitacion(codigo_hab, complejo_real, max_imagenes=3)
            plano = obtener_plano_habitacion(codigo_hab, complejo_real)
            
            with st.container(border=True):
                c1, c2 = st.columns([1, 1.5])
                
                with c1:
                    # Galeria de imagenes
                    if imgs:
                        if len(imgs) > 1:
                            # Simple "carousel" using tabs or just first image
                            st.image(imgs[0], width="stretch", caption=f"{nombre_hab} (Vista Principal)")
                            with st.expander(f"Ver más fotos ({len(imgs)})"):
                                for img in imgs:
                                    st.image(img, caption=os.path.basename(img))
                        else:
                            st.image(imgs[0], width="stretch")
                    else:
                        st.markdown(f"**{nombre_hab}** (Sin foto)")
                        
                    # Plano
                    if plano and os.path.exists(plano):
                        with st.expander("VER PLANO / DISTRIBUCIÓN"):
                            st.image(plano, caption="Plano de la habitación")

                with c2:
                    st.subheader(nombre_hab)
                    st.markdown(f"_{desc_hab}_")
                    
                    st.markdown("#### Características:")
                    # Renderizar bullet points en 2 columnas si son muchos
                    sc1, sc2 = st.columns(2)
                    mid = (len(servicios) + 1) // 2
                    for s in servicios[:mid]:
                        sc1.markdown(f"• {s}")
                    for s in servicios[mid:]:
                        sc2.markdown(f"• {s}")
                    
                    st.markdown(f"### **{precio_display}** / noche")
                    
                    # BOTON RESERVAR - SECONDARY
                    if st.button("RESERVAR AHORA", key=f"btn_res_{codigo_hab}", type="secondary"):
                        st.session_state.reserva_habitacion = codigo_hab
                        st.session_state.reserva_complejo = complejo_real
                        st.session_state.wizard_step = 4
                        st.rerun()
            
            st.markdown("---")
            
        if st.button("Atrás", key="back_to_2", type="secondary"):
            st.session_state.wizard_step = 2
            st.rerun()
    
    # -------------------------------------------------------------------------
    # PASO 4: DATOS DEL HUÉSPED
    # -------------------------------------------------------------------------
    elif st.session_state.wizard_step == 4:
        st.subheader("Datos del Huésped")
        
        col_datos1, col_datos2 = st.columns(2)
        
        with col_datos1:
            nombre = st.text_input("Nombre completo *", key="w_nombre")
            email = st.text_input("Email *", key="w_email")
            telefono = st.text_input("Teléfono", key="w_telefono")
            
        with col_datos2:
            pais_opciones = ["ESPAÑA", "USA", "CANADA", "UK", "FRANCIA", "ALEMANIA", "ITALIA", "MEXICO", "BRASIL", "ARGENTINA", "OTRO"]
            pais = st.selectbox("País de residencia", pais_opciones, key="w_pais")
            
            # Caja Palladium Rewards
            st.markdown(textwrap.dedent(f"""
                <div class="rewards-box" style="background-color: {COLOR_VERDE_OSCURO}; color: white !important;">
                    <div style="font-family: 'Cinzel', serif; letter-spacing: 5px; font-weight: 500;">PALLADIUM</div>
                    <div style="font-family: 'Cinzel', serif; letter-spacing: 3px; font-size: 0.7rem; opacity: 0.8;">R E W A R D S</div>
                </div>
            """), unsafe_allow_html=True)
            
            # Si ya está verificado por el agente o previamente
            if st.session_state.get('rewards_verificado', False):
                 st.success(f"✅ Miembro Verificado ({st.session_state.get('cliente_fidelidad', 'Palladium Rewards')})")
                 st.info("Descuento especial del 5% aplicado al precio final.")
            else:
                rewards_email = st.text_input("Email de miembro Rewards", key="w_rewards_email")
                if st.button("VERIFICAR MIEMBRO", type="primary", key="btn_verify_rewards"):
                    if rewards_email and "@" in rewards_email:
                        st.success("Miembro verificado. Precio especial aplicado.")
                        st.session_state.rewards_verificado = True
                        st.session_state.cliente_fidelidad = "Palladium Rewards"
                        st.rerun()
                    else:
                        st.error("Email no válido o no encontrado.")
                        st.session_state.rewards_verificado = False
            
        st.markdown("---")
        
        col_resumen, col_btn = st.columns([2, 1])
        
        with col_resumen:
             # Recuperar datos para resumen detallado
             llegada = st.session_state.get('reserva_llegada', datetime.date.today())
             noches = st.session_state.get('reserva_noches', 7)
             salida = llegada + datetime.timedelta(days=noches)
             hab = st.session_state.get('reserva_habitacion', 'Habitación Estándar')
             adultos = st.session_state.get('reserva_adultos', 2)
             ninos = st.session_state.get('reserva_ninos', 0)
             hotel_sel = st.session_state.get('hotel_seleccionado_paso3', 'Hotel Palladium')
             
             # Estilo igual a Inputs
             # Calcular PRECIO REAL para el resumen
             try:
                 total_resumen = calcular_coste_estancia(
                    hotel_nombre=hotel_sel,
                    habitacion=hab,
                    llegada=llegada,
                    noches=noches,
                    pax=adultos + ninos # Asumimos pax total
                 )
             except:
                 total_resumen = 0.0

             estilo_resumen = f"""
<div style="background-color: {COLOR_CREMA}; border-radius: 4px; padding: 2rem; color: {COLOR_NEGRO} !important; font-size: 0.95rem; margin-bottom: 20px; border: 1px solid {COLOR_DORADO}; box-shadow: 0 4px 15px rgba(0,0,0,0.05);">
<div style="font-size: 0.75rem; letter-spacing: 2px; text-transform: uppercase; margin-bottom: 1rem; opacity: 0.7; color: {COLOR_DORADO_OSCURO} !important; font-weight: 600;">Resumen de su Selección</div>
<h3 style="margin: 0 0 10px 0; color: {COLOR_DORADO_OSCURO} !important; font-weight: 500; font-family: 'Cinzel', serif;">{hotel_sel}</h3>
<p style="margin: 2px 0; color: {COLOR_NEGRO} !important;"><strong>{hab}</strong></p>
<p style="margin: 2px 0; font-size: 0.85rem; color: {COLOR_GRIS} !important;">{st.session_state.get('destino_seleccionado', 'Destino')} - {st.session_state.get('reserva_complejo', 'Complejo')}</p>
<div style="margin-top: 1.5rem; display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; border-top: 1px solid {COLOR_BEIGE}; padding-top: 1.5rem;">
<div>
<div style="font-size: 0.7rem; text-transform: uppercase; opacity: 0.7; margin-bottom: 0.3rem;">Estancia</div>
<div style="font-size: 0.95rem; color: {COLOR_NEGRO} !important;">{llegada.strftime('%d/%m/%Y')} - {salida.strftime('%d/%m/%Y')}</div>
<div style="font-size: 0.85rem; opacity: 0.8; color: {COLOR_GRIS} !important;">{noches} noches</div>
</div>
<div>
<div style="font-size: 0.7rem; text-transform: uppercase; opacity: 0.7; margin-bottom: 0.3rem;">Huéspedes</div>
<div style="font-size: 0.95rem; color: {COLOR_NEGRO} !important;">{adultos} Adultos, {ninos} Niños</div>
</div>
</div>
<div style="margin-top: 1.5rem; text-align: right; border-top: 1px solid {COLOR_BEIGE}; padding-top: 1rem;">
<div style="font-size: 0.8rem; opacity: 0.8; color: {COLOR_GRIS} !important;">Precio total aproximado</div>
<div style="font-size: 2.5rem; font-weight: 300; color: {COLOR_DORADO_OSCURO} !important;">{total_resumen:,.0f} EUR</div>
</div>
</div>
"""
             st.markdown(estilo_resumen, unsafe_allow_html=True)
             
        with col_btn:
            if st.button("CONFIRMAR RESERVA", type="primary", width="stretch", key="btn_confirm_final"):
                if not nombre or not email:
                    st.error("Por favor, complete los campos obligatorios (*)")
                else:
                    # Guardar datos en session
                    st.session_state.cliente_nombre = nombre
                    st.session_state.cliente_email = email
                    st.session_state.cliente_pais = pais
                    st.session_state.cliente_telefono = telefono

                    # Si el usuario está verificado como Rewards, marcamos fidelidad.
                    st.session_state.cliente_fidelidad = (
                        "Palladium Rewards"
                        if st.session_state.get("rewards_verificado", False)
                        else "Sin programa"
                    )
                    
                    # Calcular valor REAL
                    st.session_state.reserva_valor = calcular_coste_estancia(
                        hotel_nombre=st.session_state.get("hotel_seleccionado_paso3", ""),
                        habitacion=st.session_state.get("reserva_habitacion", ""),
                        llegada=st.session_state.get("reserva_llegada", datetime.date.today()),
                        noches=st.session_state.get("reserva_noches", 7),
                        pax=st.session_state.get("reserva_pax", 2),
                    )
                    
                    # Avanzar a confirmación final real
                    st.session_state.wizard_step = 5  # Pantalla de éxito
                    st.rerun()

        if st.button("Atrás", key="back_from_4", type="secondary"):
            st.session_state.wizard_step = 3
            st.rerun()

    # -------------------------------------------------------------------------
    # PASO 5: CONFIRMACION FINAL (Pantalla de éxito)
    # -------------------------------------------------------------------------
    elif st.session_state.wizard_step == 5:
        # Recuperamos datos de toda la sesión para guardar la reserva
        hotel = st.session_state.get('hotel_seleccionado_paso3', '')
        complejo = st.session_state.get('reserva_complejo', '')
        habitacion = st.session_state.get('reserva_habitacion', '')
        llegada = st.session_state.get('reserva_llegada', datetime.date.today())
        noches = st.session_state.get('reserva_noches', 7)
        pax = st.session_state.get('reserva_pax', 2)
        adultos = st.session_state.get('reserva_adultos', 2)
        ninos = st.session_state.get('reserva_ninos', 0)
        cunas = st.session_state.get('reserva_cunas', 0)
        valor = st.session_state.get('reserva_valor', 0)
        
        nombre = st.session_state.get('cliente_nombre', '')
        email = st.session_state.get('cliente_email', '')
        telefono = st.session_state.get('cliente_telefono', '')
        pais = st.session_state.get('cliente_pais', 'ESPAÑA')
        fidelidad = st.session_state.get('cliente_fidelidad', 'Sin programa')
        destino = st.session_state.get('destino_seleccionado', 'MEXICO')
        
        # Generamos ID de reserva
        id_reserva = generar_id_reserva(complejo)
        
        # Calculamos prediccion
        cancel_prob = 0.0
        try:
            model = load_model()
            fecha_toma = datetime.datetime.now()
            llegada_dt = pd.to_datetime(llegada)
            fidelidad_val = None if fidelidad == "Sin programa" else fidelidad
            
            processed_df = get_features(
                LLEGADA=llegada_dt, NOCHES=noches, PAX=pax, ADULTOS=adultos,
                CLIENTE="ROIBACK (GLOBAL OBI S.L.)", FECHA_TOMA=fecha_toma,
                FIDELIDAD=fidelidad_val, PAIS=pais,
                SEGMENTO="Loyalty" if fidelidad_val else "BAR",
                FUENTE_NEGOCIO="DIRECT SALES",
                NOMBRE_HOTEL=complejo,
                NOMBRE_HABITACION=habitacion,
                VALOR_RESERVA=valor
            )
            
            features = processed_df
            if hasattr(model, 'feature_name_'):
                 features = processed_df[model.feature_name_]
            
            if model is not None:
                cancel_prob = model.predict_proba(features)[0][1]
        except Exception as e:
            print(f"Error prediccion step 5: {e}")
            cancel_prob = 0.0
            
        # Construimos el diccionario de reserva
        reserva = {
            'id': id_reserva,
            'nombre': nombre,
            'email': email,
            'telefono': telefono,
            'pais': pais,
            'destino': destino,
            'hotel': hotel,
            'complejo': complejo,
            'habitacion': habitacion,
            'llegada': llegada,
            'noches': noches,
            'adultos': adultos,
            'ninos': ninos,
            'cunas': cunas,
            'pax': pax,
            'valor': valor,
            'cancel_prob': cancel_prob,
            'segmento': "Loyalty" if fidelidad != "Sin programa" else "BAR",
            'fidelidad': fidelidad,
            'fuente_negocio': "DIRECT SALES",
            'fecha_reserva': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'estado': 'Confirmada'
        }
        
        # Guardar en memoria de sesión
        st.session_state.reservations.append(reserva)
        
        # Guardar en archivo CSV persistente
        guardar_reserva_csv(reserva)
        
        # Mostramos confirmacion
        st.markdown(f"""
<div style="background-color: {COLOR_VERDE_OSCURO}; padding: 3rem; text-align: center; border-left: 5px solid {COLOR_DORADO}; margin-bottom: 2rem;">
<div style="font-size: 0.9rem; letter-spacing: 3px; text-transform: uppercase; margin-bottom: 1rem; color: white !important; -webkit-text-fill-color: white !important; opacity: 0.9;">Reserva realizada con éxito</div>
<div style="color: white !important; -webkit-text-fill-color: white !important; font-size: 2.2rem; margin-bottom: 1.5rem; font-family: 'Cinzel', serif; font-weight: 400;">Gracias, {nombre}</div>
<div style="background: white; padding: 1.5rem; display: inline-block; margin-bottom: 1.5rem; color: {COLOR_NEGRO}; border: 2px solid {COLOR_DORADO};">
<div style="font-size: 0.8rem; opacity: 0.7; margin-bottom: 0.5rem; font-weight: 600; color: {COLOR_DORADO_OSCURO};">CÓDIGO DE LOCALIZADOR</div>
<div style="font-size: 2.5rem; font-weight: bold; letter-spacing: 4px; color: {COLOR_NEGRO};">{id_reserva}</div>
</div>
<div style="max-width: 600px; margin: 0 auto 2rem auto; text-align: left; background: {COLOR_CREMA}; padding: 2rem; border-radius: 4px; border: 1px solid {COLOR_DORADO}; color: {COLOR_NEGRO} !important;">
<div style="font-size: 0.8rem; letter-spacing: 1px; text-transform: uppercase; margin-bottom: 1rem; border-bottom: 1px solid {COLOR_BEIGE}; padding-bottom: 0.5rem; color: {COLOR_DORADO_OSCURO}; font-weight: 600;">Detalles del Cliente</div>
<p style="margin: 0.3rem 0; color: {COLOR_NEGRO} !important;"><strong>Titular:</strong> {nombre}</p>
<p style="margin: 0.3rem 0; color: {COLOR_NEGRO} !important;"><strong>Email:</strong> {email}</p>
<p style="margin: 0.3rem 0; color: {COLOR_NEGRO} !important;"><strong>Teléfono:</strong> {telefono if telefono else "No proporcionado"}</p>
<p style="margin: 0.3rem 0; color: {COLOR_NEGRO} !important;"><strong>País:</strong> {pais}</p>
<p style="margin: 0.3rem 0; color: {COLOR_NEGRO} !important;"><strong>Programa:</strong> {fidelidad}</p>
<div style="font-size: 0.8rem; letter-spacing: 1px; text-transform: uppercase; margin: 1.5rem 0 1rem 0; border-bottom: 1px solid {COLOR_BEIGE}; padding-bottom: 0.5rem; color: {COLOR_DORADO_OSCURO}; font-weight: 600;">Detalles de la Estancia</div>
<p style="margin: 0.3rem 0; color: {COLOR_NEGRO} !important;"><strong>Hotel:</strong> {hotel}</p>
<p style="margin: 0.3rem 0; color: {COLOR_NEGRO} !important;"><strong>Habitación:</strong> {habitacion}</p>
<p style="margin: 0.3rem 0; color: {COLOR_NEGRO} !important;"><strong>Llegada:</strong> {llegada.strftime('%d/%m/%Y')}</p>
<p style="margin: 0.3rem 0; color: {COLOR_NEGRO} !important;"><strong>Noches:</strong> {noches}</p>
<p style="margin: 0.3rem 0; color: {COLOR_NEGRO} !important;"><strong>Huéspedes:</strong> {pax} (Adultos: {adultos}, Niños: {ninos})</p>
<p style="margin: 1rem 0 0 0; font-size: 1.2rem; color: {COLOR_DORADO_OSCURO} !important;"><strong>Total: {valor:,.2f} EUR</strong></p>
</div>
<p style="font-size: 1rem; opacity: 0.9; color: white !important;">
Se ha enviado un correo de confirmación a <b>{email}</b>.
<br>
Revise su bandeja de entrada y/o spam para completar el proceso.
</p>
</div>
""", unsafe_allow_html=True)
        
        st.markdown("")
        if st.button("Nueva Reserva", key="new_reservation"):
            st.session_state.wizard_step = 1
            st.rerun()


# =============================================================================
# VISTA INTRANET
# =============================================================================
def render_vista_intranet():
    """
    Renderiza el panel de intranet con busqueda y prediccion.
    """
    
    st.markdown("""
<div style="background: #292929; color: white !important; padding: 1rem; margin-bottom: 1.5rem;">
<div style="margin: 0; font-weight: 300; letter-spacing: 2px; color: white !important; -webkit-text-fill-color: white !important; font-size: 1.5rem; font-family: 'Cinzel', serif;">INTRANET - GESTION DE RESERVAS</div>
</div>
""", unsafe_allow_html=True)
    
    # -------------------------------------------------------------------------
    # BUSQUEDA POR CODIGO
    # -------------------------------------------------------------------------
    st.subheader("Buscar Reserva")
    
    col_search, col_btn = st.columns([3, 1])
    
    with col_search:
        codigo_busqueda = st.text_input("Codigo de reserva", placeholder="Ej: 123456710601", key="search_code")
    
    with col_btn:
        st.markdown("<br>", unsafe_allow_html=True)
        buscar = st.button("Buscar", key="btn_search", type="primary")
    
    if buscar and codigo_busqueda:
        reservations = st.session_state.get('reservations', [])
        encontrada = None
        
        for r in reservations:
            if str(r['id']) == codigo_busqueda:
                encontrada = r
                break
        
        if encontrada:
            st.success(f"Reserva encontrada: {encontrada['id']}")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown(f"""
<div class="form-section">
<strong>Cliente:</strong> {encontrada['nombre']}<br>
<strong>Email:</strong> {encontrada['email']}<br>
<strong>Pais:</strong> {encontrada['pais']}<br>
<strong>Fidelidad:</strong> {encontrada.get('fidelidad', 'N/A')}
</div>
""", unsafe_allow_html=True)
            
            with col2:
                st.markdown(f"""
<div class="form-section">
<strong>Hotel:</strong> {encontrada['hotel']}<br>
<strong>Habitacion:</strong> {encontrada['habitacion']}<br>
<strong>Llegada:</strong> {encontrada['llegada']}<br>
<strong>Noches:</strong> {encontrada['noches']} | <strong>PAX:</strong> {encontrada['pax']}<br>
<strong>Valor:</strong> {encontrada['valor']:,.2f} EUR
</div>
""", unsafe_allow_html=True)
            
            # Mostramos riesgo
            prob = encontrada['cancel_prob']
            if prob >= 0.6:
                risk_class = "risk-high"
                risk_label = "RIESGO ALTO"
            elif prob >= 0.35:
                risk_class = "risk-medium"
                risk_label = "RIESGO MEDIO"
            else:
                risk_class = "risk-low"
                risk_label = "RIESGO BAJO"
            
            st.markdown(f"""
<div class="result-card {risk_class}" style="margin-top: 1rem;">
<p class="result-label">Probabilidad de Cancelacion</p>
<p class="result-value">{prob:.2%}</p>
<p class="result-label">{risk_label}</p>
</div>
""", unsafe_allow_html=True)
            
        else:
            st.warning(f"No se encontro reserva con codigo {codigo_busqueda}")
    
    st.markdown("---")
    
    # -------------------------------------------------------------------------
    # LISTADO DE RESERVAS
    # -------------------------------------------------------------------------
    st.subheader("Reservas Recientes")
    
    reservations = st.session_state.get('reservations', [])
    
    if reservations:
        for r in reversed(reservations[-10:]):  # Ultimas 10
            prob = r['cancel_prob']
            if prob >= 0.6:
                color = COLOR_RIESGO_ALTO
            elif prob >= 0.35:
                color = COLOR_RIESGO_MEDIO
            else:
                color = COLOR_RIESGO_BAJO
            
            st.markdown(f"""
<div class="content-card" style="display: flex; justify-content: space-between; align-items: center;">
<div>
<strong>{r['id']}</strong> - {r['nombre']}<br>
<span style="color: #757171;">{r['hotel']} | {r['llegada']} | {r['valor']:,.2f} EUR</span>
</div>
<div style="text-align: right;">
<span style="color: {color}; font-weight: 500;">{prob:.1%}</span><br>
<span style="color: #757171; font-size: 0.85rem;">{r.get('estado', 'Confirmada')}</span>
</div>
</div>
""", unsafe_allow_html=True)
    else:
        st.info("No hay reservas registradas")
    
    st.markdown("---")
    
    # -------------------------------------------------------------------------
    # PREDICCION MANUAL
    # -------------------------------------------------------------------------
    with st.expander("Prediccion Manual (Avanzado)"):
        model = load_model()
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            llegada_date = st.date_input("Llegada", value=datetime.date(2026, 6, 1), key="m_llegada")
            noches = st.number_input("Noches", min_value=1, value=7, key="m_noches")
            pax = st.number_input("PAX", min_value=1, value=2, key="m_pax")
            adultos = st.number_input("Adultos", min_value=1, value=2, key="m_adultos")
            valor = st.number_input("Valor USD", min_value=0.0, value=2500.0, key="m_valor")
        
        with col2:
            cliente = st.selectbox("Cliente", CLIENTES, key="m_cliente")
            fidelidad = st.selectbox("Fidelidad", PROGRAMAS_FIDELIDAD, key="m_fidelidad")
            pais = st.selectbox("Pais", PAISES, key="m_pais")
            segmento = st.selectbox("Segmento", SEGMENTOS, key="m_segmento")
            fuente = st.selectbox("Fuente", FUENTES_NEGOCIO, key="m_fuente")
        
        with col3:
            complejo = st.selectbox("Complejo", list(HOTELES_POR_COMPLEJO.keys()), key="m_complejo")
            # Obtener todas las habitaciones del complejo
            habitaciones_complejo = []
            for hotel_info in HOTELES_POR_COMPLEJO.get(complejo, {}).values():
                habitaciones_complejo.extend(hotel_info.get('habitaciones', []))
            habitacion = st.selectbox("Habitacion", habitaciones_complejo, key="m_habitacion")
        
        if st.button("Calcular Riesgo", type="primary", key="btn_calc"):
            with st.spinner("Calculando..."):
                fecha_toma = datetime.datetime.now()
                fidelidad_val = None if fidelidad == "Sin programa" else fidelidad
                
                try:
                    processed_df = get_features(
                        LLEGADA=pd.to_datetime(llegada_date), NOCHES=noches, PAX=pax, ADULTOS=adultos,
                        CLIENTE=cliente, FECHA_TOMA=fecha_toma, FIDELIDAD=fidelidad_val,
                        PAIS=pais, SEGMENTO=segmento, FUENTE_NEGOCIO=fuente,
                        NOMBRE_HOTEL=complejo, NOMBRE_HABITACION=habitacion,
                        VALOR_RESERVA=valor
                    )
                    
                    features = processed_df
                    if hasattr(model, 'feature_name_'):
                        features = processed_df[model.feature_name_]
                    
                    prob = model.predict_proba(features)[0][1]
                    
                    if prob >= 0.6:
                        st.error(f"Probabilidad: {prob:.2%} - RIESGO ALTO")
                    elif prob >= 0.35:
                        st.warning(f"Probabilidad: {prob:.2%} - RIESGO MEDIO")
                    else:
                        st.success(f"Probabilidad: {prob:.2%} - RIESGO BAJO")
                        
                except Exception as e:
                    st.error(f"Error: {e}")



def ejecutar_acciones_agente(accion_container, acciones):
    """
    Ejecuta las acciones devueltas por el agente actualizando el session_state.
    Permite mostrar info adicional (como fotos de habitaciones) en el chat.
    """
    for accion in acciones:
        funcion = accion.get("funcion")
        params = accion.get("parametros", {})
        
        try:
            if funcion == "seleccionar_destino":
                pais = params.get("pais", "").upper()
                if "MEXICO" in pais or "MÉXICO" in pais:
                    st.session_state.destino_seleccionado = "MEXICO"
                elif "DOMINICANA" in pais or "PUNTA CANA" in pais:
                    st.session_state.destino_seleccionado = "REPUBLICA DOMINICANA"
                
                # Si tenemos fechas y pax, podríamos avanzar al paso 2
                # Pero mejor dejar que el usuario o el agente lo decidan con el botón "Buscar" o flujo natural
                # st.session_state.wizard_step = 2

            elif funcion == "seleccionar_complejo":
                complejo = str(params.get("complejo", "")).strip().lower()
                if "costa" in complejo or "mujer" in complejo:
                    st.session_state.reserva_complejo = "Complejo Costa Mujeres"
                    st.session_state.destino_seleccionado = "MEXICO"
                elif "riviera" in complejo or "maya" in complejo:
                    st.session_state.reserva_complejo = "Complejo Riviera Maya"
                    st.session_state.destino_seleccionado = "MEXICO"
                elif "punta" in complejo or "cana" in complejo or "dominican" in complejo:
                    st.session_state.reserva_complejo = "Complejo Punta Cana"
                    st.session_state.destino_seleccionado = "REPUBLICA DOMINICANA"

            elif funcion == "configurar_fechas":
                llegada = params.get("llegada")
                noches_val = params.get("noches")
                
                if llegada:
                    try:
                        fecha_dt = datetime.datetime.strptime(llegada, "%Y-%m-%d").date()

                        # El proyecto está orientado a reservas 2026. Si el LLM devuelve un año pasado
                        # (muy común cuando el usuario no especifica año), ajustamos para evitar
                        # romper el `min_value` del date_input.
                        today = datetime.date.today()
                        target_year = 2026 if today.year <= 2026 else today.year
                        if fecha_dt.year != target_year:
                            try:
                                fecha_dt = fecha_dt.replace(year=target_year)
                            except ValueError:
                                # Caso raro (p.ej. 29/02) -> clampa al último día del mes.
                                from calendar import monthrange

                                last_day = monthrange(target_year, fecha_dt.month)[1]
                                fecha_dt = fecha_dt.replace(year=target_year, day=min(fecha_dt.day, last_day))

                        if fecha_dt < today:
                            # Mantener mes/día pero mover al próximo año para que sea válida.
                            try:
                                fecha_dt = fecha_dt.replace(year=max(fecha_dt.year + 1, today.year))
                            except ValueError:
                                fecha_dt = today

                        st.session_state.reserva_llegada = fecha_dt
                        st.session_state["w_llegada"] = fecha_dt
                    except:
                        pass
                
                if noches_val:
                    st.session_state.reserva_noches = int(noches_val)
                    st.session_state["w_noches"] = int(noches_val)

            elif funcion == "configurar_huespedes":
                adultos_val = params.get("adultos")
                ninos_val = params.get("ninos")
                
                if adultos_val:
                    st.session_state.reserva_adultos = int(adultos_val)
                    st.session_state["w_adultos"] = int(adultos_val)
                if ninos_val:
                    st.session_state.reserva_ninos = int(ninos_val)
                    st.session_state["w_ninos"] = int(ninos_val)
                
                # Calcular pax total
                st.session_state.reserva_pax = st.session_state.get('reserva_adultos', 2) + st.session_state.get('reserva_ninos', 0)
            
            elif funcion == "seleccionar_hotel":
                hotel_input = params.get("hotel")
                if hotel_input:
                    resuelto = resolver_hotel_y_complejo(hotel_input)
                    if resuelto:
                        complejo, hotel_canon = resuelto
                    else:
                        # Si no resolvemos, no pisamos el complejo si ya estaba seleccionado.
                        complejo = st.session_state.get("reserva_complejo") or obtener_complejo_de_hotel(hotel_input)
                        hotel_canon = hotel_input

                    st.session_state.reserva_complejo = complejo
                    st.session_state.hotel_seleccionado_paso3 = hotel_canon
                    # Avanzar al Paso 3 (Habitación)
                    st.session_state.wizard_step = 3
                
            elif funcion == "seleccionar_habitacion":
                habitacion_input = params.get("habitacion")
                
                # Intentar encontrar la clave correcta en DESCRIPCIONES
                habitacion_key = None
                if habitacion_input:
                     for k, v in DESCRIPCIONES_HABITACIONES.items():
                         # Coincidencia exacta de nombre o clave
                         if k == habitacion_input or v.get('nombre') == habitacion_input:
                             habitacion_key = k
                             break
                     if not habitacion_key:
                         # Coincidencia parcial
                         for k, v in DESCRIPCIONES_HABITACIONES.items():
                             if habitacion_input.lower() in v.get('nombre', '').lower():
                                 habitacion_key = k
                                 break
                
                habitacion = habitacion_key if habitacion_key else habitacion_input
                
                if habitacion:
                    st.session_state.reserva_habitacion = habitacion
                    
                    # Mostrar foto
                    complejo = st.session_state.get('reserva_complejo', 'Complejo Riviera Maya')
                    img_habs = obtener_imagenes_habitacion(habitacion, complejo, max_imagenes=1)
                    
                    if img_habs and os.path.exists(img_habs[0]):
                        with accion_container:
                            with st.chat_message("assistant"):
                                st.markdown(f"**{habitacion}**")
                                st.image(img_habs[0])
                        
                        st.session_state.chat_history.append({
                            "role": "assistant", 
                            "content": f"Aquí tienes una foto de la **{habitacion}**.",
                            "image": img_habs[0]
                        })
                    
                    # Avanzar al Paso 4 (Datos)
                    st.session_state.wizard_step = 4
                
            elif funcion == "confirmar_reserva":
                # Asegurar que estamos en el paso final
                if st.session_state.get('cliente_nombre') and st.session_state.get('cliente_email'):
                     st.session_state.wizard_step = 5
                else:
                     st.session_state.wizard_step = 4
                
            elif funcion == "marcar_fidelidad":
                es_fidelizado = params.get("es_fidelizado", False)
                if isinstance(es_fidelizado, str):
                    es_fidelizado = es_fidelizado.lower() == "true"
                
                if es_fidelizado:
                    st.session_state.rewards_verificado = True
                    st.session_state.cliente_fidelidad = "Palladium Rewards"
                else:
                    st.session_state.rewards_verificado = False
                    st.session_state.cliente_fidelidad = "Sin programa"
            
            elif funcion == "registrar_datos_cliente":
                nombre = params.get("nombre")
                email = params.get("email")
                pais = params.get("pais")
                telefono = params.get("telefono")
                
                if nombre:
                    st.session_state.cliente_nombre = nombre
                    st.session_state["w_nombre"] = nombre
                if email:
                    st.session_state.cliente_email = email
                    st.session_state["w_email"] = email
                if telefono:
                    st.session_state.cliente_telefono = telefono
                    st.session_state["w_telefono"] = telefono
                if pais:
                    pais_upper = pais.upper()
                    if pais_upper in PAISES:
                         st.session_state.cliente_pais = pais_upper
                         st.session_state["w_pais"] = pais_upper
                    else:
                        match = next((p for p in PAISES if pais_upper in p or p in pais_upper), None)
                        if match:
                             st.session_state.cliente_pais = match
                             st.session_state["w_pais"] = match

                # Avanzar logicamente
                # Si tenemos nombre y email, podemos ir directos a CONFIRMACION (Paso 5)
                if st.session_state.cliente_nombre and st.session_state.cliente_email:
                    # Calcular valor final antes de ir
                    st.session_state.reserva_valor = calcular_coste_estancia(
                        hotel_nombre=st.session_state.get('hotel_seleccionado_paso3', ''),
                        habitacion=st.session_state.get('reserva_habitacion', ''),
                        llegada=st.session_state.get('reserva_llegada', datetime.date.today()),
                        noches=st.session_state.get('reserva_noches', 7),
                        pax=st.session_state.get('reserva_pax', 2)
                    )
                    st.session_state.wizard_step = 5
                else:
                    # Si faltan datos, ir al formulario (Paso 4)
                    st.session_state.wizard_step = 4

            elif funcion == "recomendar_turismo":
                zona = params.get("zona", "")
                info_texto = agent_v2.TOURIST_INFO.get(zona, "")
                if info_texto:
                    info_limpia = textwrap.dedent(info_texto).strip()
                    with accion_container:
                        with st.chat_message("assistant"):
                            st.markdown(f"### 🌴 {zona}")
                            st.markdown(info_limpia)
                    st.session_state.chat_history.append({
                        "role": "assistant",
                        "content": f"### 🌴 {zona}\n\n{info_limpia}"
                    })

            elif funcion == "mostrar_info_hotel":
                hotel = params.get("hotel", "")
                if hotel:
                    img_rel_path = agent_v2.obtener_imagen_hotel(hotel)
                    img_path = os.path.join(os.path.dirname(__file__), img_rel_path) if img_rel_path else None
                    contenido = f"📍 **{hotel}**"
                    msg = {"role": "assistant", "content": contenido}
                    if img_path and os.path.exists(img_path):
                        msg["image"] = img_path
                    st.session_state.chat_history.append(msg)

        except Exception as e:
            print(f"Error ejecutando accion {funcion}: {e}")
            
    # Hacemos un único rerun al final si hubo acciones
    if acciones:
        st.rerun()

# =============================================================================
# MAIN
# =============================================================================
def main():
    """
    Funcion principal.
    """
    
    # Sidebar - Asistente IA
    with st.sidebar:
        base_dir = os.path.dirname(__file__)
        st.markdown("""
        <style>
            /* Ocultar navegación multipágina (app / intranet / mantenimiento) */
            [data-testid="stSidebarNav"] {
                display: none !important;
            }
            [data-testid="stSidebarNavSeparator"] {
                display: none !important;
            }
            /* Contenedor del sidebar específico para Branding Palladium */
            [data-testid="stSidebar"] {
                background: linear-gradient(180deg, #FCF4E4 0%, #F0EDE6 100%);
            }
            .palladium-brand {
                color: #2b2b2b;
                font-family: 'Helvetica Neue', sans-serif;
                padding: 0.9rem 0.6rem 1rem 0.6rem;
                text-align: center;
                border: 1px solid #E3D7C1;
                border-bottom: 3px solid #BB9719; /* Dorado Palladium */
                margin: 0.6rem 0 1rem 0;
                border-radius: 12px;
                background: #FFFFFF;
                box-shadow: 0 6px 18px rgba(0,0,0,0.08);
            }
            .palladium-subtitle {
                font-weight: 300;
                font-size: 0.8rem;
                color: #6b6456;
                letter-spacing: 2px;
            }
        </style>
        """, unsafe_allow_html=True)
        render_team_collage_sidebar(base_dir)
        st.markdown("""
        <div class="palladium-brand">
            <div class="palladium-subtitle">INTELLIGENCE AGENT</div>
        </div>
        """, unsafe_allow_html=True)
        agent_icon_path = os.path.join(base_dir, "media", "general", "agent_icon.png")
        
        # Inicializar historial del chat
        if "chat_history" not in st.session_state:
            st.session_state.chat_history = []
        
        # Contenedor del chat
        chat_container = st.container()
        
        # Input del usuario
        user_input_text = st.chat_input("¿En qué puedo ayudarte?", key="chat_input")
        audio_value = st.audio_input("O habla con Sophie 🎙️", key="audio_input")
        
        prompt = None
        
        # Inicializar estado de audio procesado si no existe
        if "last_audio_id" not in st.session_state:
            st.session_state.last_audio_id = None

        # Prioridad: Audio > Texto
        if audio_value:
            try:
                # Usamos el ID del objeto o un hash simple para saber si es nuevo
                # st.audio_input devuelve un objeto UploadedFile-like, su .id es único por subida
                current_audio_id = audio_value.id if hasattr(audio_value, 'id') else audio_value.size 
                
                if current_audio_id != st.session_state.last_audio_id:
                    with st.spinner("Escuchando..."):
                        # Leer bytes para enviar ya que audio_value es file-like
                        # Importante: rewind o leer bytes. Requests acepta file-like pero si lo leemos aqui mejor pasamos bytes
                        audio_bytes = audio_value.getvalue()
                        transcripcion = agent_v2.stt_groq_whisper(audio_bytes)
                        if transcripcion:
                            st.info(f"🎤 Escuchado: '{transcripcion}'")
                            prompt = transcripcion
                            st.session_state.last_audio_id = current_audio_id
                        else:
                            st.warning("⚠️ No se pudo entender el audio. Por favor intente de nuevo o escriba.")
            except Exception as e:
                st.error(f"Error procesando audio: {e}")
                print(f"DEBUG AUDIO ERROR: {e}")
        
        elif user_input_text:
            prompt = user_input_text
        
        # Mostrar historial
        with chat_container:
            for message in st.session_state.chat_history:
                avatar = agent_icon_path if message["role"] == "assistant" and os.path.exists(agent_icon_path) else None
                with st.chat_message(message["role"], avatar=avatar):
                    st.markdown(message["content"])
                    # Mostrar imagen si existe en el mensaje
                    if "image" in message and message["image"]:
                        if os.path.exists(message["image"]):
                            st.image(message["image"])
                    # Mostrar audio si existe
                    if "audio_bytes" in message and message["audio_bytes"]:
                        st.audio(message["audio_bytes"], format="audio/mp3")
        
        # Procesar input
        if prompt:
            # 1. Mostrar mensaje usuario
            with chat_container:
                with st.chat_message("user"):
                    st.markdown(prompt)
            st.session_state.chat_history.append({"role": "user", "content": prompt})
            
            # 2. Llamar al agente
            with st.spinner("Pensando..."):
                # Construir estado actual para el contexto del agente
                estado_actual = {
                    "modo": "CLIENTE", # Por defecto en esta app
                    "destino": st.session_state.get('destino_seleccionado'),
                    "hotel": st.session_state.get('hotel_seleccionado_paso3'),
                    "llegada": str(st.session_state.get('reserva_llegada')),
                    "noches": st.session_state.get('reserva_noches'),
                    "adultos": st.session_state.get('reserva_adultos'),
                    "paso_wizard": st.session_state.get('wizard_step')
                }
                
                # Convertir historial al formato del agente (lista de dicts)
                # IMPORTANTE: Filtrar campos extra como 'audio_bytes' o 'image' que no son serializables o necesarios para el LLM
                historial_completo = st.session_state.chat_history[-5:] # Últimos 5 para ahorrar tokens (Rate Limit Groq)
                historial_agente = [
                    {"role": m["role"], "content": m["content"]}
                    for m in historial_completo
                ]
                
                respuesta = agent_v2.chat_con_acciones(prompt, historial_agente, estado_actual)
            
            # 3. Mostrar respuesta agente
            mensaje_texto = respuesta.get("mensaje", "")
            img_path = None
            if mensaje_texto:
                # Procesar imagen de HOTEL si viene en la respuesta directa
                if "imagen" in respuesta and respuesta["imagen"]:
                     img_rel_path = agent_v2.obtener_imagen_hotel(respuesta["imagen"])
                     if img_rel_path:
                         img_path = os.path.join(os.path.dirname(__file__), img_rel_path)

                with chat_container:
                    avatar = agent_icon_path if os.path.exists(agent_icon_path) else None
                    with st.chat_message("assistant", avatar=avatar):
                        st.markdown(mensaje_texto)
                        # Mostrar imagen si la hay
                        if img_path and os.path.exists(img_path):
                             st.image(img_path, caption=respuesta["imagen"])
                        
                        # Reproducir audio si viene en la respuesta
                        if "audio_bytes" in respuesta:
                            st.audio(respuesta["audio_bytes"], format="audio/mp3", autoplay=True)
                
                # Guardar en historial con la imagen y audio
                msg_data = {"role": "assistant", "content": mensaje_texto}
                if img_path:
                    msg_data["image"] = img_path
                if "audio_bytes" in respuesta:
                    msg_data["audio_bytes"] = respuesta["audio_bytes"]
                
                st.session_state.chat_history.append(msg_data)
            
            # 4. Ejecutar acciones
            acciones = respuesta.get("acciones", [])
            if acciones:
                ejecutar_acciones_agente(accion_container=chat_container, acciones=acciones)
                st.rerun()



    
    # Cabecera con logo
    logo_path = os.path.join(os.path.dirname(__file__), "media", "general", "Logo.jpg")
    
    # Estrategia de centrado robusta
    col_space1, col_logo, col_space2 = st.columns([3, 4, 3])
    with col_logo:
        if os.path.exists(logo_path):
            st.image(logo_path, width="stretch")
        else:
            st.markdown("<h1 style='text-align:center;'>PALLADIUM HOTELS</h1>", unsafe_allow_html=True)
    
    # Navegacion
    # Navegacion
    col_spacer, col_buttons = st.columns([5, 1.5])
    
    with col_buttons:
        col_btn1, col_btn2 = st.columns(2)
        
        with col_btn1:
            # Estamos en Cliente (app.py)
            st.button("Cliente", key="nav_cliente", type="primary", disabled=True)
        
        with col_btn2:
            if st.button("Intranet", key="nav_intranet", type="secondary"):
                st.switch_page("pages/1_Intranet.py")
    
    st.markdown("---")
    
    # Contenido - Siempre renderiza Cliente aqui, la Intranet esta en otra pagina
    render_vista_cliente()
    
    # Pie
    st.markdown("---")
    pie_path = os.path.join(os.path.dirname(__file__), "media", "general", "Pie.png")
    if os.path.exists(pie_path):
        st.image(pie_path, width="stretch")


if __name__ == "__main__":
    main()
