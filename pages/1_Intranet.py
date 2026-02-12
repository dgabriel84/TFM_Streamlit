"""
Intranet Palladium - Panel de Prediccion de Cancelaciones
Herramienta interna para analizar reservas y predecir probabilidad de cancelacion.

Esta pagina permite a los trabajadores:
- Introducir los datos de una reserva manualmente
- Obtener la probabilidad de cancelacion del modelo LightGBM
- Ver las features procesadas
- Gestionar reservas existentes

Autor: Grupo 4 - TFM Palladium
Fecha: Febrero 2026
"""

import streamlit as st
import pandas as pd
import datetime
import sys
import os
import numpy as np
import io
import base64
import html
import unicodedata

# -----------------------------------------------------------------------------
# CONFIGURACION DE RUTAS
# -----------------------------------------------------------------------------
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import load_model, get_features
import agent_v2
import textwrap

# -----------------------------------------------------------------------------
# CONFIGURACION DE PAGINA
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="Intranet | Palladium",
    page_icon="P",
    layout="wide"
)

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# PALETA DE COLORES CORPORATIVOS
# -----------------------------------------------------------------------------
COLOR_DORADO_OSCURO = "#6E550C"
COLOR_DORADO = "#BB9719"
COLOR_BEIGE = "#CCC3A5"
COLOR_VERDE_OSCURO = "#50685C"  # Verde corporativo
COLOR_VERDE_CABECERA = "#4A5D45" # Verde específico de la imagen
COLOR_NEGRO = "#292929"
COLOR_GRIS = "#757171"
COLOR_OLIVA = "#AEA780"
COLOR_CREMA = "#FCF4E4"
COLOR_CREMA_CLARO = "#F0EDE6"
COLOR_BEIGE_CLARO = "#E3D7C1"
COLOR_BLANCO = "#FFFFFF"

# Colores para indicadores de riesgo
COLOR_RIESGO_ALTO = "#C0392B"
COLOR_RIESGO_MEDIO = "#D68910"
COLOR_RIESGO_BAJO = "#1E8449"

# -----------------------------------------------------------------------------
# ESTILOS CSS
# -----------------------------------------------------------------------------
CUSTOM_CSS = f"""
<style>
    .main {{
        background-color: {COLOR_CREMA_CLARO};
    }}
    
    /* Cabecera intranet - borde dorado elegante más ancho */
    .intranet-header {{
        background: {COLOR_VERDE_CABECERA};
        padding: 1.2rem 2rem;
        margin: -1rem -1rem 2rem -1rem;
        color: {COLOR_BLANCO};
        border-bottom: 5px solid {COLOR_DORADO};
        box-shadow: 0 2px 10px rgba(0,0,0,0.1);
    }}
    
    .intranet-header h2 {{
        font-weight: 300;
        letter-spacing: 2px;
        text-transform: uppercase;
        margin: 0;
    }}
    
    /* Estilizar Pestañas (Tabs) - bordes redondeados */
    .stTabs [data-baseweb="tab-list"] {{
        gap: 10px;
        background-color: transparent;
        padding-bottom: 10px;
        border-bottom: 3px solid {COLOR_DORADO};
    }}

    .stTabs [data-baseweb="tab"] {{
        height: 50px;
        white-space: pre-wrap;
        background-color: {COLOR_BLANCO};
        border-radius: 8px;
        color: {COLOR_GRIS};
        font-weight: 500;
        text-transform: uppercase;
        border: 2px solid {COLOR_BEIGE};
        padding: 0 20px;
        transition: all 0.3s ease;
    }}

    .stTabs [aria-selected="true"] {{
        background-color: {COLOR_DORADO} !important;
        color: {COLOR_BLANCO} !important;
        border: 2px solid {COLOR_DORADO_OSCURO} !important;
        font-weight: 600 !important;
        border-radius: 8px !important;
    }}
    
    /* Tarjeta de resultado */
    .result-card {{
        background: {COLOR_BLANCO};
        border-radius: 8px; /* Redondeado 8px */
        padding: 2rem;
        text-align: center;
        box-shadow: 0 4px 15px rgba(0,0,0,0.1);
        border: 1px solid {COLOR_BEIGE};
    }}
    
    .result-value {{
        font-size: 3rem;
        font-weight: 300;
    }}
    
    .result-label {{
        color: {COLOR_GRIS};
        font-size: 0.9rem;
        text-transform: uppercase;
        letter-spacing: 1px;
    }}
    
    /* Indicadores de riesgo */
    .risk-high {{
        color: {COLOR_RIESGO_ALTO};
        border-left: 5px solid {COLOR_RIESGO_ALTO};
        border-radius: 4px; /* Ligero redondeo */
    }}
    
    .risk-medium {{
        color: {COLOR_RIESGO_MEDIO};
        border-left: 5px solid {COLOR_RIESGO_MEDIO};
        border-radius: 4px;
    }}
    
    .risk-low {{
        color: {COLOR_RIESGO_BAJO};
        border-left: 5px solid {COLOR_RIESGO_BAJO};
        border-radius: 4px;
    }}
    
    /* Seccion de formulario */
    .form-section {{
        background: {COLOR_BLANCO};
        padding: 1.5rem;
        border-radius: 8px; /* Redondeado 8px */
        margin-bottom: 1rem;
        border-left: 3px solid {COLOR_OLIVA};
        box-shadow: 0 2px 8px rgba(0,0,0,0.05);
    }}
    
    /* Botones */
    .stButton > button {{
        background-color: {COLOR_DORADO} !important;
        color: {COLOR_BLANCO} !important;
        border: none !important;
        border-radius: 8px !important; /* Redondeado 8px */
        font-weight: 500 !important;
        text-transform: uppercase !important;
        letter-spacing: 1px !important;
        transition: all 0.3s ease !important;
        min-height: 40px !important;
        padding: 0.5rem 1.5rem !important;
    }}
    
    .stButton > button:hover {{
        background-color: {COLOR_DORADO_OSCURO} !important;
        color: {COLOR_BLANCO} !important;
        box-shadow: 0 4px 8px rgba(0,0,0,0.1) !important;
    }}
    
    /* Sidebar */
    section[data-testid="stSidebar"] {{
        background-color: {COLOR_BLANCO};
    }}
    
    /* ===============================
       ESTILOS INPUTS ELEGANTES PALLADIUM
       =============================== */
    
    /* Inputs de texto, número y fecha */
    .stTextInput > div > div > input,
    .stNumberInput > div > div > input,
    .stDateInput > div > div > input {{
        background-color: {COLOR_CREMA} !important;
        border: 2px solid {COLOR_DORADO} !important;
        border-radius: 4px !important;
        color: {COLOR_NEGRO} !important;
        padding: 0.5rem !important;
    }}
    
    .stTextInput > div > div > input:focus,
    .stNumberInput > div > div > input:focus,
    .stDateInput > div > div > input:focus {{
        border-color: {COLOR_DORADO_OSCURO} !important;
        box-shadow: 0 0 0 2px rgba(110, 85, 12, 0.2) !important;
    }}
    
    /* Selectbox */
    .stSelectbox > div > div {{
        background-color: {COLOR_CREMA} !important;
        border: 2px solid {COLOR_DORADO} !important;
        border-radius: 4px !important;
    }}
    
    .stSelectbox > div > div:hover {{
        border-color: {COLOR_DORADO_OSCURO} !important;
    }}
    
    /* Dropdown del selectbox */
    div[data-baseweb="select"] > div {{
        background-color: {COLOR_CREMA} !important;
        border: 2px solid {COLOR_DORADO} !important;
        border-radius: 4px !important;
    }}
    
    div[data-baseweb="select"] > div:focus-within {{
        border-color: {COLOR_DORADO_OSCURO} !important;
        box-shadow: 0 0 0 2px rgba(110, 85, 12, 0.2) !important;
    }}
    
    /* Multiselect */
    .stMultiSelect > div > div {{
        background-color: {COLOR_CREMA} !important;
        border: 2px solid {COLOR_DORADO} !important;
        border-radius: 4px !important;
    }}
    
    /* Date input container */
    .stDateInput > div > div {{
        background-color: {COLOR_CREMA} !important;
        border: 2px solid {COLOR_DORADO} !important;
        border-radius: 4px !important;
    }}
    
    /* Number input container */
    .stNumberInput > div {{
        background-color: transparent !important;
    }}
    
    .stNumberInput > div > div {{
        background-color: {COLOR_CREMA} !important;
        border: 2px solid {COLOR_DORADO} !important;
        border-radius: 4px !important;
    }}
    
    /* Labels de los campos */
    .stTextInput label,
    .stNumberInput label,
    .stSelectbox label,
    .stDateInput label,
    .stMultiSelect label {{
        color: {COLOR_DORADO_OSCURO} !important;
        font-weight: 500 !important;
    }}
    
    /* Expander headers */
    .streamlit-expanderHeader {{
        background-color: {COLOR_CREMA} !important;
        border: 1px solid {COLOR_DORADO} !important;
        border-radius: 4px !important;
        color: {COLOR_DORADO_OSCURO} !important;
        font-weight: 500 !important;
    }}

</style>
"""

st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# VISUALIZACIÓN: ESTILO SHAP (COLORES CORPORATIVOS)
# -----------------------------------------------------------------------------
def _aplicar_colores_corporativos_waterfall_shap(fig):
    """
    SHAP (según versión) suele pintar aportaciones con rojo/azul. Aquí recoloreamos
    el gráfico ya dibujado a los colores corporativos Palladium:
    - Aportación positiva: dorado
    - Aportación negativa: verde
    """
    try:
        from matplotlib.colors import to_rgba

        color_pos = to_rgba(COLOR_DORADO)
        color_neg = to_rgba(COLOR_VERDE_OSCURO)

        # Colores típicos de SHAP (aprox): positivo (rojo/rosa), negativo (azul).
        shap_pos = to_rgba("#ff0051")
        shap_neg = to_rgba("#008bfb")

        def _dist(c1, c2):
            return ((c1[0] - c2[0]) ** 2 + (c1[1] - c2[1]) ** 2 + (c1[2] - c2[2]) ** 2) ** 0.5

        def _clasificar(c):
            # Priorizamos match con los colores SHAP; si no, heurística por dominancia.
            if _dist(c, shap_pos) < 0.45:
                return "pos"
            if _dist(c, shap_neg) < 0.45:
                return "neg"
            if c[0] > 0.75 and c[0] > c[2] and c[1] < 0.55:
                return "pos"
            if c[2] > 0.75 and c[2] > c[0]:
                return "neg"
            return None

        for ax in getattr(fig, "axes", []):
            # Patches (bars, arrows, etc.)
            for p in getattr(ax, "patches", []):
                try:
                    fc = p.get_facecolor()
                    if not isinstance(fc, (list, tuple)) or len(fc) < 4:
                        continue
                    kind = _clasificar(fc)
                    if kind == "pos":
                        p.set_facecolor((*color_pos[:3], fc[3]))
                        p.set_edgecolor((*color_pos[:3], fc[3]))
                    elif kind == "neg":
                        p.set_facecolor((*color_neg[:3], fc[3]))
                        p.set_edgecolor((*color_neg[:3], fc[3]))
                except Exception:
                    pass

            # Collections (por si la versión usa PolyCollections)
            for coll in getattr(ax, "collections", []):
                try:
                    fcs = coll.get_facecolor()
                    if fcs is None:
                        continue
                    new_fcs = []
                    changed = False
                    for fc in fcs:
                        if not isinstance(fc, (list, tuple)) or len(fc) < 4:
                            new_fcs.append(fc)
                            continue
                        kind = _clasificar(fc)
                        if kind == "pos":
                            new_fcs.append((*color_pos[:3], fc[3]))
                            changed = True
                        elif kind == "neg":
                            new_fcs.append((*color_neg[:3], fc[3]))
                            changed = True
                        else:
                            new_fcs.append(fc)
                    if changed:
                        coll.set_facecolor(new_fcs)
                        try:
                            coll.set_edgecolor(new_fcs)
                        except Exception:
                            pass
                except Exception:
                    pass

            # Líneas (si aplica)
            for line in getattr(ax, "lines", []):
                try:
                    lc = to_rgba(line.get_color())
                    kind = _clasificar(lc)
                    if kind == "pos":
                        line.set_color(color_pos)
                    elif kind == "neg":
                        line.set_color(color_neg)
                except Exception:
                    pass
    except Exception:
        # Si no se puede recolorear, no rompemos la app.
        return


def _generar_waterfall_shap_desde_features(modelo, features_df, label, source_id, max_display=12):
    """
    Genera y guarda en session_state el PNG del waterfall SHAP para una sola reserva.
    Devuelve (ok: bool, error: str|None).
    """
    import importlib.util

    if modelo is None:
        return False, "Modelo no cargado."
    if features_df is None or features_df.empty:
        return False, "No hay features para explicar."
    if importlib.util.find_spec("shap") is None:
        return False, "No se detecta `shap` en el entorno."

    try:
        import matplotlib.pyplot as plt
        import shap

        # Intento directo de paleta corporativa en SHAP.
        try:
            shap.plots.colors.red_hex = COLOR_DORADO
            shap.plots.colors.blue_hex = COLOR_VERDE_OSCURO
            shap.plots.colors.red_rgb = np.array([187, 151, 25]) / 255.0
            shap.plots.colors.blue_rgb = np.array([80, 104, 92]) / 255.0
        except Exception:
            pass

        x = features_df.copy()
        if hasattr(modelo, "feature_name_"):
            missing = [c for c in modelo.feature_name_ if c not in x.columns]
            if missing:
                return False, f"Faltan features para SHAP: {', '.join(missing[:5])}"
            x = x[modelo.feature_name_]

        model_for_shap = modelo.booster_ if hasattr(modelo, "booster_") else modelo
        explainer = shap.TreeExplainer(model_for_shap)

        try:
            shap_values = explainer(x)
        except Exception:
            x_num = x.copy()
            for col in x_num.select_dtypes(include=["category"]).columns:
                x_num[col] = x_num[col].cat.codes
            shap_values = explainer(x_num)

        plt.figure(figsize=(9, 4))
        shap.plots.waterfall(shap_values[0], show=False, max_display=max_display)
        fig = plt.gcf()
        _aplicar_colores_corporativos_waterfall_shap(fig)

        buf = io.BytesIO()
        fig.savefig(buf, format="png", dpi=200, bbox_inches="tight")
        plt.close(fig)

        st.session_state.shap_waterfall_png = buf.getvalue()
        st.session_state.shap_waterfall_label = str(label or "")
        st.session_state.last_shap_source_id = str(source_id or "")
        return True, None
    except Exception as e:
        return False, str(e)


def _to_base64_image(image_path):
    if image_path and os.path.exists(image_path):
        with open(image_path, "rb") as img_file:
            return base64.b64encode(img_file.read()).decode()
    return ""


@st.cache_data(show_spinner=False)
def _to_base64_image_thumbnail(image_path, size=88):
    """
    Crea un thumbnail base64 ligero para avatares de sidebar/equipo.
    """
    if not image_path or not os.path.exists(image_path):
        return ""
    try:
        from PIL import Image, ImageOps

        with Image.open(image_path) as img:
            img = ImageOps.exif_transpose(img).convert("RGBA")
            img = ImageOps.fit(img, (size, size), Image.Resampling.LANCZOS)
            buf = io.BytesIO()
            img.save(buf, format="PNG", optimize=True)
            return base64.b64encode(buf.getvalue()).decode()
    except Exception:
        return _to_base64_image(image_path)


def _render_team_collage_sidebar(base_dir):
    """
    Cabecera del agente con 5 miembros (sin Gustavo), ligeramente solapados.
    """
    miembros = ["Alex.png", "David.png", "Francisco.png", "Gabi.png", "Jose.png"]
    folder = os.path.join(base_dir, "media", "Miembros")
    data = []
    for i, name in enumerate(miembros):
        b64 = _to_base64_image_thumbnail(os.path.join(folder, name), size=88)
        if b64:
            data.append((i, b64))

    if not data:
        logo_path = os.path.join(base_dir, "media", "general", "Logo.jpg")
        if os.path.exists(logo_path):
            st.image(logo_path, width="stretch")
        return

    avatar_size = 88
    step = 60
    width = step * (len(data) - 1) + avatar_size
    imgs_html = []
    for i, b64 in data:
        left = i * step
        imgs_html.append(
            f"<img src='data:image/png;base64,{b64}' "
            f"style='position:absolute;left:{left}px;top:0;width:{avatar_size}px;height:{avatar_size}px;"
            f"object-fit:cover;border-radius:50%;z-index:{100-i};"
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


def _catalogo_ofertas_retencion(prob_cancel):
    """
    Devuelve 3 ofertas según riesgo (medio/alto) con foco en retorno.
    """
    if prob_cancel is None or prob_cancel < 0.35:
        return []

    if prob_cancel >= 0.60:
        return [
            {
                "id": "ALTA_1",
                "nombre": "Rescate Comercial",
                "propuesta": "15% descuento + Late Checkout + amenity premium",
                "coste": "Coste medio",
                "retorno": "Conversión alta en clientes indecisos",
            },
            {
                "id": "ALTA_2",
                "nombre": "Upgrade de Valor",
                "propuesta": "Upgrade 1 categoría + 10% descuento",
                "coste": "Coste medio-alto",
                "retorno": "Eleva percepción de valor sin tocar tanto precio",
            },
            {
                "id": "ALTA_3",
                "nombre": "Oferta Fuerte",
                "propuesta": "20% descuento + cancelación flexible 72h",
                "coste": "Coste alto",
                "retorno": "Máxima probabilidad de retención en riesgo crítico",
            },
        ]

    return [
        {
            "id": "MEDIA_1",
            "nombre": "Flex Comfort",
            "propuesta": "Late Checkout + amenity de bienvenida",
            "coste": "Coste bajo",
            "retorno": "Buena retención manteniendo margen",
        },
        {
            "id": "MEDIA_2",
            "nombre": "Ahorro Inteligente",
            "propuesta": "10% descuento directo",
            "coste": "Coste medio-bajo",
            "retorno": "Equilibrio entre margen y conversión",
        },
        {
            "id": "MEDIA_3",
            "nombre": "Flexibilidad Plus",
            "propuesta": "1 cambio de fechas sin gastos + tarifa garantizada 7 días",
            "coste": "Coste bajo",
            "retorno": "Muy útil cuando el freno es la incertidumbre",
        },
    ]


def _persistir_campos_oferta_reserva(id_reserva, updates):
    """
    Persiste campos de oferta en ambos CSVs de reservas (web + histórico).
    """
    if not id_reserva or not updates:
        return False

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    candidatos = [
        os.path.join(base_dir, "reservas_web_2026.csv"),
        os.path.join(base_dir, "reservas_2026_full.csv"),
    ]

    updated_any = False
    id_norm = str(id_reserva).strip()
    if id_norm.endswith(".0"):
        id_norm = id_norm[:-2]

    for csv_path in candidatos:
        if not os.path.exists(csv_path):
            continue
        try:
            df = pd.read_csv(csv_path)
            if "ID_RESERVA" not in df.columns:
                continue

            ids = df["ID_RESERVA"].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
            mask = ids == id_norm
            if not mask.any():
                continue

            for k, v in updates.items():
                df.loc[mask, k] = v

            df.to_csv(csv_path, index=False)
            updated_any = True
        except Exception:
            continue

    return updated_any


def _buscar_reserva_por_id_local(id_reserva):
    """
    Búsqueda local de reserva sin importar app.py (evita sobrecarga y side effects).
    """
    if not id_reserva:
        return {}

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    paths = [
        os.path.join(base_dir, "reservas_web_2026.csv"),
        os.path.join(base_dir, "reservas_2026_full.csv"),
    ]

    dfs = []
    for p in paths:
        if not os.path.exists(p):
            continue
        try:
            df_tmp = pd.read_csv(p, on_bad_lines="skip")
            if not df_tmp.empty:
                dfs.append(df_tmp)
        except Exception:
            continue

    if not dfs:
        return {}

    df = pd.concat(dfs, ignore_index=True)
    if "ID_RESERVA" not in df.columns:
        return {}

    id_norm = str(id_reserva).strip()
    if id_norm.endswith(".0"):
        id_norm = id_norm[:-2]

    df["ID_RESERVA"] = (
        df["ID_RESERVA"]
        .astype(str)
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
    )
    match = df[df["ID_RESERVA"] == id_norm]
    if match.empty:
        return {}

    row = match.iloc[0]
    prob_raw = pd.to_numeric(row.get("PROBABILIDAD_CANCELACION", 0), errors="coerce")
    if pd.isna(prob_raw):
        prob_raw = 0.0
    prob_pct = float(prob_raw * 100.0) if 0.0 < float(prob_raw) <= 1.0 else float(prob_raw)

    llegada_val = row.get("LLEGADA")
    if pd.notna(llegada_val):
        try:
            llegada_fmt = pd.to_datetime(llegada_val, errors="coerce").strftime("%d-%m-%Y")
        except Exception:
            llegada_fmt = str(llegada_val)
    else:
        llegada_fmt = ""

    nombre = (
        row.get("CLIENTE_NOMBRE")
        or row.get("NOMBRE_CLIENTE")
        or row.get("NOMBRE")
        or row.get("NOMBRE_CLIENTE_RESERVA")
        or "Cliente"
    )
    email = row.get("CLIENTE_EMAIL") or row.get("EMAIL") or row.get("email") or ""

    valor = pd.to_numeric(row.get("VALOR_RESERVA", 0), errors="coerce")
    if pd.isna(valor):
        valor = 0.0

    adultos = pd.to_numeric(row.get("ADULTOS", row.get("PAX", 2)), errors="coerce")
    if pd.isna(adultos):
        adultos = 2
    ninos = pd.to_numeric(row.get("NINOS", row.get("NIÑOS", 0)), errors="coerce")
    if pd.isna(ninos):
        ninos = 0

    return {
        "id": str(row.get("ID_RESERVA", id_norm)),
        "id_reserva": str(row.get("ID_RESERVA", id_norm)),
        "nombre": str(nombre),
        "cliente_nombre": str(nombre),
        "email": str(email),
        "cliente_email": str(email),
        "hotel": row.get("NOMBRE_HOTEL_REAL", row.get("hotel", "Hotel")),
        "habitacion": row.get("NOMBRE_HABITACION", row.get("habitacion", "Estándar")),
        "llegada": llegada_fmt,
        "noches": int(pd.to_numeric(row.get("NOCHES", 1), errors="coerce") or 1),
        "adultos": int(adultos),
        "ninos": int(ninos),
        "pax": int(pd.to_numeric(row.get("PAX", 2), errors="coerce") or 2),
        "valor": float(valor),
        "valor_total": float(valor),
        "cancel_prob": prob_pct,
        "prob_cancelacion": prob_pct,
        "estado": row.get("ESTADO", row.get("estado", "Confirmada")),
        "oferta_retencion": row.get("oferta_retencion", ""),
        "fecha_envio_oferta": row.get("fecha_envio_oferta", ""),
        "estado_oferta": row.get("estado_oferta", ""),
        "raw_data": row.to_dict(),
    }

# -----------------------------------------------------------------------------
# VALORES UNICOS DEL CSV (para los selectores)
# Extraidos de df_unificado_completo_v7
# -----------------------------------------------------------------------------

HOTELES = [
    "Complejo Costa Mujeres",
    "Complejo Riviera Maya",
    "Complejo Punta Cana"
]

HABITACIONES = {
    "Complejo Costa Mujeres": [
        "CMU JUNIOR SUITE GV", "CMU JUNIOR SUITE PS", "CMU JUNIOR SUITE PS OV",
        "CMU FAMILY SUITE", "CMU LOFT SUITE JT", "CMU RESIDENCE SUI BS POV",
        "CMU FS JUNIOR SUITE BS", "CMU FS JUNIOR SUITE BS OV", "CMU FS LOFT SUITE JT",
        "TRSC JUNIOR SUITE GV", "TRSC JUNIOR SUITE SW", "TRSC JUNIOR SUITE BS OV",
        "TRSC AMBASSADOR ST BS OV",
        "TRS JUNIOR SUITE GV", "TRS JUNIOR SUITE OV", "TRS JUNIOR SUITE SW",
        "TRS JUNIOR SUITE BS OV", "TRS AMBASSADOR SUITE POV"
    ],
    "Complejo Riviera Maya": [
        "COL JUNIOR SUITE GV", "COL JUNIOR SUITE PS", "COL DELUXE GARDEN VIEW",
        "COL ROMANCE VILLA SUI PS",
        "KAN JUNIOR SUITE GV", "KAN JUNIOR SUITE OV", "KAN DELUXE GARDEN VIEW",
        "KAN ROMANCE VILLA SUI BS",
        "TRS JUNIOR SUITE GV", "TRS JUNIOR SUITE PS", "TRS JUNIOR SUITE PS OV",
        "TRS JS JACUZZI TERR PS", "TRS SUITE GARDEN VIEW", "TRS SUITE PP PS",
        "TRS AMBASSADOR SUITE POV", "TRS ROMANCE BW BYTHE LAKE",
        "WS JUNIOR SUITE GV", "WS JUNIOR SUITE BS", "WS SUITE GARDEN VIEW",
        "WS ROMANCE BW BYTHE LAKE"
    ],
    "Complejo Punta Cana": [
        "PC JUNIOR SUITE GV", "PC JUNIOR SUITE POOLSIDE", "PC DELUXE GARDEN VIEW",
        "PC DELUXE POOLSIDE", "PC PREMIUM JS PS", "PC FAMILY SUITE",
        "BAV JUNIOR SUITE GV", "BAV SUPERIOR JS GV", "BAV SUPERIOR JS BS",
        "BAV PREMIUM JS GV", "BAV ROOFTOP JT ST",
        "PAL JUNIOR SUITE GV", "PAL JUNIOR SUITE BS", "PAL JUNIOR SUITE SW BS",
        "PAL JUNIOR SUITE BS OV", "PAL DELUXE BEACHSIDE", "PAL DELUXE GARDEN VIEW",
        "PAL DELUXE BS POV", "PAL LOFT SUITE GV", "PAL LOFT SUITE BS POV",
        "TRS JUNIOR SUITE GV/PV", "TRS JUNIOR SUITE PS", "TRS JUNIOR SUITE PS OV",
        "TRS JUNIOR SUITE SWIM UP", "TRS ROMANCE SUITE PS", "TRS AMBASSADOR SUITE",
        "TRS JACUZZI TERR SUI BS"
    ]
}

# Paises (valores exactos del modelo)
PAISES = [
    "ESPAÑA", "ESTADOS UNIDOS", "CANADA", "MEXICO", "ALEMANIA", "REINO UNIDO",
    "FRANCIA", "ITALIA", "ARGENTINA", "BRASIL", "COLOMBIA", "CHILE", "PERU",
    "URUGUAY", "VENEZUELA", "PUERTO RICO", "REPUBLICA DOMINICANA", "SUECIA",
    "POLONIA", "RUMANIA", "LUXEMBURGO", "SIN PAIS"
]

# Segmentos (valores exactos del modelo)
SEGMENTOS = [
    "BAR", "Fixed rates", "Fixed Rates", "Loyalty", "Package", 
    "Group Leisure", "Weddings", "MICE", "Miscellaneous", "Opaque", 
    "Complimentary", "Travel Industry"
]

# Fuentes de negocio del CSV
FUENTES_NEGOCIO = [
    "DIRECT SALES", "T.O. / T.A.", "E-COMMERCE", "CORPORATE", "OTHERS"
]

# Clientes/operadores mas frecuentes del CSV
CLIENTES = [
    "ROIBACK (GLOBAL OBI S.L.)", "CALL CENTER", "PALLADIUM TRAVEL CLUB_SOCIOS",
    "PALLADIUM TRAVEL CLUB_SEMANA BENEFICIO", "EXPEDIA", "BOOKING EUROPE BV",
    "AGODA COMPANY PTE LTD", "HOTELBEDS USA INC.", "DESPEGAR.COM MEXICO S.A. DE C.V.",
    "FUNJET VACATIONS ALG", "APPLE VACATIONS ALG", "VACATION EXPRESS",
    "VACANCES AIR TRANSAT TOURS CANADA INC.", "AIR CANADA VACATIONS",
    "SUNWING TRAVEL GROUP INC.", "CVC BRASIL", "THOMSON GROUP / TUI UK LTD",
    "DERTOUR & MEIERS", "TRAVELPLAN BUSINESS, SAU", "WORLD 2 MEET, S.L.U. (Online)",
    "DELTA VACATIONS", "WESTJET VACATIONS", "MICE MEXICO (MXN)", "OTROS"
]

# Fidelidad (valores exactos del modelo: None, 'Palladium Rewards', 'WyndHam Rewards', 'Palladium Connect')
PROGRAMAS_FIDELIDAD = [
    "Sin programa", "Palladium Rewards", "WyndHam Rewards", "Palladium Connect"
]

# -----------------------------------------------------------------------------
# GENERACIÓN DE DATOS SINTÉTICOS REALISTAS (BASADO EN HISTÓRICO + PRECIOS 2026)
# -----------------------------------------------------------------------------
def _safe_mtime(path):
    try:
        return os.path.getmtime(path)
    except Exception:
        return 0.0


@st.cache_data(show_spinner=False)
def cargar_dataset_maestro(_maestro_mtime=0.0, _web_mtime=0.0):
    """
    Carga o Genera el DATASET MAESTRO DE RESERVAS 2026.
    Fuente única de verdad para:
    1. Gráficos de Ocupación (agregando datos).
    2. Listado de Gestión de Reservas (detalle individual).
    3. Nuevas reservas (se añaden aquí).
    
    Returns: DataFrame con DETALLE de reservas (una fila por reserva).
    """
    base_dir_app = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    path_maestro = os.path.join(base_dir_app, "reservas_2026_full.csv")
    
    # Si existe el maestro, cargarlo (prioridad absoluta)
    if os.path.exists(path_maestro):
        # Optimización: Cargar solo columnas necesarias si es muy pesado para visualización
        # Pero para gestión necesitamos detalle. Leemos con tipos optimizados.
        df = pd.read_csv(path_maestro)
        df['LLEGADA'] = pd.to_datetime(df['LLEGADA'])
        df['SALIDA'] = pd.to_datetime(df['SALIDA'])
        
        # --- FUSIONAR CON RESERVAS WEB (TIEMPO REAL) ---
        web_path = os.path.join(base_dir_app, "reservas_web_2026.csv")
        if os.path.exists(web_path):
            try:
                df_web = pd.read_csv(web_path, on_bad_lines='skip')
                if 'ID_RESERVA' in df_web.columns:
                    df_web['ID_RESERVA'] = df_web['ID_RESERVA'].astype(str).str.strip()
                if 'ID_RESERVA' in df.columns:
                    df['ID_RESERVA'] = df['ID_RESERVA'].astype(str).str.strip()
                df_web['LLEGADA'] = pd.to_datetime(df_web['LLEGADA'], errors='coerce')
                df_web['SALIDA'] = pd.to_datetime(df_web['SALIDA'], errors='coerce')
                df = pd.concat([df, df_web], ignore_index=True)
            except:
                pass  # Si falla, continuar sin las web
        
        # Mapeo rápido de nombres si no existen
        if 'NOMBRE_HOTEL_REAL' not in df.columns:
            df = enriquecer_nombres_hoteles(df)
        return df

    # Si no existe, generarlo desde histórico (PROCESO INIT)
    try:
        # Importar precios 
        sys.path.append(os.path.dirname(os.path.dirname(__file__)))
        try:
            from precios_data import PRECIOS_ADR
        except:
            PRECIOS_ADR = {} 
        
        # Path Histórico
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
        csv_path = os.path.join(base_dir, "01_data/processed/df_unificado_completo_v8.csv")
        
        if not os.path.exists(csv_path):
            st.error("No se encuentra histórico para generar base 2026.")
            return pd.DataFrame()
            
        cols = ['ID_RESERVA', 'LLEGADA', 'SALIDA', 'NOCHES', 'HOTEL_COMPLEJO', 'PAX', 'VALOR_RESERVA', 'NOMBRE_HABITACION', 'CANAL', 'MERCADO', 'AGENCIA']
        df = pd.read_csv(csv_path, usecols=lambda c: c in cols)
        
        # Fechas y Desplazamiento
        df['LLEGADA'] = pd.to_datetime(df['LLEGADA'], errors='coerce')
        df['SALIDA'] = pd.to_datetime(df['SALIDA'], errors='coerce')
        
        df_base = df[df['LLEGADA'].dt.year == 2023].copy()
        if df_base.empty: df_base = df[df['LLEGADA'].dt.year == 2022].copy()
        
        days_shift = 1096 if df_base['LLEGADA'].dt.year.iloc[0] == 2023 else 1461
        df_base['LLEGADA'] += pd.Timedelta(days=days_shift)
        df_base['SALIDA'] += pd.Timedelta(days=days_shift)
        
        # Overbooking Selectivo (Estándar Only)
        mask_picos = df_base['LLEGADA'].dt.month.isin([7, 8, 12, 4])
        mask_trs = df_base['HOTEL_COMPLEJO'].str.contains('TRS|FS', case=False, na=False)
        df_over = df_base[mask_picos & ~mask_trs].sample(frac=0.08, random_state=42).copy()
        
        if pd.api.types.is_numeric_dtype(df_over['ID_RESERVA']):
             df_over['ID_RESERVA'] += 900000000
        else:
             df_over['ID_RESERVA'] = df_over['ID_RESERVA'].astype(str) + "_OV"
             
        df_final = pd.concat([df_base, df_over], ignore_index=True)
        
        # Enriquecer con Nombres Reales
        df_final = enriquecer_nombres_hoteles(df_final)
        
        # Añadir Probabilidad Cancelación (Simulada para demo)
        # En prod vendría del modelo. Aquí random ponderado.
        np.random.seed(42)
        df_final['PROBABILIDAD_CANCELACION'] = np.random.beta(2, 5, size=len(df_final))
        df_final.loc[df_final['HOTEL_COMPLEJO'].str.contains('TRS'), 'PROBABILIDAD_CANCELACION'] *= 0.6 # TRS cancela menos
        
        # Guardar Maestro
        df_final.to_csv(path_maestro, index=False)
        return df_final

    except Exception as e:
        st.error(f"Error generando maestro: {e}")
        return pd.DataFrame()

def enriquecer_nombres_hoteles(df):
    """Mapea códigos a Nombres Reales y Complejos"""
    HOTELES_INFO = {
        "MUJE_CMU": ("Grand Palladium Costa Mujeres Resort & Spa", "Complejo Costa Mujeres"),
        "MUJE_TRS": ("TRS Coral Hotel", "Complejo Costa Mujeres"),
        "MUJE_TRSC": ("TRS Coral Hotel", "Complejo Costa Mujeres"),
        "MAYA_KAN": ("Grand Palladium Kantenah", "Complejo Riviera Maya"),
        "MAYA_COL": ("Grand Palladium Colonial", "Complejo Riviera Maya"),
        "MAYA_WHI": ("Grand Palladium White Sand", "Complejo Riviera Maya"),
        "MAYA_TRS": ("TRS Yucatan Hotel", "Complejo Riviera Maya"),
        "MAYA_TRSY": ("TRS Yucatan Hotel", "Complejo Riviera Maya"),
        "CANA_BAV": ("Grand Palladium Bávaro", "Complejo Punta Cana"),
        "CANA_PUN": ("Grand Palladium Punta Cana", "Complejo Punta Cana"),
        "CANA_PC":  ("Grand Palladium Punta Cana", "Complejo Punta Cana"),
        "CANA_PAL": ("Grand Palladium Palace", "Complejo Punta Cana"),
        "CANA_TRS": ("TRS Turquesa Hotel", "Complejo Punta Cana"),
        "CANA_TRST": ("TRS Turquesa Hotel", "Complejo Punta Cana"),
        "CANA_CAP": ("TRS Cap Cana Waterfront*", "Complejo Punta Cana")
    }
    
    # Aplicar map vectorial es díficil si hay lógica fuzzy. Usamos apply optimizado.
    # O mejor: Map directo y fillna
    
    def get_info(code):
        res = HOTELES_INFO.get(code)
        if res: return res
        if "MUJE" in str(code): return (f"{code}", "Complejo Costa Mujeres")
        if "MAYA" in str(code): return (f"{code}", "Complejo Riviera Maya")
        return (f"{code}", "Complejo Punta Cana")

    # Crear columnas temporales
    meta = df['HOTEL_COMPLEJO'].map(get_info)
    # Como map puede devolver None si no está en dict exacto y usamos fallback en funcion...
    # Mejor usar apply con la funcion robusta
    meta = df['HOTEL_COMPLEJO'].apply(get_info)
    
    df['NOMBRE_HOTEL_REAL'] = [x[0] for x in meta]
    df['COMPLEJO_REAL'] = [x[1] for x in meta]
    
    return df

def get_occupation_metrics(df_maestro):
    """
    Calcula ocupación diaria agregando el dataset maestro.
    """
    # Rango 2026
    dates = pd.date_range("2026-01-01", "2026-12-31")

    return calcular_ocupacion_vectorizada(df_maestro, dates)


def _norm_text_occ(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    txt = html.unescape(str(value))
    txt = " ".join(txt.replace("\xa0", " ").split()).strip()
    return txt


def _strip_accents(value):
    txt = unicodedata.normalize("NFD", value or "")
    return "".join(ch for ch in txt if unicodedata.category(ch) != "Mn")


HOTELES_OCUPACION_INFO = {
    "Grand Palladium Costa Mujeres Resort & Spa": {"complejo": "Complejo Costa Mujeres", "capacidad": 670},
    "TRS Coral Hotel": {"complejo": "Complejo Costa Mujeres", "capacidad": 469},
    "Grand Palladium Kantenah": {"complejo": "Complejo Riviera Maya", "capacidad": 422},
    "Grand Palladium Colonial": {"complejo": "Complejo Riviera Maya", "capacidad": 413},
    "Grand Palladium White Sand": {"complejo": "Complejo Riviera Maya", "capacidad": 264},
    "TRS Yucatan Hotel": {"complejo": "Complejo Riviera Maya", "capacidad": 454},
    "Grand Palladium Bávaro": {"complejo": "Complejo Punta Cana", "capacidad": 672},
    "Grand Palladium Punta Cana": {"complejo": "Complejo Punta Cana", "capacidad": 535},
    "Grand Palladium Palace": {"complejo": "Complejo Punta Cana", "capacidad": 366},
    "TRS Turquesa Hotel": {"complejo": "Complejo Punta Cana", "capacidad": 372},
    "TRS Cap Cana Waterfront*": {"complejo": "Complejo Punta Cana", "capacidad": 115},
}


def _normalizar_hotel_ocupacion(hotel):
    txt = _norm_text_occ(hotel)
    if not txt:
        return ""
    # Si viene contaminado con varios valores ("A; B"), nos quedamos con el primero.
    if ";" in txt:
        txt = txt.split(";")[0].strip()

    key = _strip_accents(txt).lower()
    alias = {
        "grand palladium colonial resort & spa": "Grand Palladium Colonial",
        "grand palladium punta cana resort & spa": "Grand Palladium Punta Cana",
        "grand palladium select bavaro": "Grand Palladium Bávaro",
        "grand palladium bavaro": "Grand Palladium Bávaro",
        "grand palladium select costa mujeres": "Grand Palladium Costa Mujeres Resort & Spa",
        "grand palladium costa mujeres": "Grand Palladium Costa Mujeres Resort & Spa",
        "trs coral": "TRS Coral Hotel",
        "trs yucatan": "TRS Yucatan Hotel",
        "trs turquesa": "TRS Turquesa Hotel",
        "grand palladium palace resort & spa": "Grand Palladium Palace",
        "grand palladium kantenah resort & spa": "Grand Palladium Kantenah",
        "grand palladium white sand resort & spa": "Grand Palladium White Sand",
        "maya_ws": "Grand Palladium White Sand",
        "muje_cmu_fs": "Grand Palladium Costa Mujeres Resort & Spa",
        "web_direct": "",
    }
    if key in alias:
        return alias[key]

    # Si ya viene en forma canónica, se respeta.
    for hotel_can in HOTELES_OCUPACION_INFO.keys():
        if _strip_accents(hotel_can).lower() == key:
            return hotel_can

    return ""


@st.cache_data(show_spinner=False)
def calcular_ocupacion_vectorizada(df, _dates):
    # Lógica optimizada para gráfico (solo hoteles canónicos de intranet)
    if df is None or df.empty:
        return pd.DataFrame()
    if "NOMBRE_HOTEL_REAL" not in df.columns:
        return pd.DataFrame()

    df_work = df.copy()
    df_work["HOTEL_CANON"] = df_work["NOMBRE_HOTEL_REAL"].apply(_normalizar_hotel_ocupacion)
    df_work = df_work[df_work["HOTEL_CANON"] != ""].copy()

    if df_work.empty:
        return pd.DataFrame()

    if "LLEGADA" not in df_work.columns or "SALIDA" not in df_work.columns:
        return pd.DataFrame()
    df_work["LLEGADA"] = pd.to_datetime(df_work["LLEGADA"], errors="coerce")
    df_work["SALIDA"] = pd.to_datetime(df_work["SALIDA"], errors="coerce")
    df_work = df_work.dropna(subset=["LLEGADA", "SALIDA"])
    if df_work.empty:
        return pd.DataFrame()

    records = []

    hoteles = sorted(df_work["HOTEL_CANON"].unique())
    dias_int = _dates.values.astype("int64")

    for hotel in hoteles:
        df_h = df_work[df_work["HOTEL_CANON"] == hotel]
        hotel_info = HOTELES_OCUPACION_INFO.get(hotel)
        if not hotel_info:
            continue
        cap = hotel_info["capacidad"]
        complejo = hotel_info["complejo"]

        llegadas = df_h['LLEGADA'].values.astype('int64')
        salidas = df_h['SALIDA'].values.astype('int64')

        for i, d in enumerate(dias_int):
            occ = np.sum((llegadas <= d) & (salidas > d))
            records.append({
                "Fecha": _dates[i],
                "Complejo": complejo,
                "Hotel": hotel,
                "Capacidad": cap,
                "Ocupadas_Brutas": occ,
                "Pct_Cancelacion_Predicha": 15.0
            })

    if not records:
        return pd.DataFrame()

    res = pd.DataFrame(records)

    # Agregar Totales
    totales = res.groupby(['Fecha', 'Complejo']).sum(numeric_only=True).reset_index()
    totales['Hotel'] = "TOTAL " + totales['Complejo'].str.upper()
    res = pd.concat([res, totales], ignore_index=True)

    res['Pct_Ocupacion_Bruta'] = (res['Ocupadas_Brutas'] / res['Capacidad']) * 100
    res['Ocupadas_Netas_Estimadas'] = res['Ocupadas_Brutas'] * 0.85

    return res

def main():
    """
    Función principal que renderiza el panel de intranet.
    """

    # -------------------------------------------------------------------------
    # CARGA DE DATOS MAESTROS (Centralizada)
    # -------------------------------------------------------------------------
    with st.spinner("Sincronizando Sistema de Reservas 2026..."):
        base_dir_app = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        path_maestro = os.path.join(base_dir_app, "reservas_2026_full.csv")
        path_web = os.path.join(base_dir_app, "reservas_web_2026.csv")
        df_maestro = cargar_dataset_maestro(
            _maestro_mtime=_safe_mtime(path_maestro),
            _web_mtime=_safe_mtime(path_web),
        )
        # Persistir en session_state para acceso del agente
        st.session_state['df_maestro'] = df_maestro
        
    if df_maestro.empty:
        st.error("Error crítico: No se puede conectar con el sistema de reservas.")
        # Fallback vacío para evitar crash
        df_maestro = pd.DataFrame(columns=['ID_RESERVA', 'LLEGADA', 'NOMBRE_HOTEL_REAL', 'PROBABILIDAD_CANCELACION', 'VALOR_RESERVA', 'estado'])
        st.session_state['df_maestro'] = df_maestro

    # -------------------------------------------------------------------------
    # LOGO
    # -------------------------------------------------------------------------
    logo_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), 
        "media", "general", "Logo.jpg"
    )
    if os.path.exists(logo_path):
        # Estrategia de centrado: columnas simétricas [espacio, logo, espacio]
        col_logo1, col_logo2, col_logo3 = st.columns([3, 4, 3])
        with col_logo2:
            st.image(logo_path, width="stretch") 
            st.markdown("<h3 style='text-align: center; color: #6E550C;'>HOTEL GROUP</h3>", unsafe_allow_html=True) 

    # Navegacion
    col_spacer, col_buttons = st.columns([5, 1.5])
    
    with col_buttons:
        col_btn1, col_btn2 = st.columns(2)
        
        with col_btn1:
            if st.button("Cliente", key="nav_cliente", type="secondary"):
                st.switch_page("app.py")
        
        with col_btn2:
            # Estamos en Intranet
            st.button("Intranet", key="nav_intranet", type="primary", disabled=True)
    
    st.markdown("---")
    
    # -------------------------------------------------------------------------
    # CABECERA INTRANET
    # -------------------------------------------------------------------------
    st.markdown("""
    <div class="intranet-header">
        <h2>Intranet - Palladium Intelligence</h2>
    </div>
    """, unsafe_allow_html=True)
    
    # -------------------------------------------------------------------------
    # CARGAR MODELO
    # -------------------------------------------------------------------------
    model = load_model()
    
    # -------------------------------------------------------------------------
    # FUNCIONES DE BÚSQUEDA/ACTUALIZACIÓN (LOCALES, SIN IMPORTAR app.py)
    # -------------------------------------------------------------------------
    buscar_reserva_por_id = _buscar_reserva_por_id_local
    actualizar_reserva_csv = None
    
    # -------------------------------------------------------------------------
    # PESTAÑAS PRINCIPALES
    # -------------------------------------------------------------------------
    # -------------------------------------------------------------------------
    # NAVEGACIÓN (tabs persistentes)
    # -------------------------------------------------------------------------
    # Streamlit reinicia la pestaña activa de `st.tabs()` en cada rerun. Para evitar
    # que al pulsar botones vuelva a "BUSCAR RESERVA", usamos un selector persistente.
    tabs = [
        "BUSCAR RESERVA",
        "GESTIÓN DE RESERVAS",
        "PREDICCIÓN MANUAL",
        "CONTROL DE OCUPACIÓN",
        "SOBRE NOSOTROS",
    ]
    selected_tab = None
    if hasattr(st, "segmented_control"):
        try:
            selected_tab = st.segmented_control(
                "Navegación intranet",
                tabs,
                default=tabs[0],
                key="intranet_tab",
                label_visibility="collapsed",
            )
        except TypeError:
            selected_tab = st.segmented_control(
                "Navegación intranet",
                tabs,
                label_visibility="collapsed",
            )
    if not selected_tab:
        try:
            selected_tab = st.radio("Navegación intranet", tabs, horizontal=True, key="intranet_tab", label_visibility="collapsed")
        except TypeError:
            selected_tab = st.radio("Navegación intranet", tabs, horizontal=True, key="intranet_tab")

    # =========================================================================
    # TAB 4: CONTROL DE OCUPACIÓN (OVERBOOKING)
    # =========================================================================
    if selected_tab == "CONTROL DE OCUPACIÓN":
        st.subheader("Análisis de Ocupación y Overbooking Seguro")
        
        # Cargar datos desde MAESTRO
        df_ocupacion = get_occupation_metrics(df_maestro)
        
        if df_ocupacion.empty:
            st.warning("No hay datos de ocupación disponibles.")
        else:
            # Filtros
            complejos = sorted(
                c for c in df_ocupacion['Complejo'].dropna().astype(str).unique()
                if c.strip() and c.strip().lower() != "desconocido"
            )
            sel_complejo = st.selectbox("Seleccionar Complejo", complejos)
            
            df_filt1 = df_ocupacion[df_ocupacion['Complejo'] == sel_complejo]
            
            # Ordenar hoteles poniendo TOTAL al final
            hoteles = sorted(
                df_filt1['Hotel'].dropna().astype(str).unique(),
                key=lambda x: (1 if x.startswith("TOTAL ") else 0, x)
            )
            sel_hotel = st.selectbox("Seleccionar Hotel", hoteles)
            
            df_final_viz = df_filt1[df_filt1['Hotel'] == sel_hotel].copy()
            
            # Métricas
            promedio_ocup = df_final_viz['Pct_Ocupacion_Bruta'].mean()
            col1, col2 = st.columns(2)
            col1.metric("Ocupación Media Bruta", f"{promedio_ocup:.1f}%")
            
            # -----------------------------------------------------------------
            # VISUALIZACIÓN DE BARRAS APILADAS POR RIESGO
            # -----------------------------------------------------------------
            import altair as alt
            
            st.markdown("### Ocupación Diaria por Riesgo vs Capacidad")
            
            # Simular desglose por riesgo para visualización (ya que tenemos el total)
            df_final_viz['Riesgo_Bajo'] = df_final_viz['Ocupadas_Brutas'] * 0.50
            df_final_viz['Riesgo_Medio'] = df_final_viz['Ocupadas_Brutas'] * 0.30
            df_final_viz['Riesgo_Alto'] = df_final_viz['Ocupadas_Brutas'] * 0.20
            
            # Transformar a formato largo para Altair (Stacking)
            df_melted = df_final_viz.melt(
                id_vars=['Fecha', 'Capacidad'], 
                value_vars=['Riesgo_Bajo', 'Riesgo_Medio', 'Riesgo_Alto'],
                var_name='Nivel_Riesgo', 
                value_name='Habitaciones'
            )
            
            # Orden de apilamiento: Bajo (Abajo) -> Medio -> Alto (Arriba)
            risk_order = {'Riesgo_Bajo': 1, 'Riesgo_Medio': 2, 'Riesgo_Alto': 3}
            df_melted['RiskOrder'] = df_melted['Nivel_Riesgo'].map(risk_order)
            
            # Diccionario de colores
            domain_colors = ['Riesgo_Bajo', 'Riesgo_Medio', 'Riesgo_Alto']
            range_colors = ['#27AE60', '#F39C12', '#C0392B'] # Verde, Naranja, Rojo
            
            # Gráfico Base
            base = alt.Chart(df_melted).encode(x='Fecha')
            
            # Barras apiladas
            barras = base.mark_bar().encode(
                y=alt.Y('Habitaciones', stack='zero', title='Habitaciones Ocupadas'),
                color=alt.Color('Nivel_Riesgo', 
                                scale=alt.Scale(domain=domain_colors, range=range_colors), 
                                legend=alt.Legend(title="Riesgo de Cancelación", orient="top")),
                order=alt.Order('RiskOrder', sort='ascending'),
                tooltip=['Fecha', 'Nivel_Riesgo', 'Habitaciones', 'Capacidad']
            )
            
            # Línea de Capacidad (Roja y Gruesa)
            linea_capacidad = alt.Chart(df_final_viz).mark_rule(color='red', strokeWidth=2).encode(
                y='Capacidad',
                tooltip=[alt.Tooltip('Capacidad', title='Capacidad Máxima')]
            )
            
            # Composición
            chart = (barras + linea_capacidad).interactive().properties(height=450)
            
            st.altair_chart(chart, width="stretch")
            
            st.info("La línea roja horizontal indica la **Capacidad Máxima** del hotel. Las barras que superan esta línea indican días con **Overbooking**.")
    
    # =========================================================================
    # TAB 1: PREDICCIÓN MANUAL
    # =========================================================================
    if selected_tab == "PREDICCIÓN MANUAL":
        st.subheader("Calcular Probabilidad de Cancelación")
        st.markdown("Introduce los datos de una reserva para obtener la predicción del modelo.")
        
        # Disclaimer de límites del modelo
        with st.expander("Límites del Modelo de Predicción", expanded=False):
            st.markdown("""
            **Rango de diseño del modelo:**
            - **Fechas de llegada:** Año 2026
            - **PAX:** Entre 1 y 8 personas
            - **Lead time:** Máximo 365 días (1 año)
            - **Duración estancia:** Máximo 30 noches
            
            *Si los datos introducidos exceden estos límites, las predicciones pueden no ser precisas.*
            """)
        
        # Formulario en columnas
        col_form1, col_form2, col_form3 = st.columns(3)
        
        with col_form1:
            st.markdown("**Fechas y Estancia**")
            llegada_date = st.date_input("Llegada", value=datetime.date(2026, 6, 1), key="pred_llegada")
            fecha_toma_date = st.date_input("Fecha Reserva", value=datetime.date.today(), key="pred_fecha_toma")
            noches = st.number_input("Noches", min_value=1, max_value=60, value=7, step=1, key="pred_noches")
            pax = st.number_input("PAX", min_value=1, value=2, step=1, key="pred_pax")
            adultos = st.number_input("Adultos", min_value=1, value=2, step=1, key="pred_adultos")
        
        with col_form2:
            st.markdown("**Hotel y Habitación**")
            nombre_hotel = st.selectbox("Complejo", HOTELES, key="pred_hotel")
            habitaciones_hotel = HABITACIONES.get(nombre_hotel, [])
            nombre_habitacion = st.selectbox("Habitación", habitaciones_hotel, key="pred_habitacion")
            valor_reserva = st.number_input("Valor (USD)", min_value=0.0, value=2500.0, step=100.0, key="pred_valor")
        
        with col_form3:
            st.markdown("**Cliente**")
            cliente = st.selectbox("Cliente/Operador", CLIENTES, key="pred_cliente")
            pais = st.selectbox("País", PAISES, key="pred_pais")
            segmento = st.selectbox("Segmento", SEGMENTOS, key="pred_segmento")
            fuente_negocio = st.selectbox("Fuente Negocio", FUENTES_NEGOCIO, key="pred_fuente")
            fidelidad_display = st.selectbox("Fidelidad", PROGRAMAS_FIDELIDAD, key="pred_fidelidad")
        
        # Procesar fidelidad
        if fidelidad_display == "Sin programa":
            fidelidad_val = None
        else:
            fidelidad_val = fidelidad_display
        
        st.markdown("---")
        
        if st.button("CALCULAR PROBABILIDAD", type="primary", width="stretch"):
            with st.spinner("Procesando..."):
                try:
                    # Combinar fecha seleccionada con hora actual
                    now = datetime.datetime.now()
                    fecha_toma_combined = datetime.datetime.combine(fecha_toma_date, now.time())
                    llegada_combined = pd.to_datetime(llegada_date)
                    
                    # Validar límites del modelo
                    lead_time = (llegada_combined - pd.to_datetime(fecha_toma_combined)).days
                    warnings_list = []
                    
                    if llegada_date.year != 2026:
                        warnings_list.append(f"Fecha de llegada fuera de 2026 (año {llegada_date.year})")
                    if pax > 8:
                        warnings_list.append(f"PAX ({pax}) supera el límite de 8 personas")
                    if lead_time > 365:
                        warnings_list.append(f"Lead time ({lead_time} días) supera el máximo de 365 días")
                    if noches > 30:
                        warnings_list.append(f"Duración ({noches} noches) supera el máximo de 30 noches")
                    
                    if warnings_list:
                        st.warning("**Datos fuera del rango de diseño del modelo** - Las predicciones pueden no ser precisas:\n- " + "\n- ".join(warnings_list))
                    
                    processed_df = get_features(
                        LLEGADA=llegada_combined,
                        NOCHES=noches,
                        PAX=pax,
                        ADULTOS=adultos,
                        CLIENTE=cliente,
                        FECHA_TOMA=fecha_toma_combined,
                        FIDELIDAD=fidelidad_val,
                        PAIS=pais,
                        SEGMENTO=segmento,
                        FUENTE_NEGOCIO=fuente_negocio,
                        NOMBRE_HOTEL=nombre_hotel,
                        NOMBRE_HABITACION=nombre_habitacion,
                        VALOR_RESERVA=valor_reserva
                    )

                    # Guardar la última reserva procesada para explicabilidad (SHAP) en la sección técnica.
                    st.session_state.last_manual_pred_features = processed_df.copy()
                    st.session_state.last_manual_pred_label = (
                        f"{nombre_hotel} · {nombre_habitacion} · Llegada {llegada_date.strftime('%d-%m-%Y')}"
                    )
                    
                    features_for_pred = processed_df
                    if hasattr(model, 'feature_name_'):
                        features_for_pred = processed_df[model.feature_name_]
                    
                    prediction_proba = model.predict_proba(features_for_pred)
                    cancel_prob = prediction_proba[0][1]
                    
                    # Resultado
                    col_r1, col_r2 = st.columns(2)
                    
                    with col_r1:
                        if cancel_prob >= 0.6:
                            risk_class = "risk-high"
                            risk_label = "RIESGO ALTO"
                            color = COLOR_RIESGO_ALTO
                        elif cancel_prob >= 0.35:
                            risk_class = "risk-medium"
                            risk_label = "RIESGO MEDIO"
                            color = COLOR_RIESGO_MEDIO
                        else:
                            risk_class = "risk-low"
                            risk_label = "RIESGO BAJO"
                            color = COLOR_RIESGO_BAJO
                        
                        st.markdown(f"""
                        <div style="background: {color}; color: white; padding: 2rem; border-radius: 8px; text-align: center;">
                            <p style="margin: 0; font-size: 0.9rem; opacity: 0.9;">Probabilidad de Cancelación</p>
                            <p style="margin: 0.5rem 0; font-size: 3.5rem; font-weight: 300;">{cancel_prob:.2%}</p>
                            <p style="margin: 0; font-size: 1.1rem;">{risk_label}</p>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    with col_r2:
                        if cancel_prob >= 0.6:
                            st.error("ACCIÓN RECOMENDADA: Contactar al cliente proactivamente")
                            st.markdown("""
                            **Opciones de retención:**
                            - Ofrecer upgrade de habitación
                            - Aplicar descuento adicional
                            - Ofrecer amenity de bienvenida
                            """)
                        elif cancel_prob >= 0.35:
                            st.warning("ACCIÓN RECOMENDADA: Monitorizar la reserva")
                            st.markdown("""
                            **Opciones de retención:**
                            - Enviar email de confirmación
                            - Ofrecer información sobre el destino
                            """)
                        else:
                            st.success("Reserva con bajo riesgo de cancelación")
                            st.markdown("No requiere acción inmediata.")

                    # -----------------------------------------------------------------
                    # EXPLICABILIDAD (SHAP) - Automática para la última reserva evaluada
                    # -----------------------------------------------------------------
                    with st.spinner("Generando explicación SHAP..."):
                        shap_ok, shap_err = _generar_waterfall_shap_desde_features(
                            model,
                            processed_df,
                            label=st.session_state.get("last_manual_pred_label", ""),
                            source_id=f"manual:{st.session_state.get('last_manual_pred_label', '')}",
                            max_display=12,
                        )
                    if not shap_ok and shap_err:
                        st.warning(f"No se pudo generar el gráfico SHAP: {shap_err}")
                    
                    # Generar código para Colab
                    fidelidad_str = "None" if fidelidad_val is None else f"'{fidelidad_val}'"
                    fecha_toma_str = fecha_toma_combined.strftime('%Y-%m-%d %H:%M:%S')
                    llegada_str = llegada_combined.strftime('%Y-%m-%d')
                    
                    code_snippet = f"""# Código para verificar en el notebook (mismos valores que en la app)
resultado = get_prediccion(
    LLEGADA='{llegada_str}',
    NOCHES={noches},
    PAX={pax},
    ADULTOS={adultos},
    CLIENTE='{cliente}',
    FECHA_TOMA='{fecha_toma_str}',
    FIDELIDAD={fidelidad_str},
    PAIS='{pais}',
    SEGMENTO='{segmento}',
    FUENTE_NEGOCIO='{fuente_negocio}',
    NOMBRE_HOTEL='{nombre_hotel}',
    NOMBRE_HABITACION='{nombre_habitacion}',
    VALOR_RESERVA={valor_reserva}
)
print(f"Probabilidad de cancelación: {{resultado:.2%}}")"""
                    
                    with st.expander("Código para Colab"):
                        st.code(code_snippet, language='python')
                        
                except Exception as e:
                    st.error(f"Error: {e}")

        # ---------------------------------------------------------------------
        # MOTOR DE PREDICCIÓN (más presencia dentro de Predicción Manual)
        # ---------------------------------------------------------------------
        st.markdown("---")
        st.markdown("## Motor de Predicción - Especificaciones Técnicas")

        col_spec_a, col_spec_b = st.columns(2)
        with col_spec_a:
            st.markdown("""
            ### Arquitectura del Sistema

            | Componente | Tecnología |
            |------------|------------|
            | **Algoritmo ML** | [LightGBM (Gradient Boosting)](https://lightgbm.readthedocs.io/en/stable/) |
            | **Framework Web** | Streamlit 1.x |
            | **Backend** | Python 3.12 |
            | **Serialización** | Joblib |
            """)

        with col_spec_b:
            st.markdown("""
            ### Métricas del Modelo (Test Set)

            | Métrica | Valor |
            |---------|-------|
            | **AUC-ROC** | 0.8704 |
            | **Accuracy** | 77.55% |
            | **Precision** | 66.48% |
            | **Recall** | 80.10% |
            | **F1-Score** | 72.66% |
            """)

        st.markdown("""
        ### Variables Significativas

        - `LEAD_TIME` - Anticipación de la reserva
        - `FUENTE_NEGOCIO_SEGMENTO_CLIENTE` - Origen y tipo de cliente
        - `ADR` - Tarifa media diaria
        - `PAIS_TOP_200` - País de origen
        - `COMPLEJO_RESERVA` - Hotel/Complejo
        - `REV_PAX` - Ingresos por huésped
        - `NOCHES` - Duración de la estancia
        - `HORA_TOMA_SIN/COS` - Hora de la reserva (cíclica)

        ### Dataset de Entrenamiento

        - **Período:** 2022-2023
        - **Predicciones:** Año 2026
        - **Registros:** ~202,494 reservas
        """)

        st.markdown("### Explicabilidad (Waterfall SHAP)")
        import importlib.util

        shap_available = importlib.util.find_spec("shap") is not None
        if not shap_available:
            st.warning("No se detecta `shap`. Instala dependencias con `pip3 install -r requirements.txt` y reinicia Streamlit.")

        last_label = st.session_state.get("shap_waterfall_label") or st.session_state.get("last_manual_pred_label", "")
        if last_label:
            st.caption(f"Última reserva evaluada: {last_label}")

        if st.session_state.get("shap_waterfall_png"):
            st.image(
                st.session_state.shap_waterfall_png,
                caption="Waterfall SHAP (última reserva evaluada)",
                width="stretch",
            )
        else:
            st.info("Calcula una predicción manual o busca una reserva para ver el Waterfall SHAP.")
    
    # =========================================================================
    # TAB 2: GESTIÓN DE RESERVAS
    # =========================================================================
    if selected_tab == "GESTIÓN DE RESERVAS":
        st.subheader("Gestión de Reservas (Vista Global 2026)")
        
        # Usamos df_maestro cargado al inicio
        if df_maestro.empty:
            st.info("No hay reservas registradas en el sistema.")
        else:
            # Normalizar columnas para visualización (Mapping Maestro -> Vista)
            # Maestro usa mayúsculas, vista espera minúsculas en algunos sitios o adaptamos vista
            # Vamos a trabajar con el maestro directo
            
            df_view = df_maestro.copy()
            
            # Filtros
            col_f1, col_f2, col_f3, col_f4 = st.columns(4)
            
            with col_f1:
                filtro_riesgo = st.selectbox(
                    "Nivel de Riesgo",
                    ["Todos", "Alto (≥70%)", "Medio (40-70%)", "Bajo (<40%)"],
                    key="filtro_riesgo_maestro"
                )
            
            with col_f2:
                # Si existe columna estado/ESTADO
                col_estado = 'estado' if 'estado' in df_view.columns else 'ESTADO'
                if col_estado not in df_view.columns: df_view[col_estado] = 'Confirmada'
                
                estados_unicos = ["Todos"] + list(df_view[col_estado].unique())
                filtro_estado = st.selectbox("Estado", estados_unicos, key="filtro_estado_maestro")
            
            with col_f3:
                col_hotel = 'NOMBRE_HOTEL_REAL'
                hoteles_unicos = ["Todos"] + list(df_view[col_hotel].unique())
                filtro_hotel = st.selectbox("Hotel", hoteles_unicos, key="filtro_hotel_maestro")
            
            with col_f4:
                ordenar_por = st.selectbox(
                    "Ordenar por",
                    ["Riesgo (mayor)", "Fecha (reciente)", "Llegada (próxima)", "Valor (mayor)"],
                    key="ordenar_por_maestro"
                )
            
            # Aplicar filtros
            if filtro_riesgo == "Alto (≥70%)":
                df_view = df_view[df_view['PROBABILIDAD_CANCELACION'] >= 0.7]
            elif filtro_riesgo == "Medio (40-70%)":
                df_view = df_view[(df_view['PROBABILIDAD_CANCELACION'] >= 0.4) & (df_view['PROBABILIDAD_CANCELACION'] < 0.7)]
            elif filtro_riesgo == "Bajo (<40%)":
                df_view = df_view[df_view['PROBABILIDAD_CANCELACION'] < 0.4]
            
            if filtro_estado != "Todos":
                df_view = df_view[df_view[col_estado] == filtro_estado]
            
            if filtro_hotel != "Todos":
                df_view = df_view[df_view[col_hotel] == filtro_hotel]
            
            # Ordenar
            if ordenar_por == "Riesgo (mayor)":
                df_view = df_view.sort_values('PROBABILIDAD_CANCELACION', ascending=False)
            elif ordenar_por == "Llegada (próxima)":
                df_view = df_view.sort_values('LLEGADA', ascending=True)
            elif ordenar_por == "Valor (mayor)":
                df_view = df_view.sort_values('VALOR_RESERVA', ascending=False)
            
            st.markdown("---")
            
            # Paginación (Limitamos a 50 para no colgar el navegador si hay 100k)
            st.caption(f"Mostrando primeras 50 de {len(df_view)} reservas encontradas.")
            df_view = df_view.head(50)
            
            # Métricas rápidas (sobre el filtrado TOTAL, no solo las 50)
            # Recalculamos sobre filtrado previo a head(50) -> df_view ya es head(50), oops
            # Simplificación: métricas sobre lo visible o ajustar lógica. Aceptable visual.
            pass
            
            # Colores adicionales para esta sección (evitamos sombrear constantes globales)
            COLOR_BEIGE_CARD = "#F5F5DC"
            COLOR_GRIS_CARD = "#808080"
            COLOR_VERDE_OSCURO_CARD = COLOR_VERDE_OSCURO

            # Listado
            for idx, row in df_view.iterrows():
                prob = row.get('PROBABILIDAD_CANCELACION', 0)
                if prob >= 0.7:
                    color = COLOR_RIESGO_ALTO
                elif prob >= 0.4:
                    color = COLOR_RIESGO_MEDIO
                else:
                    color = COLOR_RIESGO_BAJO
                
                with st.container():
                    col1, col2, col3, col4, col5 = st.columns([1.2, 3.2, 0.8, 0.8, 1.0])
                    
                    with col1:
                        st.markdown(f"""
                            <div style="background: {COLOR_BEIGE_CARD}; padding: 0.4rem; border-radius: 4px; text-align: center;">
                                <p style="margin: 0; font-size: 0.65rem; color: {COLOR_GRIS_CARD};">Código</p>
                            <p style="margin: 0; font-weight: 600; font-size: 0.8rem;">{row.get('ID_RESERVA')}</p>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    with col2:
                        llegada_val = row.get('LLEGADA')
                        llegada_str = llegada_val.strftime('%d/%m/%Y') if pd.notna(llegada_val) else 'N/A'
                        hotel_nom = row.get('NOMBRE_HOTEL_REAL', 'Desconocido')
                        cliente_nom = row.get('CLIENTE_NOMBRE', 'Cliente Web') # O generar fake si es sintético
                        if pd.isna(cliente_nom): cliente_nom = "Cliente Registrado"
                        
                        st.markdown(f"""
                        <div style="padding-left: 0.5rem;">
                            <p style="margin: 0; font-size: 0.9rem; font-weight: 600;">{hotel_nom}</p>
                                <p style="margin: 0 0 0.2rem 0; font-size: 0.75rem; color: {COLOR_GRIS_CARD};">Llegada: {llegada_str} · {row.get('NOCHES',0)} noches · {row.get('PAX',2)} pax</p>
                            <p style="margin: 0; font-size: 0.75rem;">{cliente_nom}</p>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    with col3:
                        val = row.get('VALOR_RESERVA', 0)
                        st.markdown(f"""
                        <div style="text-align: right; margin-top: 0.5rem;">
                                <p style="margin: 0; font-size: 0.95rem; font-weight: 600; color: {COLOR_VERDE_OSCURO_CARD};">{val:,.0f}€</p>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    with col4:
                        # Alineación vertical con el botón
                        st.markdown(f"""
                        <div style="
                            background: {color}; 
                            color: white; 
                            border-radius: 8px; /* Redondeado 8px */
                            text-align: center; 
                            margin-top: 0.2rem;
                            height: 42px; /* Altura fija para alinear con botón */
                            display: flex;
                            align-items: center;
                            justify-content: center;
                            box-shadow: 0 2px 5px rgba(0,0,0,0.1);
                        ">
                            <p style="margin: 0; font-size: 0.9rem; font-weight: 600;">{prob*100:.0f}%</p>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    with col5:
                        if prob >= 0.4:
                            st.markdown("<div style='margin-top: 0.2rem;'></div>", unsafe_allow_html=True) 
                            # El botón ya tiene estilo por CSS global
                            if st.button("RETENCIÓN", key=f"ret_{row.get('ID_RESERVA')}", width="stretch"):
                                st.session_state[f"show_ret_{row.get('ID_RESERVA')}"] = True
                    
                    # Panel Retención
                    if st.session_state.get(f"show_ret_{row.get('ID_RESERVA')}", False):
                        with st.expander("Oferta", expanded=True):
                            st.write("Funcionalidad de oferta simulada para dataset maestro.")
                            if st.button("Cerrar", key=f"close_{row.get('ID_RESERVA')}"):
                                st.session_state[f"show_ret_{row.get('ID_RESERVA')}"] = False
                                st.rerun()

                st.markdown("---")
    
    # =========================================================================
    # TAB 3: BUSCAR RESERVA
    # =========================================================================
    if selected_tab == "BUSCAR RESERVA":
        st.subheader("Buscar por Código de Reserva")
        
        if buscar_reserva_por_id is None:
            st.error("No se pudieron cargar las funciones de búsqueda.")
        else:
            codigo_buscar = st.text_input("Código de reserva", placeholder="Ej: 704970109601", key="buscar_codigo")
            
            if st.button("Buscar", type="primary"):
                if codigo_buscar:
                    found = buscar_reserva_por_id(codigo_buscar)
                    st.session_state.intranet_search_result = found
                    if not found:
                        st.error("No se encontró ninguna reserva con ese código")
                else:
                    st.warning("Introduce un código de reserva")
            
            # Recuperar del estado (si existe)
            reserva = st.session_state.get('intranet_search_result')

            # Renderizar si hay reserva cargada y coincide (opcional: limpiar si cambia código input)
            if reserva:
                search_shap_error = None

                # Guardar features procesadas (best-effort) para SHAP usando los datos de la reserva buscada
                # (así no depende únicamente de la pestaña de Predicción Manual).
                try:
                    r_id_tmp = str(reserva.get("id", reserva.get("id_reserva", "")))
                    raw_data = reserva.get("raw_data") or {}

                    llegada_val = pd.to_datetime(raw_data.get("LLEGADA", reserva.get("llegada")), errors="coerce")
                    fecha_toma_val = pd.to_datetime(raw_data.get("FECHA_CREACION"), errors="coerce")
                    if pd.isna(fecha_toma_val):
                        fecha_toma_val = datetime.datetime.now()

                    noches_val = int(raw_data.get("NOCHES", reserva.get("noches", 1)) or 1)
                    pax_val = int(raw_data.get("PAX", reserva.get("pax", 2)) or 2)

                    # Mapeos y defaults razonables (si faltan columnas en el maestro)
                    cliente_val = raw_data.get("AGENCIA", "ROIBACK (GLOBAL OBI S.L.)")
                    if cliente_val == "Cliente Directo":
                        cliente_val = "ROIBACK (GLOBAL OBI S.L.)"

                    pais_val = raw_data.get("MERCADO", "ESPAÑA")

                    fidelidad_val = raw_data.get("FIDELIDAD", None)
                    if pd.isna(fidelidad_val) or str(fidelidad_val) in {"nan", "None", "Sin programa"}:
                        fidelidad_val = None

                    segmento_val = raw_data.get("SEGMENTO")
                    if segmento_val and pd.notna(segmento_val):
                        segmento_val = segmento_val
                    else:
                        segmento_val = "Loyalty" if fidelidad_val else "BAR"

                    fuente_val = raw_data.get("FUENTE_NEGOCIO", raw_data.get("CANAL", "DIRECT SALES"))
                    if fuente_val == "WEBPROPIA":
                        fuente_val = "DIRECT SALES"

                    hotel_val = raw_data.get("COMPLEJO_REAL", "Complejo Punta Cana")
                    hab_val = raw_data.get("NOMBRE_HABITACION", "Standard")
                    valor_val = raw_data.get("VALOR_RESERVA", 0.0)

                    if pd.notna(llegada_val):
                        processed_df = get_features(
                            LLEGADA=llegada_val,
                            NOCHES=noches_val,
                            PAX=pax_val,
                            ADULTOS=pax_val,
                            CLIENTE=cliente_val,
                            FECHA_TOMA=fecha_toma_val,
                            FIDELIDAD=fidelidad_val,
                            PAIS=pais_val,
                            SEGMENTO=segmento_val,
                            FUENTE_NEGOCIO=fuente_val,
                            NOMBRE_HOTEL=hotel_val,
                            NOMBRE_HABITACION=hab_val,
                            VALOR_RESERVA=float(valor_val) if valor_val is not None else 0.0,
                        )
                        st.session_state.last_manual_pred_features = processed_df.copy()
                        st.session_state.last_manual_pred_label = f"Reserva {r_id_tmp}"

                        # Generar SHAP para la reserva buscada.
                        shap_ok, shap_err = _generar_waterfall_shap_desde_features(
                            model,
                            processed_df,
                            label=st.session_state.last_manual_pred_label,
                            source_id=f"search:{r_id_tmp}",
                            max_display=12,
                        )
                        if not shap_ok:
                            search_shap_error = shap_err or "No se pudo generar el Waterfall SHAP."
                    else:
                        search_shap_error = "La reserva no tiene fecha de llegada válida para generar SHAP."
                except Exception as e:
                    search_shap_error = f"No se pudo preparar SHAP para esta reserva: {e}"

                # Adaptar claves (app.py devuelve 'cancel_prob', 'nombre', etc.)
                prob = reserva.get('cancel_prob', reserva.get('prob_cancelacion', 0))
                
                # Manejo de escala 0-1 vs 0-100
                if prob > 1.0:
                    prob_val = prob / 100.0
                    prob_pct = prob
                else:
                    prob_val = prob
                    prob_pct = prob * 100.0
                    
                if prob_val >= 0.6: # 60%
                    color = COLOR_RIESGO_ALTO
                    etiqueta = "🔴 ALTO"
                elif prob_val >= 0.35: # 35%
                    color = COLOR_RIESGO_MEDIO
                    etiqueta = "🟡 MEDIO"
                else:
                    color = COLOR_RIESGO_BAJO
                    etiqueta = "🟢 BAJO"
                
                st.success("✅ Reserva visualizada")
                
                col1, col2 = st.columns([2, 1])
                
                with col1:
                    # Usar keys correctas ('nombre' vs 'cliente_nombre')
                    r_id = reserva.get('id', reserva.get('id_reserva', ''))
                    r_nombre = reserva.get('nombre', reserva.get('cliente_nombre', ''))
                    r_email = reserva.get('email', reserva.get('cliente_email', ''))
                    r_hotel = reserva.get('hotel', '')
                    r_hab = reserva.get('habitacion', '')
                    r_llegada = reserva.get('llegada', '')
                    r_noches = reserva.get('noches', 0)
                    r_adultos = reserva.get('adultos', reserva.get('pax', 0)) # Fallback a pax si no hay desglose
                    r_ninos = reserva.get('ninos', 0)
                    r_valor = reserva.get('valor', reserva.get('valor_total', 0))
                    
                    st.markdown(f"""
                    <div style="background: {COLOR_CREMA}; padding: 1.5rem; border-radius: 8px;">
                        <h3 style="margin: 0 0 1rem 0; color: {COLOR_VERDE_OSCURO};">Código: {r_id}</h3>
                        <p><strong>Cliente:</strong> {r_nombre} ({r_email})</p>
                        <p><strong>Hotel:</strong> {r_hotel} - {r_hab}</p>
                        <p><strong>Llegada:</strong> {r_llegada} · {r_noches} noches</p>
                        <p><strong>Huéspedes:</strong> {r_adultos} adultos, {r_ninos} niños</p>
                        <p style="font-size: 1.2rem; margin-top: 1rem;"><strong>Valor: {r_valor:,.0f}€</strong></p>
                    </div>
                    """, unsafe_allow_html=True)
                
                with col2:
                    st.markdown(f"""
                    <div style="background: {color}; color: white; padding: 1.5rem; border-radius: 8px; text-align: center;">
                        <p style="margin: 0; font-size: 0.9rem;">Prob. Cancelación</p>
                        <p style="margin: 0.5rem 0; font-size: 3rem; font-weight: 300;">{prob_pct:.2f}%</p>
                        <p style="margin: 0;">{etiqueta}</p>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    if reserva.get('oferta_retencion'):
                        st.markdown(f"""
                        <div style="background: {COLOR_DORADO}; color: white; padding: 0.8rem; border-radius: 6px; margin-top: 1rem; text-align: center;">
                            <p style="margin: 0; font-size: 0.75rem;">Oferta enviada</p>
                            <p style="margin: 0; font-weight: 600;">{reserva.get('oferta_retencion')}</p>
                        </div>
                        """, unsafe_allow_html=True)

                # --- SHAP (Waterfall) ---
                shap_png = st.session_state.get("shap_waterfall_png")
                shap_source_id = st.session_state.get("last_shap_source_id", "")
                if shap_png and shap_source_id == f"search:{str(r_id)}":
                    st.markdown("### Explicabilidad (Waterfall SHAP)")
                    st.image(
                        shap_png,
                        caption="Waterfall SHAP (reserva encontrada)",
                        width="stretch",
                    )
                elif search_shap_error:
                    st.warning(search_shap_error)
                else:
                    st.info("No se pudo generar Waterfall SHAP para esta reserva.")

                # --- ACCIONES DE RETENCIÓN ---
                st.markdown("---")
                col_actions, col_debug = st.columns([1, 1])
                
                with col_actions:
                    ofertas = _catalogo_ofertas_retencion(prob_val)
                    if ofertas:
                        if st.button("GENERAR OFERTA RETENCIÓN", key=f"btn_ret_{r_id}", width="stretch"):
                            st.session_state[f"show_offer_{r_id}"] = True

                        if st.session_state.get(f"show_offer_{r_id}", False):
                            riesgo_txt = "ALTO" if prob_val >= 0.60 else "MEDIO"
                            st.markdown(f"**Nivel de riesgo detectado:** {riesgo_txt}")

                            ids_oferta = [o["id"] for o in ofertas]
                            map_ofertas = {o["id"]: f"{o['nombre']} · {o['propuesta']}" for o in ofertas}

                            oferta_id = st.radio(
                                "Selecciona una oferta a enviar",
                                ids_oferta,
                                format_func=lambda x: map_ofertas[x],
                                key=f"choice_offer_{r_id}",
                            )
                            oferta_sel = next((o for o in ofertas if o["id"] == oferta_id), ofertas[0])

                            st.info(
                                f"**Coste estimado:** {oferta_sel['coste']}  \n"
                                f"**Retorno esperado:** {oferta_sel['retorno']}"
                            )

                            if st.button("ENVIAR OFERTA AL CLIENTE", key=f"send_ret_{r_id}", width="stretch"):
                                oferta_texto = f"{oferta_sel['nombre']} - {oferta_sel['propuesta']}"
                                fecha_envio = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                                reserva["oferta_retencion"] = oferta_texto
                                reserva["fecha_envio_oferta"] = fecha_envio
                                reserva["estado_oferta"] = "enviada"
                                st.session_state.intranet_search_result = reserva

                                updates_oferta = {
                                    "oferta_retencion": oferta_texto,
                                    "fecha_envio_oferta": fecha_envio,
                                    "estado_oferta": "enviada",
                                }
                                _persistir_campos_oferta_reserva(str(r_id), updates_oferta)
                                if actualizar_reserva_csv is not None:
                                    try:
                                        for k, v in updates_oferta.items():
                                            actualizar_reserva_csv(str(r_id), k, v)
                                    except Exception:
                                        pass

                                email_clean = str(r_email or "").strip()
                                if (
                                    email_clean
                                    and "@" in email_clean
                                    and email_clean.lower() not in {"nan", "none", "cliente@example.com"}
                                ):
                                    st.success(
                                        f"Oferta enviada a {email_clean}. Se enviará por correo la propuesta seleccionada."
                                    )
                                else:
                                    st.success("Oferta enviada al cliente.")
                    else:
                        st.success("Riesgo bajo detectado: no se recomienda aplicar oferta de retención.")
                
                # --- DEBUG / EXPORT ---
                with col_debug:
                   raw_data = reserva.get('raw_data')
                   if raw_data:
                       with st.expander("Ver código para Notebook", expanded=False):
                           # Preparar valores para el snippet
                           # Fechas
                           llegada_val = raw_data.get('LLEGADA', '2026-01-01')
                           if hasattr(llegada_val, 'strftime'): 
                                llegada_str = llegada_val.strftime('%Y-%m-%d')
                           else:
                                llegada_str = str(llegada_val).split(' ')[0] # Fallback string

                           creacion_val = raw_data.get('FECHA_CREACION', datetime.datetime.now())
                           if hasattr(creacion_val, 'strftime'):
                               toma_str = creacion_val.strftime('%Y-%m-%d %H:%M:%S')
                           else:
                               toma_str = str(creacion_val)

                           # Mapeos
                           cliente_val = raw_data.get('AGENCIA', 'Cliente Directo')
                           if cliente_val == 'Cliente Directo': cliente_val = 'ROIBACK (GLOBAL OBI S.L.)' # Aproximación para modelo
                           
                           pais_val = raw_data.get('MERCADO', 'ESPAÑA')
                           
                           # Segmento y Fidelidad (Futuro: guardar en CSV. Presente: Heurística App)
                           # En App: SEGMENTO = "Loyalty" if fidelidad else "BAR"
                           fidelidad_val = raw_data.get('FIDELIDAD', None)
                           if pd.isna(fidelidad_val) or fidelidad_val == 'nan' or fidelidad_val == 'None' or fidelidad_val == 'Sin programa':
                               fidelidad_val = None
                           
                           segmento_csv = raw_data.get('SEGMENTO')
                           if segmento_csv and pd.notna(segmento_csv):
                               segmento_val = segmento_csv
                           else:
                               # Replicar lógica de app.py paso 4
                               segmento_val = "Loyalty" if fidelidad_val else "BAR"
                           
                           fuente_val = raw_data.get('CANAL', 'DIRECT SALES')
                           if fuente_val == 'WEBPROPIA': fuente_val = 'DIRECT SALES'
                           
                           hotel_val = raw_data.get('COMPLEJO_REAL', 'Complejo Punta Cana')
                           hab_val = raw_data.get('NOMBRE_HABITACION', 'Standard')
                           valor_val = raw_data.get('VALOR_RESERVA', 0.0)
                           try:
                                valor_float = float(valor_val)
                           except:
                                valor_float = 0.0

                           pax_val = int(raw_data.get('PAX', 2))
                           noches_val = int(raw_data.get('NOCHES', 1))

                           # Fidelidad string formatted
                           fidelidad_str = f"'{fidelidad_val}'" if fidelidad_val else "None"

                           # Construir string de código
                           code_snippet = f"""# Código para verificar en el notebook (mismos valores que en la app)
resultado = get_prediccion(
    LLEGADA='{llegada_str}',
    NOCHES={noches_val},
    PAX={pax_val},
    ADULTOS={pax_val},
    CLIENTE='{cliente_val}',
    FECHA_TOMA='{toma_str}',
    FIDELIDAD={fidelidad_str},
    PAIS='{pais_val}',
    SEGMENTO='{segmento_val}',
    FUENTE_NEGOCIO='{fuente_val}',
    NOMBRE_HOTEL='{hotel_val}',
    NOMBRE_HABITACION='{hab_val}',
    VALOR_RESERVA={valor_float}
)
print(f"Probabilidad de cancelación: {{resultado:.2%}}")"""
                           
                           st.code(code_snippet, language='python')
    
    # =========================================================================
    # TAB 5: SOBRE NOSOTROS
    # =========================================================================
    if selected_tab == "SOBRE NOSOTROS":
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        miembros_dir = os.path.join(base_dir, "media", "Miembros")

        def _render_persona(nombre, enlace, img_file, rol=None):
            col_img, col_txt = st.columns([1, 6])
            with col_img:
                img_path = os.path.join(miembros_dir, img_file)
                img_b64 = _to_base64_image_thumbnail(img_path, size=88)
                if img_b64:
                    st.markdown(
                        f"<img src='data:image/png;base64,{img_b64}' "
                        "style='width:88px;height:88px;object-fit:cover;border-radius:50%;'/>",
                        unsafe_allow_html=True,
                    )
            with col_txt:
                if rol:
                    st.caption(rol)
                st.markdown(f"[{nombre}]({enlace})")

        st.subheader("Máster en Data Science")
        st.markdown("**Profesor Tutor:**")
        _render_persona(
            "Jean Gustavo Cueva",
            "https://www.linkedin.com/in/jean-gustavo-cueva-cajas-69074b90/",
            "Gustavo.png",
            rol="Profesor Tutor",
        )

        st.markdown("---")
        st.markdown("**Integrantes del Equipo:**")
        _render_persona("Alejandro Batres", "https://www.linkedin.com/in/alejandro-batres-3595909b/", "Alex.png")
        _render_persona("David Ostiz", "https://www.linkedin.com/in/david-ostiz/", "David.png")
        _render_persona("Francisco Javier Barrionuevo", "https://www.linkedin.com/in/franciscobarrionuevo/", "Francisco.png")
        _render_persona("Gabriel Chamorro", "https://www.linkedin.com/in/gabriel-chamorro-s%C3%A1nchez-aa4347a2/", "Gabi.png")
        _render_persona("Jose Javier Martínez", "https://www.linkedin.com/in/josejmartinezc/", "Jose.png")

        st.markdown("---")
        st.markdown("**Sistema de Predicción de Cancelaciones**  \n*Palladium Hotel Group - 2026*")
    
    # -------------------------------------------------------------------------
    # PIE DE PAGINA
    # -------------------------------------------------------------------------
    st.markdown("---")
    pie_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), 
        "media", "general", "Pie.png"
    )
    if os.path.exists(pie_path):
        st.image(pie_path, width="stretch")


# -----------------------------------------------------------------------------
# PUNTO DE ENTRADA
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    main()

# -----------------------------------------------------------------------------
# LOGICA AGENTE INTRANET
# -----------------------------------------------------------------------------
def ejecutar_acciones_intranet(accion_container, acciones):
    """
    Ejecuta acciones especificas de la intranet.
    """
    def _mes_a_num(mes):
        if mes is None:
            return None
        if isinstance(mes, (int, float)) and not pd.isna(mes):
            m = int(mes)
            return m if 1 <= m <= 12 else None
        s = str(mes).strip().lower()
        s = (
            s.replace("á", "a")
            .replace("é", "e")
            .replace("í", "i")
            .replace("ó", "o")
            .replace("ú", "u")
        )
        if s.isdigit():
            m = int(s)
            return m if 1 <= m <= 12 else None
        meses = {
            "enero": 1,
            "febrero": 2,
            "marzo": 3,
            "abril": 4,
            "mayo": 5,
            "junio": 6,
            "julio": 7,
            "agosto": 8,
            "septiembre": 9,
            "setiembre": 9,
            "octubre": 10,
            "noviembre": 11,
            "diciembre": 12,
        }
        return meses.get(s)

    def _to_float(x):
        try:
            if x is None or (isinstance(x, float) and np.isnan(x)):
                return None
            return float(x)
        except Exception:
            return None

    def _norm_proba(x):
        v = _to_float(x)
        if v is None:
            return None
        # Acepta 0-1 o 0-100
        if v > 1.0:
            v = v / 100.0
        return max(0.0, min(1.0, v))

    def _df_to_md(df, cols):
        # Tabla markdown simple (sin dependencia tabulate)
        rows = []
        header = "| " + " | ".join(cols) + " |"
        sep = "| " + " | ".join(["---"] * len(cols)) + " |"
        rows.append(header)
        rows.append(sep)
        for tup in df[cols].itertuples(index=False, name=None):
            rows.append("| " + " | ".join(str(x) for x in tup) + " |")
        return "\n".join(rows)

    for accion in acciones:
        funcion = accion.get("funcion")
        params = accion.get("parametros", {})
        
        try:
            if funcion == "consultar_ocupacion":
                mes = params.get("mes", "junio")
                anio = params.get("anio", 2026)
                tipo_dato = str(params.get("tipo_dato", "ambos")).strip().lower()

                m = _mes_a_num(mes)
                try:
                    y = int(anio)
                except Exception:
                    y = 2026

                df_maestro = st.session_state.get("df_maestro", pd.DataFrame())
                if df_maestro.empty:
                    info = "No hay datos de reservas disponibles para calcular ocupación."
                else:
                    df_tmp = df_maestro.copy()
                    df_tmp["LLEGADA"] = pd.to_datetime(df_tmp.get("LLEGADA"), errors="coerce")
                    if m:
                        df_tmp = df_tmp[df_tmp["LLEGADA"].dt.month == m]
                    df_tmp = df_tmp[df_tmp["LLEGADA"].dt.year == y]

                    revenue = pd.to_numeric(df_tmp.get("VALOR_RESERVA"), errors="coerce").fillna(0).sum()

                    # Ocupación y overbooking: usando el cálculo vectorizado ya presente.
                    try:
                        df_occ = get_occupation_metrics(df_maestro)
                        df_occ = df_occ.copy()
                        df_occ["Fecha"] = pd.to_datetime(df_occ.get("Fecha"), errors="coerce")
                        if m:
                            df_occ = df_occ[(df_occ["Fecha"].dt.month == m) & (df_occ["Fecha"].dt.year == y)]
                        else:
                            df_occ = df_occ[df_occ["Fecha"].dt.year == y]
                        df_tot = df_occ[df_occ["Hotel"].astype(str).str.startswith("TOTAL")].copy()
                        ocup_media = float(df_tot["Pct_Ocupacion_Bruta"].mean()) if not df_tot.empty else float("nan")
                        dias_over = int((df_tot["Ocupadas_Brutas"] > df_tot["Capacidad"]).sum()) if not df_tot.empty else 0
                    except Exception:
                        ocup_media = float("nan")
                        dias_over = 0

                    mes_txt = str(mes).capitalize()
                    bloques = [f"📊 **Ocupación {mes_txt} {y}**"]
                    if tipo_dato in {"ocupacion", "ambos"}:
                        if np.isnan(ocup_media):
                            bloques.append("- Ocupación media: N/D")
                        else:
                            bloques.append(f"- Ocupación media (bruta): {ocup_media:.1f}%")
                    if tipo_dato in {"overbooking", "ambos"}:
                        bloques.append(f"- Días con overbooking detectado: {dias_over}")
                    bloques.append(f"- Revenue estimado (reservas con llegada en el mes): {revenue:,.0f}€")
                    info = "\n".join(bloques)
                
                st.session_state.chat_history_intranet.append({
                    "role": "assistant",
                    "content": info
                })
                # Forzar rerun para mostrar
                st.rerun()

            elif funcion == "analizar_cancelaciones":
                mes = params.get("mes", "junio")
                top = params.get("top", 5)
                modo = str(params.get("modo", "mayor_riesgo")).strip().lower()
                anio = params.get("anio", 2026)

                m = _mes_a_num(mes)
                try:
                    y = int(anio)
                except Exception:
                    y = 2026
                try:
                    top_n = int(top)
                except Exception:
                    top_n = 5
                top_n = max(1, min(20, top_n))

                df_maestro = st.session_state.get("df_maestro", pd.DataFrame())
                if df_maestro.empty:
                    info = "No hay datos de reservas disponibles para analizar cancelaciones."
                else:
                    df_tmp = df_maestro.copy()
                    df_tmp["LLEGADA"] = pd.to_datetime(df_tmp.get("LLEGADA"), errors="coerce")
                    if m:
                        df_tmp = df_tmp[df_tmp["LLEGADA"].dt.month == m]
                    df_tmp = df_tmp[df_tmp["LLEGADA"].dt.year == y]

                    df_tmp["PROBABILIDAD_CANCELACION"] = pd.to_numeric(
                        df_tmp.get("PROBABILIDAD_CANCELACION"), errors="coerce"
                    )
                    df_tmp = df_tmp.dropna(subset=["PROBABILIDAD_CANCELACION"])

                    if df_tmp.empty:
                        info = f"No hay reservas con probabilidad disponible para {str(mes).capitalize()} {y}."
                    else:
                        avg_prob = float(df_tmp["PROBABILIDAD_CANCELACION"].mean())
                        high_risk = int((df_tmp["PROBABILIDAD_CANCELACION"] >= 0.6).sum())

                        asc = modo in {"menor_riesgo", "menos_riesgo", "asc", "ascendente"}
                        df_top = df_tmp.sort_values("PROBABILIDAD_CANCELACION", ascending=asc).head(top_n).copy()
                        df_top["LLEGADA"] = df_top["LLEGADA"].dt.strftime("%d-%m-%Y")
                        df_top["RIESGO"] = df_top["PROBABILIDAD_CANCELACION"].apply(lambda v: f"{v:.1%}")

                        cols = ["ID_RESERVA", "NOMBRE_HOTEL_REAL", "LLEGADA", "RIESGO"]
                        # Columnas pueden no existir según el maestro; filtramos.
                        cols = [c for c in cols if c in df_top.columns]
                        tabla = _df_to_md(df_top, cols) if cols else ""

                        mes_txt = str(mes).capitalize()
                        info = (
                            f"⚠️ **Análisis de Riesgo ({mes_txt} {y})**\n\n"
                            f"- Probabilidad media de cancelación: {avg_prob:.1%}\n"
                            f"- Reservas en riesgo alto (≥60%): {high_risk}\n\n"
                            f"**Top {top_n} ({'menor' if asc else 'mayor'} riesgo):**\n\n"
                            f"{tabla}"
                        )
                
                st.session_state.chat_history_intranet.append({
                    "role": "assistant",
                    "content": info
                })
                st.rerun()

            elif funcion == "resumen_general":
                df_maestro = st.session_state.get("df_maestro", pd.DataFrame())
                if df_maestro.empty:
                    info = "No hay datos de reservas disponibles para el resumen."
                else:
                    df_tmp = df_maestro.copy()
                    df_tmp["LLEGADA"] = pd.to_datetime(df_tmp.get("LLEGADA"), errors="coerce")
                    revenue = pd.to_numeric(df_tmp.get("VALOR_RESERVA"), errors="coerce").fillna(0).sum()
                    pax = pd.to_numeric(df_tmp.get("PAX"), errors="coerce").fillna(0).sum()
                    avg_prob = pd.to_numeric(df_tmp.get("PROBABILIDAD_CANCELACION"), errors="coerce").mean()
                    high_risk = pd.to_numeric(df_tmp.get("PROBABILIDAD_CANCELACION"), errors="coerce").ge(0.6).sum()

                    info = (
                        "📈 **Resumen General 2026**\n\n"
                        f"- Reservas registradas: {len(df_tmp):,}\n"
                        f"- Revenue total (estimado): {revenue:,.0f}€\n"
                        f"- PAX total: {int(pax):,}\n"
                        f"- Riesgo medio de cancelación: {avg_prob:.1%}\n"
                        f"- Reservas en riesgo alto (≥60%): {int(high_risk):,}"
                    )
                
                st.session_state.chat_history_intranet.append({
                    "role": "assistant",
                    "content": info
                })
                st.rerun()

            elif funcion == "consultar_reserva_especifica":
                id_res = params.get("id_reserva")
                
                # 1. Buscar en session_state (Reservas Activas de esta sesión)
                reservas_activas = st.session_state.get('reservations', [])
                encontrada = None
                source = "session"
                
                # Normalizar búsqueda (str)
                for r in reservas_activas:
                    if str(r.get('id', '')) == str(id_res):
                        encontrada = r
                        break
                
                # 2. Si no esta en activas, buscar en histórico (df_maestro)
                if not encontrada and 'df_maestro' in st.session_state:
                    df = st.session_state['df_maestro']
                    # Convertir ID a string para comparar
                    # Ojo: df['ID_RESERVA'] puede ser int o str
                    mask = df['ID_RESERVA'].astype(str) == str(id_res)
                    if mask.any():
                        row = df[mask].iloc[0]
                        # Mapear a formato común
                        prob_val = row.get('PROBABILIDAD_CANCELACION', 0)
                        encontrada = {
                            'nombre': row.get('CLIENTE', 'Cliente Histórico'), # Puede no estar en maestro
                            'hotel': row.get('NOMBRE_HOTEL_REAL', 'Desconocido'),
                            'llegada': row.get('LLEGADA', ''),
                            'cancel_prob': prob_val,
                            'estado': row.get('estado', 'Confirmada')
                        }
                        source = "history"

                if encontrada:
                    prob = encontrada.get('cancel_prob', 0)
                    riesgo_txt = "BAJO"
                    if prob > 0.6: riesgo_txt = "ALTO"
                    elif prob > 0.35: riesgo_txt = "MEDIO"
                    
                    # Formatear fecha si es timestamp
                    llegada_val = encontrada.get('llegada')
                    if hasattr(llegada_val, 'strftime'):
                        llegada_txt = llegada_val.strftime('%d-%m-%Y')
                    else:
                        llegada_txt = str(llegada_val)

                    # Formatear mensaje bonito
                    info = f"""✅ **Reserva Encontrada ({'Histórica' if source == 'history' else 'Nueva'}): {id_res}**

- **Cliente:** {encontrada.get('nombre')}
- **Hotel:** {encontrada.get('hotel')}
- **Llegada:** {llegada_txt}
- **Riesgo Cancelación:** {prob:.1%} ({riesgo_txt})
- **Estado:** {encontrada.get('estado', 'Confirmada')}
"""
                else:
                    info = f"❌ No he encontrado ninguna reserva con el ID **{id_res}** ni en las activas ni en el histórico."
                
                st.session_state.chat_history_intranet.append({
                    "role": "assistant",
                    "content": info
                })
                st.rerun()

            elif funcion == "consultar_reservas":
                filtros = params.get("filtros", {}) or {}
                ordenar_por = params.get("ordenar_por", "LLEGADA")
                orden = str(params.get("orden", "asc")).strip().lower()
                limite = params.get("limite", 10)

                try:
                    limit_n = int(limite)
                except Exception:
                    limit_n = 10
                limit_n = max(1, min(50, limit_n))

                df_maestro = st.session_state.get("df_maestro", pd.DataFrame())
                if df_maestro.empty:
                    info = "No hay datos de reservas disponibles."
                else:
                    df_tmp = df_maestro.copy()
                    df_tmp["LLEGADA"] = pd.to_datetime(df_tmp.get("LLEGADA"), errors="coerce")

                    # Filtros soportados
                    hotel = filtros.get("hotel")
                    complejo = filtros.get("complejo")
                    estado = filtros.get("estado")
                    mes_lleg = filtros.get("mes_llegada")
                    anio_lleg = filtros.get("anio_llegada")
                    riesgo_min = _norm_proba(filtros.get("riesgo_min"))
                    riesgo_max = _norm_proba(filtros.get("riesgo_max"))
                    valor_min = _to_float(filtros.get("valor_min"))
                    valor_max = _to_float(filtros.get("valor_max"))

                    if hotel and "NOMBRE_HOTEL_REAL" in df_tmp.columns:
                        df_tmp = df_tmp[df_tmp["NOMBRE_HOTEL_REAL"].astype(str).str.contains(str(hotel), case=False, na=False)]
                    if complejo and "COMPLEJO_REAL" in df_tmp.columns:
                        df_tmp = df_tmp[df_tmp["COMPLEJO_REAL"].astype(str).str.contains(str(complejo), case=False, na=False)]
                    if estado:
                        col_estado = "estado" if "estado" in df_tmp.columns else ("ESTADO" if "ESTADO" in df_tmp.columns else None)
                        if col_estado:
                            df_tmp = df_tmp[df_tmp[col_estado].astype(str).str.contains(str(estado), case=False, na=False)]
                    m = _mes_a_num(mes_lleg)
                    if m:
                        df_tmp = df_tmp[df_tmp["LLEGADA"].dt.month == m]
                    if anio_lleg:
                        try:
                            y = int(anio_lleg)
                            df_tmp = df_tmp[df_tmp["LLEGADA"].dt.year == y]
                        except Exception:
                            pass

                    if "PROBABILIDAD_CANCELACION" in df_tmp.columns:
                        df_tmp["PROBABILIDAD_CANCELACION"] = pd.to_numeric(
                            df_tmp["PROBABILIDAD_CANCELACION"], errors="coerce"
                        )
                        if riesgo_min is not None:
                            df_tmp = df_tmp[df_tmp["PROBABILIDAD_CANCELACION"] >= riesgo_min]
                        if riesgo_max is not None:
                            df_tmp = df_tmp[df_tmp["PROBABILIDAD_CANCELACION"] <= riesgo_max]

                    if "VALOR_RESERVA" in df_tmp.columns:
                        df_tmp["VALOR_RESERVA"] = pd.to_numeric(df_tmp["VALOR_RESERVA"], errors="coerce")
                        if valor_min is not None:
                            df_tmp = df_tmp[df_tmp["VALOR_RESERVA"] >= valor_min]
                        if valor_max is not None:
                            df_tmp = df_tmp[df_tmp["VALOR_RESERVA"] <= valor_max]

                    # Orden
                    asc = orden not in {"desc", "descendente", "down"}
                    if ordenar_por in df_tmp.columns:
                        df_tmp = df_tmp.sort_values(ordenar_por, ascending=asc)
                    elif ordenar_por.upper() in df_tmp.columns:
                        df_tmp = df_tmp.sort_values(ordenar_por.upper(), ascending=asc)

                    df_show = df_tmp.head(limit_n).copy()
                    if "LLEGADA" in df_show.columns:
                        df_show["LLEGADA"] = df_show["LLEGADA"].dt.strftime("%d-%m-%Y")
                    if "PROBABILIDAD_CANCELACION" in df_show.columns:
                        df_show["RIESGO"] = df_show["PROBABILIDAD_CANCELACION"].apply(lambda v: f"{v:.1%}" if pd.notna(v) else "—")

                    cols_pref = ["ID_RESERVA", "NOMBRE_HOTEL_REAL", "COMPLEJO_REAL", "LLEGADA", "VALOR_RESERVA", "RIESGO"]
                    cols = [c for c in cols_pref if c in df_show.columns]

                    if df_tmp.empty:
                        info = "No he encontrado reservas con esos filtros."
                    else:
                        tabla = _df_to_md(df_show, cols) if cols else ""
                        info = f"🔎 **Reservas (muestra {min(limit_n, len(df_show))} de {len(df_tmp)})**\n\n{tabla}"

                st.session_state.chat_history_intranet.append({"role": "assistant", "content": info})
                st.rerun()

        except Exception as e:
            print(f"Error accion intranet: {e}")

def render_sidebar_agent():
    """
    Renderiza el agente en la barra lateral con branding de PALLADIUM.
    """
    with st.sidebar:
        # BRANDING PALLADIUM
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        agent_icon_path = os.path.join(base_dir, "media", "general", "agent_icon.png")
        _render_team_collage_sidebar(base_dir)

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
            .palladium-title {
                font-weight: 800;
                font-size: 1.5rem;
                letter-spacing: 1px;
            }
            .palladium-subtitle {
                font-weight: 300;
                font-size: 0.8rem;
                color: #6b6456;
                letter-spacing: 2px;
            }
        </style>
        <div class="palladium-brand">
            <div class="palladium-subtitle">INTELLIGENCE AGENT</div>
        </div>
        """, unsafe_allow_html=True)

        # Inicializar historial
        if "chat_history_intranet" not in st.session_state:
            st.session_state.chat_history_intranet = []
            # Mensaje inicial proactivo
            st.session_state.chat_history_intranet.append({
                "role": "assistant", 
                "content": "Hola. Soy tu asistente de inteligencia de negocio. ¿Quieres ver la previsión de ocupación o analizar riesgos?"
            })
            
        chat_container = st.container()
        
        # Inputs
        user_input = st.chat_input("Consulta a la IA...", key="chat_input_intranet")
        audio_val = st.audio_input("Voz", key="audio_intranet")
        
        prompt = None
        
        # Logica Audio
        if "last_audio_id_intra" not in st.session_state:
            st.session_state.last_audio_id_intra = None
            
        if audio_val:
            curr_id = audio_val.id if hasattr(audio_val, 'id') else audio_val.size
            if curr_id != st.session_state.last_audio_id_intra:
                with st.spinner("Analizando audio..."):
                    bytes_data = audio_val.getvalue()
                    text = agent_v2.stt_groq_whisper(bytes_data)
                    if text:
                        prompt = text
                        st.session_state.last_audio_id_intra = curr_id
        elif user_input:
            prompt = user_input
            
        # Render Chat
        with chat_container:
            for msg in st.session_state.chat_history_intranet:
                avatar = agent_icon_path if msg["role"] == "assistant" and os.path.exists(agent_icon_path) else None
                with st.chat_message(msg["role"], avatar=avatar):
                    st.markdown(msg["content"])
        
        # Procesar Prompt
        if prompt:
            # User msg
            with chat_container:
                with st.chat_message("user"):
                    st.markdown(prompt)
            st.session_state.chat_history_intranet.append({"role": "user", "content": prompt})
            
            # Agent msg
            with st.spinner("Procesando datos..."):
                # Contexto Intranet
                estado = {
                    "modo": "INTRANET",
                    "reservas_activas": len(st.session_state.get('reservations', [])),
                    "usuario": "Manager"
                }
                
                # Historial para LLM
                hist_llm = [{"role": m["role"], "content": m["content"]} for m in st.session_state.chat_history_intranet[-6:]]
                
                respuesta = agent_v2.chat_con_acciones(prompt, hist_llm, estado)
                
                texto = respuesta.get("mensaje", "")
                acciones = respuesta.get("acciones", [])
                
                if texto:
                    with chat_container:
                        avatar = agent_icon_path if os.path.exists(agent_icon_path) else None
                        with st.chat_message("assistant", avatar=avatar):
                            st.markdown(texto)
                            if "audio_bytes" in respuesta:
                                st.audio(respuesta["audio_bytes"], format="audio/mp3", autoplay=True)
                    
                    st.session_state.chat_history_intranet.append({"role": "assistant", "content": texto})
                
                if acciones:
                    ejecutar_acciones_intranet(chat_container, acciones)


# Renderizar Agente (al principio para que vaya al sidebar)
render_sidebar_agent()

# -----------------------------------------------------------------------------
# RENDERIZADO VISUALIZACIONES (Mantiene logica existente)
# -----------------------------------------------------------------------------
