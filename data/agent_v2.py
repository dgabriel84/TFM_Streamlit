# =============================================================================
# AGENTE CONVERSACIONAL V2 - CON ACCIONES (Function Calling)
# =============================================================================
# Agente que puede:
# 1. Responder preguntas sobre hoteles/precios
# 2. Ejecutar acciones (seleccionar destino, hotel, fechas, etc.)
# 3. Mostrar fotos de hoteles
# 4. Completar reservas paso a paso
# =============================================================================

import os
import json
import requests
from datetime import datetime, timedelta
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # Fallback: leer .env manualmente si no existe python-dotenv
    try:
        with open('.env', 'r') as f:
            for line in f:
                if line.strip() and not line.startswith('#') and '=' in line:
                    key, value = line.strip().split('=', 1)
                    os.environ[key] = value
    except Exception:
        pass
# Importar información turística
from tourist_info import TOURIST_INFO

# API Key de Groq (configurar como variable de entorno)
GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")
GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"
MODEL = "llama-3.1-8b-instant"

# API Key de Google Gemini

# =============================================================================
# FUNCIONES DISPONIBLES PARA EL AGENTE
# =============================================================================

FUNCIONES_CLIENTE = """
FUNCIONES QUE PUEDES EJECUTAR (CLIENTE):

1) seleccionar_destino(pais) - Selecciona México o República Dominicana.
2) seleccionar_complejo(complejo) - Selecciona: Costa Mujeres, Riviera Maya, Punta Cana.
3) seleccionar_hotel(hotel) - Selecciona un hotel específico.
4) configurar_fechas(llegada, noches) - Establece fechas (formato: YYYY-MM-DD).
5) configurar_huespedes(adultos, ninos) - Número de huéspedes.
6) seleccionar_habitacion(habitacion) - Nombre de la habitación.
7) confirmar_reserva() - Finaliza la reserva (paso final).
8) mostrar_info_hotel(hotel) - Muestra foto e info del hotel.
9) marcar_fidelidad(es_fidelizado) - Marca si es cliente Rewards (true/false).
10) registrar_datos_cliente(nombre, email, pais, telefono) - Guarda datos personales.
11) recomendar_turismo(zona) - Recomendaciones turísticas. Zona: "Costa Mujeres", "Riviera Maya", "Punta Cana".
"""

FUNCIONES_INTRANET = """
FUNCIONES QUE PUEDES EJECUTAR (INTRANET):

1) consultar_ocupacion(mes, anio, tipo_dato) - Ocupación/overbooking/revenue del mes.
2) analizar_cancelaciones(mes, anio, top, modo) - Rankings y medias de riesgo.
3) resumen_general() - Resumen ejecutivo global.
4) consultar_reserva_especifica(id_reserva) - Detalle de una reserva por ID.
5) consultar_reservas(filtros, ordenar_por, orden, limite) - Listado con filtros/ordenación.
"""

# =============================================================================
# INFORMACIÓN DE CONTEXTO
# =============================================================================

HOTELES_INFO = """
HOTELES DISPONIBLES:

MÉXICO - COSTA MUJERES (Cancún):
- Grand Palladium Select Costa Mujeres:
  * Habitaciones: 
    - Junior Suite Garden View
    - Junior Suite Poolside
    - Junior Suite Poolside Ocean View
    - Loft Suite Jacuzzi Terrace
    - Family Suite
- TRS Coral Hotel (Solo Adultos, Lujo):
  * Habitaciones: 
    - TRS Junior Suite Garden View
    - TRS Junior Suite Poolside (Acceso directo piscina) (No tiene Ocean View)

MÉXICO - RIVIERA MAYA:
- Grand Palladium Colonial/Kantenah/White Sand:
  * Habitaciones: 
    - Colonial Junior Suite Garden View
    - Kantenah Junior Suite Garden View
- TRS Yucatan Hotel (Solo Adultos):
  * Habitaciones: TRS Junior Suite Garden View

REPÚBLICA DOMINICANA - PUNTA CANA:
- Grand Palladium Bavaro/Palace/Punta Cana:
  * Habitaciones: 
    - Palace Junior Suite Garden View
    - Palace Junior Suite Beachside
    - Palace Deluxe Garden View
    - Palace Deluxe Beachside
    - Bavaro Junior Suite Garden View
- TRS Turquesa Hotel (Solo Adultos):
  * Habitaciones: TRS Junior Suite Garden/Pool View

⚠️ REGLAS ESTRICTAS DE COHERENCIA:
1.  **NO INVENTES HABITACIONES**. Solo ofrece las que están en la lista de arriba para CADA hotel.
2.  Si el usuario pide algo que no tiene el hotel (ej: "Vistas al mar" en TRS Coral), DILE QUE NO EXISTE y ofrece las alternativas disponibles de ESE hotel. **NO CAMBIES DE HOTEL** automáticamente.
3.  **Mantén el hotel seleccionado**. Si el usuario eligió TRS, solo ofrece habitaciones TRS.
4.  **Precios**: No des precios numéricos.
"""

# =============================================================================
# PROMPT DEL SISTEMA
# =============================================================================

SYSTEM_PROMPT = """Eres Sophie, la Asistente Virtual experta de Palladium Hotel Group.
Tu personalidad es profesional, cálida, eficiente y experta en turismo de lujo.

TUS OBJETIVOS:
1.  **Vender la experiencia Palladium**.
2.  **Gestionar reservas**: Guía al cliente paso a paso.
3.  **Capturar datos**: En cuanto te den Nombre, email o país, EJECUTA `registrar_datos_cliente`.

REGLAS DE ORO (IMPORTANTE):
-   **BREVEDAD NATURAL**: Tus respuestas deben ser breves pero naturales (aprox 40 palabras). No seas robótica.
-   **HABLADO, NO LEÍDO**: Usa lenguaje natural y directo. No leas listas.
-   **AÑO POR DEFECTO (2026)**: Si el cliente dice fechas sin año (ej: "del 1 al 8 de junio"), ASUME 2026 y conviértelo a `YYYY-MM-DD` (ej: `2026-06-01`).
-   **FLUJO TÉCNICO (NUEVO)**:
	    -   **PASO 1 (BUSCADOR)**: Si el cliente quiere reservar, PRIMERO necesitas: Destino, Fechas y Personas. Pídelo todo junto si puedes.
	    -   **PASO 2 (HOTEL)**: Una vez tengas lo anterior, muestra disponibilidad de hoteles (`seleccionar_destino` con los datos).
	    - **PASO 3 (HABITACIÓN)**: Ofrece SOLO las habitaciones del hotel seleccionado. Si pide una característica no disponible (ej: vistas mar en un hotel de jardín), explícalo, pero NO cambies de hotel sin preguntar. Usa `seleccionar_habitacion` con el nombre EXACTO de la lista.
	    - **PASO 4 (DATOS)**: CRÍTICO. Si el usuario te da su nombre, email, teléfono o país, **DEBES** ejecutar `registrar_datos_cliente`.
	    - **Validación Email**: Acepta cualquier formato "usuario@dominio.com". No seas pedante con los dominios.
	    - **AUTOMATIZA SI YA TIENES TODO**: Si el usuario ya te dio destino/complejo, hotel, fechas, adultos/niños, habitación y datos personales, ejecuta en ESTE turno todas las acciones necesarias (incluye `seleccionar_destino`, `seleccionar_complejo`, `seleccionar_hotel`, `configurar_fechas`, `configurar_huespedes`, `seleccionar_habitacion`, `registrar_datos_cliente`). Si además indica que es Rewards, ejecuta `marcar_fidelidad(true)`. Finalmente ejecuta `confirmar_reserva()`.

FLUJO DE RESERVA:
1.  **Buscador**: Define Destino, Fechas, Adultos/Niños.
2.  **Hotel**: Elige hotel.
3.  **Habitación**: Elige habitación (COHERENTE con el hotel).
4.  **Confirmación**: Pide los datos. USA `registrar_datos_cliente`.

INFORMACIÓN DE CONTEXTO:
{hoteles_info}

{funciones}

FORMATO DE RESPUESTA JSON OBLIGATORIO:
Debes responder SIEMPRE con un objeto JSON válido.
NO uses bloques de código markdown (```json ... ```).
NO añadidas texto fuera del JSON.

Estructura JSON:
{{
  "mensaje": "Texto que leerá tu voz (natural, aprox 40 palabras).",
  "acciones": [
      {{ "funcion": "nombre_funcion", "parametros": {{ "parametro": "valor" }} }}
  ],
  "imagen": "Nombre exacto del hotel (opcional, solo si es relevante)",
  "turismo": "Nombre de zona turística (opcional)"
}}
"""

# =============================================================================
# PROMPT INTRANET (PALLADIUM INTELLIGENCE)
# =============================================================================

INTRANET_PROMPT = """Eres el Agente de Inteligencia de Negocio de Palladium Hotel Group.
Tu personalidad es analítica, estratégica, concisa y ejecutiva.
Estás hablando con un Manager de la compañía.

TUS OBJETIVOS:
1.  **Analizar Datos**: Ocupación, Ventas, Riesgo de Cancelación.
2.  **Ayudar en la Toma de Decisiones**: Da insights claros y directos.

ACCIONES DISPONIBLES:
- `consultar_ocupacion(mes, anio, tipo_dato)`:
    - `mes` (str: "junio" o "6"), `anio` (int), `tipo_dato` ("ocupacion" | "overbooking" | "ambos").
    - Úsalo para preguntas de ocupación, % habitaciones ocupadas, overbooking o revenue del mes.
- `analizar_cancelaciones(mes, anio, top, modo)`:
    - `mes` (str), `anio` (int), `top` (int, default 5), `modo` ("mayor_riesgo" | "menor_riesgo").
    - Úsalo para rankings de riesgo, medias, top reservas más/menos seguras.
- `consultar_reservas(filtros, ordenar_por, orden, limite)`:
    - `filtros` (dict): `hotel`, `complejo`, `estado`, `mes_llegada`, `anio_llegada`, `riesgo_min`, `riesgo_max`, `valor_min`, `valor_max`.
    - `ordenar_por` (str): por ejemplo `PROBABILIDAD_CANCELACION`, `LLEGADA`, `VALOR_RESERVA`.
    - `orden` (str): "asc" | "desc". `limite` (int): máximo 50.
    - Úsalo cuando te pidan listas con filtros/ordenación (ej: "top 10 por riesgo en Punta Cana").
- `resumen_general()`: Visión global del negocio, revenue y riesgo.
- `consultar_reserva_especifica(id_reserva)`: Buscar una reserva por ID.

FORMATO DE RESPUESTA JSON OBLIGATORIO:
{
  "mensaje": "Texto ejecutivo. SI VAS A EJECUTAR UNA ACCIÓN PARA OBTENER DATOS, NO TE INVENTES EL RESULTADO EN ESTE MENSAJE. Di 'Consultando datos...'.",
  "acciones": [
      { "funcion": "nombre_funcion", "parametros": { "parametro": "valor" } }
  ]
}
"""

# =============================================================================
# FUNCIONES DE UTILIDAD
# =============================================================================

def llamar_groq(mensajes: list, max_tokens: int = 1500) -> str:
    """Llama a la API de Groq con retry automático para rate limits."""
    import time
    
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "model": MODEL,
        "messages": mensajes,
        "max_tokens": max_tokens,
        "temperature": 0.7
    }
    
    if not GROQ_API_KEY:
        return json.dumps({
            "mensaje": "⚠️ **Error de Configuración:** No se ha detectado la API Key de Groq. Por favor, configura la variable de entorno `GROQ_API_KEY` en `.env` o `.streamlit/secrets.toml`.",
            "acciones": []
        })

    # Retry con backoff exponencial para rate limits (429)
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = requests.post(GROQ_API_URL, headers=headers, json=payload, timeout=30)
            
            # Si es rate limit, esperar y reintentar
            if response.status_code == 429:
                wait_time = (2 ** attempt)  # 1s, 2s, 4s
                print(f"[GROQ] Rate limit hit. Esperando {wait_time}s antes de reintentar ({attempt+1}/{max_retries})")
                time.sleep(wait_time)
                continue
                
            response.raise_for_status()
            return response.json()["choices"][0]["message"]["content"]
            
        except requests.exceptions.HTTPError as e:
            if response.status_code == 429 and attempt < max_retries - 1:
                wait_time = (2 ** attempt)
                print(f"[GROQ] Rate limit error. Esperando {wait_time}s...")
                time.sleep(wait_time)
                continue
            return json.dumps({
                "mensaje": f"⏳ La API está ocupada. Espera unos segundos e intenta de nuevo.",
                "acciones": []
            })
        except Exception as e:
            print(f"DEBUG GROQ ERROR: {e}")
            return json.dumps({
                "mensaje": f"Lo siento, hubo un error de conexión. Intenta de nuevo en unos segundos.",
                "acciones": [],
                "error": str(e)
            })
    
    return json.dumps({
        "mensaje": "⏳ Demasiadas peticiones. Espera un momento e intenta de nuevo.",
        "acciones": []
    })




def stt_groq_whisper(audio_bytes):
    """Transcribe audio usando Groq Whisper (stt)."""
    if not GROQ_API_KEY:
        return ""
        
    url = "https://api.groq.com/openai/v1/audio/transcriptions"
    
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}"
    }
    
    # Preparar archivo para envío (multipart/form-data)
    files = {
        "file": ("audio.wav", audio_bytes, "audio/wav"),
        "model": (None, "whisper-large-v3"),
        "language": (None, "es"), # Forzar español
        "response_format": (None, "json")
    }
    
    try:
        response = requests.post(url, headers=headers, files=files, timeout=30)
        response.raise_for_status()
        return response.json().get("text", "")
    except Exception as e:
        print(f"Error transcripción: {e}")
        return ""


def generar_audio_edge(texto):
    """Genera audio MP3 usando Edge TTS (Neural Voice)."""
    try:
        import edge_tts
        import asyncio
        import io
        
        # Voz neuronal española muy natural (Elvira o Alvaro)
        # Velocidad aumentada un 10%
        voice = "es-ES-ElviraNeural"
        rate = "+10%" 
        
        async def _save_audio():
            communicate = edge_tts.Communicate(texto, voice, rate=rate)
            # Guardar en memoria
            mp3_fp = io.BytesIO()
            async for chunk in communicate.stream():
                if chunk["type"] == "audio":
                    mp3_fp.write(chunk["data"])
            mp3_fp.seek(0)
            return mp3_fp

        return asyncio.run(_save_audio())
        
    except ImportError:
        print("DEBUG: edge-tts no instalado")
        return None
    except Exception as e:
        print(f"DEBUG: Error Edge TTS: {e}")
        return None


def parsear_respuesta(respuesta_raw: str) -> dict:
    """Extrae el JSON de la respuesta del modelo usando expresiones regulares."""
    import re
    try:
        # 1. Intentar limpiar bloques de codigo markdown
        texto_limpio = re.sub(r'```json\s*', '', respuesta_raw)
        texto_limpio = re.sub(r'```', '', texto_limpio)
        
        # 2. Buscar el primer objeto JSON valido con regex
        # Busca { ... } incluso con saltos de linea
        # Ajuste para soportar {{ ... }} si el LLM se lía
        match = re.search(r'(\{.*\})', texto_limpio, re.DOTALL)
        
        if match:
            json_str = match.group(1)
            # Limpieza extra por si el LLM mete {{ }}
            if json_str.startswith("{{") and json_str.endswith("}}"):
                json_str = json_str[1:-1]
            return json.loads(json_str)
            
        # 3. Fallback: intentar encontrar indices manuales si el regex falla
        inicio = respuesta_raw.find('{')
        fin = respuesta_raw.rfind('}') + 1
        if inicio != -1 and fin > inicio:
            json_str = respuesta_raw[inicio:fin]
            return json.loads(json_str)
            
    except Exception as e:
        print(f"Error parseando JSON: {e}")
        pass
    
    # Si no es JSON válido, devolver como mensaje simple sin acciones
    return {
        "mensaje": respuesta_raw,
        "acciones": []
    }


def chat_con_acciones(mensaje: str, historial: list = None, estado_reserva: dict = None) -> dict:
    """
    Chat principal con capacidad de ejecutar acciones.
    
    Args:
        mensaje: Mensaje del usuario
        historial: Lista de mensajes anteriores
        estado_reserva: Estado actual de la reserva en curso
    
    Returns:
        dict con:
            - mensaje: Respuesta para mostrar al usuario
            - acciones: Lista de acciones a ejecutar
            - imagen: Hotel del que mostrar foto (opcional)
    """
    if historial is None:
        historial = []
    
    # Añadir contexto del estado actual si existe
    contexto_estado = ""
    modo_actual = "CLIENTE"  # Default
    
    # Extraer acciones ya ejecutadas del historial para evitar repeticiones
    acciones_previas = []
    for msg in historial:
        if msg.get("role") == "assistant":
            content = msg.get("content", "")
            # Buscar si el mensaje menciona acciones ejecutadas
            if "mostrar_info_hotel" in content.lower() or "TRS" in content or "Grand Palladium" in content:
                acciones_previas.append("mostrar_info_hotel")
            if "reserva confirmada" in content.lower() or "tu reserva" in content.lower():
                acciones_previas.append("confirmar_reserva")
    
    if estado_reserva:
        modo_actual = estado_reserva.get("modo", "Reservas")
        if modo_actual == "INTRANET":
            # YA NO SOBREESCRIBIMOS EL MODO A "EMPLEADO/INTRANET" PARA EL PROMPT
            # Simplemente dejamos que el IF de abajo seleccione INTRANET_PROMPT
            contexto_estado = ""
        else:
            modo_actual = "CLIENTE"
            contexto_estado = f"\n\nMODO ACTUAL: {modo_actual}\nEstás ayudando a un cliente a hacer una reserva.\n"
            contexto_estado += "ESTADO ACTUAL DE LA RESERVA:\n"
            for key, value in estado_reserva.items():
                if value and key != "modo":
                    contexto_estado += f"- {key}: {value}\n"
            
            # Añadir acciones ya ejecutadas
            if acciones_previas:
                contexto_estado += f"\n⚠️ ACCIONES YA EJECUTADAS (NO REPETIR): {', '.join(set(acciones_previas))}\n"
                contexto_estado += "Ahora debes AVANZAR: pregunta por fechas, huéspedes, o confirma la reserva.\n"
    
    # Preparar info turística
    info_turistica_str = "\n\nINFORMACIÓN TURÍSTICA Y DEL DESTINO:\n"
    for zona, info in TOURIST_INFO.items():
        info_turistica_str += f"{info}\n"

    system_msg = ""
    if modo_actual == "INTRANET":
        system_msg = INTRANET_PROMPT
    else:
        system_msg = SYSTEM_PROMPT.format(
            hoteles_info=HOTELES_INFO + info_turistica_str + contexto_estado,
            funciones=FUNCIONES_CLIENTE
        )
    
    mensajes = [{"role": "system", "content": system_msg}]
    mensajes.extend(historial)
    mensajes.append({"role": "user", "content": mensaje})
    
    # 1. Obtener respuesta textual de Groq
    respuesta_raw = llamar_groq(mensajes)
    print(f"DEBUG AGENTE RAW: {respuesta_raw}") # Debug
    respuesta_dict = parsear_respuesta(respuesta_raw)
    
    # 2. Generar audio (TTS) para el mensaje de respuesta
    texto_a_leer = respuesta_dict.get("mensaje", "")
    if texto_a_leer:
        print(f"DEBUG: Generando audio para texto: {texto_a_leer[:50]}...")
        # Generamos audio y lo devolvemos como bytes en el diccionario
        audio_stream = generar_audio_edge(texto_a_leer)
        if audio_stream:
            print("DEBUG: Audio generado correctamente")
            respuesta_dict["audio_bytes"] = audio_stream.read()
        else:
            print("DEBUG: Audio stream es None")
            
    return respuesta_dict


# =============================================================================
# MAPEO DE HOTELES A IMÁGENES
# =============================================================================

IMAGENES_HOTELES = {
    "Grand Palladium Select Costa Mujeres": "media/hoteles/costa_mujeres/gp_select_costa_mujeres.jpg",
    "Family Selection Costa Mujeres": "media/hoteles/costa_mujeres/family_selection_costa_mujeres.jpg",
    "TRS Coral Hotel": "media/hoteles/costa_mujeres/trs_coral.jpg",
    "Grand Palladium Colonial Resort & Spa": "media/hoteles/riviera_maya/gp_colonial.jpg",
    "Grand Palladium Kantenah Resort & Spa": "media/hoteles/riviera_maya/gp_kantenah.jpg",
    "Grand Palladium Select White Sand": "media/hoteles/riviera_maya/gp_white_sand.jpg",
    "TRS Yucatan Hotel": "media/hoteles/riviera_maya/trs_yucatan.jpg",
    "Grand Palladium Select Bavaro": "media/hoteles/punta_cana/gp_select_bavaro.jpg",
    "Grand Palladium Palace Resort & Spa": "media/hoteles/punta_cana/gp_palace.jpg",
    "Grand Palladium Punta Cana Resort & Spa": "media/hoteles/punta_cana/gp_punta_cana.jpg",
    "TRS Turquesa Hotel": "media/hoteles/punta_cana/trs_turquesa.jpg",
}

def obtener_imagen_hotel(nombre_hotel: str) -> str:
    """Devuelve la ruta de la imagen del hotel."""
    # Buscar coincidencia parcial
    for hotel, imagen in IMAGENES_HOTELES.items():
        if nombre_hotel.lower() in hotel.lower() or hotel.lower() in nombre_hotel.lower():
            return imagen
    return None


# =============================================================================
# TEST
# =============================================================================

if __name__ == "__main__":
    print("=== Test Agente V2 ===")
    resultado = chat_con_acciones("Quiero reservar en Cancún para la primera semana de junio")
    print(json.dumps(resultado, indent=2, ensure_ascii=False))
