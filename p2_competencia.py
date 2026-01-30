################################################################################
# MÓDULO 2: ANÁLISIS DE COMPETENCIA
#
# DESCRIPCIÓN:
# Para cada local, determina su Código Postal exacto mediante geocodificación inversa
# y realiza un escaneo en OpenStreetMap para detectar negocios competidores
# directos. Utiliza un diccionario (hecho por Gemini) que mapea términos coloquiales 
# a etiquetas técnicas de OSM, asegurando una detección precisa de rivales comerciales.
#
# INPUTS (Parámetros de entrada):
# - df_1: DataFrame con coordenadas y datos básicos (salida del p1).
# - radio: Distancia de búsqueda en metros (500m por default).
# - negocio: Palabra clave del negocio a montar.
#
# OUTPUT (Salida):
# - DataFrame con columnas añadidas:
#   [NUMERO, NOMBRE, DIRECCION, COORDENADAS (lat, lon), PRECIO, LINK, CODIGO_POSTAL,
#   COMPETENCIA (Lista de tuplas 'nombre, cp' de cada local)]
#
# FUENTES (WEBS/APIs):
# - Overpass API (OSM): Base de datos de puntos de interés.
# - ArcGIS: Servicio de geocodificación inversa para obtener Códigos Postales.
#
# LIBRERÍAS CLAVE:
# - Requests
# - Geopy (ArcGIS): Resolución de coordenadas a dirección postal.
################################################################################

import requests  # Realiza peticiones HTTP para conectarse a APIs externas
import time  # Gestiona pausas en la ejecución (sleep) para evitar saturar los servidores y ser bloqueado
import re  # "Expresiones Regulares": permite buscar, extraer y limpiar patrones complejos de texto
from geopy.geocoders import ArcGIS  # Servicio de geolocalización robusto para convertir coordenadas en direcciones (y viceversa)

MAPEO_CATEGORIAS = {
    "pizzería":       [("cuisine", "pizza")],
    "hamburgueseria": [("cuisine", "burger"), ("cuisine", "hamburger")],
    "hamburguesería": [("cuisine", "burger"), ("cuisine", "hamburger")],
    "sushi":          [("cuisine", "sushi"), ("cuisine", "japanese")],
    "japones":        [("cuisine", "japanese")],
    "chino":          [("cuisine", "chinese")],
    "asiatico":       [("cuisine", "asian")],
    "mexicano":       [("cuisine", "mexican"), ("cuisine", "tex-mex")],
    "indio":          [("cuisine", "indian")],
    "italiano":       [("cuisine", "italian")],
    "kebab":          [("cuisine", "kebab")],
    "tapas":          [("cuisine", "tapas")],
    "marisqueria":    [("cuisine", "seafood")],
    "asador":         [("cuisine", "steak_house"), ("cuisine", "bbq")],
    "vegetariano":    [("cuisine", "vegetarian"), ("diet:vegetarian", "yes")],
    "vegano":         [("cuisine", "vegan"), ("diet:vegan", "yes")],
    "pollo":          [("cuisine", "chicken")],
    "bocadilleria":   [("cuisine", "sandwich")],
    "wok":            [("cuisine", "wok")],
    "ramen":          [("cuisine", "ramen")],
    "tacos":          [("cuisine", "tacos")],

    # ------------------- CAFÉ, COPAS Y OCIO -------------------
    "cafeteria":      [("amenity", "cafe")],
    "café":           [("amenity", "cafe")],
    "teteria":        [("shop", "tea")],
    "bar":            [("amenity", "bar"), ("amenity", "pub")],
    "pub":            [("amenity", "pub")],
    "discoteca":      [("amenity", "nightclub")],
    "cerveceria":     [("amenity", "biergarten")],
    "heladeria":      [("amenity", "ice_cream"), ("shop", "ice_cream")],
    "churreria":      [("cuisine", "churro")],
    "shisha":         [("amenity", "hookah_lounge")],

    # ------------------- ALIMENTACIÓN (Retail) -------------------
    "supermercado":   [("shop", "supermarket"), ("shop", "convenience")],
    "hipermercado":   [("shop", "supermarket")],
    "tienda":         [("shop", "convenience")],
    "panaderia":      [("shop", "bakery")],
    "pasteleria":     [("shop", "pastry"), ("shop", "confectionery")],
    "fruteria":       [("shop", "greengrocer")],
    "carniceria":     [("shop", "butcher")],
    "pescaderia":     [("shop", "seafood")],
    "charcuteria":    [("shop", "deli")],
    "herbolario":     [("shop", "herbalist")],
    "congelados":     [("shop", "frozen_food")],
    "vinoteca":       [("shop", "wine")],
    "licoreria":      [("shop", "alcohol")],

    # ------------------- SALUD Y BELLEZA -------------------
    "farmacia":       [("amenity", "pharmacy")],
    "parafarmacia":   [("shop", "chemist")],
    "gimnasio":       [("leisure", "fitness_centre"), ("sport", "fitness")],
    "yoga":           [("leisure", "fitness_centre")],
    "pilates":        [("leisure", "fitness_centre")],
    "crossfit":       [("leisure", "fitness_centre")],
    "peluqueria":     [("shop", "hairdresser")],
    "barberia":       [("shop", "hairdresser")],
    "estetica":       [("shop", "beauty"), ("shop", "cosmetics")],
    "uñas":           [("shop", "beauty")],
    "tatuajes":       [("shop", "tattoo")],
    "dentista":       [("amenity", "dentist"), ("healthcare", "dentist")],
    "clinica":        [("amenity", "clinic")],
    "veterinario":    [("amenity", "veterinary")],
    "optica":         [("shop", "optician")],
    "fisioterapia":   [("healthcare", "physiotherapist")],

    # ------------------- COMERCIO (Tiendas) -------------------
    "ropa":           [("shop", "clothes"), ("shop", "boutique"), ("shop", "fashion")],
    "zapateria":      [("shop", "shoes")],
    "joyeria":        [("shop", "jewelry"), ("shop", "watches")],
    "floristeria":    [("shop", "florist")],
    "ferreteria":     [("shop", "hardware"), ("shop", "doityourself")],
    "bricolaje":      [("shop", "doityourself")],
    "electronica":    [("shop", "electronics"), ("shop", "computer")],
    "moviles":        [("shop", "mobile_phone")],
    "reparacion moviles": [("shop", "mobile_phone")],
    "libreria":       [("shop", "books")],
    "papeleria":      [("shop", "stationery")],
    "estanco":        [("shop", "tobacco")],
    "kiosco":         [("shop", "kiosk"), ("shop", "newsagent")],
    "jugueteria":     [("shop", "toys")],
    "deportes":       [("shop", "sports")],
    "bicicletas":     [("shop", "bicycle")],
    "musica":         [("shop", "musical_instrument")],
    "animales":       [("shop", "pet")],
    "mascotas":       [("shop", "pet")],
    "muebles":        [("shop", "furniture")],
    "decoracion":     [("shop", "interior_decoration")],
    "segunda mano":   [("shop", "second_hand")],
    "fotografia":     [("shop", "photo")],

    # ------------------- SERVICIOS Y OFICINAS -------------------
    "banco":          [("amenity", "bank")],
    "cajero":         [("amenity", "atm")],
    "inmobiliaria":   [("office", "estate_agent")],
    "seguros":        [("office", "insurance")],
    "abogado":        [("office", "lawyer")],
    "gestoria":       [("office", "accountant"), ("office", "tax_advisor")],
    "coworking":      [("office", "coworking")],
    "correos":        [("amenity", "post_office")],
    "mensajeria":     [("office", "courier")],
    "tintoreria":     [("shop", "dry_cleaning")],
    "lavanderia":     [("shop", "laundry")],
    "autoescuela":    [("amenity", "driving_school")],
    "agencia viajes": [("shop", "travel_agency")],

    # ------------------- AUTOMOCIÓN -------------------
    "taller":         [("shop", "car_repair")],
    "mecanico":       [("shop", "car_repair")],
    "gasolinera":     [("amenity", "fuel")],
    "lavado":         [("amenity", "car_wash")],
    "concesionario":  [("shop", "car"), ("shop", "motorcycle")],
    "parking":        [("amenity", "parking")],
    "alquiler coches": [("amenity", "car_rental")],

    # ------------------- EDUCACIÓN Y CULTURA -------------------
    "colegio":        [("amenity", "school")],
    "instituto":      [("amenity", "school")],
    "universidad":    [("amenity", "university")],
    "guarderia":      [("amenity", "kindergarten"), ("amenity", "childcare")],
    "academia":       [("amenity", "language_school"), ("amenity", "music_school"), ("amenity", "prep_school")],
    "biblioteca":     [("amenity", "library")],
    "cine":           [("amenity", "cinema")],
    "teatro":         [("amenity", "theatre")],

    # ------------------- ALOJAMIENTO -------------------
    "hotel":          [("tourism", "hotel")],
    "hostal":         [("tourism", "hostel"), ("tourism", "guest_house")],
    "apartamento":    [("tourism", "apartment")]
}

def obtener_cp_latlon(lat, lon):

    ################################################################################
    # Realiza una geocodificación inversa utilizando el servicio de ArcGIS para
    # obtener la dirección postal correspondiente a un par de coordenadas (latitud,
    # longitud). Busca específicamente un patrón de 5 dígitos en la dirección
    # devuelta para extraer el Código Postal, retornando "00000" si falla.
    #
    # RECIBE:
    # - lat (float): Latitud.
    # - lon (float): Longitud.
    #
    # DEVUELVE:
    # - str: Código Postal detectado o "00000".
    ################################################################################

    try: # Intentamos conectar con el servicio de geocodificación
        geo_service = ArcGIS(timeout=5) # Inicializamos el servicio de ArcGIS con un tiempo de espera de 5 segundos
        location = geo_service.reverse((lat, lon)) # Solicitamos la dirección correspondiente a las coordenadas dadas
        if location and location.address: # Verificamos si hemos recibido una respuesta con dirección válida
            match = re.search(r'\b(\d{5})\b', location.address) # Buscamos un patrón de 5 dígitos consecutivos (formato CP español) usando regex
            if match: return match.group(1) # Si encontramos el patrón, devolvemos los dígitos capturados
    except Exception: pass # Si ocurre cualquier error de conexión o búsqueda, lo ignoramos
    return "00000" # Devolvemos un código postal por defecto si no encontramos nada

def construir_query(lat, lon, radio, negocio):

    ################################################################################
    # Genera una consulta estructurada para la API Overpass de OSM.
    # Combina una búsqueda técnica basada en etiquetas específicas (según el mapeo
    # interno) y una búsqueda de texto libre por nombre para asegurar la máxima
    # cobertura de resultados dentro del radio especificado.
    #
    # RECIBE:
    # - lat, lon (float): Centro de la búsqueda.
    # - radio (int): Distancia en metros.
    # - negocio (str): Término de búsqueda (negocio).
    #
    # DEVUELVE:
    # - str: Query lista para ser enviada a la API.
    ################################################################################

    kw_normalizada = negocio.lower().strip() # Limpiamos y normalizamos la palabra clave a minúsculas
    
    # Parte A: Búsqueda por etiquetas técnicas (Si existe en el mapeo)
    query_tags = "" # Inicializamos la cadena de consulta de etiquetas vacía
    if kw_normalizada in MAPEO_CATEGORIAS: # Comprobamos si la palabra clave existe en nuestro diccionario maestro
        tags = MAPEO_CATEGORIAS[kw_normalizada] # Recuperamos la lista de etiquetas técnicas asociadas
        for k, v in tags: # Iteramos sobre cada par clave-valor de las etiquetas
            query_tags += f'node(around:{radio},{lat},{lon})["{k}"="{v}"];' # Añadimos la búsqueda de nodos con esa etiqueta
            query_tags += f'way(around:{radio},{lat},{lon})["{k}"="{v}"];' # Añadimos la búsqueda de vías/caminos con esa etiqueta
            
    # Parte B: Búsqueda por Nombre (Siempre activa como respaldo)
    # Esto encuentra "Pizzeria Luigi" aunque no tenga la etiqueta cuisine=pizza
    query_name = f""" 
      node(around:{radio},{lat},{lon})[~"name"~"{kw_normalizada}", i];
      way(around:{radio},{lat},{lon})[~"name"~"{kw_normalizada}", i];
    """ # Construimos la consulta de respaldo que busca la palabra clave dentro del nombre del local (insensible a mayúsculas)

    final_query = f"""
    [out:json][timeout:25];
    (
      {query_tags}
      {query_name}
    );
    out center;
    """ # Ensamblamos la query final uniendo las búsquedas por etiquetas y por nombre en un solo bloque
    return final_query # Devolvemos la query completa

def obtener_competencia(lat, lon, radio, negocio, cp_local_principal):

    ################################################################################
    # Ejecuta la petición HTTP a la API de Overpass para obtener los competidores
    # cercanos. Procesa la respuesta JSON, extrae los nombres y códigos postales
    # de los primeros 15 resultados encontrados, y gestiona posibles errores.
    #
    # RECIBE:
    # - lat, lon, radio, negocio: Parámetros de búsqueda.
    # - cp_local_principal: CP de referencia por si el competidor no tiene uno.
    #
    # DEVUELVE:
    # - list: Lista de tuplas [(Nombre, CP), ...] únicas.
    ################################################################################

    try: # Iniciamos el bloque de manejo de errores de red
        url = "https://overpass-api.de/api/interpreter" # Definimos la URL del endpoint de la API Overpass
        query = construir_query(lat, lon, radio, negocio) # Llamamos a la función auxiliar para construir la query
        
        response = requests.get(url, params={'data': query}, timeout=30) # Enviamos la petición GET a la API con un timeout de 30 segundos
        
        if response.status_code == 200: # Verificamos si la petición fue exitosa
            data = response.json() # Parseamos la respuesta a formato JSON
            lista = [] # Inicializamos la lista para guardar competidores
            elementos = data.get("elements", []) # Extraemos la lista de elementos encontrados
            
            for elem in elementos[:15]:  # Iteramos solo sobre los primeros 15 resultados para no saturar
                tags = elem.get("tags", {}) # Obtenemos las etiquetas del elemento
                
                nombre = tags.get("name", "") # Intentamos extraer el nombre del negocio
                if not nombre: # Si no tiene nombre definido
                    # Nombre fallback basado en etiqueta
                    cat = tags.get("amenity") or tags.get("shop") or tags.get("cuisine") or "Local" # Buscamos una categoría genérica para identificarlo
                    nombre = f"({cat})" # Asignamos la categoría entre paréntesis como nombre provisional

                cp = tags.get("addr:postcode", cp_local_principal) # Intentamos obtener el CP del competidor o usamos el del local principal si falta
                lista.append((nombre, cp)) # Añadimos la tupla con los datos a la lista
            
            return list(set(lista)) # Devolvemos la lista eliminando duplicados mediante un set
        
        elif response.status_code == 429: # Si recibimos un error de "Demasiadas peticiones"
            time.sleep(2) # Esperamos 2 segundos antes de continuar
            
    except Exception as e: # Capturamos cualquier otro error de conexión
        print(f"Error conexión OSM: {e}") # Imprimimos el error (permitido excepcionalmente para debug)
        pass # Continuamos la ejecución
        
    return [] # Devolvemos una lista vacía si falló la búsqueda

def busqueda_competencia(df_1, radio=500, negocio=""):

    ################################################################################
    # Analiza la competencia iterando sobre el DataFrame de inmuebles.
    # Para cada fila obtiene el código postal de la zona a partir de coordenadas
    # y busca competidores cercanos. Finalmente, agrega dos nuevas columnas
    # (CODIGO_POSTAL y COMPETENCIA) al DataFrame con los resultados.
    #
    # RECIBE:
    # - df_1 (DataFrame): Datos de entrada del módulo 1.
    # - radio (int): Radio de búsqueda en metros.
    # - negocio (str): Tipo de negocio para buscar rivales.
    #
    # DEVUELVE:
    # - DataFrame: df_1 enriquecido con columnas CODIGO_POSTAL y COMPETENCIA.
    ################################################################################

    print(f"\n--- INICIANDO P2: Análisis de Competencia ('{negocio}') ---")
    
    df_2 = df_1.copy() # Creamos una copia del DataFrame para no modificar el original
    if df_2.empty: return df_2 # Si el DataFrame está vacío, lo devolvemos tal cual

    lista_cps = [] # Inicializamos la lista para guardar los Códigos Postales
    lista_comp = [] # Inicializamos la lista para guardar las listas de competidores
    
    for index, row in df_2.iterrows(): # Recorremos cada fila del DataFrame
        try: # Intentamos procesar cada local
            coords = row['COORDENADAS'] # Extraemos las coordenadas de la fila actual
            if isinstance(coords, str): # Si las coordenadas vienen como texto
                try: coords = eval(coords) # Intentamos convertirlas a tupla evaluando el string
                except: coords = (0,0) # Si falla, asignamos coordenadas nulas
            
            if not isinstance(coords, (tuple, list)) or len(coords) < 2: # Verificamos si el formato de coordenadas es válido
                lista_cps.append("00000"); lista_comp.append([]) # Si no es válido, añadimos valores vacíos
                continue # Saltamos a la siguiente fila
                
            lat, lon = coords[0], coords[1] # Desempaquetamos latitud y longitud
            
            # 1. CP
            cp_zona = obtener_cp_latlon(lat, lon) # Llamamos a la función para obtener el CP de esas coordenadas
            lista_cps.append(cp_zona) # Guardamos el CP encontrado
            
            # 2. Competencia (Solo si hay negocio definido)
            competidores = [] # Inicializamos la lista de competidores local
            if negocio: # Si el usuario definió un negocio a buscar
                competidores = obtener_competencia(lat, lon, radio, negocio, cp_zona) # Buscamos la competencia usando la función auxiliar
            
            lista_comp.append(competidores) # Añadimos la lista de competidores encontrada a la lista general
            print(f"  > Local {row['NUMERO']}: CP {cp_zona} | {len(competidores)} competidores encontrados")
            time.sleep(0.5) # Hacemos una pequeña pausa para no saturar las APIs
            
        except Exception as e: # Capturamos errores en el procesamiento de la fila
            print(f"  Error fila {index}: {e}") # Imprimimos el error específico
            lista_cps.append("00000"); lista_comp.append([]) # Añadimos valores vacíos para mantener la integridad de las columnas

    df_2['CODIGO_POSTAL'] = lista_cps # Asignamos la lista de CPs a una nueva columna
    df_2['COMPETENCIA'] = lista_comp # Asignamos la lista de competidores a una nueva columna
    
    print("Módulo P2 finalizado.")
    return df_2 # Devolvemos el DataFrame enriquecido

if __name__ == "__main__":
    pass