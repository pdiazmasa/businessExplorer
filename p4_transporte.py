################################################################################
# MÓDULO 4: ANÁLISIS DE CONECTIVIDAD Y MOVILIDAD (p4_transporte.py)
#
# DESCRIPCIÓN:
# Evalúa la accesibilidad de cada inmueble mediante un enfoque híbrido de datos.
# Cuantifica las paradas de transporte público (Autobús, Metro, Tren) utilizando
# la infraestructura de OpenStreetMap y analiza la micromovilidad conectándose
# a la API global de CityBikes. Implementa una estrategia de detección de red
# inteligente que prioriza la coincidencia por nombre de ciudad para evitar
# errores de geolocalización, recurriendo a la proximidad física como respaldo.
#
# INPUTS (Parámetros de entrada):
# - df_3: DataFrame con coordenadas y métricas de reputación (salida del p3).
# - ciudad: Nombre de la ciudad para el filtrado semántico de redes de bicis.
#
# OUTPUT (Salida):
# - DataFrame con la columna añadida:
#   [NUM_TRANS_PUB (Tupla: Nº Bus, Nº Metro/Tren, Nº Estaciones Bici)]
#
# FUENTES (WEBS/APIs):
# - Overpass API (OpenStreetMap): Infraestructura de transporte pesado.
# - CityBikes API: Redes de bicicletas compartidas en tiempo real.
#
# LIBRERÍAS CLAVE:
# - Requests: Consumo de APIs externas mediante peticiones HTTP.
# - Math: Cálculos de geometría esférica (Fórmula de Haversine) para distancias.
################################################################################

import requests  # Importamos para realizar peticiones HTTP a las APIs externas
import time  # Importamos para gestionar pausas y evitar saturar los servidores
from math import radians, cos, sin, asin, sqrt  # Importamos funciones matemáticas para el cálculo de distancias

# Configuración de radios de búsqueda en metros
RADIO_BUS_METRO = 200  # Establecemos el radio de proximidad para transporte pesado
RADIO_BICIS = 300  # Establecemos el radio de proximidad para estaciones de bicicletas
MAX_RETRIES = 3  # Definimos el número máximo de reintentos para conexiones de red

################################################################################
# Implementa la fórmula del semiverseno (Haversine) para calcular la distancia
# ortodrómica entre dos puntos de la superficie terrestre. Esta métrica es
# vital para determinar la cercanía real de los servicios sin las distorsiones
# de un plano bidimensional.
#
# RECIBE:
# - lat1, lon1: Coordenadas de origen.
# - lat2, lon2: Coordenadas de destino.
#
# DEVUELVE:
# - float: Distancia en metros.
################################################################################
def calcular_distancia(lat1, lon1, lat2, lon2):
    if lat1 is None or lon1 is None or lat2 is None or lon2 is None:  # Verificamos que no existan coordenadas nulas en la entrada
        return 999999  # Devolvemos una distancia infinita como medida de seguridad
    try:  # Iniciamos el bloque de procesamiento matemático
        lat1, lon1, lat2, lon2 = map(float, [lat1, lon1, lat2, lon2])  # Aseguramos que todas las coordenadas sean números decimales
        lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])  # Convertimos los grados a radianes para los cálculos trigonométricos
        dlon = lon2 - lon1  # Hallamos la diferencia de longitud entre ambos puntos
        dlat = lat2 - lat1  # Hallamos la diferencia de latitud entre ambos puntos
        # Aplicamos la fórmula de Haversine para obtener la distancia angular
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2  
        c = 2 * asin(sqrt(a))  # Calculamos el arco de distancia sobre la superficie
        r = 6371  # Definimos el radio promedio de la Tierra en kilómetros
        return c * r * 1000  # Multiplicamos por mil para obtener y devolver el valor final en metros
    except:  # Capturamos cualquier excepción durante el cálculo
        return 999999

################################################################################
# Realiza una consulta a la API Overpass (OpenStreetMap) para realizar un
# recuento de nodos de transporte. Clasifica los resultados según las 
# etiquetas técnicas (tags) de la base de datos comunitaria de mapas.
#
# RECIBE:
# - lat, lon: Ubicación del local comercial.
#
# DEVUELVE:
# - tuple: (Nº de paradas de bus, Nº de paradas de metro/tren).
################################################################################
def contar_osm(lat, lon):
    url = "https://overpass-api.de/api/interpreter"  # Definimos el punto de acceso para el intérprete de Overpass
    query = f"""
    [out:json][timeout:25];
    (
      node["highway"="bus_stop"](around:{RADIO_BUS_METRO},{lat},{lon});
      node["public_transport"="platform"]["bus"="yes"](around:{RADIO_BUS_METRO},{lat},{lon});
      node["railway"="subway_entrance"](around:{RADIO_BUS_METRO},{lat},{lon});
      node["railway"="station"](around:{RADIO_BUS_METRO},{lat},{lon});
      node["railway"="tram_stop"](around:{RADIO_BUS_METRO},{lat},{lon});
    );
    out body;
    """  # Construimos la query estructurada filtrando por radios y etiquetas de transporte
    try:  # Iniciamos la fase de comunicación con el servidor
        response = requests.get(url, params={'data': query}, timeout=30)  # Lanzamos la petición GET con los datos de la consulta
        if response.status_code == 200:  # Validamos que el servidor haya respondido correctamente
            data = response.json()  # Extraemos la información en formato JSON
            ids_bus = set()  # Creamos un conjunto para contabilizar paradas de bus únicas
            ids_metro = set()  # Creamos un conjunto para contabilizar estaciones de metro únicas
            
            for elem in data.get("elements", []):  # Recorremos cada elemento geográfico encontrado en el radio
                tags = elem.get("tags", {})  # Obtenemos las etiquetas descriptivas del nodo
                eid = elem.get("id")  # Recuperamos su identificador único
                
                if tags.get("highway") == "bus_stop" or tags.get("bus") == "yes":  # Clasificamos el nodo si es una infraestructura de autobús
                    ids_bus.add(eid)  # Registramos el identificador en nuestro conjunto de buses
                elif "railway" in tags or tags.get("station") in ["subway", "light_rail"]:  # Clasificamos el nodo si pertenece a la red ferroviaria
                    ids_metro.add(eid)  # Registramos el identificador en nuestro conjunto de metro
                    
            return len(ids_bus), len(ids_metro)  # Devolvemos el conteo final de ambas categorías
        elif response.status_code == 429:  # Controlamos el caso de recibir un error por exceso de peticiones
            time.sleep(2)  # Pausamos la ejecución para respetar el límite de tráfico
            
    except Exception:  # Evitamos que errores de red detengan el programa
        pass  
    return 0, 0  # Retornamos valores nulos si no pudimos obtener información fiable

################################################################################
# Identifica la red de bicicletas compartidas más pertinente. Prioriza la
# coincidencia textual con el nombre de la ciudad para evitar errores en
# áreas metropolitanas densas donde varias redes podrían solaparse.
#
# RECIBE:
# - lat_ref, lon_ref: Coordenadas del primer local encontrado.
# - ciudad_busqueda: El nombre de la ciudad introducido por el usuario.
#
# DEVUELVE:
# - list: Listado de todas las estaciones de la red detectada.
################################################################################
def detectar_y_obtener_estaciones(lat_ref, lon_ref, ciudad_busqueda):
    print(f"  Buscando red de bicis para: '{ciudad_busqueda}'...")
    
    url_networks = "http://api.citybik.es/v2/networks"  # Definimos la ruta para obtener el catálogo global de redes
    
    mejor_red = None  # Inicializamos el contenedor para la red que mejor encaje
    distancia_minima = float('inf')  # Empezamos con una distancia comparativa infinita
    
    ciudad_normalizada = ciudad_busqueda.lower().strip()  # Limpiamos el texto del usuario para una comparación más precisa
    
    for i in range(MAX_RETRIES):  # Iniciamos el bucle para intentar la conexión con la API
        try:  # Intentamos recuperar el listado de redes disponibles
            resp = requests.get(url_networks, timeout=10)  # Realizamos la llamada a CityBikes
            if resp.status_code == 200:  # Comprobamos que la comunicación ha sido exitosa
                networks = resp.json().get('networks', [])  # Parseamos el listado de todas las redes mundiales
                
                # FASE 1: Filtramos solo aquellas redes que operan dentro de España
                redes_es = [n for n in networks if n.get('location', {}).get('country') == 'ES']  
                
                # FASE 2: Buscamos coincidencias por nombre para asegurar la precisión semántica
                candidatos_nombre = []  # Inicializamos la lista de redes que coinciden por texto
                
                if len(ciudad_normalizada) > 2:  # Verificamos que el usuario haya introducido una ciudad válida
                    for net in redes_es:  # Revisamos cada red española del catálogo
                        loc_api = net.get('location', {})  # Extraemos los datos de ubicación de la red
                        city_api = loc_api.get('city', '').lower()  # Obtenemos el nombre de la ciudad según la API
                        name_api = net.get('name', '').lower()  # Obtenemos el nombre comercial de la red
                        
                        if ciudad_normalizada in city_api or ciudad_normalizada in name_api:  # Comparamos si existe coincidencia con el input del usuario
                            candidatos_nombre.append(net)  # Registramos la red como candidata prioritaria
                
                # Lógica: Si encontramos redes por nombre, las priorizamos; si no, buscamos por cercanía física
                pool_busqueda = candidatos_nombre if candidatos_nombre else redes_es  
                
                if candidatos_nombre:  
                    print(f"    Prioridad: Se encontraron {len(candidatos_nombre)} redes por coincidencia de nombre.")
                else:  
                    print("    No hay coincidencia de nombre. Buscando la red más cercana por coordenadas...")

                # FASE 3: Seleccionamos la red más cercana dentro del grupo filtrado
                for net in pool_busqueda:  # Iteramos sobre las redes candidatas
                    loc = net.get('location', {})  # Obtenemos las coordenadas centrales de la red
                    d = calcular_distancia(lat_ref, lon_ref, loc.get('latitude'), loc.get('longitude'))  # Medimos la distancia a nuestro local de referencia
                    
                    if d < distancia_minima:  # Verificamos si esta red es la más cercana hasta el momento
                        distancia_minima = d  # Actualizamos nuestra distancia de referencia
                        mejor_red = net  # Designamos esta red como la opción ganadora
                break  # Salimos del bucle al haber completado la selección
                
        except Exception as e:  # Manejamos posibles fallos en la consulta de redes
            print(f"    Intento {i+1} fallido (Redes): {e}")
            time.sleep(2)  # Esperamos antes de realizar un nuevo intento
            
    if not mejor_red:  # Si tras los reintentos no encontramos ninguna red apta
        print("    No se detectó ninguna red compatible.")
        return []  # Retornamos una lista vacía
    
    nombre_red = mejor_red.get('name')  # Extraemos el nombre comercial de la red seleccionada
    ciudad_red = mejor_red.get('location', {}).get('city')  # Extraemos la ciudad donde opera
    print(f"    Red seleccionada: {nombre_red} ({ciudad_red})")
    
    # 2. Procedemos a descargar los detalles de las estaciones individuales
    href = mejor_red.get('href')  # Tomamos el enlace específico para las estaciones de esa red
    url_estaciones = f"http://api.citybik.es{href}"  # Generamos la URL completa para la consulta
    
    for i in range(MAX_RETRIES):  # Intentamos descargar el listado de estaciones
        try:  # Realizamos la petición de los puntos de anclaje
            resp = requests.get(url_estaciones, timeout=10)  # Llamamos al endpoint detallado de la red
            if resp.status_code == 200:  # Si la respuesta es satisfactoria
                return resp.json().get('network', {}).get('stations', [])  # Devolvemos la lista de estaciones encontradas
        except Exception as e:  # Controlamos fallos en la descarga de estaciones
            time.sleep(2)  # Pausamos antes del reintento
            
    return []  # Retornamos vacío ante un fallo crítico de descarga

################################################################################
# Función orquestadora del módulo. Gestiona el flujo completo: localiza la
# red de movilidad compartida y luego audita cada local para cuantificar
# su conectividad real con el transporte público.
#
# RECIBE:
# - df_3: Datos de locales.
# - ciudad: Ciudad de referencia.
#
# DEVUELVE:
# - DataFrame: Los datos originales con las nuevas métricas de transporte.
################################################################################
def analizar_transporte(df_3, ciudad=""):
    print(f"\n--- INICIANDO P4: Transporte Híbrido ---")
    
    df_4 = df_3.copy()  # Creamos una copia de trabajo para no alterar el DataFrame original
    if df_4.empty:  # Verificamos si el listado de locales está vacío
        df_4['NUM_TRANS_PUB'] = []  # Preparamos la columna de salida vacía
        return df_4  # Finalizamos el proceso de forma segura
        
    # 1. Localizamos la primera coordenada válida para ubicar la red de transporte de la ciudad
    lat_ref, lon_ref = 0, 0  # Inicializamos las variables de referencia geográfica
    for c in df_4['COORDENADAS']:  # Recorremos las coordenadas extraídas en la fase de búsqueda
        if isinstance(c, (tuple, list)) and len(c) == 2 and c[0] != 0:  # Comprobamos si el local tiene coordenadas reales
            lat_ref, lon_ref = c[0], c[1]  # Guardamos el punto para la detección de red
            break  # Interrumpimos la búsqueda tras encontrar el primer punto válido
            
    estaciones_bicis = []  # Inicializamos el contenedor para las estaciones de bicicletas
    if lat_ref != 0:  # Si hemos conseguido una ubicación de referencia válida
        # Descargamos la infraestructura de bicicletas compartidas de la ciudad
        estaciones_bicis = detectar_y_obtener_estaciones(lat_ref, lon_ref, ciudad)  
    else:  
        print("    No hay coordenadas válidas para buscar red de bicis.")
    
    columna_transporte = []  # Inicializamos la lista donde acumularemos los resultados finales por local
    
    print("Calculando distancias transporte...")
    for index, row in df_4.iterrows():  # Procesamos cada inmueble de forma individual
        try:  # Iniciamos el análisis de conectividad por local
            coords = row['COORDENADAS']  # Extraemos la ubicación específica del local
            if not isinstance(coords, (tuple, list)) or len(coords) < 2 or coords == (0,0):  # Validamos los datos geográficos
                columna_transporte.append((0, 0, 0))  # Asignamos valores nulos si los datos son corruptos
                continue  
                
            lat, lon = coords[0], coords[1]  # Desempaquetamos latitud y longitud
            
            # A. Realizamos el recuento de transporte público pesado mediante OpenStreetMap
            n_bus, n_tren = contar_osm(lat, lon)  
            
            # B. Calculamos la densidad de estaciones de bicicleta en el entorno inmediato
            n_bicis = 0  # Inicializamos el contador local de bicicletas
            for est in estaciones_bicis:  # Revisamos cada estación de la red descargada previamente
                d = calcular_distancia(lat, lon, est['latitude'], est['longitude'])  # Medimos la distancia local-estación
                if d <= RADIO_BICIS:  # Comprobamos si el servicio está dentro de nuestro radio de influencia
                    n_bicis += 1  # Incrementamos el recuento de estaciones cercanas
            
            print(f"    -> Transporte detectado (Bus, Metro, Bici): {(n_bus, n_tren, n_bicis)}")
            
            columna_transporte.append((n_bus, n_tren, n_bicis))  # Almacenamos los resultados en la tupla de métricas
            time.sleep(0.1)  # Realizamos una pausa mínima para evitar saturación en el procesamiento
            
        except Exception:  # Evitamos que errores en un local específico detengan todo el ranking
            columna_transporte.append((0, 0, 0))  # Rellenamos con datos neutros ante fallos inesperados

    df_4['NUM_TRANS_PUB'] = columna_transporte  # Inyectamos la lista de resultados en la nueva columna del DataFrame
    return df_4  # Devolvemos el DataFrame enriquecido y listo para el cálculo del ranking final

if __name__ == "__main__":
    pass