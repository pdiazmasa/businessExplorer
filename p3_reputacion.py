################################################################################
# MÓDULO 4: ANÁLISIS DE CONECTIVIDAD Y MOVILIDAD (p4_transporte.py)
#
# DESCRIPCIÓN:
# Evalúa la accesibilidad de cada inmueble mediante un enfoque híbrido de datos.
# Cuantifica las paradas de transporte público (Autobús, Metro, Tren) utilizando
# la infraestructura de OpenStreetMap y analiza la micromobilidad conectándose
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

from selenium import webdriver # Importamos el controlador principal de Selenium para automatizar el navegador
from selenium.webdriver.common.by import By # Importamos la herramienta para localizar elementos en el DOM (HTML)
import time # Importamos la librería de tiempo para gestionar pausas y esperas
import ast # Importamos AST para evaluar cadenas de texto que contienen listas de forma segura
import random # Importamos random para generar esperas aleatorias y parecer humanos
import re # Importamos expresiones regulares para buscar patrones de notas en el texto

UMBRAL_BUENO = 3.8 # Definimos la nota de corte para considerar un local como "buena competencia"

def iniciar_driver():

    ################################################################################
    # Configura e inicializa una instancia del navegador Chrome controlada por
    # Selenium. Establece parámetros críticos como el modo "headless" (sin interfaz
    # gráfica), el tamaño de ventana y un "User-Agent" rotatorio o realista para
    # reducir el riesgo de bloqueo por parte de los buscadores.
    #
    # RECIBE: nada
    #
    # DEVUELVE:
    # - driver (WebDriver): Instancia del navegador lista para navegar.
    ################################################################################

    options = webdriver.ChromeOptions() # Creamos el objeto de configuración para Chrome
    options.add_argument("--headless") # Activamos el modo invisible para que no se abra la ventana
    options.add_argument("--log-level=3") # Silenciamos los logs técnicos del driver para limpiar la salida
    options.add_argument("--window-size=1920,1080") # Establecemos una resolución estándar para evitar versiones móviles de webs
    # User Agent de navegador real para evitar que nos den versiones "móviles" reducidas
    ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36" # Definimos un User-Agent legítimo
    options.add_argument(f"user-agent={ua}") # Asignamos el User-Agent a la configuración
    options.add_argument("--lang=es-ES") # Forzamos el idioma español para que los patrones "x de 5" funcionen
    
    driver = webdriver.Chrome(options=options) # Inicializamos el navegador con las opciones configuradas
    return driver # Devolvemos la instancia del navegador lista para usar

def limpiar_nombre_busqueda(nombre):

    ################################################################################
    # Aplica una normalización contextual al nombre del negocio antes de realizar
    # la búsqueda. Si el nombre es demasiado corto o genérico (una sola palabra,
    # ej: "Domino's"), le añade sufijos semánticos (ej: "restaurante") para
    # ayudar al buscador a diferenciar el negocio de otros resultados irrelevantes.
    #
    # RECIBE:
    # - nombre (str): Nombre original del competidor.
    #
    # DEVUELVE:
    # - str: Nombre optimizado para la query de búsqueda.
    ################################################################################

    n = nombre.lower() # Convertimos el nombre a minúsculas para analizarlo
    if len(n.split()) < 2: # Si el nombre tiene menos de 2 palabras (es muy corto)
        return f"{nombre} restaurante" # Le añadimos la palabra "restaurante" para dar contexto
    return nombre # Si es largo, lo devolvemos tal cual

def buscar_nota_duckduckgo(driver, nombre, cp):
    
    ################################################################################
    # Ejecuta una búsqueda específica en DuckDuckGo combinando nombre, código postal
    # y palabras clave. Analiza el texto visible de la página de resultados (SERP)
    # buscando patrones de puntuación (ej: "4.5/5", "9 de 10") mediante expresiones
    # regulares. Normaliza cualquier escala encontrada a un rango estándar de 0 a 5.
    #
    # RECIBE:
    # - driver: Instancia del navegador activa.
    # - nombre (str): Nombre del competidor.
    # - cp (str): Código Postal para acotar geográficamente.
    #
    # DEVUELVE:
    # - float: Nota normalizada (0.0 - 5.0) o None si no se encuentra.
    ################################################################################

    try: # Iniciamos el bloque de manejo de errores
        nombre_busqueda = limpiar_nombre_busqueda(nombre) # Limpiamos el nombre antes de buscar
        # Búsqueda: Nombre + CP + "opiniones"
        query = f"{nombre_busqueda} {cp} opiniones" # Construimos la cadena de búsqueda con intención de encontrar reseñas
        url = f"https://duckduckgo.com/?q={query}&kl=es-es" # Construimos la URL de DuckDuckGo forzando región España
        
        driver.get(url) # Navegamos a la URL construida
        time.sleep(random.uniform(1.0, 1.5)) # Esperamos un tiempo aleatorio para simular comportamiento humano
        
        # Obtenemos todo el texto visible
        try: cuerpo = driver.find_element(By.TAG_NAME, "body").text # Intentamos extraer todo el texto del cuerpo de la página
        except: return None # Si falla la extracción del body, devolvemos None
            
        # --- PATRONES DE NOTA AMPLIADOS ---
        # El orden importa: primero los más específicos
        patrones = [ # Definimos una lista de expresiones regulares para cazar la nota
            r"(\d[.,]\d)\s?/\s?5",          # Buscamos formato "4.5/5"
            r"(\d[.,]\d)\s?de\s?5",         # Buscamos formato "4,5 de 5"
            r"Puntuación:\s?(\d[.,]\d)",    # Buscamos formato "Puntuación: 4.5"
            r"Valoración:\s?(\d[.,]\d)",    # Buscamos formato "Valoración: 4.5"
            r"(\d[.,]\d)\s?estrellas",      # Buscamos formato "4.5 estrellas"
            r"Rating:\s?(\d[.,]\d)"         # Buscamos formato inglés "Rating: 4.5"
        ]
        
        for patron in patrones: # Iteramos sobre cada patrón definido
            match = re.search(patron, cuerpo, re.IGNORECASE) # Buscamos el patrón en el texto de la web ignorando mayúsculas
            if match: # Si encontramos una coincidencia
                texto_nota = match.group(1).replace(",", ".") # Extraemos el número y normalizamos decimales (coma a punto)
                nota = float(texto_nota) # Convertimos el texto a número decimal
                if 0.0 <= nota <= 5.0: # Verificamos que la nota sea coherente (rango 0-5)
                    return nota # Devolvemos la nota encontrada
        
        # INTENTO SECUNDARIO: Si falla lo anterior, buscar patrones de TripAdvisor/TheFork
        # A veces sale "9.2/10". Lo convertimos a base 5.
        patrones_base10 = [ # Definimos patrones para escalas sobre 10
            r"(\d[.,]\d)\s?/\s?10", # Buscamos formato "9.2/10"
            r"(\d[.,]\d)\s?de\s?10" # Buscamos formato "9.2 de 10"
        ]
        for patron in patrones_base10: # Iteramos sobre los patrones de base 10
            match = re.search(patron, cuerpo, re.IGNORECASE) # Buscamos coincidencia en el texto
            if match: # Si encontramos coincidencia
                texto_nota = match.group(1).replace(",", ".") # Normalizamos el formato numérico
                nota = float(texto_nota) / 2 # Convertimos la nota de base 10 a base 5
                if 0.0 <= nota <= 5.0: # Verificamos la coherencia del resultado
                    return nota # Devolvemos la nota convertida

        return None # Si no encontramos ningún patrón válido, devolvemos None

    except Exception: # Capturamos cualquier error durante el proceso
        return None # Devolvemos None en caso de fallo

def analizar_reputacion(df_2):
    
    ################################################################################
    # Función principal del módulo que orquesta la auditoría de reputación.
    # Recorre el DataFrame de inmuebles y, para cada lista de competidores detectada
    # en el paso anterior, lanza búsquedas web para extraer sus valoraciones.
    # Clasifica a los rivales en "Buenos" o "Malos" según un umbral definido y
    # calcula la nota media del entorno competitivo.
    #
    # RECIBE:
    # - df_2 (DataFrame): DataFrame con la columna 'COMPETENCIA' poblada.
    #
    # DEVUELVE:
    # - DataFrame: Copia del original con la nueva columna 'NUM_COMPETENCIA'.
    ################################################################################

    print(f"\n--- INICIANDO P3: Análisis de Reputación (Búsqueda Mejorada) ---") # Imprimimos mensaje de inicio
    
    df_3 = df_2.copy() # Creamos una copia del DataFrame recibido para no alterar el original
    if df_3.empty: # Si el DataFrame está vacío
        df_3['NUM_COMPETENCIA'] = [] # Añadimos la columna vacía
        return df_3 # Devolvemos el DataFrame vacío

    driver = iniciar_driver() # Iniciamos el navegador automatizado
    resultados_metricas = [] # Inicializamos la lista donde guardaremos las tuplas de resultados
    
    try: # Iniciamos el bloque principal de procesamiento
        for index, row in df_3.iterrows(): # Iteramos por cada fila (local) del DataFrame
            competencia_raw = row['COMPETENCIA'] # Extraemos los datos crudos de la columna competencia
            lista_competidores = [] # Inicializamos la lista de competidores para esta fila
            
            if isinstance(competencia_raw, list): # Si ya es una lista
                lista_competidores = competencia_raw # La usamos directamente
            elif isinstance(competencia_raw, str): # Si es una cadena de texto (representación de lista)
                try: lista_competidores = ast.literal_eval(competencia_raw) # Intentamos evaluarla de forma segura
                except: lista_competidores = [] # Si falla, asumimos lista vacía

            total = len(lista_competidores) # Calculamos el total de competidores
            buenos = 0 # Inicializamos contador de competidores buenos
            malos = 0 # Inicializamos contador de competidores malos
            suma_notas = 0.0 # Inicializamos acumulador para la media de notas
            con_nota = 0 # Inicializamos contador de competidores que sí tienen nota
            
            # Solo imprimimos si hay competencia para no saturar la terminal
            if total > 0: # Si hay competidores para este local
                print(f"  > Local {row['NUMERO']}: {total} rivales. Buscando notas...") # Informamos al usuario

            for comp in lista_competidores: # Iteramos sobre cada competidor de la lista
                if not comp or len(comp) < 2: continue # Si el formato del competidor no es válido, saltamos
                
                nombre_comp = comp[0] # Extraemos el nombre del competidor
                cp_comp = comp[1] # Extraemos el código postal del competidor
                
                # Filtro rápido de nombres inválidos
                if len(nombre_comp) < 2 or "Local" in nombre_comp: continue # Descartamos nombres demasiado genéricos o vacíos

                nota = buscar_nota_duckduckgo(driver, nombre_comp, cp_comp) # Llamamos a la función de scraping para obtener la nota
                
                if nota is not None: # Si hemos conseguido una nota válida
                    con_nota += 1 # Incrementamos el contador de competidores con nota
                    suma_notas += nota # Sumamos la nota al acumulador
                    if nota > UMBRAL_BUENO: # Si la nota supera el umbral definido
                        buenos += 1 # Lo contamos como competidor bueno
                    else: # Si no supera el umbral
                        malos += 1 # Lo contamos como competidor malo
                    # print(f"    found: {nombre_comp} -> {nota}") # Descomentar para debug
                
            if con_nota > 0: # Si hemos encontrado notas para calcular media
                nota_media = round(suma_notas / con_nota, 2) # Calculamos la media aritmética redondeada
            else: # Si no hay notas
                nota_media = 0.0 # La media es 0
            
            metricas = (total, buenos, malos, nota_media) # Empaquetamos los resultados en una tupla
            resultados_metricas.append(metricas) # Añadimos la tupla a la lista global
            
            if total > 0: # Si había competencia
                print(f"    -> Resultado: {metricas} (Notas encontradas: {con_nota}/{total})") # Imprimimos el resultado del análisis

    except KeyboardInterrupt: # Si el usuario cancela la ejecución manualmente
        print("\nCancelado por usuario.") # Avisamos de la cancelación
    except Exception as e: # Si ocurre cualquier otro error crítico
        print(f"Error P3: {e}") # Imprimimos el error
    finally: # Al finalizar (bien o mal)
        driver.quit() # Cerramos el navegador obligatoriamente

    while len(resultados_metricas) < len(df_3): # Si la lista de resultados es más corta que el DataFrame (por error o cancelación)
        resultados_metricas.append((0,0,0,0.0)) # Rellenamos con ceros para cuadrar tamaños
        
    df_3['NUM_COMPETENCIA'] = resultados_metricas # Asignamos la lista de métricas a la nueva columna del DataFrame
    print("Módulo P3 finalizado.") # Informamos del fin del módulo
    return df_3 # Devolvemos el DataFrame con los datos de reputación añadidos

if __name__ == "__main__":
    pass