################################################################################
# MÓDULO 1: BÚSQUEDA Y EXTRACCIÓN DE INMUEBLES (p1_busqueda_local.py)
#
# DESCRIPCIÓN:
# Scraping de pisos.com que automatiza la búsqueda de locales u oficinas
# según criterios específicos. Realiza una navegación headless,
# extrae los datos relevantes de los anuncios, limpia las direcciones mediante 
# regex avanzado y las geolocaliza (obtiene coordenadas exactas) usando una 
# estrategia híbrida (ArcGIS + OpenStreetMap).
#
# INPUTS (Parámetros de entrada):
# - Ciudad (ej: "Madrid")
# - Subtipo de inmueble (ej: "Locales comerciales", "Oficinas")
# - Tipo de operación (Alquiler o Venta)
# - Presupuesto máximo (ej: 2000€)
#
# OUTPUT (Salida):
# - DataFrame de Pandas con las columnas:
#   [NUMERO, NOMBRE, DIRECCION, COORDENADAS (lat, lon), PRECIO, LINK]
#
# FUENTES (WEBS):
# - www.pisos.com
#
# LIBRERÍAS CLAVE:
# - Selenium: Navegación web automatizada y gestión de filtros dinámicos.
# - BeautifulSoup (bs4): Extracción rápida de datos HTML.
# - Geopy (ArcGIS y Nominatim): Pasar de dirección a coordenadas.
# - Pandas: Estructuración y manejo de las tablas.
################################################################################

from selenium import webdriver  # Controla el navegador web (abrir Chrome, navegar, etc.)
from selenium.webdriver.common.by import By  # Permite buscar elementos en la web (por ID, clase, CSS, etc.)
from selenium.webdriver.common.keys import Keys  # Simula pulsaciones de teclado (Enter, borrar, escribir)
from selenium.webdriver.support.ui import Select  # Maneja los menús desplegables de las webs
from bs4 import BeautifulSoup  # Analiza el HTML de la página para extraer datos (scraping puro)
from geopy.geocoders import Nominatim, ArcGIS  # Convierte direcciones postales en coordenadas (Latitud, Longitud)
import pandas as pd  # Organiza los datos en tablas (DataFrames) y permite guardar en Excel/CSV
import time  # Controla el tiempo (hacer pausas para que cargue la web)
import re  # "Expresiones Regulares": busca y limpia patrones de texto (quitar símbolos, buscar números)
import random  # Genera números aleatorios (útil para tiempos de espera variables y parecer humano)

def calcular_filtro_precio(presupuesto, es_alquiler):  # Define la función `calcular_filtro_precio`
    """Calcula el valor del precio más cercano al presupuesto."""  # Instrucción ejecutable
    if es_alquiler:     # posibles valores de filtro del alquiler en pisos.com
        opciones = [150, 250, 350, 450, 600, 750, 900, 1100, 1300, 1500,   # Asigna un valor a una variable
                    1800, 2300, 3000, 5000, 10000, 20000, 30000]  # Instrucción ejecutable
    else:               # posibles valores de filtro de compra en pisos.com
        opciones = [10000, 30000, 60000, 90000, 120000, 150000, 180000,   # Asigna un valor a una variable
                    210000, 240000, 270000, 300000, 360000, 420000, 480000,   # Instrucción ejecutable
                    600000, 1050000, 2050000, 3050000, 4050000, 6000000,   # Instrucción ejecutable
                    10000000, 20000000]  # Instrucción ejecutable
    elegido = opciones[0]  # Asigna un valor a una variable
    for opcion in opciones:  # Bucle `for`: itera sobre una secuencia
        if opcion <= presupuesto:  # Asigna un valor a una variable
            elegido = opcion  # Asigna un valor a una variable
        else:  # Instrucción ejecutable
            break  # Instrucción ejecutable
    return str(elegido)  # Devuelve el resultado desde la función

def limpiar_direccion(texto_sucio):  # Define la función `limpiar_direccion`
    ################################################################################
    # Aplica una serie de limpiezas agresivas mediante expresiones regulares sobre el
    # texto crudo de una dirección. Su objetivo es eliminar "ruido" típico de los
    # anuncios inmobiliarios (prefijos como "Local en...", redundancias tipo "Calle
    # Avenida", abreviaturas como "s/n" o aclaraciones entre paréntesis) para dejar
    # una cadena limpia que aumente la tasa de éxito de los geocodificadores.
    ################################################################################

    # 1. Eliminar prefijos de anuncio
    patron_inicio = r"^(local comercial|oficina|edificio|nave|almacén|local).*?\s+en\s+"   # Asigna un valor a una variable
    texto = re.sub(patron_inicio, "", texto_sucio, flags=re.IGNORECASE).strip()   # Asigna un valor a una variable
    
    # 2. Corregir redundancias típicas (Ej: "Calle Carrer")
    texto = re.sub(r'calle\s+carrer', 'Carrer', texto, flags=re.IGNORECASE)   # Asigna un valor a una variable
    texto = re.sub(r'calle\s+avenida', 'Avenida', texto, flags=re.IGNORECASE)   # Asigna un valor a una variable
    
    # 3. Limpiar caracteres sucios (dobles comillas, nº, s/n)
    texto = texto.replace("''", "'")   # Asigna un valor a una variable
    texto = re.sub(r'nº\.?', '', texto, flags=re.IGNORECASE)   # Asigna un valor a una variable
    texto = re.sub(r's/n', '', texto, flags=re.IGNORECASE)     # Asigna un valor a una variable
    
    # 4. Quitar paréntesis y su contenido (suelen ser zonas o aclaraciones que confunden)
    texto = re.sub(r'\(.*?\)', '', texto).strip()   # Asigna un valor a una variable
    
    # 5. Quitar espacios múltiples y comas al inicio/final
    texto = re.sub(r'\s+', ' ', texto).strip(" ,.-")   # Asigna un valor a una variable
    
    return texto   # Devuelve el resultado desde la función

def normalizar_subtipo(subtipo_input):  # Define la función `normalizar_subtipo`
    s = subtipo_input.lower()  # Asigna un valor a una variable
    if "almacen" in s: return "nave"  # Instrucción ejecutable
    if "comercial" in s: return "local"  # Instrucción ejecutable
    if "oficina" in s: return "oficina"  # Instrucción ejecutable
    if "edificio" in s: return "edificio"  # Instrucción ejecutable
    return "local"  # Devuelve el resultado desde la función

def geocodificar_inteligente(direccion, ciudad, geo_osm, geo_arcgis):  # Define la función `geocodificar_inteligente`
    
    ################################################################################
    # Ejecuta un proceso de geolocalización escalonado para convertir una dirección
    # textual en coordenadas geográficas (lat,lon). 
    ################################################################################

    busquedas = [   # Asigna un valor a una variable
        f"{direccion}, {ciudad}, España",             # Instrucción ejecutable
        f"{direccion}, España",                       # Instrucción ejecutable
    ]  # Instrucción ejecutable
    
    # ESTRATEGIA A: Intentar con ArcGIS 
    for q in busquedas:   # Bucle `for`: itera sobre una secuencia
        try:   # Inicio de bloque `try` para capturar excepciones
            loc = geo_arcgis.geocode(q, timeout=5)   # Asigna un valor a una variable
            if loc: return loc.latitude, loc.longitude   # Instrucción ejecutable
        except: pass   # Captura una excepción si ocurre dentro del `try`

    # ESTRATEGIA B: Intentar con Nominatim (OSM)
    for q in busquedas:   # Bucle `for`: itera sobre una secuencia
        try:   # Inicio de bloque `try` para capturar excepciones
            loc = geo_osm.geocode(q, timeout=5)   # Asigna un valor a una variable
            if loc: return loc.latitude, loc.longitude   # Instrucción ejecutable
        except: pass   # Captura una excepción si ocurre dentro del `try`

    # ESTRATEGIA C: Limpieza Drástica (Solo nombre de calle, sin números)
    solo_letras = re.sub(r'[0-9]', '', direccion).strip(" ,")   # Asigna un valor a una variable
    if len(solo_letras) > 3:   # Instrucción ejecutable
        q_backup = f"{solo_letras}, {ciudad}, España"   # Asigna un valor a una variable
        try:   # Inicio de bloque `try` para capturar excepciones
            loc = geo_arcgis.geocode(q_backup, timeout=3)   # Asigna un valor a una variable
            if loc: return loc.latitude, loc.longitude   # Instrucción ejecutable
        except: pass   # Captura una excepción si ocurre dentro del `try`
    
    return 0.0, 0.0   # Devuelve el resultado desde la función

def busqueda(ciudad, subtipo, operacion, presupuesto_max):  # Define la función `busqueda`

    ################################################################################
    # Realiza un scraping en pisos.com para extraer locales según los filtros 
    ################################################################################

    print(f"\n--- INICIANDO P1: Búsqueda de '{subtipo}' en '{ciudad}' ({operacion}) hasta {presupuesto_max}€ ---")  # Instrucción ejecutable

    options = webdriver.ChromeOptions()   # Asigna un valor a una variable
    options.add_argument("--headless")    # Instrucción ejecutable
    options.add_argument("--window-size=1920,1080")    # Asigna un valor a una variable
    options.add_argument("--log-level=3")    # Asigna un valor a una variable
    ua = f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{random.randint(90,110)}.0.0.0 Safari/537.36"   # Asigna un valor a una variable
    options.add_argument(f"user-agent={ua}")   # Asigna un valor a una variable

    driver = webdriver.Chrome(options=options)   # Asigna un valor a una variable
    
    resultados_dict = {}   # Asigna un valor a una variable
    
    # Inicializamos DOS geocodificadores
    geolocator_osm = Nominatim(user_agent=f"geo_app_{random.randint(1000,9999)}")   # Asigna un valor a una variable
    geolocator_arcgis = ArcGIS()   # Asigna un valor a una variable
    
    try:   # Inicio de bloque `try` para capturar excepciones
        driver.get("https://www.pisos.com/")    # Instrucción ejecutable
        time.sleep(2)   # Instrucción ejecutable
        try: driver.find_element(By.ID, "didomi-notice-agree-button").click()   # Inicio de bloque `try` para capturar excepciones
        except: pass   # Captura una excepción si ocurre dentro del `try`

        try:   # Inicio de bloque `try` para capturar excepciones
            sel_elem = driver.find_element(By.ID, "familyType")    # Asigna un valor a una variable
            Select(sel_elem).select_by_visible_text("Locales y oficinas")    # Instrucción ejecutable
            time.sleep(1)   # Instrucción ejecutable
        except: pass   # Captura una excepción si ocurre dentro del `try`

        print(f"Ubicando zona: {ciudad}...")   # Instrucción ejecutable
        caja = driver.find_element(By.NAME, "searchText")    # Asigna un valor a una variable
        caja.click(); caja.clear(); caja.send_keys(ciudad)                # Instrucción ejecutable
        time.sleep(1); caja.send_keys(Keys.RETURN); time.sleep(3)   # Instrucción ejecutable

        try:   # Inicio de bloque `try` para capturar excepciones
            driver.find_element(By.CSS_SELECTOR, "a.button__primary--darkblue").click()   # Instrucción ejecutable
            time.sleep(4)   # Instrucción ejecutable
        except: pass   # Captura una excepción si ocurre dentro del `try`

        print("Aplicando filtros...")   # Instrucción ejecutable
        es_alquiler = "alquiler" in operacion.lower()   # Asigna un valor a una variable
        if es_alquiler:   # Instrucción ejecutable
            try:   # Inicio de bloque `try` para capturar excepciones
                if "alquiler" not in driver.current_url:   # Instrucción ejecutable
                    # CAMBIO 1: Reemplazo de XPath por PARTIAL_LINK_TEXT
                    driver.find_element(By.PARTIAL_LINK_TEXT, "Alquiler").click()   # Instrucción ejecutable
                    time.sleep(4)             # Instrucción ejecutable
            except: pass   # Captura una excepción si ocurre dentro del `try`

        keyword_filtro = normalizar_subtipo(subtipo)   # Asigna un valor a una variable
        try:   # Inicio de bloque `try` para capturar excepciones
            # CAMBIO 2: Reemplazo de XPath complejo por CSS Selector estructurado
            # Usamos el contenedor data-id='subAdType' que es único para este filtro
            css_abrir = "div[data-id='subAdType'] .filters__select-tag"  # Asigna un valor a una variable
            
            try: btn_abrir = driver.find_element(By.CSS_SELECTOR, css_abrir)  # Inicio de bloque `try` para capturar excepciones
            except: btn_abrir = driver.find_element(By.CSS_SELECTOR, "div.filters__select-tag")   # Captura una excepción si ocurre dentro del `try`
            
            driver.execute_script("arguments[0].click();", btn_abrir)   # Instrucción ejecutable
            time.sleep(1.5)   # Instrucción ejecutable

            try:   # Inicio de bloque `try` para capturar excepciones
                chk_todos = driver.find_element(By.ID, "ck0")   # Asigna un valor a una variable
                if chk_todos.is_selected():   # Instrucción ejecutable
                    driver.execute_script("arguments[0].click();", chk_todos)   # Instrucción ejecutable
                    time.sleep(1)   # Instrucción ejecutable
            except: pass   # Captura una excepción si ocurre dentro del `try`

            # CAMBIO 3: Reemplazo de XPath por CSS Selector para las opciones
            css_opciones = ".filters__multioption .filters__char"  # Asigna un valor a una variable
            opciones = driver.find_elements(By.CSS_SELECTOR, css_opciones)   # Asigna un valor a una variable
            
            for op in opciones:   # Bucle `for`: itera sobre una secuencia
                try:   # Inicio de bloque `try` para capturar excepciones
                    texto = op.text.strip().lower()   # Asigna un valor a una variable
                    input_chk = op.find_element(By.TAG_NAME, "input")   # Asigna un valor a una variable
                    if keyword_filtro in texto:   # Instrucción ejecutable
                        if not input_chk.is_selected():   # Instrucción ejecutable
                            driver.execute_script("arguments[0].click();", input_chk)   # Instrucción ejecutable
                except: pass   # Captura una excepción si ocurre dentro del `try`
            
            time.sleep(1)   # Instrucción ejecutable
            driver.execute_script("arguments[0].click();", driver.find_element(By.CSS_SELECTOR, "div[data-id='subAdType'] button.js-accept"))   # Asigna un valor a una variable
            time.sleep(4)   # Instrucción ejecutable
        except Exception as e:    # Captura una excepción si ocurre dentro del `try`
            print(f"Advertencia aplicando subtipo: {e}")  # Instrucción ejecutable

        try:   # Inicio de bloque `try` para capturar excepciones
            val_precio = calcular_filtro_precio(presupuesto_max, es_alquiler)   # Asigna un valor a una variable
            Select(driver.find_element(By.NAME, "ddPrecioMax")).select_by_value(val_precio)   # Instrucción ejecutable
            time.sleep(2)   # Instrucción ejecutable
            try:   # Inicio de bloque `try` para capturar excepciones
                btn_final = driver.find_element(By.CSS_SELECTOR, "button.js-seeResultsFilters")   # Asigna un valor a una variable
                if btn_final.is_displayed(): btn_final.click()   # Instrucción ejecutable
            except: pass   # Captura una excepción si ocurre dentro del `try`
            time.sleep(4)                     # Instrucción ejecutable
        except: pass   # Captura una excepción si ocurre dentro del `try`

        print("Extrayendo datos y Geocodificando (Modo Reforzado)...")  # Instrucción ejecutable
        
        soup = BeautifulSoup(driver.page_source, "html.parser")   # Asigna un valor a una variable
        tarjetas = soup.find_all("div", class_="ad-preview")   # Asigna un valor a una variable
        
        if not tarjetas:   # Instrucción ejecutable
            print("No se encontraron tarjetas de anuncios.")  # Instrucción ejecutable

        for i, tarjeta in enumerate(tarjetas):   # Bucle `for`: itera sobre una secuencia
            try:   # Inicio de bloque `try` para capturar excepciones
                tag_titulo = tarjeta.select_one(".ad-preview__title")   # Asigna un valor a una variable
                if not tag_titulo: continue   # Instrucción ejecutable
                
                titulo = tag_titulo.text.strip()   # Asigna un valor a una variable
                enlace_relativo = tag_titulo.get('href')   # Asigna un valor a una variable
                link_completo = f"https://www.pisos.com{enlace_relativo}"   # Asigna un valor a una variable
                
                precio_num = 0   # Asigna un valor a una variable
                tag_precio = tarjeta.select_one(".ad-preview__price")   # Asigna un valor a una variable
                texto_precio = tag_precio.text.strip() if tag_precio else ""   # Asigna un valor a una variable
                if not texto_precio:   # Instrucción ejecutable
                    match = re.search(r'([\d\.]+)\s?€', tarjeta.text)   # Asigna un valor a una variable
                    if match: texto_precio = match.group(1)   # Asigna un valor a una variable
                
                digitos = "".join(filter(str.isdigit, texto_precio))   # Asigna un valor a una variable
                if digitos: precio_num = int(digitos)   # Asigna un valor a una variable

                tag_ubicacion = tarjeta.select_one(".ad-preview__location")  # Asigna un valor a una variable
                direccion_raw = tag_ubicacion.text.strip() if tag_ubicacion else titulo   # Asigna un valor a una variable
                
                # --- NUEVA LIMPIEZA ---
                direccion_limpia = limpiar_direccion(direccion_raw)   # Asigna un valor a una variable

                if 0 < precio_num <= presupuesto_max:   # Asigna un valor a una variable
                    
                    # --- NUEVA GEOCODIFICACIÓN ---
                    lat, lon = geocodificar_inteligente(direccion_limpia, ciudad, geolocator_osm, geolocator_arcgis)   # Asigna un valor a una variable

                    if lat != 0.0 and lon != 0.0:   # Asigna un valor a una variable
                        clave = titulo if titulo not in resultados_dict else f"{titulo}_{i}"   # Asigna un valor a una variable
                        resultados_dict[clave] = {   # Asigna un valor a una variable
                            "nombre": titulo,  # Instrucción ejecutable
                            "direccion": direccion_limpia,  # Instrucción ejecutable
                            "precio": precio_num,  # Instrucción ejecutable
                            "coords": (lat, lon),   # Instrucción ejecutable
                            "link": link_completo  # Instrucción ejecutable
                        }  # Instrucción ejecutable
                        print(f"  > Ubicación OK: {direccion_limpia[:30]}... ({precio_num}€)")  # Instrucción ejecutable
                    else:   # Instrucción ejecutable
                        print(f"  > Descartado (No ubi): {direccion_limpia[:30]}...")  # Instrucción ejecutable
                        
            except Exception as e:   # Captura una excepción si ocurre dentro del `try`
                print(f"Error procesando tarjeta {i}: {e}")  # Instrucción ejecutable

    except Exception as e:   # Captura una excepción si ocurre dentro del `try`
        print(f"Error crítico en selenium: {e}")  # Instrucción ejecutable
    finally:   # Bloque que se ejecuta siempre (haya o no error)
        driver.quit()  # Instrucción ejecutable

    print("Módulo P1 finalizado.\n")  # Instrucción ejecutable
    lista_para_df = []   # Asigna un valor a una variable
    contador = 1   # Asigna un valor a una variable
    
    for key, info in resultados_dict.items():     # Bucle `for`: itera sobre una secuencia
        fila = {  # Asigna un valor a una variable
            "NUMERO": f"{contador:03d}",  # Instrucción ejecutable
            "NOMBRE": info["nombre"],  # Instrucción ejecutable
            "DIRECCION": info["direccion"],  # Instrucción ejecutable
            "COORDENADAS": info["coords"],  # Instrucción ejecutable
            "PRECIO": info["precio"],  # Instrucción ejecutable
            "LINK": info["link"]  # Instrucción ejecutable
        }  # Instrucción ejecutable
        lista_para_df.append(fila)    # Instrucción ejecutable
        contador += 1  # Asigna un valor a una variable
            
    df = pd.DataFrame(lista_para_df, columns=["NUMERO", "NOMBRE", "DIRECCION", "COORDENADAS", "PRECIO", "LINK"])  # Asigna un valor a una variable
    return df  # Devuelve el resultado desde la función

if __name__ == "__main__":  # Punto de entrada cuando se ejecuta el script directamente
    pass  # Instrucción ejecutable