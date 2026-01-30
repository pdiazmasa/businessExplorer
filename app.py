################################################################################
# APP PRINCIPAL: ORQUESTADOR DE INTELIGENCIA DE NEGOCIO (app.py)
#
# DESCRIPCIÓN:
# Núcleo de la aplicación "Business Explorer". Actúa como controlador central que
# gestiona la interacción con el usuario, captura los requisitos del negocio y
# ejecuta secuencialmente la "pipeline" de datos (Búsqueda -> Competencia ->
# Reputación -> Transporte).
#
# Finalmente, aplica un algoritmo de valoración ponderada dinámica (adaptando
# los pesos según la existencia o no de competencia) para calcular una NOTA_FINAL,
# genera un ranking ordenado y exporta los resultados automáticamente a Excel/CSV.
#
# INPUTS:
# - Interacción directa por consola (Ciudad, Tipo, Presupuesto, Negocio).
#
# OUTPUTS:
# - Archivo CSV "resultados_finales_ranking.csv" en carpeta Descargas.
# - Ranking TOP 5 mostrado en pantalla.
################################################################################

import pandas as pd  # Librería para manipulación y análisis de datos estructurados (DataFrames)
import os  # Permite interactuar con el sistema operativo (rutas de archivos, limpiar pantalla)

# Importamos los módulos personalizados del proyecto
import p1_busqueda_local as p1  # Módulo de scraping y geolocalización de inmuebles
import p2_competencia as p2  # Módulo de análisis de competencia y códigos postales
import p3_reputacion as p3  # Módulo de auditoría de reputación online
import p4_transporte as p4  # Módulo de análisis de conectividad y transporte

pd.set_option('display.max_columns', None)  # Fuerza a mostrar todas las columnas al imprimir el DataFrame
pd.set_option('display.width', 1000)  # Amplía el ancho de visualización para evitar saltos de línea en consola

def limpiar_pantalla():
    os.system('cls' if os.name == 'nt' else 'clear')  # Ejecuta el comando de limpieza según el SO por buenas prácticas (sacado de la IA)

def obtener_inputs():

    ################################################################################
    # Gestiona la interfaz de usuario en la terminal (CLI). Limpia la pantalla,
    # muestra el banner de bienvenida y solicita secuencialmente los 5 datos clave
    # necesarios para iniciar el estudio de mercado, validando que el presupuesto
    # sea un valor numérico correcto.
    #
    # RECIBE: Nada.
    # DEVUELVE: Tupla (ciudad, subtipo, operacion, presupuesto, negocio).
    ################################################################################

    limpiar_pantalla()  # Llamamos a la función auxiliar para borrar la consola
    print("========================================================================")
    print("     BUSINESS EXPLORER - Buscador de Locales (puede tardar minutos)     ")
    print("            recuerda que funciona mejor con ciudades grandes            ")
    print("========================================================================")
    
    ciudad = input("1. Ciudad (ej. Madrid, Valencia): ").strip()  # Solicitamos la ciudad y eliminamos espacios extra

    subtipo = input('''2. Tipo de inmueble: 
            Opciones: 'Locales comerciales', 'Oficinas', 'Naves', 'Edificios': ''').strip()  # Solicitamos el tipo de inmueble
    
    operacion = input("3. Operación (Alquiler/Venta): ").strip()  # Solicitamos el tipo de contrato
    
    while True:  # Iniciamos un bucle infinito para validar el presupuesto
        try:
            presupuesto = int(input("4. Presupuesto Máximo (€): "))  # Intentamos convertir la entrada a número entero
            break  # Si funciona, rompemos el bucle
        except ValueError:  # Si el usuario introduce texto en vez de números
            print("   Por favor, introduce un número válido.")
            
    negocio = input("5. ¿Qué negocio vas a montar? (ej. pizzeria, farmacia): ").strip()  # Solicitamos la actividad comercial
    
    return ciudad, subtipo, operacion, presupuesto, negocio  # Devolvemos todos los datos recolectados

def calcular_puntuacion(df, presupuesto_max):

    ################################################################################
    # Motor matemático de decisión. Normaliza las métricas heterogéneas (precio en
    # euros, número de autobuses, reseñas) a una escala común de 0 a 10.
    # Aplica una lógica condicional para la nota final:
    # - Si hay competencia: 40% Precio / 30% Oportunidad / 30% Conectividad.
    # - Si NO hay competencia: (50% Precio / 50% Conectividad) - 1.
    #
    # RECIBE: df (DataFrame completo), presupuesto_max (para normalizar precios).
    # DEVUELVE: DataFrame con la columna 'NOTA_FINAL' añadida.
    ################################################################################

    print("\n--- CALCULANDO NOTAS FINALES ---")
    notas = []  # Inicializamos la lista para guardar las puntuaciones calculadas
    
    # Pre-cálculo para normalización (Min-Max)
    precios = df['PRECIO'].tolist()  # Extraemos la lista de precios de todos los locales
    max_precio = max(precios) if precios else presupuesto_max  # Buscamos el precio más alto para el techo de la escala
    min_precio = min(precios) if precios else 0  # Buscamos el precio más bajo para el suelo de la escala
    
    # Transporte
    scores_transporte = []  # Lista temporal para valores crudos de transporte
    for t in df['NUM_TRANS_PUB']:  # Iteramos sobre la tupla (Bus, Metro, Bici)
        val = (t[0] * 1) + (t[1] * 3) + (t[2] * 1)  # Asignamos pesos: Metro vale triple, Bus y Bici simple
        scores_transporte.append(val)  # Guardamos el valor ponderado
    max_transporte = max(scores_transporte) if scores_transporte and max(scores_transporte) > 0 else 1  # Hallamos el máximo para normalizar
    
    # Competencia (Oportunidad)
    scores_oportunidad = []  # Lista temporal para valores crudos de oportunidad
    for c in df['NUM_COMPETENCIA']:  # Iteramos sobre la tupla (Total, Buenos, Malos, Media)
        total, buenos, malos, _ = c  # Desempaquetamos los valores relevantes
        val = (malos * 2) + (total * 0.5) - (buenos * 3)  # Aplicamos fórmula: Malos suman (oportunidad), Buenos restan (amenaza)
        scores_oportunidad.append(val)  # Guardamos el score de oportunidad
    
    max_oport = max(scores_oportunidad) if scores_oportunidad else 1  # Máximo del grupo para normalizar
    min_oport = min(scores_oportunidad) if scores_oportunidad else 0  # Mínimo del grupo
    rango_oport = max_oport - min_oport if max_oport != min_oport else 1  # Rango total para evitar división por cero

    # --- BUCLE DE CÁLCULO ---
    for i, row in df.iterrows():  # Recorremos el DataFrame fila a fila
        # 1. Nota PRECIO (0-10)
        precio = row['PRECIO']  # Obtenemos precio del local actual
        if max_precio == min_precio:  # Si todos valen lo mismo
            nota_precio = 10  # Asignamos nota máxima
        else:
            nota_precio = 10 * (max_precio - precio) / (max_precio - min_precio)  # Fórmula inversa: más barato = más nota
        
        # 2. Nota CONECTIVIDAD (0-10)
        raw_trans = scores_transporte[i]  # Recuperamos el valor crudo calculado antes
        nota_conec = 10 * (raw_trans / max_transporte)  # Normalizamos sobre el máximo encontrado (regla de tres)
        
        # 3. Nota OPORTUNIDAD (0-10)
        raw_oport = scores_oportunidad[i]  # Recuperamos el valor crudo de oportunidad
        nota_oport = 10 * ((raw_oport - min_oport) / rango_oport)  # Normalizamos entre 0 y 10 usando el rango min-max
        
        # --- PONDERACIÓN DINÁMICA ---
        comp_data = row['NUM_COMPETENCIA']  # Obtenemos datos de competencia
        hay_competencia = comp_data[0] > 0  # Verificamos si existen rivales en la zona (Total > 0)
        
        if hay_competencia:  # ESCENARIO A: Zona con mercado activo
            # Fórmula A: 40% Precio, 30% Oportunidad, 30% Conectividad
            nota_final = (nota_precio * 0.40) + (nota_oport * 0.30) + (nota_conec * 0.30)  # Cálculo ponderado completo
        else:  # ESCENARIO B: Zona desierta o sin datos
            # Fórmula B (Sin competencia): 50% Precio, 50% Conectividad
            nota_final = (nota_precio * 0.50) + (nota_conec * 0.50) - 2 # Repartimos el peso entre lo tangible (precio y transporte) y penalizamos con -2 puntos
            
        notas.append(round(nota_final, 2))  # Redondeamos a 2 decimales y guardamos
        
    df['NOTA_FINAL'] = notas  # Insertamos la columna de notas en el DataFrame
    return df  # Devolvemos el DataFrame puntuado

def main():
    
    ################################################################################
    # Función principal. Define el flujo de ejecución del programa:
    # 1. Recoge inputs.
    # 2. Ejecuta los módulos P1, P2, P3 y P4 secuencialmente pasando los DataFrames.
    # 3. Calcula la puntuación final.
    # 4. Gestiona el guardado seguro del archivo CSV en la carpeta "Descargas".
    # 5. Imprime el informe final con el Ranking TOP 5.
    #
    # RECIBE: Nada (se ejecuta al inicio).
    # DEVUELVE: Nada (efectos secundarios en disco y consola).
    ################################################################################

    ciudad, subtipo, operacion, presupuesto, negocio = obtener_inputs()  # Ejecutamos la toma de datos
    
    # 2. EJECUCIÓN PIPELINE
    # P1: Búsqueda
    df_1 = p1.busqueda(ciudad, subtipo, operacion, presupuesto)  # Llamamos al módulo de scraping
    if df_1.empty:  # Si no hay resultados
        print("No se encontraron locales con esos criterios. Fin del programa.")
        return  # Terminamos la ejecución

    # P2: Competencia
    df_2 = p2.busqueda_competencia(df_1, radio=500, negocio=negocio)  # Llamamos al módulo de competencia
    
    # P3: Reputación
    df_3 = p3.analizar_reputacion(df_2)  # Llamamos al módulo de reputación online
    
    # P4: Transporte
    df_4 = p4.analizar_transporte(df_3, ciudad=ciudad)  # Llamamos al módulo de transporte
    
    # 3. CÁLCULO DE NOTA FINAL
    df_final = calcular_puntuacion(df_4, presupuesto)  # Ejecutamos el algoritmo de decisión
    
    # Ordenar por mejor nota
    df_final = df_final.sort_values(by='NOTA_FINAL', ascending=False)  # Ordenamos descendente (mejores primero)
    
    # 4. GUARDADO EN DESCARGAS (NUEVO)
    try:  # Intentamos guardar en la ruta ideal
        # Construye la ruta C:\Users\TuUsuario\Downloads\resultados...
        ruta_descargas = os.path.join(os.path.expanduser("~"), "Downloads")  # Obtenemos ruta dinámica a Descargas del usuario
        nombre_archivo = "resultados_finales_ranking.csv"  # Definimos el nombre del archivo
        ruta_completa = os.path.join(ruta_descargas, nombre_archivo)  # Unimos ruta y nombre
        
        df_final.to_csv(ruta_completa, index=False)  # Guardamos el CSV sin índice numérico
        print(f"\nResultados guardados EXITOSAMENTE en:\n   {ruta_completa}")
        
    except PermissionError:  # Capturamos error si el archivo está abierto por el usuario
        print(f"\nERROR: No se pudo guardar el archivo en {ruta_completa}")
        print("Cierra el archivo Excel si lo tienes abierto e inténtalo de nuevo.")
    except Exception as e:  # Capturamos cualquier otro error de ruta
        # Fallback: Si falla Descargas, intenta en la carpeta local
        print(f"\nNo se encontró la carpeta Descargas ({e}). Guardando en carpeta actual...")
        df_final.to_csv("resultados_finales_ranking.csv", index=False)  # Guardamos en la carpeta del script como respaldo
    
    # 5. PRINT TOP 5
    print("\n" + "="*60)
    print("¡¡¡ TOP 5 LOCALES RECOMENDADOS !!!")
    print("="*60 + "\n")
    
    top_5 = df_final.head(5)  # Seleccionamos las 5 primeras filas
    contador = 1  # Inicializamos contador visual
    
    for idx, row in top_5.iterrows():  # Iteramos sobre el top 5 para mostrar detalles
        comp = row['NUM_COMPETENCIA']  # Extraemos tupla de competencia
        trans = row['NUM_TRANS_PUB']  # Extraemos tupla de transporte
        
        print(f"LOCAL Nº {contador}: {row['NOMBRE']}")
        print(f"Dirección: {row['DIRECCION']}, {row['CODIGO_POSTAL']}")
        print(f"Precio: {row['PRECIO']} €")
        print(f"Nota final: {row['NOTA_FINAL']} sobre 10")
        
        print(f"Nº de locales de competencia cerca: {comp[0]}")
        print(f"Nº de locales con buenas reseñas: {comp[1]}")
        print(f"Nº de locales con malas reseñas: {comp[2]}")
        
        media_str = str(comp[3]) if comp[3] > 0 else "N/A (Sin datos)"  # Formateamos la nota media para que no salga 0.0 si no hay
        print(f"Nota media de la competencia cerca: {media_str}/5")
        
        print(f"Nº de paradas de bus cerca: {trans[0]}")
        print(f"Nº de paradas de metro/tren cerca: {trans[1]}")
        print(f"Nº de paradas de bici cerca: {trans[2]}")
        
        print("-" * 40 + "\n")
        contador += 1  # Incrementamos contador

if __name__ == "__main__":
    main()