# Business Explorer

Herramienta de inteligencia de negocio que genera un ranking de locales comerciales óptimos mediante análisis de mercado automatizado. A partir de unos requisitos introducidos por consola, busca locales y oficinas en venta o alquiler y los puntúa según precio, competencia cercana, reputación online y conectividad.

## Cómo funciona

`app.py` orquesta cuatro módulos que se ejecutan en cadena:

1. **`p1_busqueda_local.py`: búsqueda de inmuebles.** Scraping de pisos.com con Selenium (navegación headless) y BeautifulSoup, con filtros por precio y tipo de inmueble, y geocodificación de las direcciones con `geopy`.
2. **`p2_competencia.py`: competencia.** Obtiene el código postal de cada local por geocodificación inversa y consulta OpenStreetMap para detectar negocios competidores de la misma actividad en la zona.
3. **`p3_reputacion.py`: reputación online.** Busca con Selenium la valoración online de los negocios competidores para estimar la calidad de la competencia.
4. **`p4_transporte.py`: conectividad.** Cuenta paradas de autobús, metro y tren cercanas a cada local con datos de OpenStreetMap y calcula las distancias.

Con todos los datos, `calcular_puntuacion` en `app.py` puntúa cada local y genera el ranking final (`resultados_finales_ranking.csv` es un ejemplo de salida).

## Modo de uso

- Ejecutar `app.py` en la misma carpeta que el resto de scripts.
- Introducir los parámetros.
- Esperar aproximadamente 5 minutos (dependiendo de la oferta).
- Obtener el ranking, tanto en Descargas (CSV) como en la consola (los 5 mejores).

## Tecnologías

Python, Selenium, BeautifulSoup, pandas, requests, geopy y OpenStreetMap. Hace falta tener instalado Google Chrome para el scraping.

Proyecto de equipo realizado en la UPV.
