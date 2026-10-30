"""
Genera las visualizaciones del análisis batch de COVID-19 en Colombia.
Lee los CSV que deja procesamiento_batch_covid.py y guarda imágenes PNG.

Uso:
    python3 generar_graficos.py [CARPETA_DATOS] [CARPETA_SALIDA]
"""

import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

CARPETA_DATOS = sys.argv[1] if len(sys.argv) > 1 else os.path.expanduser("~/graficos_covid/datos")
CARPETA_SALIDA = sys.argv[2] if len(sys.argv) > 2 else os.path.expanduser("~/graficos_covid/salidas")
os.makedirs(CARPETA_SALIDA, exist_ok=True)

AZUL, ROJO, GRIS = "#1F5A96", "#C0392B", "#7F8C8D"


def leer(nombre):
    return pd.read_csv(os.path.join(CARPETA_DATOS, f"{nombre}.csv"))


def guardar(nombre):
    plt.tight_layout()
    plt.savefig(os.path.join(CARPETA_SALIDA, nombre), dpi=180)
    plt.close()


# 1. Casos por mes (curva epidémica)
mes = leer("casos_por_mes").sort_values("anio_mes")
plt.figure(figsize=(12, 5))
plt.plot(mes["anio_mes"], mes["casos"], color=AZUL, linewidth=2, marker="o", markersize=3)
plt.title("Casos positivos de COVID-19 por mes en Colombia")
plt.xlabel("Mes de diagnóstico")
plt.ylabel("Casos")
plt.xticks(mes["anio_mes"][::3], rotation=45)
plt.grid(alpha=0.3)
guardar("01_casos_por_mes.png")

# 2. Top 10 departamentos
dep = leer("casos_por_departamento").sort_values("casos", ascending=False).head(10)
plt.figure(figsize=(10, 6))
plt.barh(dep["departamento"][::-1], dep["casos"][::-1], color=AZUL)
plt.title("Top 10 departamentos/distritos con más casos")
plt.xlabel("Casos")
plt.grid(axis="x", alpha=0.3)
guardar("02_top_departamentos.png")

# 3. Letalidad por grupo de edad
edad = leer("letalidad_por_edad").sort_values("grupo_edad")
plt.figure(figsize=(10, 5))
barras = plt.bar(edad["grupo_edad"], edad["letalidad_pct"], color=ROJO)
for b, v in zip(barras, edad["letalidad_pct"]):
    plt.text(b.get_x() + b.get_width() / 2, b.get_height(), f"{v:.1f}%",
             ha="center", va="bottom", fontsize=9)
plt.title("Letalidad (%) por grupo de edad")
plt.xlabel("Grupo de edad (años)")
plt.ylabel("Fallecidos / casos (%)")
plt.grid(axis="y", alpha=0.3)
guardar("03_letalidad_por_edad.png")

# 4. Casos por grupo de edad y sexo
sexo = leer("casos_sexo_edad").sort_values("grupo_edad").fillna(0)
x = range(len(sexo))
plt.figure(figsize=(10, 5))
plt.bar([i - 0.2 for i in x], sexo["F"], width=0.4, label="Mujeres", color=AZUL)
plt.bar([i + 0.2 for i in x], sexo["M"], width=0.4, label="Hombres", color=GRIS)
plt.xticks(list(x), sexo["grupo_edad"])
plt.title("Casos por grupo de edad y sexo")
plt.xlabel("Grupo de edad (años)")
plt.ylabel("Casos")
plt.legend()
plt.grid(axis="y", alpha=0.3)
guardar("04_casos_sexo_edad.png")

print("Gráficos generados en", CARPETA_SALIDA)
for archivo in sorted(os.listdir(CARPETA_SALIDA)):
    print(" -", archivo)
