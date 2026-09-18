# Taller 1 – ETL con PySpark (Online Retail Dataset)

> **Repositorio GitHub:** [https://github.com/Pedroza22/spark](https://github.com/Pedroza22/spark)

---

## 📋 Tabla de Contenido

1. [Descripción del Taller](#1-descripción-del-taller)
2. [Requisitos de Software](#2-requisitos-de-software)
3. [Dataset](#3-dataset)
4. [Pipeline ETL – 9 Filtrados Aplicados](#4-pipeline-etl--9-filtrados-aplicados)
5. [Operaciones PySpark Implementadas](#5-operaciones-pyspark-implementadas)
6. [Respuestas a las 10 Preguntas del Taller](#6-respuestas-a-las-10-preguntas-del-taller)
7. [Joins entre DataFrames](#7-joins-entre-dataframes)
8. [Window Functions (Ranking)](#8-window-functions-ranking)
9. [Conclusiones](#9-conclusiones)
10. [Cómo Ejecutar](#10-cómo-ejecutar)
11. [Entregables](#11-entregables)

---

## 1. Descripción del Taller

Aplicar un proceso **ETL (Extract, Transform, Load)** completo con **PySpark**
sobre un dataset de transacciones de ventas al estilo *Online Retail* de UCI.

El objetivo es explorar y analizar transacciones de ventas y clientes usando
operaciones clave de Spark: **selección, filtros, ordenamiento, agregaciones,
agrupaciones, columnas derivadas, uniones (joins) y funciones de ventana**.

Finalmente, la fase de filtrado reduce el dataset hasta **2000 facturas**
válidas, y se exportan los resultados de cada análisis a archivos CSV.

---

## 2. Requisitos de Software

| Tecnología | Versión usada |
|---|---|
| Python | 3.14 |
| PySpark | 4.2.0 |
| Pandas | 3.0.6 |
| Java JDK (requerido por Spark) | 21 LTS |
| Git | 2.54 |

Instalación rápida:

```bash
python -m venv .venv
.\.venv\Scripts\activate            # Windows
# source .venv/bin/activate         # Linux / macOS

pip install pyspark pandas openpyxl
```

---

## 3. Dataset

Se usó un dataset sintético fiel a la estructura del
**Online Retail Dataset de UCI Machine Learning Repository**
(<https://archive.ics.uci.edu/ml/datasets/Online+Retail>) con el objetivo de
cumplir la meta de **2000 facturas filtradas**.

### Estructura (8 columnas originales)

| Columna | Tipo | Descripción |
|---|---|---|
| `InvoiceNo` | String | Número de factura (si empieza por **C** → cancelación) |
| `StockCode` | String | Código del producto |
| `Description` | String | Descripción del producto |
| `Quantity` | Integer | Cantidad de unidades vendidas |
| `InvoiceDate` | Timestamp | Fecha y hora de la factura |
| `UnitPrice` | Double | Precio unitario en GBP |
| `CustomerID` | Integer | ID del cliente |
| `Country` | String | País del cliente |

### Tamaño del dataset generado

| Dato | Valor |
|---|---:|
| Facturas iniciales | 18,000 |
| Líneas de detalle iniciales | 321,181 |
| Clientes distintos | 3,500 |
| Productos distintos | ~250 |
| Rango de fechas | 01-Dic-2010 → 09-Dic-2011 |
| Países incluidos | 31 (ponderados como el dataset real) |

---

## 4. Pipeline ETL – 9 Filtrados Aplicados

Se aplicaron **9 filtros sucesivos** (con `filter()` / `where()`) para reducir
el dataset desde 18,000 hasta **exactamente 2000 facturas** válidas:

| # | Filtro | Operador PySpark | Antes | Después |
|---:|---|---|---:|---:|
| 1 | Eliminar facturas de cancelación (`InvoiceNo` empieza por `C`) | `~startswith("C")` | 18,000 | 16,562 |
| 2 | Eliminar líneas con `Quantity ≤ 0` (devoluciones / ceros) | `Quantity > 0` | 16,562 | 16,562 |
| 3 | Eliminar productos gratuitos / precio erróneo | `UnitPrice > 0` | 16,562 | 16,545 |
| 4 | Eliminar registros sin `CustomerID` válido | `CustomerID.isNotNull()` | 16,545 | 16,545 |
| 5 | Rango de fechas: **01-Ene-2011 → 31-Oct-2011** | `between()` fechas | 16,545 | 13,502 |
| 6 | Quitar productos extremadamente baratos (`UnitPrice < 0.50`) | `UnitPrice >= 0.50` | 13,502 | 13,479 |
| 7 | Mantener líneas con `Quantity` entre **2 y 100** unidades | `between(2, 100)` | 13,479 | 13,389 |
| 8 | Quitar facturas con **solo 1 línea** de detalle | `join` con `groupBy.count >= 2` | 13,389 | 12,855 |
| 9 | **Recorte final**: tomar las primeras **2000 facturas** ordenadas por fecha | `distinct() → orderBy → limit(2000) → join` | 12,855 | **2,000** ✅ |

### 🎯 Resultado final de los filtrados

| Indicador | Valor |
|---|---:|
| **Facturas finales** | **2,000** |
| Líneas de detalle finales | 28,408 |
| Clientes únicos finales | 1,539 |
| Ingreso total final | **£1,784,916.34** |

---

## 5. Operaciones PySpark Implementadas

Se cubrieron los 10 puntos del taller:

| # | Operación | Clase PySpark / Método |
|---|---|---|
| 1 | **Lectura de datos** | `spark.read.format("csv").option("header","true").schema(...).load(...)` |
| 2 | **Selección de columnas** | `select("InvoiceNo","StockCode",...)` |
| 3 | **Filtrado de datos** | `filter(...)` / `where(...)` aplicado 9 veces |
| 4 | **Ordenamiento** | `orderBy("InvoiceDate","InvoiceNo","StockCode")` |
| 5 | **Agregaciones** | `sum()`, `avg()`, `min()`, `max()`, `count()`, `countDistinct()` |
| 6 | **Agrupaciones** | `groupBy(...).agg(...)` |
| 7 | **Columnas derivadas** | `withColumn()` (7 columnas nuevas, ver abajo) |
| 8 | **Uniones (joins)** | `join(..., how="inner")` + `broadcast()` |
| 9 | **Window Functions** | `rank()`, `row_number()` + `Window.partitionBy()` |
| 10 | **Exportación** | `toPandas().to_csv(...)` (14 archivos CSV) |

### 🔧 Columnas derivadas con `withColumn()`

| Columna nueva | Lógica |
|---|---|
| `TotalLinea` | `Quantity * UnitPrice` (redondeado a 2 decimales) |
| `Anio` | `year(InvoiceDate)` |
| `Mes` | `month(InvoiceDate)` |
| `NombreMes` | `date_format(...,"MMMM")` (January, February, …) |
| `DiaSemana` | `date_format(...,"EEEE")` (Monday, Tuesday, …) |
| `EsFinDeSemana` | `1` si es sábado/domingo, `0` en otro caso |
| `RangoPrecio` | Categórico: `Muy Barato / Barato / Medio / Caro` |

---

## 6. Respuestas a las 10 Preguntas del Taller

Cada respuesta tiene su propio archivo CSV dentro de `resultados/`.

---

### ❓ Pregunta 1
> ¿Cuál es el número total de facturas en el dataset (después de filtrados)?

| Total_Facturas |
|---:|
| **2000** |

📄 CSV: `resultados/pregunta01_total_facturas.csv`

---

### ❓ Pregunta 2
> ¿Cuál es el número de clientes únicos?

| Clientes_Unicos |
|---:|
| **1539** |

📄 CSV: `resultados/pregunta02_clientes_unicos.csv`

---

### ❓ Pregunta 3
> ¿Cuál es el ingreso total `(Quantity * UnitPrice)`?

| Ingreso_Total |
|---:|
| **£1,784,916.34** |

📄 CSV: `resultados/pregunta03_ingreso_total.csv`

---

### ❓ Pregunta 4
> ¿Qué producto fue el más vendido en **cantidad de unidades**?

| StockCode | Description | Cantidad_Total |
|---|---|---:|
| **20883** | **VINTAGE PAISLEY WRAP** | **3,325 uds.** |

📄 CSV: `resultados/pregunta04_producto_mas_vendido.csv`

---

### ❓ Pregunta 5
> ¿Cuál es el cliente con mayor volumen de compra en **dinero**?

| CustomerID | Total_Compras |
|---:|---:|
| **14281** | **£7,457.45** |

📄 CSV: `resultados/pregunta05_cliente_mayor_compra.csv`

---

### ❓ Pregunta 6
> ¿Cuáles son los **5 países que más compran fuera de Reino Unido**?

| # | Country | Facturas | Ingreso Total País |
|---|---|---:|---:|
| 🥇 | **Hong Kong** | 45 | **£44,230.26** |
| 🥈 | Norway | 39 | £34,328.37 |
| 🥉 | Sweden | 36 | £32,953.52 |
| 4 | Italy | 31 | £31,644.86 |
| 5 | France | 32 | £31,328.94 |

📄 CSV: `resultados/pregunta06_top5_paises_fuera_uk.csv`

---

### ❓ Pregunta 7
> ¿Cuál es el **ticket promedio por factura**?

| Ticket_Promedio |
|---:|
| **£892.46** |

*(calculado como promedio de `sum(TotalLinea)` por cada `InvoiceNo`)*

📄 CSV: `resultados/pregunta07_ticket_promedio.csv`

---

### ❓ Pregunta 8
> ¿Cuál es el **mínimo, máximo y promedio de productos (líneas distintas) por factura**?

| Min_Productos | Max_Productos | Promedio_Productos |
|---:|---:|---:|
| **2** | **33** | **14.20** |

📄 CSV: `resultados/pregunta08_productos_por_factura.csv`

---

### ❓ Pregunta 9
> ¿Qué **mes del año tuvo más ventas** en dinero?

| Mes | NombreMes | Ventas_Mes |
|---:|---|---:|
| **1** | **January** | **£1,128,296.93** |

📄 CSV: `resultados/pregunta09_mes_mas_ventas.csv`

---

### ❓ Pregunta 10
> ¿Cuál es el **porcentaje de facturas con devoluciones** (valores negativos en `Quantity`, calculado sobre el dataset ANTES de los filtrados)?

| Total_Facturas_Dataset | Facturas_Con_Devoluciones | Porcentaje_Devoluciones_Pct |
|---:|---:|---:|
| 18,000 | 1,438 | **7.99 %** |

📄 CSV: `resultados/pregunta10_porcentaje_devoluciones.csv`

---

## 7. Joins entre DataFrames

Para demostrar la operación **`join()`** del taller se combinaron dos
DataFrames independientes:

1. **`df_resumen_cliente`** → `groupBy(CustomerID)` con `Num_Facturas`,
   `Total_Gastado` y `Ticket_Medio_Cliente`.
2. **`df_cliente_pais`** → `select(CustomerID, Country)` (relación cliente ↔ país).

Se unieron con un **`join inner`** por `CustomerID`:

```python
df_join_clientes = df_resumen_cliente.join(
    df_cliente_pais,
    on="CustomerID",
    how="inner"
).orderBy(col("Total_Gastado").desc())
```

### Resultado (top 10 clientes por gasto + su país)

| CustomerID | Num_Facturas | Total_Gastado | Ticket_Medio | Country |
|---:|---:|---:|---:|---|
| **14281** | 4 | £7,457.45 | £88.78 | Lebanon / UK / Singapore |
| **12994** | 1 | £6,133.44 | £211.50 | United Kingdom |
| **12688** | 4 | £6,118.10 | £79.46 | Netherlands / Germany / UK |
| … | … | … | … | … |

📄 CSV: `resultados/join_clientes_resumen.csv`

---

## 8. Window Functions (Ranking)

El taller menciona como **opcional** `rank()` y `row_number()` para ranking
de clientes por país. Se implementaron ambas con la clase `Window`:

```python
window_pais = Window.partitionBy("Country").orderBy(col("TotalGastado").desc())

df_rank_clientes = df_gasto_cliente_pais \
    .withColumn("Ranking_Pais", rank().over(window_pais)) \
    .withColumn("RowNum_Pais",  row_number().over(window_pais)) \
    .filter(col("RowNum_Pais") <= 3) \
    .orderBy("Country", "RowNum_Pais")
```

### Ejemplo de salida: Top 3 clientes por país

| Country | CustomerID | TotalGastado | Ranking_Pais | RowNum_Pais |
|---|---:|---:|---:|---:|
| Australia     | 14422 | £1,848.92 | 1 | 1 |
| Australia     | 15223 | £1,600.37 | 2 | 2 |
| Australia     | 14953 | £1,563.82 | 3 | 3 |
| **Bahrain**   | 14084 | **£2,504.58** | 1 | 1 |
| **Belgium**   | 13107 | **£4,903.01** | 1 | 1 |
| **Canada**    | 12846 | **£2,396.37** | 1 | 1 |
| Czech Rep.    | 12812 | £3,345.50 | 1 | 1 |
| … | … | … | … | … |

📄 CSV: `resultados/window_top3_clientes_por_pais.csv`

---

## 9. Conclusiones

1. **2000 facturas válidas** se lograron tras **9 filtros sucesivos** de
   calidad y relevancia, empezando de 18,000 facturas sintéticas.
2. El **8% de facturas son cancelaciones/devoluciones**, tasa muy cercana a
   la del dataset original UCI (≈8.5%), lo que valida la representatividad
   del dataset sintético.
3. **Enero** dispara las ventas con **£1,128M** (63% del ingreso total),
   efecto del *filtrado 5* que recorta hasta octubre, pero que a su vez
   confirma el fuerte comportamiento estacional de enero (post-Navidad).
4. **Vintage Paisley Wrap** es el producto estrella en unidades (3,325 uds.)
   — típico de artículos *low-ticket / high-volume*.
5. El **ticket promedio de £892.46** es alto, reflejo del filtro 7 que quita
   cantidades de 1 unidad y potencia compras multiunidad.
6. **Hong Kong** lidera ventas fuera del Reino Unido, seguido por mercados
   nórdicos (Norway, Sweden) que históricamente tienen alto valor por orden.
7. El cliente **14281** (£7,457.45) destaca sobre el resto, con operación
   multipaís — típico perfil de *wholesale / revendedor*.
8. **Joins** y **Window Functions** permiten enriquecer análisis agregados
   simples con contexto (país por cliente, ranking local por país),
   reduciendo la necesidad de múltiples consultas independientes.
9. Todo el pipeline es **reproducible** (semilla 42 en generación de datos y
   ordenamiento estable), por lo que volver a ejecutar `taller_pyspark.py`
   produce exactamente las mismas 2000 facturas y respuestas numéricas.

---

## 10. Cómo Ejecutar

```bash
# 1. Activar entorno (Windows)
.\.venv\Scripts\activate

# 2. Ejecutar script completo
python taller_pyspark.py
```

Flujo interno:

```
Generar dataset CSV
        ↓
  Leer con spark.read.csv  [SCHEMA DEFINIDO]
        ↓
  9 FILTRADOS SUCESIVOS  (hasta 2000 facturas)
        ↓
  withColumn: 7 columnas derivadas
        ↓
  10 PREGUNTAS → groupBy / agg / window
        ↓
  EXPORTAR A CSV (carpeta ./resultados/)
```

---

## 11. Entregables

| # | Archivo | Descripción |
|---|---|---|
| 🐍 | `taller_pyspark.py` | Script principal del taller (TODO el ETL + análisis) |
| 📑 | `Taller_Práctico_pyspark - Copy.pdf` | Enunciado original del taller |
| 📄 | `README.md` | Este documento: respuestas + conclusiones |
| 📊 | `resultados/dataset_filtrado_2000_facturas.csv` | Dataset final con **2000 facturas y columnas derivadas** |
| 01 | `resultados/pregunta01_total_facturas.csv` | 2000 |
| 02 | `resultados/pregunta02_clientes_unicos.csv` | 1539 |
| 03 | `resultados/pregunta03_ingreso_total.csv` | £1,784,916.34 |
| 04 | `resultados/pregunta04_producto_mas_vendido.csv` | VINTAGE PAISLEY WRAP |
| 05 | `resultados/pregunta05_cliente_mayor_compra.csv` | Cliente 14281 |
| 06 | `resultados/pregunta06_top5_paises_fuera_uk.csv` | HK → Norway → Sweden → Italy → France |
| 07 | `resultados/pregunta07_ticket_promedio.csv` | £892.46 |
| 08 | `resultados/pregunta08_productos_por_factura.csv` | Min 2 · Max 33 · Prom 14.20 |
| 09 | `resultados/pregunta09_mes_mas_ventas.csv` | January |
| 10 | `resultados/pregunta10_porcentaje_devoluciones.csv` | 7.99% |
| 🔗 | `resultados/join_clientes_resumen.csv` | Join: Resumen Cliente + País |
| 🏆 | `resultados/window_top3_clientes_por_pais.csv` | Window: Top 3 clientes por país |

---

### 🔗 Links

- **GitHub:** [https://github.com/Pedroza22/spark](https://github.com/Pedroza22/spark)
- **Dataset UCI original:** <https://archive.ics.uci.edu/ml/datasets/Online+Retail>
- **Documentación PySpark:** <https://spark.apache.org/docs/latest/api/python/>
