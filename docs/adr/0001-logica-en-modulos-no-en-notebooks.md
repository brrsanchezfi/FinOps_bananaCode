# ADR 0001 — La logica de negocio vive en modulos de Python, no en notebooks

- **Estado:** aceptada
- **Fecha:** 2026-08-05

## Contexto

El pedido inicial era construir la analitica FinOps "con la logica principal en
modulos de Python y el orquestador general en un notebook". Habia que decidir
cuanto ponía cada lado.

El patron habitual en proyectos de datos sobre Databricks es escribir la logica
directamente en notebooks: es lo que la plataforma facilita, y el ciclo de
prueba es inmediato. El costo aparece despues: un notebook no se puede probar en
CI, no se puede refactorizar con seguridad, no se puede importar desde otro sitio
y su diff en git es ilegible.

## Decision

**Todo lo que decide algo vive en `src/finops/`. Los notebooks leen parametros,
llaman al paquete e imprimen resultados.**

Ademas, dentro del paquete se separa:

- **Funciones puras** (`analytics/`, `transform/pricing.py`, `transform/tags.py`,
  `alerting/rules.py`, `quality/checks.py`): reciben y devuelven `dict`, `list` y
  dataclasses. Sin Spark.
- **Adaptadores** (`spark_utils.py`, `ingestion/`, `transform/silver.py`,
  `transform/gold.py`): leen, escriben y mapean. Con Spark.

Las importaciones de `pyspark` son perezosas, de modo que el paquete se importa
en una maquina que no lo tiene instalado.

## Consecuencias

**A favor**

- La suite completa (360+ pruebas) corre en segundos, sin cluster, en cada PR.
  Un error en la deteccion de anomalias o en el calculo de un presupuesto se
  detecta antes de desplegar, no en produccion.
- La CLI puede validar configuracion y dashboards en CI sin credenciales.
- Los notebooks quedan en ~30 lineas y son legibles como documentacion.
- Refactorizar es seguro: hay pruebas que respaldan.

**En contra**

- Mas ceremonia para un cambio pequeno: hay que tocar un modulo, no una celda.
- Requiere construir e instalar un wheel para los jobs. Se mitiga permitiendo que
  los notebooks agreguen `src/` al `sys.path` cuando el wheel no esta.
- Las transformaciones de Spark siguen sin cobertura de pruebas unitarias; se
  validan al ejecutarse. Es la deuda tecnica conocida #2.

## Alternativas consideradas

**Todo en notebooks.** Descartada: sin pruebas automatizadas, un error de calculo
de costo puede pasar meses inadvertido y contaminar decisiones de presupuesto.

**Todo en un wheel, sin notebooks.** Descartada: el notebook es la interfaz
natural para exploracion y diagnostico en Databricks, y el pedido lo incluia
explicitamente. Se conservan como orquestadores y como herramienta de
investigacion (`90_exploracion`).

**dbt para las transformaciones.** Descartada para esta version: dbt cubriria
bien bronze→silver→gold, pero no la analitica (anomalias, pronostico, motor de
reglas), que igual necesitaria Python. Mantener un solo lenguaje y un solo
mecanismo de despliegue simplifica la operacion. Es una opcion razonable a futuro
si el modelo dimensional crece mucho.
