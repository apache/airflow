<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Traducción al Español de la UI de Apache Airflow

Este documento describe los principios de traducción elegidos para el idioma español.
Su propósito es documentar las decisiones de traducción y garantizar la consistencia
en futuras iteraciones.

## Tono y registro

Se utiliza un español neutro y formal, adecuado para software profesional.
Se evita el uso directo de "tú" o "usted", prefiriendo construcciones impersonales
cuando es posible (por ejemplo: "Presiona {{hotkey}} para descargar los registros").
Esto permite que la interfaz sea accesible para usuarios de cualquier región hispanohablante.

## Términos que se mantienen en inglés

Los siguientes términos no se traducen, ya que son conceptos centrales de Airflow
que aparecen de forma consistente en el código, logs y documentación:

- `Dag` / `Dags`: Concepto central de Airflow. Siempre "Dag", nunca "DAG".
  Se trata como sustantivo masculino en español (por ejemplo: "Ejecución del Dag").
- `Asset` / `Assets`: Concepto introducido en Airflow 3. Se mantiene en inglés
  (por ejemplo: "Evento de Asset", "Eventos de Asset Fuente").
- `Backfill` / `Backfills`: Concepto específico de Airflow para rellenar ejecuciones pasadas.
- `XCom` / `XComs`: Mecanismo de comunicación entre tareas.
- `Pool` / `Pools`: Concepto de pool de recursos. Una traducción precisa como
  "grupo de recursos" sería innecesariamente larga.
- `Plugins`: Término técnico ampliamente comprendido en español.
- `Executor`: Nombre del componente de Airflow.
- `Triggerer`: Nombre del componente de Airflow.
- `Bundle`: Concepto de bundle en Airflow.
- `Catchup`: Concepto específico de Airflow.
- `ID`: Identificador técnico universal.
- Niveles de log (`CRITICAL`, `ERROR`, `WARNING`, `INFO`, `DEBUG`):
  Se mantienen en inglés ya que coinciden con la salida de los logs.

## Definiciones de traducciones de términos específicos de Airflow

Para la traducción al español se han adoptado las siguientes convenciones
(en orden alfabético):

- `Actions` → `Acciones`: Traducción directa.
- `Admin` → `Administración`: Se prefiere la forma completa sobre "Admin".
- `Asset Event` → `Evento de Asset`: Se mantiene "Asset" en inglés por consistencia.
- `Browse` → `Navegar`: Traducción directa para la navegación de la UI.
- `Configuration` → `Configuración`: Traducción directa.
- `Connections` → `Conexiones`: Aunque es un término técnico en Airflow,
  la traducción directa es clara y comprensible.
- `Dag Run` → `Ejecución del Dag`: Se usa "del" (contracción de "de el")
  ya que "Dag" se trata como masculino.
- `Deferred` (estado) → `Diferido`: El término más cercano al significado original,
  ya que una tarea es delegada al componente Triggerer.
- `Delete` → `Eliminar`: Traducción estándar para acciones destructivas.
- `Documentation` → `Documentación`: Traducción directa.
- `Duration` → `Duración`: Traducción directa.
- `Failed` → `Fallido`: Estado de fallo de una ejecución o tarea.
- `Filter` → `Filtro`: Traducción directa.
- `Home` → `Inicio`: Traducción estándar para la página principal.
- `Logout` → `Cerrar Sesión`: Expresión natural en español.
- `Map Index` → `Mapa de Índice`: Traducción descriptiva para el índice
  de tareas mapeadas.
- `Operator` → `Operador`: Término técnico que coincide en ambos idiomas.
- `Owner` → `Propietario`: Traducción directa.
- `Permissions` → `Permisos`: Traducción directa.
- `Planned` → `Planificado`: Estado planificado de una tarea.
- `Providers` → `Proveedores`: Traducción directa.
- `Queued` → `En Cola`: Estado de espera en la cola de ejecución.
- `Removed` → `Removido`: Estado de tarea removida.
- `Reset` → `Restablecer`: Traducción estándar para acciones de restablecimiento.
- `Restarting` → `Reiniciando`: Estado de reinicio.
- `Roles` → `Roles`: Término idéntico en ambos idiomas.
- `Running` → `En Ejecución`: Estado de ejecución activa.
- `Scheduled` → `Programado`: Se refiere a ejecuciones cíclicas planificadas.
- `Security` → `Seguridad`: Traducción directa.
- `Skipped` → `Omitido`: Estado de tarea omitida.
- `Success` → `Exitoso`: Estado de ejecución exitosa.
- `Tags` → `Etiquetas`: Marcadores para organizar Dags.
- `Task` → `Tarea`: Traducción directa del concepto de tarea.
- `Task Instance` → `Instancia de Tarea`: Traducción directa.
- `Timezone` → `Zona Horaria`: Traducción estándar.
- `Trigger` (verbo) → `Activar`: Se usa "Activar" para la acción de iniciar
  una ejecución (por ejemplo: "Activado por").
- `Trigger Rule` → `Regla de Activación`: Consistente con la traducción del verbo.
- `Try Number` → `Intento Número`: Traducción directa.
- `Users` → `Usuarios`: Traducción directa.
- `Variables` → `Variables`: Término idéntico en ambos idiomas.
- `Wrap` / `Unwrap` → `Envolver` / `Desenvolver`: Traducción directa para
  la función de ajuste de texto.

## Notas especiales

- El verbo "parsear" (del inglés "parse") se usa como préstamo lingüístico
  aceptado en el contexto técnico (por ejemplo: "Duración del parseo",
  "Último Parseado").
- `states.none` y `states.no_status` se traducen ambos como **"Sin Estado"**.
- Se usa capitalización tipo título para encabezados, botones y elementos
  de navegación (por ejemplo: "Todas las Ejecuciones").
- Se usa capitalización tipo oración para descripciones y mensajes
  (por ejemplo: "No se encontraron resultados").
