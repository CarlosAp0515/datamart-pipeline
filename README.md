# DataMart S.A.S - Pipeline ETL con Apache Airflow

Este repositorio contiene la solución funcional de extremo a extremo para el caso DataMart S.A.S. El pipeline ingesta datos operativos diarios e históricos, limpia anomalías, aplica reglas de negocio y carga los resultados en un Data Warehouse dimensional en PostgreSQL.

Todo el entorno está dockerizado para garantizar su ejecución en **menos de 10 minutos** sin configuraciones manuales.

##  Requisitos Previos
* **Docker** y **Docker Compose** instalados.
* **Git** para clonar el repositorio.

##  Instrucciones de Ejecución (Paso a Paso)

### 1. Clonar el repositorio y preparar credenciales
```bash
git clone <URL_DEL_REPOSITORIO>
cd datamart-etl
cp .env.example .env
```
*(El archivo `.env` ya contiene credenciales seguras por defecto para Airflow y PostgreSQL. No es necesario modificarlas para esta prueba).*

### 2. Levantar la Infraestructura
Ejecuta el siguiente comando para levantar todos los servicios (Airflow Webserver, Scheduler, Base de datos de Metadatos y el Data Warehouse destino):
```bash
docker compose up -d
```
Durante el arranque, el script interno `docker/init_airflow.sh` aprovisionará automáticamente:
* El usuario administrador de Airflow.
* La Connection a PostgreSQL (`dw_postgres`).
* Las Variables operativas (`source_timezone`, `max_rejection_pct`, rutas de archivos).

### 3. Validar Configuración y Ejecutar el Pipeline
1. Abre tu navegador e ingresa a: [http://localhost:8080](http://localhost:8080)
2. Inicia sesión con las credenciales definidas en tu `.env`:
   * **Username:** `admin`
   * **Password:** `Admin_2026_Dm`
3. **Validación Automática:**
   * Ve a **Admin > Connections** y verifica que `dw_postgres` exista y apunte a `dw-db`.
   * Ve a **Admin > Variables** y verifica la existencia de `source_timezone` (Europe/London) y el umbral de rechazos.
4. **Disparar la Extracción:**
   * En la vista de DAGs, busca `datamart_etl_pipeline`.
   * Actívalo usando el interruptor (toggle azul).
   * Haz clic en **Play (Trigger DAG)**.
   * Espera a que todas las tareas queden en verde.

### 4. Verificar Datos en el Repositorio Analítico
Una vez finalizado el DAG, puedes validar que los datos llegaron correctamente y responden a las preguntas de negocio ejecutando:

```bash
docker compose exec -T dw-db psql -U dw_user -d datamart_dw < sql/validation/queries.sql
```

## 📁 Estructura Principal
* `/dags/datamart_etl`: Lógica modular en Python (`extract.py`, `transform.py`, `quality.py`, `load.py`).
* `/data`: Volúmenes montados para lectura de `raw` y escritura de `staging`.
* `/sql/ddl`: Script `01_schema.sql` (Modelo de estrella).
* `/sql/validation`: Consultas de validación (`queries.sql`).
* `/docs`: Diagrama DBML y documento de decisiones (`DECISIONES.md`).

## 📊 Diagrama del Modelo de Datos

![Esquema de Base de Datos](docs/diagrama_modelo.png)