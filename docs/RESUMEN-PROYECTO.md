# PymesDataStrategyBackEnd - Documentación del Proyecto

**Repositorio:** https://github.com/jcgmU/PymesDataStrategyBackEnd.git  
**Fecha de actualización:** 12 de Marzo, 2026  
**Estado actual:** MVP Fase 1 COMPLETADO - Sistema ETL funcional con 493 tests pasando

---

## 1. Introducción

### Qué problema resuelve

Las pequeñas y medianas empresas (PYMES) manejan datos críticos para su operación diaria: listas de clientes, inventarios, ventas, proveedores. Estos datos suelen estar dispersos en archivos Excel, CSV, y sistemas desconectados, con problemas comunes:

- **Inconsistencias**: El mismo cliente aparece como "Juan Pérez", "J. Perez", "Juan Perez S.A."
- **Datos faltantes**: Campos vacíos, telefones incompletos, direcciones sin formato
- **Duplicados**: Registros repetidos que complican análisis y reportes
- **Formatos mixtos**: Fechas como "01/03/2024", "1-Mar-24", "Marzo 1, 2024"

### Por qué Human-in-the-Loop (HITL)

La limpieza automática de datos puede ser peligrosa: un algoritmo podría eliminar registros importantes o hacer transformaciones incorrectas. **Human-in-the-Loop** significa que:

1. **La IA sugiere** qué hacer (ej: "Detecté 15 duplicados potenciales")
2. **El usuario decide** si aplicar la sugerencia (ej: revisar duplicados y confirmar)
3. **El sistema ejecuta** la transformación aprobada
4. **El resultado se valida** antes de finalizar

Esto combina la **velocidad del procesamiento automático** con la **precisión del criterio humano**.

### Público objetivo

PYMES que necesitan:
- Limpiar datos de Excel/CSV para análisis
- Integrar múltiples fuentes de datos
- Preparar datos para Business Intelligence
- Migrar de sistemas legacy a nuevas plataformas

---

## 2. Estado del Proyecto

### 🎯 MVP Fase 1: COMPLETADO ✅

**Total: 493 tests pasando**

| Día | Componente | Tests | Estado |
|-----|-----------|--------|--------|
| **DÍA 1** | API + Storage (MinIO, Prisma, BullMQ Producer) | 240 ✅ | Completado |
| **DÍA 2** | Worker + Processing (Parser, Transformaciones) | 243 ✅ | Completado |
| **DÍA 3** | Integration (ETLJobProcessor, Health Checks, E2E) | 253 ✅ | Completado |

### Funcionalidades implementadas

✅ **Gestión de Datasets**
- Subir archivos CSV, Excel, JSON, Parquet (hasta 100MB)
- CRUD completo de datasets
- Validación de tipos de archivo

✅ **Sistema de Trabajos**
- Cola asíncrona con BullMQ (Node.js ↔ Python)
- Estados: QUEUED → PROCESSING → COMPLETED
- Consulta de estado vía `GET /api/v1/jobs/:jobId`
- Manejo de errores y reintentos

✅ **Procesamiento ETL**
- Parser universal para 4 formatos
- 11 transformaciones implementadas
- Arquitectura hexagonal en ambos servicios

✅ **Infraestructura**
- Health checks completos
- Docker Compose para desarrollo
- Base de datos PostgreSQL con migraciones

---

## 3. Arquitectura del Sistema

### Visión general

```
┌─────────────────────────────────────────────────────────────────┐
│                         PYMES Backend                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────┐    BullMQ     ┌─────────────┐                 │
│  │ API Gateway │──────────────▶│ Worker ETL  │                 │
│  │   (Node.js) │               │   (Python)  │                 │
│  │   :3000     │               │   :8000     │                 │
│  └──────┬──────┘               └──────┬──────┘                 │
│         │                             │                         │
│         │ Prisma                      │ SQLAlchemy              │
│         │                             │                         │
│  ┌──────┴──────┐               ┌──────┴──────┐                 │
│  │ PostgreSQL  │               │    MinIO    │                 │
│  │   :5433     │               │   :9000     │                 │
│  └─────────────┘               └─────────────┘                 │
│                                                                 │
│                    ┌─────────────┐                              │
│                    │    Redis    │                              │
│                    │   :6380     │                              │
│                    └─────────────┘                              │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Flujo de datos

```
1. Usuario sube archivo (CSV/Excel/JSON/Parquet)
                    ↓
2. API Gateway valida y guarda en MinIO
                    ↓
3. API crea registro en PostgreSQL (estado: PENDING)
                    ↓
4. API envía trabajo a cola Redis (BullMQ)
                    ↓
5. Worker ETL toma trabajo de la cola
                    ↓
6. Worker descarga archivo desde MinIO
                    ↓
7. Worker parsea y aplica transformaciones
                    ↓
8. Worker guarda resultado en MinIO
                    ↓
9. Worker actualiza estado en PostgreSQL (COMPLETED)
                    ↓
10. Usuario puede descargar resultado procesado
```

### Arquitectura Hexagonal

Ambos servicios (API y Worker) siguen **Arquitectura Hexagonal** para separación limpia:

```
┌─────────────────────────────────────────────┐
│                 HEXAGONAL                   │
├─────────────────────────────────────────────┤
│                                             │
│  ┌─────────────────────────────────────┐   │
│  │            DOMINIO                  │   │  ← Lógica de negocio pura
│  │  Entidades, Value Objects, Errors   │   │    Sin dependencias externas
│  └─────────────────────────────────────┘   │
│                     ↕                       │
│  ┌─────────────────────────────────────┐   │
│  │           APLICACIÓN                │   │  ← Casos de uso
│  │     Use Cases, DTOs, Ports          │   │    Orquesta el dominio
│  └─────────────────────────────────────┘   │
│                     ↕                       │
│  ┌─────────────────────────────────────┐   │
│  │         INFRAESTRUCTURA             │   │  ← Adaptadores
│  │  HTTP, Database, Storage, Queue     │   │    Implementa los puertos
│  └─────────────────────────────────────┘   │
│                                             │
└─────────────────────────────────────────────┘
```

**Beneficios:**
- **Testeable**: El dominio se puede testear sin base de datos
- **Mantenible**: Cambiar Express por Fastify no afecta la lógica
- **Escalable**: Agregar nuevos adaptadores es sencillo

---

## 4. Stack Tecnológico

### API Gateway (Node.js)
- **Express 4.21** - Framework web minimalista y maduro
- **TypeScript 5.7** - Tipado estático para JavaScript
- **Prisma 6.4** - ORM moderno con generación de tipos
- **BullMQ 5.34** - Producer de cola de trabajos (Redis)
- **Zod 3.24** - Validación de esquemas con tipos inferidos
- **Vitest** - Framework de testing rápido

### Worker ETL (Python)
- **FastAPI 0.115** - Framework web moderno con OpenAPI automático
- **Polars 1.23** - Procesamiento de datos ultrarrápido (10x más rápido que Pandas)
- **Pandas 2.2** - Manipulación de datos (fallback para casos específicos)
- **BullMQ 2.9** - Consumer de cola de trabajos (binding Python)
- **Pydantic 2.10** - Configuración y validación con tipos
- **pytest** - Framework de testing estándar para Python

### Infraestructura
- **PostgreSQL 16** - Base de datos relacional con soporte JSONB
- **Redis 7.4** - Broker de mensajes para BullMQ
- **MinIO** - Almacenamiento de objetos compatible con S3
- **Docker Compose** - Orquestación para desarrollo local

---

## 5. Modelo de Datos

### Diagrama del Schema

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│      User       │     │    Dataset      │     │TransformationJob│
├─────────────────┤     ├─────────────────┤     ├─────────────────┤
│ id: cuid        │────▶│ id: cuid        │────▶│ id: cuid        │
│ email: unique   │     │ name: string    │     │ type: enum      │
│ name: string?   │     │ description?    │     │ status: enum    │
│ role: enum      │     │ status: enum    │     │ parameters: json│
│ preferences:json│     │ storageKey      │     │ aiSuggestions?  │
│ createdAt       │     │ fileSizeBytes   │     │ resultStorageKey│
│ updatedAt       │     │ mimeType        │     │ errorMessage?   │
│ lastLoginAt?    │     │ schema: json    │     │ bullmqJobId?    │
└─────────────────┘     │ metadata: json  │     │ createdAt       │
                        │ statistics: json│     │ startedAt?      │
                        │ userId          │     │ completedAt?    │
                        │ createdAt       │     └─────────────────┘
                        │ updatedAt       │              │
                        │ processedAt?    │              │
                        └─────────────────┘              │
                                 │                       │
                                 └───────────────────────┘
                                                         │
                        ┌─────────────────┐              │
                        │   AuditLog      │              │
                        ├─────────────────┤              │
                        │ id: cuid        │◀─────────────┘
                        │ action: string  │
                        │ entityType      │
                        │ entityId        │
                        │ userId?         │
                        │ changes: json   │
                        │ ipAddress?      │
                        │ userAgent?      │
                        │ createdAt       │
                        └─────────────────┘
```

### Estados del flujo

**Dataset Status:**
- `PENDING` → Subido, esperando procesamiento
- `PROCESSING` → Worker está procesando el archivo
- `READY` → Listo para transformaciones
- `ERROR` → Error en el procesamiento
- `ARCHIVED` → Eliminado lógicamente

**Job Status:**
- `QUEUED` → En cola, esperando worker
- `PROCESSING` → Worker está ejecutando
- `COMPLETED` → Completado exitosamente
- `FAILED` → Falló con error
- `CANCELLED` → Cancelado por usuario

---

## 6. API REST

### Endpoints disponibles

| Método | Endpoint | Descripción | Auth |
|--------|----------|-------------|------|
| POST | `/api/v1/datasets` | Subir nuevo dataset | Header: x-user-id |
| GET | `/api/v1/datasets` | Listar datasets | Query: userId |
| GET | `/api/v1/datasets/:id` | Obtener dataset por ID | - |
| DELETE | `/api/v1/datasets/:id` | Eliminar dataset | - |
| POST | `/api/v1/datasets/:id/transform` | Iniciar transformación ETL | Header: x-user-id |
| GET | `/api/v1/datasets/:id/download` | Obtener URL de descarga | - |
| GET | `/api/v1/jobs/:jobId` | Consultar estado de un job | - |
| GET | `/health` | Health check del API | - |

### Ejemplos con curl

#### Subir un dataset

```bash
curl -X POST http://localhost:3000/api/v1/datasets \
  -H "x-user-id: user123" \
  -F "name=ventas-enero-2024" \
  -F "description=Datos de ventas del primer trimestre" \
  -F "file=@ventas.csv"
```

**Respuesta:**
```json
{
  "success": true,
  "data": {
    "id": "cm3abc123def456ghi789",
    "storageKey": "datasets/cm3abc123def456ghi789/ventas.csv",
    "status": "PENDING",
    "jobId": "process-dataset:1710234567890"
  }
}
```

#### Listar datasets

```bash
curl "http://localhost:3000/api/v1/datasets?userId=user123&limit=10&offset=0"
```

**Respuesta:**
```json
{
  "success": true,
  "data": [
    {
      "id": "cm3abc123def456ghi789",
      "name": "ventas-enero-2024",
      "description": "Datos de ventas del primer trimestre",
      "status": "READY",
      "originalFileName": "ventas.csv",
      "fileSizeBytes": 2048576,
      "mimeType": "text/csv",
      "userId": "user123",
      "createdAt": "2026-03-12T10:30:00.000Z",
      "updatedAt": "2026-03-12T10:35:00.000Z"
    }
  ],
  "pagination": {
    "limit": 10,
    "offset": 0
  }
}
```

#### Obtener dataset específico

```bash
curl http://localhost:3000/api/v1/datasets/cm3abc123def456ghi789
```

**Respuesta:**
```json
{
  "success": true,
  "data": {
    "id": "cm3abc123def456ghi789",
    "name": "ventas-enero-2024",
    "description": "Datos de ventas del primer trimestre",
    "status": "READY",
    "originalFileName": "ventas.csv",
    "storageKey": "datasets/cm3abc123def456ghi789/ventas.csv",
    "fileSizeBytes": 2048576,
    "mimeType": "text/csv",
    "schema": {
      "columns": ["fecha", "cliente", "producto", "cantidad", "precio"],
      "types": ["date", "string", "string", "integer", "float"]
    },
    "metadata": {
      "source": "sistema-ventas-v2"
    },
    "statistics": {
      "rows": 15420,
      "columns": 5,
      "nulls": 23,
      "duplicates": 7
    },
    "userId": "user123",
    "createdAt": "2026-03-12T10:30:00.000Z",
    "updatedAt": "2026-03-12T10:35:00.000Z"
  }
}
```

#### Health check

```bash
curl http://localhost:3000/health
```

**Respuesta:**
```json
{
  "status": "ok",
  "checks": {
    "database": true,
    "redis": true,
    "storage": true
  },
  "timestamp": "2026-03-12T15:45:30.123Z"
}
```

---

## 7. Sistema de Transformaciones

### Catálogo de transformaciones

El Worker ETL implementa **11 transformaciones** usando Polars para máximo rendimiento:

| Transformación | Descripción | Parámetros | Ejemplo |
|----------------|-------------|------------|---------|
| **CLEAN_NULLS** | Elimina filas que contienen valores nulos | `columns` (opcional) | Eliminar filas donde 'email' sea null |
| **FILL_NULLS** | Rellena valores nulos con un valor especificado | `fill_value`, `columns` | Rellenar nulls en 'telefono' con "N/A" |
| **TRIM_WHITESPACE** | Elimina espacios en blanco al inicio y final | `columns` | "  Juan  " → "Juan" |
| **UPPERCASE** | Convierte texto a mayúsculas | `columns` | "juan pérez" → "JUAN PÉREZ" |
| **LOWERCASE** | Convierte texto a minúsculas | `columns` | "JUAN PÉREZ" → "juan pérez" |
| **REMOVE_DUPLICATES** | Elimina filas duplicadas | `subset` (columnas a considerar) | Eliminar clientes duplicados por email |
| **CONVERT_TYPE** | Convierte el tipo de datos de una columna | `target_type`, `columns` | "2024-01-15" (str) → 2024-01-15 (date) |
| **RENAME_COLUMN** | Renombra una columna | `old_name`, `new_name` | "nombre_completo" → "nombre" |
| **DROP_COLUMN** | Elimina columnas del dataset | `columns` | Eliminar columna "id_interno" |
| **FILTER_ROWS** | Filtra filas que cumplen una condición | `condition` | Mantener solo ventas > $1000 |
| **MAP_VALUES** | Mapea valores específicos a otros valores | `mapping` | "M"→"Masculino", "F"→"Femenino" |

### Ejemplo de configuración

```json
{
  "transformations": [
    {
      "type": "TRIM_WHITESPACE",
      "columns": ["nombre", "empresa", "direccion"]
    },
    {
      "type": "UPPERCASE",
      "columns": ["nombre"]
    },
    {
      "type": "FILL_NULLS",
      "columns": ["telefono"],
      "params": {
        "fill_value": "No disponible"
      }
    },
    {
      "type": "MAP_VALUES",
      "columns": ["genero"],
      "params": {
        "mapping": {
          "M": "Masculino",
          "F": "Femenino",
          "m": "Masculino",
          "f": "Femenino"
        }
      }
    },
    {
      "type": "REMOVE_DUPLICATES",
      "params": {
        "subset": ["email"]
      }
    }
  ]
}
```

### Flujo de transformación

```
DataFrame Original
        ↓
┌───────────────────┐
│  CLEAN_NULLS      │ → Elimina filas con nulls críticos
└───────────────────┘
        ↓
┌───────────────────┐
│  TRIM_WHITESPACE  │ → Normaliza espacios en blanco
└───────────────────┘
        ↓
┌───────────────────┐
│  CONVERT_TYPE     │ → Convierte tipos de datos
└───────────────────┘
        ↓
┌───────────────────┐
│  MAP_VALUES       │ → Estandariza valores categóricos
└───────────────────┘
        ↓
┌───────────────────┐
│ REMOVE_DUPLICATES │ → Elimina registros duplicados
└───────────────────┘
        ↓
DataFrame Limpio
```

---

## 8. Formatos de Archivo Soportados

### Formatos implementados

| Formato | Extensión | MIME Type | Límite | Parser |
|---------|-----------|-----------|---------|---------|
| **CSV** | `.csv` | `text/csv` | 100MB | Polars + pandas fallback |
| **Excel** | `.xlsx` | `application/vnd.openxmlformats-officedocument.spreadsheetml.sheet` | 100MB | Polars read_excel |
| **JSON** | `.json` | `application/json` | 100MB | Polars read_json |
| **Parquet** | `.parquet` | `application/octet-stream` | 100MB | Polars read_parquet |

### Características del parser

**Detección automática:**
- Encoding (UTF-8, Latin-1, Windows-1252)
- Separadores CSV (`,`, `;`, `\t`)
- Tipos de datos (string, int, float, date, boolean)

**Manejo de errores:**
- Filas malformadas se reportan pero no detienen el proceso
- Fallback a pandas para casos complejos de CSV
- Validación de estructura antes del procesamiento

**Estadísticas generadas:**
```json
{
  "rows": 15420,
  "columns": 5,
  "nulls": 23,
  "duplicates": 7,
  "column_types": {
    "fecha": "date",
    "cliente": "string",
    "producto": "string",
    "cantidad": "integer",
    "precio": "float"
  },
  "memory_usage_mb": 12.5
}
```

---

## 9. Cómo Ejecutar el Proyecto

### Requisitos previos

- **Docker** y **Docker Compose** instalados
- **Node.js 20+** (para desarrollo local)
- **Python 3.12+** con **uv** (para desarrollo local)
- **pnpm** como package manager para Node.js

### Instalación

```bash
# Clonar el repositorio
git clone https://github.com/jcgmU/PymesDataStrategyBackEnd.git
cd PymesDataStrategyBackEnd

# Copiar variables de entorno
cp .env.example .env

# Instalar dependencias
make install
```

### Levantar servicios

```bash
# Opción 1: Todo en Docker (recomendado para empezar)
make up

# Opción 2: Solo infraestructura + desarrollo local
make up-infra
make api-dev    # Terminal 1: API en desarrollo
make worker-dev # Terminal 2: Worker en desarrollo
```

### Verificar que funciona

```bash
# Verificar servicios
make ps

# Health checks
curl http://localhost:3000/health  # API Gateway
curl http://localhost:8000/health  # Worker ETL

# Ver logs en tiempo real
make logs

# Consola de MinIO (storage)
open http://localhost:9001
# Usuario: minioadmin
# Password: minioadmin
```

### Base de datos

```bash
# Ejecutar migraciones (primera vez)
make db-migrate

# Abrir Prisma Studio (interfaz web)
make db-studio

# CLI de PostgreSQL
make psql
```

---

## 10. Cómo Ejecutar Tests

### Tests del API (Node.js + TypeScript)

```bash
cd api

# Ejecutar todos los tests (240 tests)
pnpm test

# Tests con cobertura
pnpm test:coverage

# Tests en modo watch
pnpm test:watch

# Tests específicos
pnpm test -- --grep "DatasetController"
```

**Tipos de tests:**
- **Unitarios**: 192 tests (domain, application layer)
- **Integración**: 48 tests (repository, storage, queue)

### Tests del Worker (Python)

```bash
cd worker

# Ejecutar todos los tests (253 tests)
uv run pytest

# Tests con cobertura
uv run pytest --cov=src --cov-report=html

# Tests específicos
uv run pytest tests/application/test_transformer.py

# Tests en modo verbose
uv run pytest -v
```

**Tipos de tests:**
- **Unitarios**: 201 tests (transformaciones, parsers)
- **Integración**: 52 tests (ETL completo, storage)

### Cobertura de tests

| Componente | Cobertura | Tests |
|------------|-----------|-------|
| API Domain | 95% | 89 tests |
| API Application | 92% | 79 tests |
| API Infrastructure | 85% | 72 tests |
| Worker Transformations | 98% | 134 tests |
| Worker Parsers | 90% | 67 tests |
| Worker Integration | 88% | 52 tests |
| **Total** | **91%** | **493 tests** |

---

## 11. Decisiones de Arquitectura (ADRs)

### ADR-001: PostgreSQL + JSONB sobre MongoDB

**Contexto:** Necesitamos almacenar tanto datos estructurados (usuarios, datasets) como semi-estructurados (metadatos, configuraciones).

**Decisión:** Usar PostgreSQL con columnas JSONB.

**Razones:**
- ✅ **Consistencia ACID**: Transacciones confiables para operaciones críticas
- ✅ **SQL familiar**: El equipo conoce SQL mejor que MongoDB
- ✅ **JSONB flexible**: Columnas como `metadata`, `schema`, `statistics` se adaptan
- ✅ **Herramientas maduras**: Prisma ORM, pgAdmin, backups estándar
- ✅ **Un solo motor**: Simplifica deployment y mantenimiento

**Consecuencias:** Las consultas complejas en campos JSONB requieren índices específicos.

### ADR-002: BullMQ para mensajería Node.js ↔ Python

**Contexto:** El API (Node.js) debe comunicarse asincrónicamente con el Worker (Python).

**Decisión:** Usar BullMQ con Redis como broker.

**Razones:**
- ✅ **Soporte nativo**: BullMQ funciona nativamente en Node.js
- ✅ **Python binding**: Existe bullmq-python para el worker
- ✅ **Redis simple**: Un solo servicio para cola y cache
- ✅ **Features avanzadas**: Retry, delay, priority, cron jobs
- ✅ **Dashboard**: UI web para monitorear trabajos

**Alternativas descartadas:**
- ❌ **RabbitMQ**: Más complejo para nuestro caso de uso
- ❌ **Apache Kafka**: Overkill para el volumen actual
- ❌ **HTTP polling**: Ineficiente y propenso a errores

### ADR-003: Arquitectura Hexagonal

**Contexto:** El código debe ser mantenible, testeable, y adaptable a cambios futuros.

**Decisión:** Implementar Arquitectura Hexagonal en ambos servicios.

**Razones:**
- ✅ **Testeable**: Dominio aislado se testea sin dependencias externas
- ✅ **Flexible**: Cambiar de Express a Fastify no afecta la lógica de negocio
- ✅ **Escalable**: Agregar nuevos adaptadores (GraphQL, gRPC) es directo
- ✅ **Claro**: Separación explícita entre dominio, aplicación, e infraestructura

**Estructura:**
```
src/
├── domain/          # Entidades, value objects, errores (sin imports externos)
├── application/     # Use cases, DTOs, ports (interfaces)
└── infrastructure/  # Adaptadores (HTTP, DB, storage, queue)
```

---

## 12. Próximos Pasos - Fase 2

### MVP Fase 2: Human-in-the-Loop + IA (Siguiente iteración)

#### Funcionalidades prioritarias

**1. Autenticación y usuarios**
- JWT authentication
- Registro/login de usuarios
- Roles y permisos (ADMIN, USER, VIEWER)

**2. Sugerencias de IA**
- Detección automática de problemas en datasets
- Sugerencias de transformaciones basadas en patrones
- Scoring de calidad de datos

**3. Preview de transformaciones**
- Vista previa antes de aplicar transformaciones
- Comparación lado a lado (antes/después)
- Rollback de transformaciones

**4. Frontend web**
- Dashboard para gestionar datasets
- Wizard para aplicar transformaciones
- Visualización de estadísticas y resultados

#### Tecnologías candidatas

**Frontend:**
- **Next.js 15** con App Router
- **Tailwind CSS** para estilos
- **Zustand** para estado global
- **React Query** para data fetching

**IA/ML:**
- **OpenAI API** para sugerencias inteligentes
- **Scikit-learn** para detección de anomalías
- **spaCy** para procesamiento de texto

#### Estimación de tiempo

- **Autenticación**: 3 días
- **Sugerencias de IA**: 5 días
- **Preview de transformaciones**: 4 días
- **Frontend básico**: 7 días
- **Integración y tests**: 3 días

**Total Fase 2: 22 días (~4.5 semanas)**

---

## 13. Estructura del Proyecto

```
backend/
├── api/                      # API Gateway (Node.js + Express + TypeScript)
│   ├── src/
│   │   ├── domain/           # 🔵 Lógica de negocio (núcleo hexagonal)
│   │   │   ├── entities/     #   User, Dataset, TransformationJob
│   │   │   ├── value-objects/ #   DatasetId, UserId, StorageKey
│   │   │   ├── ports/        #   Interfaces (repository, storage, queue)
│   │   │   └── errors/       #   ValidationError, StorageError
│   │   ├── application/      # 🟡 Casos de uso y DTOs
│   │   │   ├── use-cases/    #   CreateDatasetUseCase
│   │   │   └── dtos/         #   CreateDatasetDto
│   │   └── infrastructure/   # 🟢 Adaptadores (implementaciones)
│   │       ├── config/       #   Container (DI), database config
│   │       ├── http/         #   Express, controllers, routes, middleware
│   │       │   ├── controllers/ # DatasetController
│   │       │   ├── routes/   #   dataset.routes.ts
│   │       │   ├── schemas/  #   Validación Zod
│   │       │   └── middleware/ # Error handling, validation
│   │       ├── persistence/  #   Prisma repositories
│   │       ├── storage/      #   MinioStorageService
│   │       └── messaging/    #   BullMQJobQueueService
│   ├── tests/               # Tests (237 total)
│   ├── prisma/              # Database schema y migraciones
│   └── package.json         # Dependencias Node.js
│
├── worker/                   # Worker ETL (Python + FastAPI)
│   ├── src/
│   │   ├── domain/           # 🔵 Lógica de negocio
│   │   │   ├── entities/     #   Dataset, TransformationJob
│   │   │   ├── value_objects/ #   TransformationType, JobStatus
│   │   │   ├── ports/        #   StoragePort, QueuePort
│   │   │   └── errors/       #   TransformationError, ParseError
│   │   ├── application/      # 🟡 Casos de uso
│   │   │   ├── transformations/ # DataTransformer (11 transformaciones)
│   │   │   ├── parsers/      #   DatasetParser (CSV, Excel, JSON, Parquet)
│   │   │   ├── processors/   #   ETLJobProcessor
│   │   │   └── use_cases/    #   ProcessDatasetUseCase
│   │   └── infrastructure/   # 🟢 Adaptadores
│   │       ├── config/       #   Settings, container
│   │       ├── http/         #   FastAPI app, health routes
│   │       ├── storage/      #   MinioStorageService
│   │       └── messaging/    #   BullMQWorker
│   ├── tests/               # Tests (253 total)
│   └── pyproject.toml       # Dependencias Python
│
├── docker/                   # Configuraciones Docker
│   ├── api.Dockerfile       # Imagen para API
│   ├── worker.Dockerfile    # Imagen para Worker  
│   └── docker-compose.yml   # Orquestación local
│
├── docs/                     # Documentación
│   └── RESUMEN-PROYECTO.md  # Este documento
│
├── openspec/                 # Artefactos SDD (Spec-Driven Development)
│   ├── specs/               # Especificaciones del sistema
│   └── changes/             # Historial de cambios implementados
│       └── archive/         # Cambios completados
│
├── .env.example             # Variables de entorno template
├── Makefile                 # Comandos de desarrollo
└── README.md                # Documentación técnica
```

### Explicación de directorios clave

**🔵 `domain/`**: Lógica de negocio pura, sin dependencias externas. Se puede testear completamente sin base de datos ni HTTP.

**🟡 `application/`**: Orquesta el dominio. Los use cases implementan los casos de uso del sistema.

**🟢 `infrastructure/`**: Adaptadores que conectan con el mundo exterior (HTTP, base de datos, storage, etc.). Implementa las interfaces definidas en `domain/ports/`.

**`tests/`**: Tests organizados por capas. Los tests de dominio son ultrarrápidos, los de infrastructure requieren containers.

**`openspec/`**: Sistema SDD para gestionar cambios de forma estructurada (especificaciones → diseño → tareas → implementación → verificación).

---

## 14. Glosario

**API Gateway:** Servicio que actúa como punto de entrada único para todas las requests del cliente. Maneja autenticación, validación, y routing.

**Arquitectura Hexagonal:** Patrón que separa la lógica de negocio (dominio) de los detalles de implementación (infraestructura) usando puertos y adaptadores.

**BullMQ:** Librería para manejo de colas de trabajos basada en Redis. Permite procesamiento asíncrono y distribuido.

**CUID:** Collision-resistant Unique Identifier. Tipo de ID más legible y URL-safe que UUID.

**Dataset:** Conjunto de datos cargado por el usuario (archivo CSV, Excel, etc.) junto con sus metadatos.

**ETL:** Extract, Transform, Load. Proceso de extraer datos de una fuente, transformarlos, y cargarlos en un destino.

**Human-in-the-Loop (HITL):** Enfoque donde los humanos participan en el proceso de toma de decisiones de un sistema automatizado.

**JSONB:** Tipo de dato de PostgreSQL que almacena JSON en formato binario optimizado para consultas.

**MinIO:** Servidor de almacenamiento de objetos compatible con Amazon S3, optimizado para contenedores.

**Polars:** Librería de Python para manipulación de datos ultrarrápida, hasta 10x más rápida que Pandas.

**Prisma:** ORM (Object-Relational Mapping) moderno que genera tipos TypeScript automáticamente desde el schema de base de datos.

**PYMES:** Pequeñas Y Medianas Empresas. Organizaciones que manejan datos importantes pero sin equipos técnicos especializados.

**SDD:** Spec-Driven Development. Metodología donde se escriben especificaciones detalladas antes de implementar código.

**Worker:** Servicio especializado que ejecuta trabajos de procesamiento de datos de forma asíncrona y escalable.

---

## Contacto y Recursos

- **Repositorio:** https://github.com/jcgmU/PymesDataStrategyBackEnd.git
- **Documentación técnica:** `README.md` en la raíz del proyecto  
- **Especificaciones SDD:** `openspec/` directory
- **Health checks:** API (`localhost:3000/health`) y Worker (`localhost:8000/health`)
- **Tests:** 493 tests pasando (91% cobertura)

**MVP Fase 1 completado exitosamente** - Sistema ETL funcional listo para evolucionar hacia Human-in-the-Loop con IA.