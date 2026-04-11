# PymesDataStrategyBackEnd

Sistema ETL con Human-in-the-Loop para limpieza de datos asistida por IA, orientado a PyMEs (Bogotá).

**Repositorio:** https://github.com/jcgmU/PymesDataStrategyBackEnd.git  
**Código académico:** GIIS SW-005

**Estado: Wave 2 COMPLETA** — NL Edit + Gemini directo + SSE global + fixes de tiempo real

---

## Arquitectura

```
┌──────────────────────────────────────────────────────────────────────┐
│                          PYMES Platform                              │
├──────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌──────────────┐   NextAuth v5   ┌──────────────┐                  │
│  │  Frontend    │────────────────▶│ API Gateway  │                  │
│  │  (Next.js)   │◀── SSE global ──│  (Node.js)   │                  │
│  │   :3001      │                 │   :3000      │                  │
│  └──────────────┘                 └──────┬───────┘                  │
│                                          │ BullMQ                   │
│                                   ┌──────▼───────┐                  │
│                                   │ Worker ETL   │◀── Gemini API    │
│                                   │  (Python)    │    (directo)     │
│                                   │   :8000      │                  │
│                                   └──────┬───────┘                  │
│                                          │                          │
│          ┌───────────────┐    ┌──────────┴──────┐   ┌───────────┐  │
│          │  PostgreSQL   │    │     MinIO        │   │   Redis   │  │
│          │    :5433      │    │  :9000 / :9001   │   │   :6380   │  │
│          └───────────────┘    └─────────────────┘   └───────────┘  │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Inicio Rápido (Levantamiento Completo)

### Prerrequisitos

- **Docker Desktop** instalado y corriendo
- **Git**
- Los puertos `3000`, `3001`, `5433`, `6380`, `8000`, `9000`, `9001` disponibles

### 1. Clonar repositorios

El proyecto usa dos repositorios separados (backend y frontend):

```bash
# Crear carpeta raíz del proyecto
mkdir proyecto && cd proyecto

# Clonar backend
git clone https://github.com/jcgmU/PymesDataStrategyBackEnd.git backend

# Clonar frontend (en la misma carpeta raíz)
git clone https://github.com/jcgmU/PymesDataStrategyFrontEnd.git frontend
```

La estructura final debe quedar así:

```
proyecto/
├── backend/    ← este repositorio
└── frontend/   ← repositorio del frontend
```

> **Importante:** El `docker-compose.yml` del backend referencia al frontend con la ruta relativa `../frontend`. Ambas carpetas deben estar al mismo nivel.

### 2. Configurar variables de entorno

```bash
cd backend
cp .env.example .env
```

Edita `.env` y completa las variables requeridas:

| Variable | Requerida | Descripción |
|----------|-----------|-------------|
| `GEMINI_API_KEY` | **Sí, para NL Edit** | Obtener en [Google AI Studio](https://aistudio.google.com/app/apikey) |
| `GEMINI_MODEL` | No (default: `gemini-2.5-flash`) | Modelo Gemini a usar |
| `JWT_SECRET` | Sí en producción | Secreto para tokens JWT (cambiar en prod) |
| `NEXTAUTH_SECRET` | Sí en producción | Secreto para NextAuth v5 (cambiar en prod) |
| `N8N_SUGGESTIONS_WEBHOOK_URL` | No | Solo si usas n8n como fallback de sugerencias AI |

> **Nota sobre `GEMINI_API_KEY`:** Sin esta clave el NL Edit funciona en modo literal (acepta números, `null` y strings entre comillas). Para instrucciones en lenguaje natural como "rellena con la media" que requieren interpretación avanzada (condicionales, etc.) se necesita la clave.

> **Nota sobre `GEMINI_MODEL`:** Usar `gemini-2.5-flash`. Los modelos `gemini-2.0-flash` y `gemini-2.0-flash-exp` ya no están disponibles para cuentas nuevas de Google AI Studio.

### 3. Levantar el stack completo

```bash
cd backend

# Primera vez: construir imágenes y levantar todo
docker compose build
docker compose up -d

# Verificar que todos los servicios están healthy
docker compose ps
```

Esperar ~30-60 segundos hasta que todos los servicios aparezcan como `healthy`. Luego acceder a:

| Servicio | URL |
|----------|-----|
| **Frontend (App)** | http://localhost:3001 |
| **API Swagger Docs** | http://localhost:3000/api/docs |
| **MinIO Consola** | http://localhost:9001 (usuario: `minioadmin`, pass: `minioadmin123`) |

### 4. Primera vez: crear usuario

En http://localhost:3001 registrarse con email y contraseña. O via API:

```bash
curl -X POST http://localhost:3000/api/v1/auth/register \
  -H "Content-Type: application/json" \
  -d '{"email":"tu@email.com","password":"tupassword","name":"Tu Nombre"}'
```

### Comandos de gestión

```bash
# Ver logs en tiempo real
docker compose logs -f

# Ver logs de un servicio específico
docker compose logs -f pymes-api
docker compose logs -f pymes-worker

# Detener todo
docker compose down

# Detener y borrar volúmenes (resetea la BD)
docker compose down -v

# Reconstruir imágenes (tras cambios en dependencias)
docker compose build --no-cache
docker compose up -d
```

---

## Servicios Docker

| Servicio | URL | Descripción |
|----------|-----|-------------|
| `pymes-frontend` | http://localhost:3001 | Next.js 16 + NextAuth v5 |
| `pymes-api` | http://localhost:3000 | API Gateway (Express + TypeScript) |
| `pymes-worker` | http://localhost:8000 | Worker ETL (FastAPI + Python) |
| `pymes-minio` | http://localhost:9001 (consola) | Almacenamiento de objetos (S3-compatible) |
| `pymes-postgres` | localhost:5433 | PostgreSQL 16 |
| `pymes-redis` | localhost:6380 | Redis 7 (BullMQ broker) |
| `pymes-minio-init` | — | Crea buckets al inicio (one-shot) |

> Los puertos locales difieren de los internos del contenedor (5433 en vez de 5432, 6380 en vez de 6379) para evitar conflictos con instalaciones locales.

---

## Funcionalidades

### Core ETL
- **Subida de archivos** — CSV y Excel (.xlsx, .xls) hasta 10 MB
- **Detección automática de anomalías** — valores nulos (`MISSING_VALUE`) y outliers estadísticos (`OUTLIER`, Z-score > 3)
- **6 transformaciones ETL** — imputación, eliminación de outliers, normalización, deduplicación, formato de fechas, escalado

### Human-in-the-Loop (HITL)
- **Revisión de anomalías** — el usuario ve cada anomalía con su contexto (columna, tipo, valor original)
- **3 acciones por anomalía** — `APPROVED` (mantener), `CORRECTED` (corregir), `DISCARDED` (eliminar fila)
- **Aplicación automática** — el Worker aplica las decisiones y genera el archivo limpio
- **Descarga del resultado** — archivo Excel corregido disponible tras completar la revisión

### NL Edit (Natural Language Edit)
- **Instrucciones en español** — el usuario escribe cómo quiere corregir cada anomalía en lenguaje natural
- **Parser basado en reglas** — instrucciones simples se resuelven localmente sin llamar a Gemini:
  - `"rellena con la media"` / `"usa la mediana"` / `"usa la moda"`
  - `"elimina"` / `"borra"` / `"quita esta fila"`
  - `"mantén"` / `"conserva"` / `"deja igual"`
  - Literales: `"0"`, `"null"`, `'"sin datos"'`
- **Fallback a Gemini** — instrucciones complejas (condicionales, transformaciones) se parsean con `gemini-2.5-flash`
- **Vista previa antes de confirmar** — muestra descripción en español de lo que hará la corrección
- **Badge de origen** — indica si la corrección fue por regla directa o interpretada por IA

### Sugerencias AI automáticas (Opción C — Worker → Gemini)
- El Worker llama directamente a Gemini con contexto completo del DataFrame (tipos Polars, estadísticas, valores outlier)
- Genera sugerencias estructuradas `{"actionType": "FILL|DELETE|KEEP", "value": "...", "reason": "..."}` guardadas en la BD
- Si no hay `GEMINI_API_KEY`, hace fallback al webhook de n8n (si está configurado)

### Tiempo real (SSE)
- **Estado de jobs** — `GET /api/v1/jobs/:id/events` notifica cada cambio de estado del job (PENDING → PROCESSING → COMPLETED)
- **SSE global del workspace** — `GET /api/v1/events` notifica cambios de datasets al frontend (status_changed, anomalies_ready)
- La tabla de datasets se actualiza automáticamente sin necesidad de refrescar la página

### Autenticación
- **NextAuth v5** — gestión de sesión en el frontend
- **JWT** — tokens firmados para API Gateway
- **Registro y login** via UI o API REST

---

## Flujo Completo de Uso

```
1. Registrarse/Iniciar sesión en http://localhost:3001

2. Subir archivo CSV o Excel desde el Dashboard
   └── El Worker detecta anomalías automáticamente

3. Esperar que el status cambie a "Procesando"
   └── La tabla se actualiza en tiempo real via SSE

4. Clic en "Ver Detalle" para revisar anomalías
   └── Cada anomalía muestra: columna, tipo, valor original, sugerencia AI

5. Para cada anomalía elegir:
   ├── Aprobar (mantener valor)
   ├── Editar con instrucción NL: "rellena con la media"
   │   └── Ver vista previa → Confirmar
   └── Descartar (eliminar fila)

6. Enviar todas las decisiones
   └── El Worker aplica las correcciones y genera el archivo limpio

7. Clic en "Descargar" para obtener el Excel corregido
```

---

## Endpoints API

Base: `/api/v1` — Swagger UI: http://localhost:3000/api/docs

### Autenticación
| Método | Endpoint | Descripción |
|--------|----------|-------------|
| POST | `/auth/register` | Registrar usuario |
| POST | `/auth/login` | Login, retorna JWT |
| GET | `/auth/users/me` | Usuario autenticado |
| PATCH | `/auth/users/me` | Actualizar perfil |

### Datasets
| Método | Endpoint | Descripción |
|--------|----------|-------------|
| POST | `/datasets` | Subir archivo (multipart) |
| GET | `/datasets` | Listar datasets del usuario |
| GET | `/datasets/:id` | Obtener dataset |
| DELETE | `/datasets/:id` | Eliminar dataset |
| POST | `/datasets/:id/transform` | Iniciar ETL |
| GET | `/datasets/:id/anomalies` | Listar anomalías detectadas |
| POST | `/datasets/:id/decisions` | Enviar decisiones HITL |
| POST | `/datasets/:id/anomalies/:anomalyId/parse-instruction` | Parsear instrucción NL a IR |
| GET | `/datasets/:id/download` | URL de descarga del archivo procesado |

### Jobs y Tiempo Real
| Método | Endpoint | Descripción |
|--------|----------|-------------|
| GET | `/jobs/:id` | Estado de un job |
| GET | `/jobs/:id/events` | SSE stream de estado del job |
| GET | `/events` | SSE stream global del workspace |

### Estadísticas y Health
| Método | Endpoint | Descripción |
|--------|----------|-------------|
| GET | `/stats` | Métricas globales del dashboard |
| GET | `/health` | Health check básico |

---

## Stack Tecnológico

### API Gateway (Node.js)
- **Express 4.21** + **TypeScript 5.7** — Framework web, arquitectura hexagonal
- **Prisma 6** — ORM + migraciones PostgreSQL
- **BullMQ 5** — Cola de jobs (productor)
- **@google/generative-ai 0.24** — NL Edit parser con Gemini
- **Zod 3.24** — Validación de esquemas y variables de entorno

### Worker ETL (Python)
- **FastAPI 0.115** + **Polars 1.39** — Framework web y procesamiento de datos
- **google-generativeai 0.8** — Sugerencias AI directas (Opción C)
- **BullMQ 2.9** — Consumidor de cola (binding Python)
- **SQLAlchemy 2** + **asyncpg** — Persistencia PostgreSQL
- **boto3** — Cliente MinIO/S3

### Frontend (repositorio separado)
- **Next.js 16** + **React 19** + **TypeScript**
- **NextAuth v5** — Autenticación
- **TanStack Query v5** — Estado del servidor + polling/SSE
- **Zustand** — Estado global del cliente
- **Tailwind CSS** — Diseño Neo-Brutalism

### Infraestructura
- **PostgreSQL 16** + **Redis 7** + **MinIO** — BD, broker, almacenamiento
- **Docker Compose** — 7 servicios orquestados

---

## Solución de Problemas

### El worker no inicia / `ModuleNotFoundError: No module named 'google'`

La imagen Docker está desactualizada. Reconstruir:

```bash
docker compose build worker
docker compose up -d worker
```

### Los modelos Gemini retornan 404

Los modelos `gemini-2.0-flash` y `gemini-2.0-flash-exp` ya no están disponibles para cuentas nuevas. Actualizar en `.env`:

```
GEMINI_MODEL=gemini-2.5-flash
```

Y reinicar los contenedores:

```bash
docker compose up -d --force-recreate api worker
```

### La lista de datasets no se actualiza

Verificar que el SSE global está conectado (sin errores en la consola del navegador). El polling de respaldo es cada 3 segundos cuando hay datasets en procesamiento.

### Conflictos de puertos

Ajustar en `.env`:
```
POSTGRES_PORT=5433    # default, cambiar si el 5433 está ocupado
REDIS_PORT=6380       # default, cambiar si el 6380 está ocupado
```

### Problemas de BD / migraciones

```bash
# Las migraciones se aplican automáticamente al iniciar
# Si hay problemas, aplicar manualmente:
docker exec pymes-api pnpm exec prisma migrate deploy --schema=prisma/schema.prisma

# Resetear BD (ADVERTENCIA: borra todos los datos)
docker compose down -v
docker compose up -d
```

---

## Estructura del Proyecto

```
backend/
├── api/                          # API Gateway (Node.js + TypeScript)
│   └── src/
│       ├── domain/               # Núcleo hexagonal (entidades, puertos, errores)
│       │   ├── entities/
│       │   ├── value-objects/
│       │   ├── ports/
│       │   └── ir/               # IR (Intermediate Representation) para NL Edit
│       ├── application/          # Casos de uso
│       │   ├── use-cases/        # CreateDataset, SubmitDecisions, ParseInstruction...
│       │   └── services/         # InstructionParser (reglas + Gemini)
│       └── infrastructure/       # Adaptadores HTTP, BD, cola, SSE
│           ├── http/
│           │   ├── controllers/  # Dataset, Job, Events, Webhook
│           │   ├── middleware/   # Auth JWT
│           │   └── routes/
│           ├── messaging/
│           │   └── bullmq/       # BullMQJobQueue, BullMQQueueEvents, DatasetStatusSyncListener
│           └── persistence/
├── worker/                       # Worker ETL (Python + FastAPI)
│   └── src/
│       ├── domain/               # Entidades, puertos (incluyendo AiSuggestionService)
│       ├── application/          # ProcessDatasetUseCase (HITL + NL Edit execution)
│       └── infrastructure/
│           ├── ai/               # GeminiSuggestionService (Opción C)
│           └── persistence/      # PostgreSQL (SQLAlchemy)
├── prisma/                       # Schema + 3 migraciones aplicadas
├── docker/                       # Configuraciones Docker por servicio
├── docker-compose.yml            # 7 servicios orquestados
├── Makefile                      # Comandos de desarrollo
└── .env.example                  # Plantilla de variables de entorno
```

---

## Decisiones de Arquitectura

1. **Arquitectura Hexagonal** — Dominio puro en ambos servicios (Node.js y Python), sin dependencias externas en el núcleo
2. **BullMQ sobre RabbitMQ** — Funciona en Node.js y Python, Redis ya en el stack
3. **PostgreSQL + JSONB** — Stack simplificado; JSONB cubre necesidades documentales (schema de datasets, IR de correcciones)
4. **SSE sobre WebSockets** — Unidireccional suficiente para estado de jobs; menor complejidad operativa
5. **Worker → Gemini directo (Opción C)** — El Worker tiene acceso al DataFrame completo (tipos Polars, estadísticas reales) que n8n nunca tendría; prompts más precisos
6. **IR estructurado para NL Edit** — Las instrucciones en lenguaje natural se parsean a un árbol IR validado antes de persistirse; seguridad y reproducibilidad

---

## Variables de Entorno

```bash
cp .env.example .env
```

| Variable | Default | Descripción |
|----------|---------|-------------|
| `POSTGRES_USER` | `pymes` | Usuario PostgreSQL |
| `POSTGRES_PASSWORD` | `pymes_dev_password` | Contraseña PostgreSQL |
| `POSTGRES_PORT` | `5433` | Puerto local PostgreSQL |
| `REDIS_PORT` | `6380` | Puerto local Redis |
| `API_PORT` | `3000` | Puerto API Gateway |
| `WORKER_PORT` | `8000` | Puerto Worker ETL |
| `JWT_SECRET` | (dev value) | **Cambiar en producción** |
| `NEXTAUTH_SECRET` | (dev value) | **Cambiar en producción** |
| `GEMINI_API_KEY` | vacío | API key Google AI Studio |
| `GEMINI_MODEL` | `gemini-2.5-flash` | Modelo Gemini |
| `INSTRUCTION_PARSER_ENABLED` | `true` | Habilita NL Edit con Gemini |
| `N8N_SUGGESTIONS_WEBHOOK_URL` | vacío | Fallback n8n (opcional) |

---

## Licencia

MIT
