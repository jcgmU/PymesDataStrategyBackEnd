# PymesDataStrategyBackEnd

> **Proyecto de Grado GIIS SW-005 — Fundación Universitaria Compensar**
> API Gateway + Worker ETL con IA y Human-in-the-Loop para PYMES en Bogotá, Colombia.

![Estado](https://img.shields.io/badge/Wave%202-COMPLETADA-brightgreen)
![API Tests](https://img.shields.io/badge/API%20tests-337%20pasando-brightgreen)
![Worker Tests](https://img.shields.io/badge/Worker%20tests-308%20pasando-brightgreen)
![Node.js](https://img.shields.io/badge/Node.js-20-green)
![Python](https://img.shields.io/badge/Python-3.12-blue)
![TypeScript](https://img.shields.io/badge/TypeScript-strict-blue)
![Licencia](https://img.shields.io/badge/licencia-MIT-blue)

---

## 📋 Descripción

**PymesDataStrategyBackEnd** es el núcleo de la plataforma **PymesDataStrategy**, un sistema ETL potenciado con Inteligencia Artificial y un proceso de validación **Human-in-the-Loop (HITL)** diseñado para pequeñas y medianas empresas (PYMES) de Bogotá.

El backend orquesta la carga y procesamiento de archivos CSV/Excel, detecta anomalías automáticamente, genera sugerencias AI con Gemini, y permite que un analista humano revise y apruebe cada corrección antes de producir el archivo de datos limpio.

### 🔗 Repositorios

| Repositorio | Enlace |
|---|---|
| **Backend** (este repo) | https://github.com/jcgmU/PymesDataStrategyBackEnd.git |
| **Frontend** | https://github.com/jcgmU/PymesDataStrategyFrontEnd.git |

---

## 🏗️ Arquitectura

```
┌──────────────────────────────────────────────────────────────────────┐
│                          PYMES Platform                              │
├──────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌──────────────┐   NextAuth v5   ┌──────────────────────────────┐  │
│  │  Frontend    │────────────────▶│      API Gateway             │  │
│  │  (Next.js)   │◀── SSE global ──│   Node.js + Express          │  │
│  │   :3001      │◀── SSE jobs ────│   TypeScript + Prisma        │  │
│  └──────────────┘                 │      :3000                   │  │
│                                   └──────────────┬───────────────┘  │
│                                                  │ BullMQ           │
│                                   ┌──────────────▼───────────────┐  │
│                                   │       Worker ETL             │  │
│                                   │   Python + FastAPI           │──┼──▶ Gemini API
│                                   │   Polars + SQLAlchemy        │  │    (directo)
│                                   │      :8000                   │  │
│                                   └──────────────┬───────────────┘  │
│                                                  │                  │
│       ┌──────────────┐   ┌─────────────────┐   ┌┴──────────┐       │
│       │  PostgreSQL  │   │      MinIO       │   │   Redis   │       │
│       │  (Prisma ORM)│   │ :9000 / :9001   │   │  BullMQ   │       │
│       │    :5433     │   │  (S3-compat.)   │   │   :6380   │       │
│       └──────────────┘   └─────────────────┘   └───────────┘       │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 🛠️ Stack Tecnológico

### API Gateway (Node.js)

| Tecnología | Versión | Uso |
|---|---|---|
| **Node.js** | 20 | Runtime |
| **Express** | 4.21 | Framework web |
| **TypeScript** | 5.7 | Tipado estático — arquitectura hexagonal |
| **Prisma** | 6 | ORM + migraciones PostgreSQL |
| **BullMQ** | 5.34 | Productor de cola de trabajos |
| **@google/generative-ai** | 0.24 | NL Edit — parser de instrucciones con Gemini |
| **Zod** | 3.24 | Validación de esquemas y variables de entorno |
| **Vitest** | — | 337 tests unitarios e integración |

### Worker ETL (Python)

| Tecnología | Versión | Uso |
|---|---|---|
| **Python** | 3.12 | Runtime |
| **FastAPI** | 0.115 | Framework web |
| **Polars** | 1.39 | Procesamiento de datos de alto rendimiento |
| **google-generativeai** | 0.8 | Sugerencias AI directas (Opción C) |
| **BullMQ** | 2.9 | Consumidor de cola (binding Python) |
| **SQLAlchemy** | 2 | ORM + asyncpg para PostgreSQL |
| **boto3** | — | Cliente MinIO / S3 |
| **pytest** | — | 308 tests |

### Infraestructura

| Servicio | Versión | Rol |
|---|---|---|
| **PostgreSQL** | 16 | Base de datos relacional |
| **Redis** | 7 | Broker de mensajes (BullMQ) |
| **MinIO** | RELEASE.2024 | Almacenamiento objetos (S3-compatible) |
| **Docker Compose** | — | Orquestación de 7 servicios |

---

## ✅ Funcionalidades Implementadas

### Core ETL y HITL

| # | Funcionalidad | Estado |
|---|---|---|
| 1 | Subida de archivos CSV / Excel (.xlsx, .xls) hasta 10 MB | ✅ |
| 2 | Detección automática de anomalías (MISSING_VALUE + OUTLIER Z-score > 3) | ✅ |
| 3 | Revisión Human-in-the-Loop — aprobar / corregir / descartar por anomalía | ✅ |
| 4 | 6 transformaciones ETL — imputación, outliers, normalización, deduplicación, fechas, escalado | ✅ |
| 5 | Descarga del archivo Excel corregido desde MinIO | ✅ |
| 6 | Dashboard de métricas globales (datasets, jobs, anomalías) | ✅ |

### Inteligencia Artificial

| # | Funcionalidad | Estado |
|---|---|---|
| 7 | **NL Edit** — correcciones en lenguaje natural español por anomalía | ✅ |
| 8 | Parser de reglas local (media, mediana, eliminar, mantener, literales) sin costo API | ✅ |
| 9 | Fallback Gemini para instrucciones complejas (condicionales, transformaciones) | ✅ |
| 10 | **Sugerencias AI automáticas** — Worker → Gemini directo con contexto del DataFrame | ✅ |
| 11 | Vista previa del IR antes de confirmar la corrección | ✅ |

### Tiempo Real

| # | Funcionalidad | Estado |
|---|---|---|
| 12 | SSE por job — `GET /api/v1/jobs/:id/events` — estado en tiempo real | ✅ |
| 13 | SSE global del workspace — `GET /api/v1/events` — notifica cambios de datasets | ✅ |
| 14 | Tabla de datasets se actualiza automáticamente sin recargar la página | ✅ |

### Autenticación

| # | Funcionalidad | Estado |
|---|---|---|
| 15 | Registro y login con JWT | ✅ |
| 16 | Middleware de autenticación — Bearer header o `?token=` query param (SSE) | ✅ |
| 17 | Swagger / OpenAPI interactivo en `/api/docs` | ✅ |

---

## 🚀 Inicio Rápido (Levantamiento Completo)

### Prerrequisitos

- **Docker Desktop** instalado y corriendo
- **Git**
- Puertos `3000`, `3001`, `5433`, `6380`, `8000`, `9000`, `9001` disponibles

### 1. Clonar repositorios

```bash
# Crear carpeta raíz del proyecto
mkdir proyecto && cd proyecto

# Clonar backend (este repositorio)
git clone https://github.com/jcgmU/PymesDataStrategyBackEnd.git backend

# Clonar frontend al mismo nivel
git clone https://github.com/jcgmU/PymesDataStrategyFrontEnd.git frontend
```

> ⚠️ **Importante:** Ambas carpetas deben estar al mismo nivel. El `docker-compose.yml` del backend referencia al frontend con ruta relativa `../frontend`.

```
proyecto/
├── backend/    ← este repositorio
└── frontend/   ← repositorio del frontend
```

### 2. Configurar variables de entorno

```bash
cd backend
cp .env.example .env
```

Editar `.env` y completar:

| Variable | Requerida | Descripción |
|---|---|---|
| `GEMINI_API_KEY` | Para NL Edit y AI | Obtener en [aistudio.google.com](https://aistudio.google.com/app/apikey) |
| `GEMINI_MODEL` | No (default: `gemini-2.5-flash`) | Modelo Gemini a usar |
| `JWT_SECRET` | Sí en producción | Cambiar valor por defecto |
| `NEXTAUTH_SECRET` | Sí en producción | Cambiar valor por defecto |

> **Sin `GEMINI_API_KEY`** el NL Edit solo acepta instrucciones literales. Las sugerencias AI automáticas no se generan.

> **Modelo recomendado:** `gemini-2.5-flash`. Los modelos `gemini-2.0-flash` y `gemini-2.0-flash-exp` ya no están disponibles para cuentas nuevas de Google AI Studio.

### 3. Construir y levantar el stack

```bash
cd backend

# Primera vez — construir imágenes
docker compose build

# Levantar todos los servicios
docker compose up -d

# Verificar estado
docker compose ps
```

Esperar ~30-60 segundos hasta que todos los servicios aparezcan como `healthy`.

### Comandos disponibles

| Comando | Descripción |
|---|---|
| `docker compose up -d` | Levantar todos los servicios en background |
| `docker compose ps` | Ver estado de los servicios |
| `docker compose logs -f` | Logs en tiempo real (todos los servicios) |
| `docker compose logs -f pymes-worker` | Logs de un servicio específico |
| `docker compose down` | Detener todos los servicios |
| `docker compose down -v` | Detener y borrar volúmenes (resetea BD) |
| `docker compose build --no-cache` | Reconstruir imágenes desde cero |
| `make test-api` | Ejecutar tests del API Gateway |
| `make test-worker` | Ejecutar tests del Worker ETL |

---

## 🐳 Servicios Docker

| Servicio | URL | Descripción |
|---|---|---|
| `pymes-frontend` | http://localhost:3001 | Next.js 16 + NextAuth v5 |
| `pymes-api` | http://localhost:3000 | API Gateway (Express + TypeScript) |
| `pymes-api` | http://localhost:3000/api/docs | Swagger UI interactivo |
| `pymes-worker` | http://localhost:8000 | Worker ETL (FastAPI + Python) |
| `pymes-minio` | http://localhost:9001 | Consola MinIO (admin: `minioadmin` / `minioadmin123`) |
| `pymes-postgres` | localhost:5433 | PostgreSQL 16 |
| `pymes-redis` | localhost:6380 | Redis 7 (BullMQ broker) |
| `pymes-minio-init` | — | Crea buckets al inicio (one-shot) |

> Los puertos locales usan 5433 y 6380 (en vez de 5432 y 6379) para evitar conflictos con instalaciones locales.

---

## 🔧 Variables de Entorno

```bash
cp .env.example .env
```

| Variable | Default | Descripción |
|---|---|---|
| `POSTGRES_USER` | `pymes` | Usuario PostgreSQL |
| `POSTGRES_PASSWORD` | `pymes_dev_password` | Contraseña PostgreSQL |
| `POSTGRES_PORT` | `5433` | Puerto local PostgreSQL |
| `REDIS_PORT` | `6380` | Puerto local Redis |
| `API_PORT` | `3000` | Puerto API Gateway |
| `WORKER_PORT` | `8000` | Puerto Worker ETL |
| `JWT_SECRET` | (dev value) | **Cambiar en producción** |
| `NEXTAUTH_SECRET` | (dev value) | **Cambiar en producción** |
| `GEMINI_API_KEY` | vacío | API key de Google AI Studio |
| `GEMINI_MODEL` | `gemini-2.5-flash` | Modelo Gemini |
| `INSTRUCTION_PARSER_ENABLED` | `true` | Habilita NL Edit con Gemini |
| `N8N_SUGGESTIONS_WEBHOOK_URL` | vacío | Fallback n8n (opcional) |

Ver `.env.example` para la lista completa con descripciones.

---

## 🔌 Endpoints API

Base: `/api/v1` — Documentación interactiva: http://localhost:3000/api/docs

### Autenticación

| Método | Endpoint | Descripción |
|---|---|---|
| POST | `/auth/register` | Registrar nuevo usuario |
| POST | `/auth/login` | Login — retorna JWT |
| GET | `/auth/users/me` | Usuario autenticado |
| PATCH | `/auth/users/me` | Actualizar perfil |

### Datasets y ETL

| Método | Endpoint | Descripción |
|---|---|---|
| POST | `/datasets` | Subir archivo CSV/Excel (multipart) |
| GET | `/datasets` | Listar datasets del usuario |
| GET | `/datasets/:id` | Obtener dataset por ID |
| DELETE | `/datasets/:id` | Eliminar dataset |
| POST | `/datasets/:id/transform` | Iniciar job ETL |
| GET | `/datasets/:id/anomalies` | Listar anomalías detectadas |
| POST | `/datasets/:id/decisions` | Enviar decisiones HITL |
| POST | `/datasets/:id/anomalies/:anomalyId/parse-instruction` | Parsear instrucción NL a IR |
| GET | `/datasets/:id/download` | URL de descarga del archivo procesado |

### Jobs y Tiempo Real

| Método | Endpoint | Descripción |
|---|---|---|
| GET | `/jobs/:id` | Estado de un job |
| GET | `/jobs/:id/events` | SSE — estado del job en tiempo real |
| GET | `/events` | SSE — cambios globales del workspace |

### Estadísticas y Health

| Método | Endpoint | Descripción |
|---|---|---|
| GET | `/stats` | Métricas globales (datasets, jobs, anomalías) |
| GET | `/health` | Health check básico |

---

## 🔄 Flujo Human-in-the-Loop

```
1. Usuario sube CSV/Excel         →  POST /api/v1/datasets/transform
2. Worker detecta anomalías       →  MISSING_VALUE + OUTLIER (Z-score > 3)
3. Worker llama Gemini (directo)  →  genera sugerencias AI por anomalía
4. Dataset queda en estado        →  PROCESSING (esperando decisiones)
5. Usuario revisa anomalías       →  GET /api/v1/datasets/:id/anomalies
6. Usuario decide por cada una:
   ├── Aprobar                    →  mantiene el valor original
   ├── Corregir (manual/NL Edit)  →  escribe instrucción → vista previa → confirma
   └── Descartar                  →  elimina la fila
7. Envía todas las decisiones     →  POST /api/v1/datasets/:id/decisions
8. Worker aplica correcciones     →  genera archivo limpio en MinIO
9. Dataset pasa a READY           →  usuario descarga el Excel corregido
```

---

## 📁 Estructura del Proyecto

```
backend/
├── api/                              # API Gateway (Node.js + TypeScript)
│   └── src/
│       ├── domain/                   # Núcleo hexagonal — sin dependencias externas
│       │   ├── entities/             # Dataset, Anomaly, Decision, User
│       │   ├── value-objects/        # DatasetId, etc.
│       │   ├── ports/                # Interfaces de repositorios y servicios
│       │   ├── ir/                   # IR (Intermediate Representation) — NL Edit
│       │   └── errors/               # Errores de dominio tipados
│       ├── application/              # Casos de uso
│       │   ├── use-cases/            # CreateDataset, SubmitDecisions, ParseInstruction...
│       │   └── services/             # InstructionParser (reglas + Gemini)
│       └── infrastructure/           # Adaptadores
│           ├── config/               # Container DI, env validation (Zod)
│           ├── http/
│           │   ├── controllers/      # Dataset, Job, Events, Webhook, Stats
│           │   ├── middleware/       # Auth JWT
│           │   └── routes/           # Registro de rutas + Swagger
│           ├── messaging/
│           │   └── bullmq/           # JobQueue, QueueEvents, DatasetStatusSyncListener
│           ├── persistence/          # Prisma repositories
│           └── storage/              # MinIO adapter
├── worker/                           # Worker ETL (Python + FastAPI)
│   └── src/
│       ├── domain/                   # Entidades, puertos (AiSuggestionService port)
│       ├── application/              # ProcessDatasetUseCase — detecta, sugiere, aplica
│       └── infrastructure/
│           ├── ai/                   # GeminiSuggestionService (Opción C — directo)
│           ├── config/               # Container DI, Settings (Pydantic)
│           ├── http/                 # FastAPI app, health endpoint
│           ├── messaging/            # BullMQ consumer
│           ├── parsers/              # Dataset parser (CSV/Excel → Polars)
│           └── persistence/          # SQLAlchemy models + repositories
├── prisma/                           # Schema + 3 migraciones aplicadas
│   ├── schema.prisma
│   └── migrations/
│       ├── 20260310.../              # Tablas base (users, datasets, jobs)
│       ├── 20260315.../              # Tablas HITL (anomalies, decisions)
│       └── 20260406.../              # ai_suggestion en anomalies (Wave 2)
├── docker/                           # Configuraciones Docker por servicio
├── docker-compose.yml                # 7 servicios orquestados
├── Makefile                          # Comandos de desarrollo
└── .env.example                      # Plantilla de variables de entorno
```

---

## 🧪 Tests

### API Gateway (Vitest)

```bash
# Ejecutar los 337 tests
make test-api

# Con cobertura
cd api && pnpm test:coverage
```

| Suite | Tests | Descripción |
|---|---|---|
| Use cases | ~150 | CreateDataset, SubmitDecisions, ParseInstruction... |
| Services | ~50 | InstructionParser (reglas + Gemini mock) |
| Controllers | ~80 | HTTP handlers + validación |
| Repositories | ~57 | Prisma adapters |

### Worker ETL (pytest)

```bash
# Ejecutar los 308 tests
make test-worker

# Con cobertura HTML
cd worker && uv run pytest --cov=src --cov-report=html
```

| Suite | Tests | Descripción |
|---|---|---|
| ProcessDatasetUseCase | ~120 | ETL completo, HITL, NL Edit execution |
| GeminiSuggestionService | ~30 | Sugerencias AI directas |
| Repositories | ~80 | PostgreSQL persistence |
| Parsers | ~78 | CSV/Excel parsing con Polars |

---

## 🔍 Solución de Problemas

### Worker no inicia — `ModuleNotFoundError: No module named 'google'`

La imagen Docker está desactualizada. Reconstruir:

```bash
docker compose build worker
docker compose up -d worker
```

### Modelos Gemini retornan 404

Los modelos `gemini-2.0-flash` y `gemini-2.0-flash-exp` ya no están disponibles para cuentas nuevas. Actualizar en `.env`:

```env
GEMINI_MODEL=gemini-2.5-flash
```

Luego reiniciar:

```bash
docker compose up -d --force-recreate api worker
```

### La tabla de datasets no se actualiza

Verificar que el SSE global está conectado (sin errores en la consola del navegador en `http://localhost:3001`). El polling de respaldo es cada 3 segundos mientras haya datasets en procesamiento.

### Conflictos de puertos

Ajustar en `.env`:

```env
POSTGRES_PORT=5433    # cambiar si el 5433 está ocupado
REDIS_PORT=6380       # cambiar si el 6380 está ocupado
```

### Problemas con BD / migraciones

```bash
# Las migraciones se aplican automáticamente al iniciar
# Si hay problemas, aplicar manualmente:
docker exec pymes-api pnpm exec prisma migrate deploy --schema=prisma/schema.prisma

# Resetear BD — ADVERTENCIA: borra todos los datos
docker compose down -v
docker compose up -d
```

---

## 🗺️ Próximos Pasos — Wave 3

- [ ] **Roles y permisos** — Diferenciación entre administrador y analista
- [ ] **Migración a `google.genai`** — El SDK `google-generativeai` está deprecado; actualizar a `google-genai`
- [ ] **Historial de correcciones** — Trazabilidad completa por dataset y usuario
- [ ] **Webhooks configurables** — Notificaciones externas al completar el ETL

---

## 📄 Licencia

Este proyecto está licenciado bajo la **Licencia MIT**.

```
MIT License

Copyright (c) 2025 GIIS SW-005 — Fundación Universitaria Compensar

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
```

---

<div align="center">

**Proyecto de Grado GIIS SW-005**
Fundación Universitaria Compensar — Bogotá, Colombia

</div>
