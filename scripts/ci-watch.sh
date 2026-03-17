#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# ci-watch.sh — Loop autónomo CI para PymesDataStrategyBackEnd
#
# Uso:
#   ./scripts/ci-watch.sh [--max-attempts N]
#
# Qué hace:
#   1. Espera a que el último workflow run de GitHub Actions termine
#   2. Si pasó  → imprime ✅ y sale con código 0
#   3. Si falló → imprime los logs de los steps fallidos y sale con código 1
#
# El agente (Claude Code) llama este script después de cada push,
# lee el output si falló, aplica fixes, hace push y repite.
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

REPO="jcgmU/PymesDataStrategyBackEnd"
MAX_WAIT=600   # segundos máximos esperando que aparezca un run nuevo
POLL_INTERVAL=10

# ── Colores ──────────────────────────────────────────────────────────────────
GREEN="\033[0;32m"
RED="\033[0;31m"
YELLOW="\033[1;33m"
CYAN="\033[0;36m"
RESET="\033[0m"

log()  { echo -e "${CYAN}[ci-watch]${RESET} $*"; }
ok()   { echo -e "${GREEN}[ci-watch] ✅ $*${RESET}"; }
err()  { echo -e "${RED}[ci-watch] ❌ $*${RESET}"; }
warn() { echo -e "${YELLOW}[ci-watch] ⚠️  $*${RESET}"; }

# ── Verificar gh autenticado ──────────────────────────────────────────────────
if ! gh auth status &>/dev/null; then
  err "gh CLI no está autenticado. Ejecuta: gh auth login"
  exit 1
fi

# ── Obtener el commit actual ──────────────────────────────────────────────────
CURRENT_SHA=$(git -C "$(dirname "$0")/.." rev-parse HEAD)
log "Commit actual: ${CURRENT_SHA:0:8}"

# ── Esperar a que aparezca el run del commit actual ───────────────────────────
log "Esperando que GitHub registre el run para este commit..."
WAITED=0
RUN_ID=""

while [[ $WAITED -lt $MAX_WAIT ]]; do
  RUN_ID=$(gh run list \
    --repo "$REPO" \
    --limit 10 \
    --json databaseId,headSha,status \
    --jq ".[] | select(.headSha == \"$CURRENT_SHA\") | .databaseId" 2>/dev/null | head -1)

  if [[ -n "$RUN_ID" ]]; then
    log "Run encontrado: #${RUN_ID}"
    break
  fi

  sleep $POLL_INTERVAL
  WAITED=$((WAITED + POLL_INTERVAL))
done

if [[ -z "$RUN_ID" ]]; then
  err "No apareció ningún run para el commit ${CURRENT_SHA:0:8} en ${MAX_WAIT}s"
  exit 1
fi

# ── Esperar a que el run termine ──────────────────────────────────────────────
log "Esperando resultado del run #${RUN_ID}..."
gh run watch "$RUN_ID" --repo "$REPO" --exit-status 2>/dev/null || true

# ── Verificar conclusión ──────────────────────────────────────────────────────
CONCLUSION=$(gh run view "$RUN_ID" \
  --repo "$REPO" \
  --json conclusion \
  --jq '.conclusion')

# ── Imprimir detalle de jobs ──────────────────────────────────────────────────
echo ""
log "Resultado por job:"
gh run view "$RUN_ID" \
  --repo "$REPO" \
  --json jobs \
  --jq '.jobs[] | "\(.conclusion // "pending") \(.name)"' | \
  while IFS= read -r line; do
    if [[ "$line" == success* ]]; then
      ok "$line"
    else
      err "$line"
    fi
  done

echo ""

# ── Resultado final ───────────────────────────────────────────────────────────
if [[ "$CONCLUSION" == "success" ]]; then
  ok "CI VERDE — todos los checks pasaron ✅"
  exit 0
else
  err "CI FALLÓ — logs de los steps fallidos:"
  echo "══════════════════════════════════════════════════════════════════"
  gh run view "$RUN_ID" \
    --repo "$REPO" \
    --log-failed 2>/dev/null || \
  gh run view "$RUN_ID" \
    --repo "$REPO" \
    --log 2>/dev/null | tail -200
  echo "══════════════════════════════════════════════════════════════════"
  exit 1
fi
