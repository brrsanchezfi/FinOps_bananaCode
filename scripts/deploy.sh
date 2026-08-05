#!/usr/bin/env bash
# Despliegue del bundle FinOps: render de dashboards + validate + deploy.
#
#   bash scripts/deploy.sh dev
#   bash scripts/deploy.sh prd --no-deploy     # solo valida
#
# Requiere: databricks CLI v0.230+, python 3.10+, y `pip install -e .` en el
# entorno virtual activo (el render lee la configuracion via el paquete finops).
set -euo pipefail

ENV="${1:-}"
shift || true

if [[ -z "${ENV}" ]]; then
  echo "Uso: bash scripts/deploy.sh <dev|qa|prd> [--no-deploy]" >&2
  exit 1
fi

case "${ENV}" in
  dev|qa|prd) ;;
  *) echo "Entorno invalido '${ENV}'. Validos: dev, qa, prd" >&2; exit 1 ;;
esac

SOLO_VALIDAR=false
for arg in "$@"; do
  [[ "${arg}" == "--no-deploy" ]] && SOLO_VALIDAR=true
done

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"

echo "==> 1/4 Validando la configuracion de ${ENV}"
python -m finops.cli validate --env "${ENV}"

echo "==> 2/4 Renderizando dashboards para ${ENV}"
python scripts/dashboards.py render --env "${ENV}"

echo "==> 3/4 Validando el bundle"
databricks bundle validate -t "${ENV}"

if [[ "${SOLO_VALIDAR}" == "true" ]]; then
  echo "==> Listo (solo validacion, no se desplego nada)"
  exit 0
fi

echo "==> 4/4 Desplegando a ${ENV}"
databricks bundle deploy -t "${ENV}"

echo
echo "Despliegue completo. Siguientes pasos:"
echo "  databricks bundle run finops_pipeline_diario -t ${ENV}"
echo "  databricks bundle summary -t ${ENV}"
