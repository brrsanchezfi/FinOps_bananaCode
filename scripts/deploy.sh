#!/usr/bin/env bash
# Despliegue del bundle FinOps: validar configuracion + bundle + deploy.
#
#   bash scripts/deploy.sh --profile finops
#   bash scripts/deploy.sh --profile finops --no-deploy     # solo valida
#
# Hay UNA instalacion por cuenta (target `finops`): cubre todos los ambientes
# del cliente, porque el modelo lee system.billing.usage de toda la cuenta.
#
# El workspace destino sale del perfil del CLI (--profile) o de DATABRICKS_HOST:
# no esta escrito en databricks.yml. El `warehouse_id` de los dashboards se pasa
# con BUNDLE_VAR_warehouse_id.
#
# Requiere: databricks CLI v0.230+, python 3.10+, y `pip install -e .` en el
# entorno virtual activo (la verificacion de dashboards usa el paquete finops).
set -euo pipefail

# Nombre de la instalacion y del target del bundle. Ya no es un entorno a
# elegir: existe uno solo. Se deja sobrescribible para quien despliegue mas de
# una instalacion en la misma cuenta (un banco de pruebas junto al productivo).
ENV="finops"

SOLO_VALIDAR=false
PERFIL=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --no-deploy) SOLO_VALIDAR=true; shift ;;
    --profile|-p) PERFIL="${2:-}"; shift 2 ;;
    --target|-t) ENV="${2:-}"; shift 2 ;;
    *) echo "Argumento desconocido '$1'" >&2; exit 1 ;;
  esac
done

# Se arma como arreglo para que, sin perfil, no se pase una cadena vacia al CLI
# (el CLI la tomaria como nombre de perfil y fallaria).
PERFIL_ARGS=()
[[ -n "${PERFIL}" ]] && PERFIL_ARGS=(-p "${PERFIL}")

if [[ -z "${PERFIL}" && -z "${DATABRICKS_HOST:-}" ]]; then
  echo "ATENCION: sin --profile ni DATABRICKS_HOST, el CLI usara el perfil DEFAULT." >&2
  echo "          Verifica que apunte al workspace que esperas." >&2
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"

echo "==> 1/5 Validando la configuracion de la instalacion ${ENV}"
python -m finops.cli validate --env "${ENV}"

echo "==> 2/5 Verificando que los dashboards esten al dia"
# Los versionados estan resueltos contra la configuracion NEUTRA. Este paso solo
# avisa si difieren del generador.
if ! python scripts/dashboards.py check; then
  echo "Regenerando..."
  python scripts/dashboards.py generate
  echo "ATENCION: los dashboards cambiaron. Revisa el diff y commitea." >&2
fi

echo "==> 3/5 Resolviendo los dashboards para esta instalacion"
# Reescribe los tableros contra el catalogo real (conf/local.yml incluido) en
# build/dashboards/, que es a donde apunta resources/dashboards.yml. Sin este
# paso el deploy falla; si apuntara directo a los versionados, no fallaria y los
# cuatro tableros saldrian vacios contra un catalogo ajeno.
python scripts/dashboards.py render --env "${ENV}"

echo "==> 4/5 Validando el bundle"
databricks bundle validate -t "${ENV}" "${PERFIL_ARGS[@]}"

if [[ "${SOLO_VALIDAR}" == "true" ]]; then
  echo "==> Listo (solo validacion, no se desplego nada)"
  exit 0
fi

echo "==> 5/5 Desplegando a ${ENV}"
databricks bundle deploy -t "${ENV}" "${PERFIL_ARGS[@]}"

echo
echo "Despliegue completo. Siguientes pasos:"
echo "  databricks bundle run finops_pipeline_diario -t ${ENV} ${PERFIL_ARGS[*]}"
echo "  databricks bundle summary -t ${ENV} ${PERFIL_ARGS[*]}"
