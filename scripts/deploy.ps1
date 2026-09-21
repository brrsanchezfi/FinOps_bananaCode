<#
.SYNOPSIS
    Despliegue del bundle FinOps: validar configuracion + bundle + deploy.

.EXAMPLE
    pwsh scripts/deploy.ps1 -Env dev -DatabricksProfile finops
    pwsh scripts/deploy.ps1 -Env dev -OnlyValidate

.DESCRIPTION
    El workspace destino sale del perfil del CLI (-Profile) o de DATABRICKS_HOST:
    no esta escrito en databricks.yml, porque el repositorio es producto y se
    despliega sobre la cuenta de cada cliente. El warehouse_id de los dashboards
    se pasa con la variable de entorno BUNDLE_VAR_warehouse_id.

.NOTES
    Requiere databricks CLI v0.230+, Python 3.10+ y `pip install -e .` en el
    entorno virtual activo (la verificacion de dashboards usa el paquete finops).
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('dev', 'qa', 'prd')]
    [string]$Env,

    # Perfil del CLI de Databricks (~/.databrickscfg). Define el workspace destino.
    # No se llama -Profile porque $PROFILE es una variable automatica de
    # PowerShell y un parametro con ese nombre la sombrearia dentro del script.
    [string]$DatabricksProfile,

    [switch]$OnlyValidate
)

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

# Arreglo vacio cuando no hay perfil: pasar una cadena vacia haria que el CLI la
# tomara como nombre de perfil y fallara.
$perfilArgs = @()
if ($DatabricksProfile) {
    $perfilArgs = @('-p', $DatabricksProfile)
}
elseif (-not $env:DATABRICKS_HOST) {
    Write-Warning "Sin -DatabricksProfile ni DATABRICKS_HOST, el CLI usara el perfil DEFAULT. Verifica que apunte al workspace que esperas."
}

Write-Host "==> 1/4 Validando la configuracion de $Env" -ForegroundColor Cyan
python -m finops.cli validate --env $Env
if ($LASTEXITCODE -ne 0) { throw "La validacion de configuracion fallo" }

# Los dashboards estan versionados ya resueltos por entorno. Este paso solo
# avisa si difieren del generador; no es un paso de build previo al deploy.
Write-Host "==> 2/4 Verificando que los dashboards esten al dia" -ForegroundColor Cyan
python scripts/dashboards.py check
if ($LASTEXITCODE -ne 0) {
    Write-Host "Regenerando..." -ForegroundColor Yellow
    python scripts/dashboards.py generate
    if ($LASTEXITCODE -ne 0) { throw "La generacion de dashboards fallo" }
    Write-Warning "Los dashboards cambiaron. Revisa el diff y commitea."
}

Write-Host "==> 3/4 Validando el bundle" -ForegroundColor Cyan
databricks bundle validate -t $Env @perfilArgs
if ($LASTEXITCODE -ne 0) { throw "La validacion del bundle fallo" }

if ($OnlyValidate) {
    Write-Host "==> Listo (solo validacion, no se desplego nada)" -ForegroundColor Green
    exit 0
}

Write-Host "==> 4/4 Desplegando a $Env" -ForegroundColor Cyan
databricks bundle deploy -t $Env @perfilArgs
if ($LASTEXITCODE -ne 0) { throw "El deploy fallo" }

Write-Host ""
Write-Host "Despliegue completo. Siguientes pasos:" -ForegroundColor Green
Write-Host "  databricks bundle run finops_pipeline_diario -t $Env $perfilArgs"
Write-Host "  databricks bundle summary -t $Env $perfilArgs"
