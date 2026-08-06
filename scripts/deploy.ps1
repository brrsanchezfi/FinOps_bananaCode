<#
.SYNOPSIS
    Despliegue del bundle FinOps: validar configuracion + bundle + deploy.

.EXAMPLE
    pwsh scripts/deploy.ps1 -Env dev
    pwsh scripts/deploy.ps1 -Env prd -OnlyValidate

.NOTES
    Requiere databricks CLI v0.230+, Python 3.10+ y `pip install -e .` en el
    entorno virtual activo (la verificacion de dashboards usa el paquete finops).
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('dev', 'qa', 'prd')]
    [string]$Env,

    [switch]$OnlyValidate
)

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

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
databricks bundle validate -t $Env
if ($LASTEXITCODE -ne 0) { throw "La validacion del bundle fallo" }

if ($OnlyValidate) {
    Write-Host "==> Listo (solo validacion, no se desplego nada)" -ForegroundColor Green
    exit 0
}

Write-Host "==> 4/4 Desplegando a $Env" -ForegroundColor Cyan
databricks bundle deploy -t $Env
if ($LASTEXITCODE -ne 0) { throw "El deploy fallo" }

Write-Host ""
Write-Host "Despliegue completo. Siguientes pasos:" -ForegroundColor Green
Write-Host "  databricks bundle run finops_pipeline_diario -t $Env"
Write-Host "  databricks bundle summary -t $Env"
