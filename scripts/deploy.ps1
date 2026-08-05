<#
.SYNOPSIS
    Despliegue del bundle FinOps: render de dashboards + validate + deploy.

.EXAMPLE
    pwsh scripts/deploy.ps1 -Env dev
    pwsh scripts/deploy.ps1 -Env prd -OnlyValidate

.NOTES
    Requiere databricks CLI v0.230+, Python 3.10+ y `pip install -e .` en el
    entorno virtual activo (el render lee la configuracion via el paquete finops).
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

Write-Host "==> 2/4 Renderizando dashboards para $Env" -ForegroundColor Cyan
python scripts/dashboards.py render --env $Env
if ($LASTEXITCODE -ne 0) { throw "El render de dashboards fallo" }

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
