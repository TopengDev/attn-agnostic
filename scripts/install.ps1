<#
.SYNOPSIS
  One-line installer for the attn-agnostic stack on Windows.

.DESCRIPTION
  Windows equivalent of scripts/install.sh. Detects the CPU arch, installs the
  daemon + CLI + MCP server + the Go harness bridges to
  %LOCALAPPDATA%\Programs\attn (on PATH), generates a loopback-only identity on
  first run (the private key is written to the platform config dir and is NEVER
  printed), and registers a per-user logon Scheduled Task so attnd starts at
  login (no admin required). Idempotent — safe to re-run to upgrade.

  Two install methods, auto-selected:
    1. from source — when run inside a checkout AND Go 1.25+ is on PATH.
    2. download    — fetch a prebuilt .zip from $env:ATTN_RELEASE_BASE.
       (No public releases exist yet, so this needs ATTN_RELEASE_BASE.)

.NOTES
  Run:  irm https://raw.githubusercontent.com/TopengDev/attn-agnostic/main/scripts/install.ps1 | iex
  Or, from a checkout:  pwsh -File scripts/install.ps1   (also runs on Windows PowerShell 5.1)

  Env knobs (set before piping to iex):
    ATTN_BIN_DIR       install dir            (default: %LOCALAPPDATA%\Programs\attn)
    ATTN_VERSION       version to embed/fetch (default: git describe / latest)
    ATTN_RELEASE_BASE  base URL for the prebuilt .zip (enables the download path)
    ATTN_REPO_DIR      path to a checkout to build from
    ATTN_SKIP_SERVICE  if set, do not register the logon task
#>

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$Binaries = 'attnd', 'attn', 'attn-mcp', 'attnctl', 'attn-opencode', 'attn-hermes-bridge'
$BuildPkgs = [ordered]@{
    'attnd'              = './cmd/attnd'
    'attn'               = './cmd/attn'
    'attn-mcp'           = './cmd/attn-mcp'
    'attnctl'            = './cmd/attnctl'
    'attn-opencode'      = './cmd/attn-opencode'
    'attn-hermes-bridge' = './adapters/hermes/cmd/attn-hermes-bridge'
}
$BuildInfoPkg = 'github.com/TopengDev/attn-agnostic/internal/buildinfo'

function Write-Step($m) { Write-Host "==> $m" -ForegroundColor Cyan }
function Write-Note($m) { Write-Host "    $m" }
function Die($m) { Write-Error "install.ps1: $m"; exit 1 }
function Have($name) { return [bool](Get-Command $name -ErrorAction SilentlyContinue) }

function Get-Arch {
    switch ($env:PROCESSOR_ARCHITECTURE) {
        'AMD64' { return 'amd64' }
        'ARM64' { return 'arm64' }
        'x86'   { Die 'unsupported architecture: x86 (need 64-bit Windows)' }
        default { Die "unsupported architecture: $($env:PROCESSOR_ARCHITECTURE)" }
    }
}

function Find-Repo {
    if ($env:ATTN_REPO_DIR -and (Test-Path (Join-Path $env:ATTN_REPO_DIR 'go.mod'))) {
        return (Resolve-Path $env:ATTN_REPO_DIR).Path
    }
    # $PSScriptRoot is empty when the script is piped through iex.
    if ($PSScriptRoot) {
        $root = Split-Path -Parent $PSScriptRoot
        $gomod = Join-Path $root 'go.mod'
        if ((Test-Path $gomod) -and (Select-String -Path $gomod -Pattern 'module github.com/TopengDev/attn-agnostic' -Quiet)) {
            return $root
        }
    }
    return $null
}

function Get-Version {
    if ($env:ATTN_VERSION) { return $env:ATTN_VERSION }
    if (Have 'git') {
        $v = (& git describe --tags --always --dirty 2>$null)
        if ($LASTEXITCODE -eq 0 -and $v) { return $v.Trim() }
    }
    return 'dev'
}

function Build-FromSource($repo, $arch, $binDir) {
    if (-not (Have 'go')) {
        Die "building from source needs Go 1.25+ (https://go.dev/dl). Or set ATTN_RELEASE_BASE to download prebuilt binaries."
    }
    $version = Get-Version
    $commit = 'none'
    if (Have 'git') {
        $c = (& git -C $repo rev-parse --short HEAD 2>$null)
        if ($LASTEXITCODE -eq 0 -and $c) { $commit = $c.Trim() }
    }
    $date = [DateTime]::UtcNow.ToString('yyyy-MM-ddTHH:mm:ssZ')
    $ldflags = "-s -w -X $BuildInfoPkg.Version=$version -X $BuildInfoPkg.Commit=$commit -X $BuildInfoPkg.Date=$date"

    Write-Step "building from source ($repo) for windows/$arch (version $version)"
    Push-Location $repo
    try {
        $env:CGO_ENABLED = '0'
        $env:GOOS = 'windows'
        $env:GOARCH = $arch
        foreach ($name in $BuildPkgs.Keys) {
            $out = Join-Path $binDir "$name.exe"
            & go build -trimpath -ldflags $ldflags -o $out $BuildPkgs[$name]
            if ($LASTEXITCODE -ne 0) { Die "build failed: $name" }
            Write-Note "built $name.exe"
        }
    }
    finally { Pop-Location }
}

function Install-FromRelease($arch, $binDir) {
    if (-not $env:ATTN_RELEASE_BASE) {
        Die @"
no source checkout found and ATTN_RELEASE_BASE is not set.
Prebuilt releases are not published yet. Install from source instead:
  git clone https://github.com/TopengDev/attn-agnostic
  cd attn-agnostic; .\scripts\install.ps1
(or set `$env:ATTN_RELEASE_BASE to a release URL once binaries are published).
"@
    }
    $version = Get-Version
    $archive = "attn-agnostic_${version}_windows-$arch.zip"
    $url = "$($env:ATTN_RELEASE_BASE)/$archive"
    $tmp = Join-Path ([System.IO.Path]::GetTempPath()) ([System.IO.Path]::GetRandomFileName())
    New-Item -ItemType Directory -Path $tmp -Force | Out-Null
    $zip = Join-Path $tmp $archive
    Write-Step "downloading $url"
    Invoke-WebRequest -Uri $url -OutFile $zip
    Expand-Archive -Path $zip -DestinationPath $binDir -Force
}

function Initialize-Identity($binDir) {
    Write-Step 'initializing identity (key written to the config dir, never printed)'
    & (Join-Path $binDir 'attnd.exe') -init
}

function Register-AttndService($binDir) {
    if ($env:ATTN_SKIP_SERVICE) {
        Write-Step "skipping service setup (ATTN_SKIP_SERVICE set) — run: $binDir\attnd.exe"
        return
    }
    $attnd = Join-Path $binDir 'attnd.exe'
    Write-Step "registering logon Scheduled Task 'attnd' (per-user, no admin)"
    try {
        $action = New-ScheduledTaskAction -Execute $attnd
        $trigger = New-ScheduledTaskTrigger -AtLogOn
        $settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1)
        Register-ScheduledTask -TaskName 'attnd' -Action $action -Trigger $trigger -Settings $settings -Description 'attn-agnostic daemon (attnd)' -Force | Out-Null
        Start-ScheduledTask -TaskName 'attnd'
        Write-Note "task 'attnd' registered + started (manage via Task Scheduler, or: Get-ScheduledTask attnd)"
    }
    catch {
        Write-Warning "could not register the Scheduled Task: $_"
        Write-Note 'Alternatives:'
        Write-Note "  • run on demand:   $attnd"
        Write-Note "  • Windows service (admin):  New-Service -Name attnd -BinaryPathName '$attnd'"
        Write-Note "  • nssm (recommended for a real service):  nssm install attnd $attnd"
    }
}

function Update-UserPath($binDir) {
    $userPath = [Environment]::GetEnvironmentVariable('Path', 'User')
    if (-not $userPath) { $userPath = '' }
    if ($userPath.Split(';') -notcontains $binDir) {
        [Environment]::SetEnvironmentVariable('Path', "$binDir;$userPath", 'User')
        $env:Path = "$binDir;$($env:Path)"
        Write-Note "added $binDir to your user PATH (restart the shell for new windows to see it)"
    }
}

function Main {
    $arch = Get-Arch
    Write-Step "attn-agnostic installer — target windows/$arch"

    $binDir = $env:ATTN_BIN_DIR
    if (-not $binDir) { $binDir = Join-Path $env:LOCALAPPDATA 'Programs\attn' }
    New-Item -ItemType Directory -Path $binDir -Force | Out-Null

    $repo = Find-Repo
    if ($repo) {
        Build-FromSource $repo $arch $binDir
    }
    else {
        Install-FromRelease $arch $binDir
    }

    Update-UserPath $binDir
    Initialize-Identity $binDir
    Register-AttndService $binDir

    Write-Host ''
    Write-Step 'done. next steps:'
    Write-Note 'check the daemon:    attn status'
    Write-Note 'set your handle:     attn register-name <you>   (GATED — costs 0.001 ETH on Base)'
    Write-Note "MCP-native harness:  point it at '$binDir\attn-mcp.exe' (stdio) — see docs/INSTALL.md"
    Write-Note 'pi / opencode / hermes adapters: see docs/INSTALL.md'
}

Main
