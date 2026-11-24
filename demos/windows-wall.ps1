# VIGIL demo wall for Windows. Host runs the broker; client joins it over the LAN.
[CmdletBinding()]
param([switch]$Client)

$ErrorActionPreference = 'Stop'
Set-Location (Join-Path $PSScriptRoot '..')

Add-Type -AssemblyName System.Windows.Forms
Add-Type @"
using System;
using System.Runtime.InteropServices;
public class VigilWin {
    [DllImport("user32.dll")] public static extern bool MoveWindow(IntPtr h, int x, int y, int w, int ht, bool repaint);
}
"@

function Invoke-Py([string[]]$PyArgs) {
    if (Get-Command py -ErrorAction SilentlyContinue) { & py -3 @PyArgs }
    else { & python @PyArgs }
}

$py = if (Get-Command py -ErrorAction SilentlyContinue) { 'py -3' } else { 'python' }

if (-not $Client) {
    $busy = Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -like '*src\broker.py*' }
    if ($busy) {
        Write-Host 'A broker is already running. Stop it before launching the wall.'
        exit 1
    }
    if (Test-Path 'src\common\data') {
        $backup = Join-Path $env:TEMP ('vigil-data-' + (Get-Date -Format 'yyyyMMdd-HHmmss'))
        Copy-Item -Recurse 'src\common\data' $backup
        Write-Host "Data backed up to $backup"
    }
    $pyCmd = if (Get-Command py -ErrorAction SilentlyContinue) { 'py' } else { 'python' }
    $pyPre = if ($pyCmd -eq 'py') { @('-3') } else { @() }
    Start-Process -FilePath $pyCmd -ArgumentList ($pyPre + @('-u', 'src/tools/discovery.py', 'beacon')) -WindowStyle Hidden | Out-Null
    $hostArg = ''
} else {
    $hostIp = (Invoke-Py @('src/tools/discovery.py', 'discover', '--wait', '5') | Select-Object -First 1)
    if (-not $hostIp) { $hostIp = $env:VIGIL_BROKER_HOST }
    if (-not $hostIp) { $hostIp = Read-Host 'Broker IP' }
    if (-not $hostIp) { Write-Host 'No broker address found.'; exit 1 }
    $env:VIGIL_BROKER_HOST = $hostIp
    $hostArg = "--host $hostIp"
    Write-Host "Joining broker at $hostIp"
}

$area = [System.Windows.Forms.Screen]::PrimaryScreen.WorkingArea
if ($Client) {
    $middle = [int]($area.Height / 2)
    $subTop = $area.Y
    $subBot = $area.Y + $middle
    $pubTop = $subBot
} else {
    $strip = [int]($area.Height * 26 / 100)
    $middle = [int](($strip + $area.Height) / 2)
    $subTop = $area.Y + $strip
    $subBot = $area.Y + $middle
    $pubTop = $subBot
}
$pubBot = $area.Y + $area.Height
$c1 = [int]($area.Width / 3)
$c2 = [int](2 * $area.Width / 3)

function Launch($role, $cols, $rows, $x, $y, $w, $h, $hostArg) {
    $cmdText = "mode con cols=$cols lines=$rows >nul 2>&1 & $py -u src\tools\tui.py --role $role $hostArg"
    $proc = Start-Process -FilePath cmd.exe -ArgumentList '/k', $cmdText -PassThru
    for ($i = 0; $i -lt 40; $i++) {
        $proc.Refresh()
        if ($proc.MainWindowHandle -ne [IntPtr]::Zero) { break }
        Start-Sleep -Milliseconds 250
    }
    if ($proc.MainWindowHandle -eq [IntPtr]::Zero) {
        Write-Host "No window found for $role"
        return
    }
    [VigilWin]::MoveWindow($proc.MainWindowHandle, $x, $y, $w, $h, $true) | Out-Null
}

if (-not $Client) {
    Launch 'broker' 231 13 $area.X $area.Y $area.Width ($subTop - $area.Y) $hostArg
    Start-Sleep -Seconds 2
}

$subscribers = @(
    @('ResponseTeam',   $area.X,          $c1),
    @('PublicShield',   $c1,              $c2),
    @('ResearchArchive', $c2,             $area.Width)
)
foreach ($node in $subscribers) {
    Launch $node[0] 75 21 $node[1] $subTop ($node[2] - $node[1]) ($subBot - $subTop) $hostArg
}

$publishers = @(
    @('SightingReports',     $area.X, $c1),
    @('PoltergeistActivity', $c1,     $c2),
    @('ParanormalReadings',  $c2,     $area.Width)
)
foreach ($node in $publishers) {
    Launch $node[0] 75 21 $node[1] $pubTop ($node[2] - $node[1]) ($pubBot - $pubTop) $hostArg
}

Write-Host 'Wall launched. Broker pane needs 120+ columns; 231x13 is the target.'
