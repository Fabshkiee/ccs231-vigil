# VIGIL seven-window demo wall for Windows. Each pane owns its own real child.
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

$py = if (Get-Command py -ErrorAction SilentlyContinue) { 'py -3' } else { 'python' }
$area = [System.Windows.Forms.Screen]::PrimaryScreen.WorkingArea
$strip = [int]($area.Height * 26 / 100)
$mid = [int](($strip + $area.Height) / 2)
$c1 = [int]($area.Width / 3)
$c2 = [int](2 * $area.Width / 3)

function Launch($role, $cols, $rows, $x, $y, $w, $h) {
    $cmdText = "mode con cols=$cols lines=$rows >nul 2>&1 & $py -u src\tools\tui.py --role $role"
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

Launch 'broker' 231 13 $area.X $area.Y $area.Width $strip
Start-Sleep -Seconds 2

$subscribers = @(
    @('ResponseTeam',   $area.X,          $c1),
    @('PublicShield',   $c1,              $c2),
    @('ResearchArchive', $c2,             $area.Width)
)
foreach ($node in $subscribers) {
    Launch $node[0] 75 21 $node[1] ($area.Y + $strip) ($node[2] - $node[1]) ($mid - $strip)
}

$publishers = @(
    @('SightingReports',     $area.X, $c1),
    @('PoltergeistActivity', $c1,     $c2),
    @('ParanormalReadings',  $c2,     $area.Width)
)
foreach ($node in $publishers) {
    Launch $node[0] 75 21 $node[1] ($area.Y + $mid) ($node[2] - $node[1]) ($area.Height - $mid)
}

Write-Host 'Wall launched. Broker needs 120+ columns; 231x13 is the target.'
