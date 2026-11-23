# Tiles the VIGIL windows the same way mac-demo.command does.
Add-Type -AssemblyName System.Windows.Forms
Add-Type @"
using System;
using System.Runtime.InteropServices;
public class Win {
    [DllImport("user32.dll", CharSet = CharSet.Unicode)] public static extern IntPtr FindWindow(string cls, string title);
    [DllImport("user32.dll")] public static extern bool MoveWindow(IntPtr h, int x, int y, int w, int ht, bool repaint);
}
"@

$a = [System.Windows.Forms.Screen]::PrimaryScreen.WorkingArea
$bh = [int]($a.Height * 18 / 100)
$c1 = [int]($a.Width / 3)
$c2 = [int](2 * $a.Width / 3)
$mid = [int](($bh + $a.Height) / 2)

function Place($title, $x, $y, $w, $h) {
    for ($i = 0; $i -lt 20; $i++) {
        $win = [Win]::FindWindow($null, $title)
        if ($win -ne [IntPtr]::Zero) { break }
        Start-Sleep -Milliseconds 250
    }
    if ($win -eq [IntPtr]::Zero) {
        Write-Host "no window titled $title"
        return
    }
    [Win]::MoveWindow($win, $x, $y, $w, $h, $true) | Out-Null
}

Place "broker"              $a.X          $a.Y          $a.Width          $bh
Place "ResponseTeam"        $a.X          ($a.Y + $bh)  $c1               ($mid - $bh)
Place "PublicShield"        ($a.X + $c1)  ($a.Y + $bh)  ($c2 - $c1)       ($mid - $bh)
Place "ResearchArchive"     ($a.X + $c2)  ($a.Y + $bh)  ($a.Width - $c2)  ($mid - $bh)
Place "SightingReports"     $a.X          ($a.Y + $mid) $c1               ($a.Height - $mid)
Place "PoltergeistActivity" ($a.X + $c1)  ($a.Y + $mid) ($c2 - $c1)       ($a.Height - $mid)
Place "ParanormalReadings"  ($a.X + $c2)  ($a.Y + $mid) ($a.Width - $c2)  ($a.Height - $mid)
