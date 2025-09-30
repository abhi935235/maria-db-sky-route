Param(
    [string]$OutDir = "data"
)

New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

$base = "https://raw.githubusercontent.com/MariaDB/openflights/master/data"
$files = @(
    "airports.dat",
    "airlines.dat",
    "routes.dat",
    "countries.dat"
)

foreach ($f in $files) {
    $url = "$base/$f"
    $dest = Join-Path $OutDir $f
    Write-Host "Downloading $url -> $dest"
    Invoke-WebRequest -Uri $url -OutFile $dest
}

Write-Host "Downloaded OpenFlights dataset to $OutDir"


