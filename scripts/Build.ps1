param([string]$Destination='bin')
$ErrorActionPreference='Stop'
$root=[IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$destinationPath=if([IO.Path]::IsPathRooted($Destination)){$Destination}else{Join-Path $root $Destination}
New-Item -ItemType Directory -Path $destinationPath -Force | Out-Null
$savedWork=$env:GOWORK;$savedProxy=$env:GOPROXY
try {
 $env:GOWORK='off';$env:GOPROXY='off'
 Push-Location $root
 try {
  $suffix=if($IsWindows){'.exe'}else{''}
  foreach($command in @('runtime','migrate','healthcheck')) {
   & go build -mod=vendor -trimpath -ldflags='-s -w' -o (Join-Path $destinationPath ($command+$suffix)) ("./cmd/"+$command)
   if($LASTEXITCODE){throw "Build failed: $command"}
  }
  $hashes=Get-ChildItem -LiteralPath $destinationPath -File | Where-Object Name -ne 'sha256.json' | ForEach-Object {@{name=$_.Name;sha256=(Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash.ToLowerInvariant()}}
  [IO.File]::WriteAllText((Join-Path $destinationPath 'sha256.json'),(ConvertTo-Json -InputObject @($hashes)))
 }finally{Pop-Location}
}finally{$env:GOWORK=$savedWork;$env:GOPROXY=$savedProxy}
