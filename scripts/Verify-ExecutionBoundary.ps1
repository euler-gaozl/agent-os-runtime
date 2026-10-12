$ErrorActionPreference='Stop'
$root=[IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$expected=@('github.com/euler-gaozl/agent-os-runtime/execution','github.com/euler-gaozl/agent-os-runtime/internal/runstate')
$deps=@(& go -C $root list -mod=vendor -deps ./execution)
if($LASTEXITCODE){throw 'Execution dependency inspection failed'}
$owner=@($deps|Where-Object {$_.StartsWith('github.com/euler-gaozl/agent-os-runtime/')})
if(@(Compare-Object ($expected|Sort-Object) ($owner|Sort-Object)).Count){throw 'Worker protocol depends on service API, effects or migrations'}
if(@($deps|Where-Object {$_ -match '/(servicehttp|servicehost|serviceclient|mcp|mcpsecurity)$'}).Count){throw 'Execution core depends on a network/service implementation'}
Write-Output 'Execution dependency closure contains only the public facade and private execution state.'
