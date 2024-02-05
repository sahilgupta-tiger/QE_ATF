@echo OFF
title Empty Out DATF Reports
echo ::Deleting all files from Charts folder and Protocol PDFs::
powershell.exe "Get-ChildItem -Path 'DBTablesDemo' -File -Recurse | Remove-Item -Recurse -Force"
powershell.exe "Get-ChildItem -Path 'DBTablesDemo' -Directory -Recurse | Remove-Item -Recurse -Confirm:$false -Force"
powershell.exe "Get-ChildItem -Path 'charts' -File -Recurse | Remove-Item -Force"