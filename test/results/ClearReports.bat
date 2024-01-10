@echo on
title Empty Out Allure Results
echo ::Deleting all files from Charts folder and Protocol PDFs::
powershell.exe "Get-ChildItem -Path 'DBTablesDemo' -File -Recurse | Remove-Item -Force"
echo "Deleted files from 'DBTablesDemo' folder"
powershell.exe "Get-ChildItem -Path 'charts' -File -Recurse | Remove-Item -Force"
echo "Deleted files from 'charts' folder"
timeout 5