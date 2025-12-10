@echo off

tasklist|find /i "winMixture.exe"
if %errorlevel%==0 ( 
	echo "Project Has Already Launch"
	timeout /t 3
) else (
	cd ../../
	dir
	
	if not exist log (md log&echo "Creat log Dir" log) else echo "Has log"
	cd ./log/
	if not exist moduleLog (md moduleLog&echo "Creat moduleLog Dir" moduleLog) else echo "Has moduleLog"
	cd ../

	start "winMixture.exe gateway_server 1" bin/winMixture.exe gateway_server 1
	start "winMixture.exe actor_server 1" bin/winMixture.exe actor_server 1
	start "winMixture.exe interactive_server 1" bin/winMixture.exe interactive_server 1
	start "winMixture.exe modularized_server 1" bin/winMixture.exe modularized_server 1
	start "winMixture.exe web_server 1" bin/winMixture.exe web_server 1

	echo "Start Success"
	timeout /t 5
)
