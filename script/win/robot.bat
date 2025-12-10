@echo off

cd ../../
dir
	
if not exist log (md log&echo "Creat log Dir" log) else echo "Has log"
	
start bin/winMixture.exe robot_test 1

echo "Robot Start Success"
timeout /t 5
