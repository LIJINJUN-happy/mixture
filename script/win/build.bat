echo "Begin To Build Mixture Project"

rem First deal with compilation in unix environment
rem go env -w CGO_ENABLED=1 GOOS=linux GOARCH=amd64
rem go build -o ../../bin/unixMixture  -x ../../main.go

rem Reprocess compilation in windows environment (incidentally switching back to windows environment)
go env -w GOOS=windows
go build -o ../../bin/winMixture.exe  -x ../../main.go

echo "Build Mixture Project Finish"
echo Press AnyKey To Continue...
pause>nul