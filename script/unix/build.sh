echo "Begin To Build Mixture Project"
dn=$(dirname $0)
cd ${dn}
#First deal with the compilation of windows environment
#go env -w CGO_ENABLED=1 GOOS=windows GOARCH=amd64
#go build -o ../../bin/winMixture.exe  -x ../../main.go

#Reprocessing compilation in unix environment (incidentally switching back to unix environment)
go env -w GOOS=linux
go build -o ../../bin/unixMixture  -x ../../main.go

echo "Build Mixture Project Finish"