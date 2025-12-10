dn=$(dirname $0)
cd ${dn} ; cd .. ; cd ..
#ls

if [ ! -d "/log/" ];then
	mkdir log
fi

./bin/unixMixture  robot_test 1 &