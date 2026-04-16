dn=$(dirname $0)
cd ${dn} ; cd .. ; cd ..
#ls

if [ ! -d "/log/" ];then
	mkdir -p  log
	mkdir -p  log/moduleLog
fi

./bin/unixMixture  actor_server 1 &
./bin/unixMixture  modularized_server 1 &
./bin/unixMixture  interactive_server 1 &
./bin/unixMixture  web_server 1 &
./bin/unixMixture  gateway_server 1 &


