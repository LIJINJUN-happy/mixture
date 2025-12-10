ps -ef|grep "unixMixture*"|grep "_server 1"|grep -v grep |awk '{print $2}'|xargs kill -15
