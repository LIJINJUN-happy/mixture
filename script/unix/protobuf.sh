echo "(1)Only InteriorPb (2)Mutual protocol"

dn=$(dirname $0)
enterOutOfProjectDir(){
  cd ${dn}
  cd ..
  cd ..
}

onlyInteriorPb(){
  enterOutOfProjectDir
  # into dir ,Do Dispose with other xxx.proto file
  cd proto/interior_pb/pb_src
  protoc -I . --go_out=../   ./element.proto
  protoc -I . --go_out=../   ./hotfix_settings.proto
}

mutualProtocol(){
  enterOutOfProjectDir

  # into dir ,Do Dispose with other xxx.proto file
  cd proto/mutual_pb/pb_src
  protoc -I . --go_out=../   ./test.proto

  cd ../../../
  cd proto

  # 创建文件(清空内容)
  > ./mutual_pb/pb_src/allprotocol.json
  echo { >> ./mutual_pb/pb_src/allprotocol.json

  #先筛选出符合逻辑的行
  iCal=0
  infile=${1:-distribute_config.json}
  while IFS= read -r line || [ -n "$line" ]; do
      # 1. 去掉行首行尾空白后长度为 0 → 跳过
      [ -z "${line#"${line%%[! ]*}"}" ] && continue
      # 2.含 SS_ → 立即退出整个循环（等价 break）
      case $line in
          *SS_* ) break ;;
      esac

      if [[ $line == *"CS_modularized_server"* ]]; then
          iCal=1500
      fi

      if [[ $line == *"CS_interactive_server"* ]]; then
          iCal=3000
      fi

      if [[ $line == *"CS_gateway_server"* ]]; then
          iCal=4500
      fi

      if [[ $line == *"CS_web_server"* ]]; then
          iCal=6000
      fi

      # 3. 含 { } [ ] 任意字符 → 跳过
      case $line in
          *{*|*}*|*[*|*]* ) continue ;;
      esac

      #清除去逗号
      line=$(echo "$line" | sed 's/,//g')

      iCal=$((iCal + 1))   # 每轮加 1

      #拼接
      line=$line:$iCal,
      echo $line >> ./mutual_pb/pb_src/allprotocol.json
  done < "$infile"

  # 定义字符集
  letters="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
  numbers="0123456789"
  random_string="" # 初始化随机字符串

  # 生成10个随机字母
  for (( i=0; i<10; i++ )); do
      random_index=$((RANDOM % ${#letters}))
      random_string+="${letters:$random_index:1}"
  done

  # 生成20个随机数字
  for (( i=0; i<20; i++ )); do
      random_index=$((RANDOM % ${#numbers}))
      random_string+="${numbers:$random_index:1}"
  done
  echo \"_Version_\":\"$random_string\" >> ./mutual_pb/pb_src/allprotocol.json
  echo } >> ./mutual_pb/pb_src/allprotocol.json
}

read -p "Enter A Number As An Argument: " cmdStr

if [ $cmdStr == 1 ]
then
   onlyInteriorPb
elif [ $cmdStr == 2 ]
then
   mutualProtocol
else
   echo "type error"
   exit 0
fi

echo "Compile Protocol Finish"
