@echo off
echo (1)Only InteriorPb (2)Mutual protocol
:choose
set /p doCmd=Enter A Number As An Argument :

if %doCmd% EQU 1 (
    echo "Only InteriorPb"
    cd ../../
    rem into dir ,Do Dispose with other xxx.proto file
    cd proto/interior_pb/pb_src
    protoc -I . --go_out=../   ./element.proto
    protoc -I . --go_out=../   ./hotfix_settings.proto
    protoc -I . --go_out=../   ./gate_broadcast.proto

    echo "Dispose protocol  Finish"
    echo Press AnyKey To Continue...
    pause>nul
    exit
)

if %doCmd% EQU 2 (
    echo "Mutual protocol"
    cd ../../

    rem into dir ,Do Dispose with other xxx.proto file
    cd proto/mutual_pb/pb_src
    protoc -I . --go_out=../   ./test.proto
    protoc -I . --go_out=../   ./client_broadcast.proto
	
	rem 开始提取协议和协议号内容
	cd ../../../
	cd ./proto/
	rem 创建文件
	echo  > ./mutual_pb/pb_src/allprotocol.json
	rem 清空内容
	type nul > ./mutual_pb/pb_src/allprotocol.json
	setlocal enabledelayedexpansion
	set "target=SS_"
    set "title1=:"
    set "title2=,"
    set cnt=1

	echo {>>./mutual_pb/pb_src/allprotocol.json

    rem 先去掉{
    for /f "tokens=*" %%i in ('findstr /v "{" distribute_config.json') do (

        rem 假如碰到SS_直接推出for循环
        set "lineStr=%%i"
        :: === 核心：包含子串就跳出 ===
        echo !lineStr! | findstr /l /c:"!target!" >nul
        if !errorlevel!==0 goto :GetAllCS

        echo !lineStr! | findstr "CS_modularized_server" >nul
        if !errorlevel!==0 (
            set /a cnt=1500
        )

        echo !lineStr! | findstr "CS_interactive_server" >nul
        if !errorlevel!==0 (
            set /a cnt=3000
        )

        echo !lineStr! | findstr "CS_gateway_server" >nul
        if !errorlevel!==0 (
            set /a cnt=4500
        )

        echo !lineStr! | findstr "CS_web_server" >nul
        if !errorlevel!==0 (
            set /a cnt=6000
        )

        rem 再去掉]
        set "line=!lineStr!"
        echo !line!|findstr /l "]" >nul || (

            rem 再去掉[
            set "newLine=!line!"
            echo !newLine!|findstr /l "[" >nul || (
                rem 如果包含逗号，去掉逗号
                echo !newLine! | findstr "," >nul
                if !errorlevel! equ 0 (
                    set "newLine=!newLine:,=!"
                )

                echo !newLine!!title1!!cnt!!title2! >>./mutual_pb/pb_src/allprotocol.json
                set /a cnt+=1
            )
        )
    )
)

echo "type args error"
goto choose

:GetAllCS
rem 生成随机数字部分
set "random_string1="
for /l %%i in (1,1,10) do (
   set /a num=!random! %% 100 + 1
   set "random_string1=!random_string1!!num!"
)

rem 定义原始字符串(将字符串中的1替换为a,将字符串中的2替换为b)
setlocal enabledelayedexpansion
set "random_string2="
    for /l %%i in (1,1,10) do (
        set /a num=!random! %% 9 + 1
        set "random_string2=!random_string2!!num!"
    )
set "random_string2=!random_string2:1=a!"
set "random_string2=!random_string2:2=b!"
set "random_string2=!random_string2:3=Z!"
set "random_string2=!random_string2:4=F!"
set "random_string2=!random_string2:5=I!"
set "random_string2=!random_string2:6=A!"
set "random_string2=!random_string2:7=Y!"
set "random_string2=!random_string2:8=E!"
set "random_string2=!random_string2:9=Q!"

rem 将两部分连接起来
set "random_string=!random_string1!!random_string2!"

rem 输出结果并形成版本号
echo "_Version_":"!random_string!">>./mutual_pb/pb_src/allprotocol.json
echo }>>./mutual_pb/pb_src/allprotocol.json

echo "Dispose protocol  Finish"
echo Press AnyKey To Continue...
pause>nul
exit
