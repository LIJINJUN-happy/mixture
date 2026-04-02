/*----------------------------------------该包存放的都是进程相关的地址和端口获取的接口--------------------------------------*/
package database

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"mixture/src/common/printer"
	"strconv"
	"strings"
)

func GetSQLiteConnDB() (error, *sql.DB) {
	db, err := sql.Open("sqlite3", "net_sql.db")
	if err != nil {
		printer.TextError("SqlLite Database Open Failed" + err.Error())
		return err, nil
	} else {
		version := ""
		err = db.QueryRow("SELECT SQLITE_VERSION();").Scan(&version) // QueryRow() 执行查询，返回版本号
		if err != nil {
			printer.TextError("Database creation failed: " + err.Error())
			return err, nil
		}
		// 连接成功，调试模式下打印出"database connected：版本号"
		printer.TextDebug("SQLite Open Successful" + version)
	}
	return nil, db
}

func GetGateIPAddressById(gateId int, db *sql.DB) (bool, string) {
	address := ""
	err := db.QueryRow(`SELECT gatewaySerAddress FROM "gatewaySerConfig" WHERE gatewaySerId=?`, gateId).Scan(&address)
	//printer.TextDebug("GetGateIPAddressById", "Get Address", address, " By GateID", gateId)
	if (err == nil) && (len(address) >= 1) {
		return true, address
	}
	return false, ""
}

func GetGatePortNumById(gateId int, db *sql.DB) (bool, int) {
	portString := ""
	queryErr := db.QueryRow(`SELECT gatewaySerPort FROM "gatewaySerConfig" WHERE gatewaySerId=?`, gateId).Scan(&portString)
	//printer.TextDebug("GetGatePortNumById", "Get Port", portString, " By GateID", gateId)
	port, err := strconv.Atoi(portString)
	if (queryErr == nil) && (err == nil) && (port > 0) {
		return true, port
	}
	return false, 0
}

func GetInteractiveSerIdByGateId(gateId int, db *sql.DB) (bool, int) {
	interactiveSerId := ""
	queryErr := db.QueryRow(`SELECT interactiveSerId FROM "gatewaySerConfig" WHERE gatewaySerId=?`, gateId).Scan(&interactiveSerId)
	//printer.TextDebug("GetInteractiveSerIdByGateId", "Get InteractiveSerId", interactiveSerId, " By GateID", gateId)
	sId, err := strconv.Atoi(interactiveSerId)
	if (queryErr == nil) && (err == nil) {
		return true, sId
	}
	return false, 0
}

func GetSerAddressById(id int, db *sql.DB, serName string) string {
	address := ""
	sqlString := strings.Replace("SELECT %Address FROM \"%Config\" where %Id=="+strconv.Itoa(id), "%", serName, -1)
	err := db.QueryRow(sqlString).Scan(&address)
	//printer.TextDebug("GetSerAddressById", "sqlString", sqlString, "Get Address", address, " By modularizedSerId", id)
	if (err == nil) && (len(address) >= 1) {
		return address
	}
	return ""
}

func GetSerPortById(id int, db *sql.DB, serName string) int {
	portString := ""
	sqlString := strings.Replace("SELECT %Port FROM \"%Config\" where %Id=="+strconv.Itoa(id), "%", serName, -1)
	queryErr := db.QueryRow(sqlString).Scan(&portString)
	//printer.TextDebug("GetSerPortById", "sqlString", sqlString, "Get Port", portString, " By modularizedSerId", id)
	port, err := strconv.Atoi(portString)
	if (queryErr == nil) && (err == nil) && (port > 0) {
		return port
	}
	return -1
}

func GetDeBugModePort(serName, id string, db *sql.DB) string {
	//处理serName字符串（去除_后的部分）
	tagIdx := strings.LastIndex(serName, "_")
	if tagIdx == -1 {
		return ""
	}
	serName = strings.Replace(serName, serName[tagIdx:], "Ser", 1)
	portString := ""
	sqlString := strings.Replace("SELECT deBugModePort FROM \"%Config\" where %Id=="+id, "%", serName, -1)
	queryErr := db.QueryRow(sqlString).Scan(&portString)
	//printer.TextDebug("GetDeBugModePort", "sqlString", sqlString, "Get Port", portString)
	if (queryErr == nil) && (len(portString) > 0) {
		return portString
	}
	return ""
}

func GetMemCacheAddressAndPort(serName, id string, db *sql.DB) (string, string) {
	//处理serName字符串（去除_后的部分）
	tagIdx := strings.LastIndex(serName, "_")
	if tagIdx == -1 {
		return "", ""
	}
	serName = strings.Replace(serName, serName[tagIdx:], "Ser", 1)
	addrString, portString := "", ""
	sqlString := strings.Replace("SELECT memCacheAddress,memCachePort FROM \"%Config\" where %Id=="+id, "%", serName, -1)
	queryErr := db.QueryRow(sqlString).Scan(&addrString, &portString)
	printer.TextDebug("GetMemCacheAddressAndPort", "sqlString", sqlString, "Get Addr", addrString, "Get Port", portString)
	if (queryErr == nil) && (len(portString) > 0) {
		return addrString, portString
	}
	return "", ""
}

func GetMongoAddressAndPort(serName, id string, db *sql.DB) (string, string, string, string) {
	//处理serName字符串（去除_后的部分）
	tagIdx := strings.LastIndex(serName, "_")
	if tagIdx == -1 {
		return "", "", "", ""
	}
	serName = strings.Replace(serName, serName[tagIdx:], "Ser", 1)
	readAddrString, readPortString, writeAddrString, writePortString := "", "", "", ""
	sqlString := strings.Replace("SELECT mongoReadAddress,mongoReadPort,mongoWriteAddress,mongoWritePort FROM \"%Config\" where %Id=="+id, "%", serName, -1)
	queryErr := db.QueryRow(sqlString).Scan(&readAddrString, &readPortString, &writeAddrString, &writePortString)
	printer.TextDebug("GetMongoAddressAndPort", "sqlString", sqlString, "Get ReadAddr", readAddrString, "Get ReadPort", readPortString, "Get WriteAddr", writeAddrString, "Get WritePort", writePortString)
	if (queryErr == nil) && (len(readPortString) > 0) && (len(writePortString) > 0) {
		return readAddrString, readPortString, writeAddrString, writePortString
	}
	return "", "", "", ""
}
