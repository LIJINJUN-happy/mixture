/*----------------------------------------该包存放的都是进程相关的地址和端口获取的接口--------------------------------------*/
package database

import (
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"mixture/src/common/printer"
	"strconv"
	"strings"
)

func GetSQLiteConnDB() (*sql.DB, error) {
	db, err := sql.Open("sqlite3", "net_sql.db")
	if err != nil {
		printer.TextError("SqlLite Database Open Failed" + err.Error())
		return nil, err
	}
	version := ""
	if err = db.QueryRow("SELECT SQLITE_VERSION();").Scan(&version); err != nil {
		printer.TextError("Database creation failed: " + err.Error())
		return nil, err
	}
	printer.TextDebug("SQLite Open Successful" + version)
	return db, nil
}

func GetGateIPAddressById(gateId int, db *sql.DB) (string, error) {
	address := ""
	err := db.QueryRow(`SELECT gatewaySerAddress FROM "gatewaySerConfig" WHERE gatewaySerId=?`, gateId).Scan(&address)
	//printer.TextDebug("GetGateIPAddressById", "Get Address", address, " By GateID", gateId)
	if err != nil {
		return "", fmt.Errorf("GetGateIPAddressById gateId=%d: %w", gateId, err)
	}
	if len(address) < 1 {
		return "", fmt.Errorf("GetGateIPAddressById gateId=%d: empty address", gateId)
	}
	return address, nil
}

func GetGatePortNumById(gateId int, db *sql.DB) (int, error) {
	portString := ""
	queryErr := db.QueryRow(`SELECT gatewaySerPort FROM "gatewaySerConfig" WHERE gatewaySerId=?`, gateId).Scan(&portString)
	//printer.TextDebug("GetGatePortNumById", "Get Port", portString, " By GateID", gateId)
	if queryErr != nil {
		return 0, fmt.Errorf("GetGatePortNumById gateId=%d: %w", gateId, queryErr)
	}
	port, err := strconv.Atoi(portString)
	if err != nil {
		return 0, fmt.Errorf("GetGatePortNumById gateId=%d invalid port %q: %w", gateId, portString, err)
	}
	if port <= 0 {
		return 0, fmt.Errorf("GetGatePortNumById gateId=%d: non-positive port %d", gateId, port)
	}
	return port, nil
}

func GetInteractiveSerIdByGateId(gateId int, db *sql.DB) (int, error) {
	interactiveSerId := ""
	queryErr := db.QueryRow(`SELECT interactiveSerId FROM "gatewaySerConfig" WHERE gatewaySerId=?`, gateId).Scan(&interactiveSerId)
	//printer.TextDebug("GetInteractiveSerIdByGateId", "Get InteractiveSerId", interactiveSerId, " By GateID", gateId)
	if queryErr != nil {
		return 0, fmt.Errorf("GetInteractiveSerIdByGateId gateId=%d: %w", gateId, queryErr)
	}
	sId, err := strconv.Atoi(interactiveSerId)
	if err != nil {
		return 0, fmt.Errorf("GetInteractiveSerIdByGateId gateId=%d invalid id %q: %w", gateId, interactiveSerId, err)
	}
	return sId, nil
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
