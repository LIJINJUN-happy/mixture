package database

import (
	"context"
	"errors"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"mixture/src/common/printer"
	"mixture/src/common/tools"
	"time"
)

var mongoOpTimeout = 5 * time.Second
var writeMongoClient *mongo.Client = nil
var readMongoClient *mongo.Client = nil
var maxPoolSize, minPoolSize = uint64(100), uint64(0)
var readClientOptions, writeClientOptions *options.ClientOptions = nil, nil
var mongoReadDBAddr, mongoReadDBPort, mongoWriteDBAddr, mongoWriteDBPort string

func MongoDBConnectClientInit(serName, processId string) {
	if serName != "robot_test" && serName != "gateway_server" {
		res, netDB := GetSQLiteConnDB()
		if res == nil {
			defer func() {
				if netDB.Close() != nil {
					panic("MongoDB Init Error: netDB Close Fail")
				}
			}()
			userAndPassWord := tools.GetSysValueByKey("mongoPassword")
			mongoReadDBAddr, mongoReadDBPort, mongoWriteDBAddr, mongoWriteDBPort = GetMongoAddressAndPort(serName, processId, netDB)
			readClientOptions = options.Client().ApplyURI("mongodb://" + userAndPassWord + mongoReadDBAddr + ":" + mongoReadDBPort)
			writeClientOptions = options.Client().ApplyURI("mongodb://" + userAndPassWord + mongoWriteDBAddr + ":" + mongoWriteDBPort)

			//创建读写池（读写分离）
			readMongoClient = CreateReadWriteMongoClient(readClientOptions)
			writeMongoClient = CreateReadWriteMongoClient(writeClientOptions)
		} else {
			panic("MongoDB Init Error: netDB Connect Fail: " + res.Error())
		}
	}
}

func CreateReadWriteMongoClient(opc *options.ClientOptions) *mongo.Client {
	opc.SetMaxPoolSize(maxPoolSize)
	opc.SetMinPoolSize(minPoolSize)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, opc)
	if err != nil {
		printer.TextError("createReadWriteMongoPool Write Connect failed: " + err.Error())
	}
	if client != nil {
		pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer pingCancel()
		err = client.Ping(pingCtx, readpref.Primary()) // ping一下WriteMongoDB测试下是否调通
		if err != nil {
			printer.TextError("createReadWriteMongoPool Write Ping failed: " + err.Error())
		}
	}
	return client
}

func GetReadMongoClient() *mongo.Client {
	/*if err := readMongoClient.Ping(context.TODO(), readpref.Primary()); err != nil {
		readMongoClient = CreateReadWriteMongoClient(readClientOptions)
	}*/
	return readMongoClient
}

func GetWriteMongoClient() *mongo.Client {
	/*if err := writeMongoClient.Ping(context.TODO(), readpref.Primary()); err != nil {
		writeMongoClient = CreateReadWriteMongoClient(writeClientOptions)
	}*/
	return writeMongoClient
}

// 插入文档
func MongoInsert(fileDBName, collectionName string, bJsonInterface interface{}) (bool, string) {
	var m *mongo.Client = nil
	if m = GetWriteMongoClient(); m == nil {
		return false, "Mongo Get Write Client Fail While MongoInsert"
	}

	//选择fileDBName文档数据库的collectionName集合
	resultString := ""
	collection := m.Database(fileDBName).Collection(collectionName)
	ctx, cancel := context.WithTimeout(context.Background(), mongoOpTimeout)
	defer cancel()
	insertResult, err := collection.InsertOne(ctx, bJsonInterface)
	if err != nil {
		return false, fmt.Sprintf("ERRO:%v", err.Error())
	}

	if insertResult != nil {
		resultString = fmt.Sprintf("Document CmdResult With ID:%v", insertResult.InsertedID)
	}
	return true, resultString
}

// 删除文档
func MongoDelete(fileDBName, collectionName string, bJsonInterface interface{}) (bool, string) {
	var m *mongo.Client = nil
	if m = GetWriteMongoClient(); m == nil {
		return false, "Mongo Get Write Client Fail While MongoDelete"
	}

	resultString := ""
	collection := m.Database(fileDBName).Collection(collectionName)
	ctx, cancel := context.WithTimeout(context.Background(), mongoOpTimeout)
	defer cancel()
	deleteResult, err := collection.DeleteOne(ctx, bJsonInterface)
	if err != nil {
		return false, fmt.Sprintf("ERRO:%v", err.Error())
	}

	if deleteResult != nil {
		resultString = fmt.Sprintf("Deleted documents:%v", deleteResult.DeletedCount)
	}
	return true, resultString
}

// 更新文档
func MongoUpdate(fileDBName, collectionName string, bJsonOldInterface, bJsonNewInterface interface{}) (bool, string) {
	var m *mongo.Client = nil
	if m = GetWriteMongoClient(); m == nil {
		return false, "Mongo Get Write Client Fail While MongoUpdate"
	}

	resultString := ""
	collection := m.Database(fileDBName).Collection(collectionName)
	ctx, cancel := context.WithTimeout(context.Background(), mongoOpTimeout)
	defer cancel()
	updateResult, err := collection.UpdateOne(ctx, bJsonOldInterface, bJsonNewInterface)
	if err != nil {
		return false, fmt.Sprintf("ERRO:%v", err.Error())
	}

	if updateResult != nil {
		resultString = fmt.Sprintf("Matched Documents:%v", updateResult.MatchedCount)
		resultString = fmt.Sprintf("%v, Modified Documents:%v", resultString, updateResult.ModifiedCount)
	}

	return true, resultString
}

// FindOne方法：查询文档
func MongoFindOne(fileDBName, collectionName string, searchInterface interface{}) (bool, string, bson.M) {
	var m *mongo.Client = nil
	if m = GetReadMongoClient(); m == nil {
		return false, "Mongo Get Read Client Fail While MongoFind", nil
	}

	//获取文档对象
	collection := m.Database(fileDBName).Collection(collectionName)
	if collection == nil {
		return false, fmt.Sprintf("Database(%v).Collection(%v) Fail", fileDBName, collectionName), nil
	}

	//查找符合索引条件的结果
	var singleResult bson.M
	ctx, cancel := context.WithTimeout(context.Background(), mongoOpTimeout)
	defer cancel()
	mSingleResult := collection.FindOne(ctx, searchInterface).Decode(&singleResult)
	if mSingleResult == nil {
		return true, "", singleResult

	} else if errors.Is(mSingleResult, mongo.ErrNoDocuments) {
		return false, fmt.Sprintf("No Document Found ,searchInterface = %v", searchInterface), nil
	}

	erStr := fmt.Sprintf("Mongo MongoFindOne Error: " + mSingleResult.Error())
	printer.TextError(erStr)
	return false, erStr, nil
}

// 计算集合行数的接口
func MongoCountLine(fileDBName, collectionName string) (bool, string, int64) {
	var m *mongo.Client = nil
	if m = GetReadMongoClient(); m == nil {
		return false, "Mongo Get Read Client Fail While MongoCountLine", 0
	}

	collection := m.Database(fileDBName).Collection(collectionName)
	if collection == nil {
		return false, fmt.Sprintf("Database(%v).Collection(%v) Fail", fileDBName, collectionName), 0
	}

	ctx, cancel := context.WithTimeout(context.Background(), mongoOpTimeout)
	defer cancel()
	count, e := collection.CountDocuments(ctx, bson.D{})
	if e != nil {
		return false, fmt.Sprintf("fileDBName(%v).collection(%v).CountDocuments() Fail", fileDBName, collectionName), 0
	}
	return true, "", count
}
