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
	"sync/atomic"
	"time"
)

var mongoOpTimeout = 5 * time.Second
var writeMongoClient atomic.Pointer[mongo.Client]
var readMongoClient atomic.Pointer[mongo.Client]
var maxPoolSize, minPoolSize = uint64(100), uint64(10)
var readClientOptions, writeClientOptions *options.ClientOptions = nil, nil
var mongoReadDBAddr, mongoReadDBPort, mongoWriteDBAddr, mongoWriteDBPort string

func MongoDBConnectClientInit(serName, processId string) {
	if serName != "robot_test" && serName != "gateway_server" {
		netDB, res := GetSQLiteConnDB()
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
			readMongoClient.Store(CreateMongoClient(readClientOptions))
			writeMongoClient.Store(CreateMongoClient(writeClientOptions))
		} else {
			panic("MongoDB Init Error: netDB Connect Fail: " + res.Error())
		}
	}
}

func CreateMongoClient(opc *options.ClientOptions) *mongo.Client {
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

// StartMongoHealthCheck 后台定期 ping，连接断开时自动重建客户端，不影响业务调用路径
func StartMongoHealthCheck(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			// 检查读客户端 - 避免重复Load
			if readClient := readMongoClient.Load(); readClient != nil && readClientOptions != nil {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				if err := readClient.Ping(ctx, readpref.Primary()); err != nil {
					printer.TextWarn("MongoHealthCheck", "ReadClient ping fail, reconnecting", err.Error())
					readMongoClient.Store(CreateMongoClient(readClientOptions))
				}
				cancel()
			}
			// 检查写客户端 - 避免重复Load
			if writeClient := writeMongoClient.Load(); writeClient != nil && writeClientOptions != nil {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				if err := writeClient.Ping(ctx, readpref.Primary()); err != nil {
					printer.TextWarn("MongoHealthCheck", "WriteClient ping fail, reconnecting", err.Error())
					writeMongoClient.Store(CreateMongoClient(writeClientOptions))
				}
				cancel()
			}
		}
	}()
}

// 插入文档
func MongoInsert(fileDBName, collectionName string, bJsonInterface interface{}) (bool, string) {
	m := writeMongoClient.Load()
	if m == nil {
		return false, "Mongo Get Write Client Fail While MongoInsert"
	}

	collection := m.Database(fileDBName).Collection(collectionName)
	ctx, cancel := context.WithTimeout(context.Background(), mongoOpTimeout)
	defer cancel()
	insertResult, err := collection.InsertOne(ctx, bJsonInterface)
	if err != nil {
		return false, fmt.Sprintf("ERRO:%v", err.Error())
	}

	return true, fmt.Sprintf("Document CmdResult With ID:%v", insertResult.InsertedID)
}

// 删除文档
func MongoDelete(fileDBName, collectionName string, bJsonInterface interface{}) (bool, string) {
	m := writeMongoClient.Load()
	if m == nil {
		return false, "Mongo Get Write Client Fail While MongoDelete"
	}

	collection := m.Database(fileDBName).Collection(collectionName)
	ctx, cancel := context.WithTimeout(context.Background(), mongoOpTimeout)
	defer cancel()
	deleteResult, err := collection.DeleteOne(ctx, bJsonInterface)
	if err != nil {
		return false, fmt.Sprintf("ERRO:%v", err.Error())
	}

	return true, fmt.Sprintf("Deleted documents:%v", deleteResult.DeletedCount)
}

// 更新文档
func MongoUpdate(fileDBName, collectionName string, bJsonOldInterface, bJsonNewInterface interface{}) (bool, string) {
	m := writeMongoClient.Load()
	if m == nil {
		return false, "Mongo Get Write Client Fail While MongoUpdate"
	}

	collection := m.Database(fileDBName).Collection(collectionName)
	ctx, cancel := context.WithTimeout(context.Background(), mongoOpTimeout)
	defer cancel()
	updateResult, err := collection.UpdateOne(ctx, bJsonOldInterface, bJsonNewInterface)
	if err != nil {
		return false, fmt.Sprintf("ERRO:%v", err.Error())
	}

	return true, fmt.Sprintf("Matched Documents:%v, Modified Documents:%v", updateResult.MatchedCount, updateResult.ModifiedCount)
}

// FindOne方法：查询文档
func MongoFindOne(fileDBName, collectionName string, searchInterface interface{}) (bool, string, bson.M) {
	m := readMongoClient.Load()
	if m == nil {
		return false, "Mongo Get Read Client Fail While MongoFind", nil
	}

	var singleResult bson.M
	ctx, cancel := context.WithTimeout(context.Background(), mongoOpTimeout)
	defer cancel()

	collection := m.Database(fileDBName).Collection(collectionName)
	if err := collection.FindOne(ctx, searchInterface).Decode(&singleResult); err == nil {
		return true, "", singleResult
	} else if errors.Is(err, mongo.ErrNoDocuments) {
		return false, fmt.Sprintf("No Document Found ,searchInterface = %v", searchInterface), nil
	} else {
		errStr := "Mongo MongoFindOne Error: " + err.Error()
		printer.TextError(errStr)
		return false, errStr, nil
	}
}

// 计算集合行数的接口
func MongoCountLine(fileDBName, collectionName string) (bool, string, int64) {
	m := readMongoClient.Load()
	if m == nil {
		return false, "Mongo Get Read Client Fail While MongoCountLine", 0
	}

	ctx, cancel := context.WithTimeout(context.Background(), mongoOpTimeout)
	defer cancel()

	collection := m.Database(fileDBName).Collection(collectionName)
	count, err := collection.EstimatedDocumentCount(ctx)
	if err != nil {
		return false, fmt.Sprintf("fileDBName(%v).collection(%v).EstimatedDocumentCount() Fail", fileDBName, collectionName), 0
	}
	return true, "", count
}

// MongoBatchInsert 批量插入文档（提升性能）
func MongoBatchInsert(fileDBName, collectionName string, documents []interface{}) (bool, string) {
	if len(documents) == 0 {
		return false, "Empty documents array"
	}

	m := writeMongoClient.Load()
	if m == nil {
		return false, "Mongo Get Write Client Fail While MongoBatchInsert"
	}

	collection := m.Database(fileDBName).Collection(collectionName)
	ctx, cancel := context.WithTimeout(context.Background(), mongoOpTimeout)
	defer cancel()

	insertResult, err := collection.InsertMany(ctx, documents)
	if err != nil {
		return false, fmt.Sprintf("ERRO:%v", err.Error())
	}

	return true, fmt.Sprintf("Inserted %d documents", len(insertResult.InsertedIDs))
}

// MongoBatchDelete 批量删除文档
func MongoBatchDelete(fileDBName, collectionName string, filter interface{}) (bool, string) {
	m := writeMongoClient.Load()
	if m == nil {
		return false, "Mongo Get Write Client Fail While MongoBatchDelete"
	}

	collection := m.Database(fileDBName).Collection(collectionName)
	ctx, cancel := context.WithTimeout(context.Background(), mongoOpTimeout)
	defer cancel()

	deleteResult, err := collection.DeleteMany(ctx, filter)
	if err != nil {
		return false, fmt.Sprintf("ERRO:%v", err.Error())
	}

	return true, fmt.Sprintf("Deleted %d documents", deleteResult.DeletedCount)
}

// MongoBatchUpdate 批量更新文档
func MongoBatchUpdate(fileDBName, collectionName string, filter, update interface{}) (bool, string) {
	m := writeMongoClient.Load()
	if m == nil {
		return false, "Mongo Get Write Client Fail While MongoBatchUpdate"
	}

	collection := m.Database(fileDBName).Collection(collectionName)
	ctx, cancel := context.WithTimeout(context.Background(), mongoOpTimeout)
	defer cancel()

	updateResult, err := collection.UpdateMany(ctx, filter, update)
	if err != nil {
		return false, fmt.Sprintf("ERRO:%v", err.Error())
	}

	return true, fmt.Sprintf("Matched:%v Modified:%v", updateResult.MatchedCount, updateResult.ModifiedCount)
}

// MongoFind 查询多个文档（支持limit和skip分页）
func MongoFind(fileDBName, collectionName string, filter interface{}, limit, skip int64) (bool, string, []bson.M) {
	m := readMongoClient.Load()
	if m == nil {
		return false, "Mongo Get Read Client Fail While MongoFind", nil
	}

	collection := m.Database(fileDBName).Collection(collectionName)
	ctx, cancel := context.WithTimeout(context.Background(), mongoOpTimeout)
	defer cancel()

	findOptions := options.Find()
	if limit > 0 {
		findOptions.SetLimit(limit)
	}
	if skip > 0 {
		findOptions.SetSkip(skip)
	}

	cursor, err := collection.Find(ctx, filter, findOptions)
	if err != nil {
		return false, fmt.Sprintf("ERRO:%v", err.Error()), nil
	}
	defer cursor.Close(ctx)

	var results []bson.M
	if err = cursor.All(ctx, &results); err != nil {
		return false, fmt.Sprintf("ERRO:%v", err.Error()), nil
	}

	return true, fmt.Sprintf("Found %d documents", len(results)), results
}

// MongoCreateIndex 创建索引（优化查询性能）
func MongoCreateIndex(fileDBName, collectionName string, keys bson.D, unique bool) (bool, string) {
	m := writeMongoClient.Load()
	if m == nil {
		return false, "Mongo Get Write Client Fail While MongoCreateIndex"
	}

	collection := m.Database(fileDBName).Collection(collectionName)
	ctx, cancel := context.WithTimeout(context.Background(), mongoOpTimeout)
	defer cancel()

	indexModel := mongo.IndexModel{
		Keys: keys,
	}
	if unique {
		indexModel.Options = options.Index().SetUnique(true)
	}

	indexName, err := collection.Indexes().CreateOne(ctx, indexModel)
	if err != nil {
		return false, fmt.Sprintf("ERRO:%v", err.Error())
	}

	printer.TextInfo("MongoCreateIndex", "Created index", indexName, "on", collectionName)
	return true, fmt.Sprintf("Index created: %s", indexName)
}
