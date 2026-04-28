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
			readURI := fmt.Sprintf("mongodb://%s%s:%s", userAndPassWord, mongoReadDBAddr, mongoReadDBPort)
			writeURI := fmt.Sprintf("mongodb://%s%s:%s", userAndPassWord, mongoWriteDBAddr, mongoWriteDBPort)
			readClientOptions = options.Client().ApplyURI(readURI)
			writeClientOptions = options.Client().ApplyURI(writeURI)

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
func MongoInsert(fileDBName, collectionName string, bJsonInterface interface{}) error {
	m := writeMongoClient.Load()
	if m == nil {
		return errors.New("MongoInsert: write client is nil")
	}

	collection := m.Database(fileDBName).Collection(collectionName)
	ctx, cancel := context.WithTimeout(context.Background(), mongoOpTimeout)
	defer cancel()
	insertResult, err := collection.InsertOne(ctx, bJsonInterface)
	if err != nil {
		return fmt.Errorf("MongoInsert %s.%s: %w", fileDBName, collectionName, err)
	}

	printer.TextInfo("MongoInsert", "db", fileDBName, "collection", collectionName, "insertedID", insertResult.InsertedID)
	return nil
}

// 删除文档
func MongoDelete(fileDBName, collectionName string, bJsonInterface interface{}) error {
	m := writeMongoClient.Load()
	if m == nil {
		return errors.New("MongoDelete: write client is nil")
	}

	collection := m.Database(fileDBName).Collection(collectionName)
	ctx, cancel := context.WithTimeout(context.Background(), mongoOpTimeout)
	defer cancel()
	deleteResult, err := collection.DeleteOne(ctx, bJsonInterface)
	if err != nil {
		return fmt.Errorf("MongoDelete %s.%s: %w", fileDBName, collectionName, err)
	}

	printer.TextInfo("MongoDelete", "db", fileDBName, "collection", collectionName, "deletedCount", deleteResult.DeletedCount)
	return nil
}

// 更新文档
func MongoUpdate(fileDBName, collectionName string, bJsonOldInterface, bJsonNewInterface interface{}) error {
	m := writeMongoClient.Load()
	if m == nil {
		return errors.New("MongoUpdate: write client is nil")
	}

	collection := m.Database(fileDBName).Collection(collectionName)
	ctx, cancel := context.WithTimeout(context.Background(), mongoOpTimeout)
	defer cancel()
	updateResult, err := collection.UpdateOne(ctx, bJsonOldInterface, bJsonNewInterface)
	if err != nil {
		return fmt.Errorf("MongoUpdate %s.%s: %w", fileDBName, collectionName, err)
	}

	printer.TextInfo("MongoUpdate", "db", fileDBName, "collection", collectionName, "matched", updateResult.MatchedCount, "modified", updateResult.ModifiedCount)
	return nil
}

// FindOne方法：查询文档（未找到记录返回 mongo.ErrNoDocuments，调用方可通过 errors.Is 判断）
func MongoFindOne(fileDBName, collectionName string, searchInterface interface{}) (bson.M, error) {
	m := readMongoClient.Load()
	if m == nil {
		return nil, errors.New("MongoFindOne: read client is nil")
	}

	var singleResult bson.M
	ctx, cancel := context.WithTimeout(context.Background(), mongoOpTimeout)
	defer cancel()

	collection := m.Database(fileDBName).Collection(collectionName)
	err := collection.FindOne(ctx, searchInterface).Decode(&singleResult)
	if err == nil {
		return singleResult, nil
	}
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, fmt.Errorf("MongoFindOne %s.%s: %w", fileDBName, collectionName, err)
	}
	printer.TextError("MongoFindOne Error", "err", err.Error())
	return nil, fmt.Errorf("MongoFindOne %s.%s: %w", fileDBName, collectionName, err)
}

// 计算集合行数的接口
func MongoCountLine(fileDBName, collectionName string) (int64, error) {
	m := readMongoClient.Load()
	if m == nil {
		return 0, errors.New("MongoCountLine: read client is nil")
	}

	ctx, cancel := context.WithTimeout(context.Background(), mongoOpTimeout)
	defer cancel()

	collection := m.Database(fileDBName).Collection(collectionName)
	count, err := collection.EstimatedDocumentCount(ctx)
	if err != nil {
		return 0, fmt.Errorf("MongoCountLine %s.%s: %w", fileDBName, collectionName, err)
	}
	return count, nil
}

// MongoBatchInsert 批量插入文档（提升性能）
func MongoBatchInsert(fileDBName, collectionName string, documents []interface{}) error {
	if len(documents) == 0 {
		return errors.New("MongoBatchInsert: empty documents array")
	}

	m := writeMongoClient.Load()
	if m == nil {
		return errors.New("MongoBatchInsert: write client is nil")
	}

	collection := m.Database(fileDBName).Collection(collectionName)
	ctx, cancel := context.WithTimeout(context.Background(), mongoOpTimeout)
	defer cancel()

	insertResult, err := collection.InsertMany(ctx, documents)
	if err != nil {
		return fmt.Errorf("MongoBatchInsert %s.%s: %w", fileDBName, collectionName, err)
	}

	printer.TextInfo("MongoBatchInsert", "db", fileDBName, "collection", collectionName, "inserted", len(insertResult.InsertedIDs))
	return nil
}

// MongoBatchDelete 批量删除文档
func MongoBatchDelete(fileDBName, collectionName string, filter interface{}) error {
	m := writeMongoClient.Load()
	if m == nil {
		return errors.New("MongoBatchDelete: write client is nil")
	}

	collection := m.Database(fileDBName).Collection(collectionName)
	ctx, cancel := context.WithTimeout(context.Background(), mongoOpTimeout)
	defer cancel()

	deleteResult, err := collection.DeleteMany(ctx, filter)
	if err != nil {
		return fmt.Errorf("MongoBatchDelete %s.%s: %w", fileDBName, collectionName, err)
	}

	printer.TextInfo("MongoBatchDelete", "db", fileDBName, "collection", collectionName, "deleted", deleteResult.DeletedCount)
	return nil
}

// MongoBatchUpdate 批量更新文档
func MongoBatchUpdate(fileDBName, collectionName string, filter, update interface{}) error {
	m := writeMongoClient.Load()
	if m == nil {
		return errors.New("MongoBatchUpdate: write client is nil")
	}

	collection := m.Database(fileDBName).Collection(collectionName)
	ctx, cancel := context.WithTimeout(context.Background(), mongoOpTimeout)
	defer cancel()

	updateResult, err := collection.UpdateMany(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("MongoBatchUpdate %s.%s: %w", fileDBName, collectionName, err)
	}

	printer.TextInfo("MongoBatchUpdate", "db", fileDBName, "collection", collectionName, "matched", updateResult.MatchedCount, "modified", updateResult.ModifiedCount)
	return nil
}

// MongoFind 查询多个文档（支持limit和skip分页）
func MongoFind(fileDBName, collectionName string, filter interface{}, limit, skip int64) ([]bson.M, error) {
	m := readMongoClient.Load()
	if m == nil {
		return nil, errors.New("MongoFind: read client is nil")
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
		return nil, fmt.Errorf("MongoFind %s.%s: %w", fileDBName, collectionName, err)
	}
	defer cursor.Close(ctx)

	var results []bson.M
	if err = cursor.All(ctx, &results); err != nil {
		return nil, fmt.Errorf("MongoFind %s.%s cursor.All: %w", fileDBName, collectionName, err)
	}

	return results, nil
}

// MongoCreateIndex 创建索引（优化查询性能）
func MongoCreateIndex(fileDBName, collectionName string, keys bson.D, unique bool) error {
	m := writeMongoClient.Load()
	if m == nil {
		return errors.New("MongoCreateIndex: write client is nil")
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
		return fmt.Errorf("MongoCreateIndex %s.%s: %w", fileDBName, collectionName, err)
	}

	printer.TextInfo("MongoCreateIndex", "Created index", indexName, "on", collectionName)
	return nil
}
