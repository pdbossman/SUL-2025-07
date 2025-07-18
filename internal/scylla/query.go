package scylla

import (
	"github.com/gocql/gocql"
	"go.uber.org/zap"
)

func SelectQuery(session *gocql.Session, logger *zap.Logger) {
	logger.Info("Displaying Results:")
	qIP := session.Query("SELECT cast(listen_address as varchar) as listen_address FROM system.local")
	var listen_address string
	itIP := qIP.Iter()
	defer func() {
		if err := itIP.Close(); err != nil {
			logger.Warn("select system.local", zap.Error(err))
		}
	}()
	for itIP.Scan(&listen_address) {
		logger.Info("\t" + listen_address + " " )
	}

	q := session.Query("SELECT partitionkey1,clusterkey1,data1,data2 FROM kstest1.tbtest")
	var partitionkey1,clusterkey1,data1,data2 string
	it := q.Iter()
	defer func() {
		if err := it.Close(); err != nil {
			logger.Warn("select kstest1.tbtest", zap.Error(err))
		}
	}()
	for it.Scan(&partitionkey1, &clusterkey1, &data1, &data2) {
		logger.Info("\t" + partitionkey1 + " " + clusterkey1)
	}
}
