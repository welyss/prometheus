package collectors

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/prometheus/client_golang/prometheus"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	ENV_AUTO_DISCOVER_DATABASE_INTERVAL = "AUTO_DISCOVER_DATABASE_INTERVAL"
	ENV_DATABASES_FOR_SLEEP             = "DATABASES_FOR_SLEEP"
	ENV_DATABASE_DRIVER                 = "mysql"
)

var (
	dbsForSleep              = make(map[string]*sql.DB)
	autoDiscoverInterval int = 10
)

type sqlCollector struct {
	sqlDesc *prometheus.Desc
}

func init() {
	if autoDDI := os.Getenv(ENV_AUTO_DISCOVER_DATABASE_INTERVAL); autoDDI != "" {
		autoDiscoverInterval, _ = strconv.Atoi(autoDDI)
	}
	go autoDiscoverDbs()
}

func NewSQLCollector() *sqlCollector {
	return &sqlCollector{
		prometheus.NewDesc("mysqlsleepconn", "count sleep connections", []string{"dbinstance", "id", "user", "host", "db", "command", "state"}, nil),
	}
}

// Describe returns all descriptions of the collector.
func (c *sqlCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.sqlDesc
}

// Collect returns the current state of all metrics of the collector.
func (c *sqlCollector) Collect(ch chan<- prometheus.Metric) {
	//	ch <- prometheus.MustNewConstMetric(c.sqlDesc, prometheus.GaugeValue, float64(2222), "192.168.1.2", "acdb")
	for dbinstance, v := range dbsForSleep {
		rows, err := v.Query("show processlist")
		if err != nil {
			log.Println(err)
		} else {
			defer rows.Close()
			for rows.Next() {
				var id, user, host, db, command, time, state, info sql.NullString
				if err := rows.Scan(&id, &user, &host, &db, &command, &time, &state, &info); err == nil {
					sindex := strings.LastIndex(host.String, ":")
					var hostWithoutPort string
					if sindex > 0 {
						hostWithoutPort = host.String[0:sindex]
					} else {
						hostWithoutPort = host.String
					}
					timeInt, _ := strconv.Atoi(time.String)
					ch <- prometheus.MustNewConstMetric(c.sqlDesc, prometheus.GaugeValue, float64(timeInt),
						 dbinstance, id.String, user.String, hostWithoutPort, db.String, command.String, state.String)
				} else {
					fmt.Println(err)
				}
			}
		}
	}
}

func autoDiscoverDbs() {
	if dbsEnv := os.Getenv(ENV_DATABASES_FOR_SLEEP); dbsEnv != "" {
		for _, db := range strings.Split(dbsEnv, ",") {
			if _, ok := dbsForSleep[db]; !ok {
				// add db
				dbstr := strings.ToUpper(db)
				dbHost := os.Getenv("HBEC_DBINSTANCE_" + dbstr + "_HOST")
				dbUser := os.Getenv("HBEC_DBINSTANCE_" + dbstr + "_USER")
				dbPass := os.Getenv("HBEC_DBINSTANCE_" + dbstr + "_PASSWORD")
				if dbHost != "" && dbUser != "" {
					dbProtocol := os.Getenv("HBEC_DBINSTANCE_" + dbstr + "_PROTOCOL")
					if dbProtocol == "" {
						dbProtocol = "tcp"
					}
					dbPort := os.Getenv("HBEC_DBINSTANCE_" + dbstr + "_PORT")
					if dbPort == "" {
						dbPort = "3306"
					}
					dbName := os.Getenv("HBEC_DBINSTANCE_" + dbstr + "_DBNAME")
					dsn := dbUser + ":" + dbPass + "@" + dbProtocol + "(" + dbHost + ":" + dbPort + ")/" + dbName
					if datasource, err := sql.Open(ENV_DATABASE_DRIVER, dsn); err == nil {
						maxIdleConns := os.Getenv("HBEC_DBINSTANCE_" + dbstr + "_MAXIDLECONNS")
						if maxIdleConns != "" {
							ai, _ := strconv.Atoi(maxIdleConns)
							datasource.SetMaxIdleConns(ai)
						} else {
							datasource.SetMaxIdleConns(1)
						}
						maxOpenConns := os.Getenv("HBEC_DBINSTANCE_" + dbstr + "_MAXOPENCONNS")
						if maxOpenConns != "" {
							ai, _ := strconv.Atoi(maxOpenConns)
							datasource.SetMaxOpenConns(ai)
						} else {
							datasource.SetMaxOpenConns(10)
						}
						connMaxLifetime := os.Getenv("HBEC_DBINSTANCE_" + dbstr + "_CONNMAXLIFETIME")
						if connMaxLifetime != "" {
							ai, _ := strconv.Atoi(connMaxLifetime)
							datasource.SetConnMaxLifetime(time.Duration(ai))
						}
						dbsForSleep[db] = datasource
					}
				}
			}
		}
	}
}
