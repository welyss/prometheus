package collectors

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	ENV_DATABASE_DRIVER              = "mysql"
	ENV_DBMONITOR_CONFIGURATION_PATH = "DBMONITOR_CONFIGURATION"
)

var (
	dbs              = make(map[string]*sql.DB)
	yamlPath                 = os.Getenv(ENV_DBMONITOR_CONFIGURATION_PATH)
	conf                     = new(Configuration)
)

type sqlCollector struct {
	sqlDesc *prometheus.Desc
}

type Configuration struct {
	AutoDiscoverInterval int `yaml:"autoDiscoverInterval"`
	Datasource           struct {
		Mysql []struct {
			Instance string
			Schema                   string
			Host                     string
			Port                     string
			User                     string
			Password                 string
			Protocol                 string
			MaxIdleConns             int `yaml:"maxIdleConns"`
			MaxOpenConns             int `yaml:"maxOpenConns"`
			ConnMaxLifeTimeInSeconds int `yaml:"connMaxLifeTimeInSeconds"`
		} `yaml:"mysql"`
	} `yaml:"datasource"`
}

func init() {
	log.SetOutput(os.Stdout)
	if yamlPath == "" {
		yamlPath = "/dbmonitor/conf.yaml"
	}
	autoDiscoverDbs()
}

func NewSQLCollector() *sqlCollector {
	return &sqlCollector{
		prometheus.NewDesc("mysqlconn", "count sleep connections", []string{"dbinstance", "id", "user", "host", "db", "command", "state"}, nil),
	}
}

// Describe returns all descriptions of the collector.
func (c *sqlCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.sqlDesc
}

// Collect returns the current state of all metrics of the collector.
func (c *sqlCollector) Collect(ch chan<- prometheus.Metric) {
	//	ch <- prometheus.MustNewConstMetric(c.sqlDesc, prometheus.GaugeValue, float64(2222), "192.168.1.2", "acdb")
	for dbinstance, v := range dbs {
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
	if data, err := ioutil.ReadFile(yamlPath); err != nil {
		log.Fatalf("error: %v", err)
	} else {
		if err = yaml.Unmarshal(data, &conf); err != nil {
			log.Fatalf("error: %v", err)
		}
	}
	log.Printf("AutoDiscoverInterval: %d\n", conf.AutoDiscoverInterval)
	for _, ds := range conf.Datasource.Mysql {
		if _, ok := dbs[ds.Instance]; !ok {
			// add db
			if ds.Host != "" && ds.User != "" {
				if ds.Protocol == "" {
					ds.Protocol = "tcp"
				}
				if ds.Port == "" {
					ds.Port = "3306"
				}
				dsn := ds.User + ":" + ds.Password + "@" + ds.Protocol + "(" + ds.Host + ":" + ds.Port + ")/" + ds.Schema
				if datasource, err := sql.Open(ENV_DATABASE_DRIVER, dsn); err == nil {
					datasource.SetMaxIdleConns(ds.MaxIdleConns)
					datasource.SetMaxOpenConns(ds.MaxOpenConns)
					datasource.SetConnMaxLifetime(time.Second * time.Duration(ds.ConnMaxLifeTimeInSeconds))
					dbs[ds.Instance] = datasource
					log.Printf("Instance: %v(MaxIdleConns:%v, MaxOpenConns:%v, ConnMaxLifeTimeInSeconds:%v) has been loaded.\n",
						ds.Instance, ds.MaxIdleConns, ds.MaxOpenConns, ds.ConnMaxLifeTimeInSeconds)
				}
			}
		}
	}
}
