package main

import (
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/spf13/viper"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"
)

var attrKeys struct {
	credFile  string
	queryFile string
	database  string
	scvFile   string
}

var mysqlCredentials struct {
	user string
	pass string
}

var mysqlQuery struct {
	queries     []string
	queryResult [][]string
}

func getArguments() {
	flag.StringVar(&attrKeys.credFile, "creds", attrKeys.credFile, "Path to file with credentials")
	flag.StringVar(&attrKeys.queryFile, "query", attrKeys.queryFile, "Path to file with queries")
	flag.StringVar(&attrKeys.database, "db", attrKeys.database, "Database name. Default: mysql")
	flag.StringVar(&attrKeys.scvFile, "csv", attrKeys.scvFile, "File for saving result")

	flag.Parse()

	if attrKeys.credFile == "" || attrKeys.queryFile == "" {
		fmt.Println("Flag credentials and queries are required")
		os.Exit(2)
	}
}

func runQuery() {
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(127.0.0.1:3306)/%s", mysqlCredentials.user, mysqlCredentials.pass, attrKeys.database))
	defer db.Close()
	if err != nil {
		log.Fatal(err)
	}
	mysqlQuery.queryResult = [][]string{
		{"CHECK TIME;ID;USER;HOST;DB;COMMAND;TIME;STATE;INFO;"},
	}

	for _, query := range mysqlQuery.queries {
		if query != "" {
			result, err := db.Query(query)
			if err != nil {
				log.Fatal(err)
			}

			defer func(result *sql.Rows) {
				err := result.Close()
				if err != nil {
					log.Fatal(err)
				}
			}(result)

			for result.Next() {
				var ID, TIME int
				var USER, HOST, COMMAND, STATE string
				var DB sql.NullString
				var INFO sql.NullString

				err := result.Scan(&ID, &USER, &HOST, &DB, &COMMAND, &TIME, &STATE, &INFO)
				if err != nil {
					log.Fatal(err)
				}
				DATE := time.Now().Format("2006-01-02 15:04:05")
				mysqlQuery.queryResult = append(mysqlQuery.queryResult, []string{
					fmt.Sprintf("%s;%d;%s;%s;%v;%s;%d;%v;%s;", DATE, ID, USER, HOST, DB.String, COMMAND, TIME, STATE, INFO.String),
				})
			}
		}
	}
	defer db.Close()

}

func readQueryFile() {
	file, err := os.Open(attrKeys.queryFile)
	if err != nil {
		fmt.Println("CRITICAL: Cant read file!")
		os.Exit(2)
	}

	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			fmt.Println("CRITICAL: Cant close file!")
			os.Exit(2)
		}
	}(file)

	data, err := ioutil.ReadAll(file)
	if err != nil {
		fmt.Printf("Perhaps file corrupted: %s\n", err)
		os.Exit(2)
	}
	mysqlQuery.queries = strings.Split(string(data), "\n")

}

func readCredentialsFile() {
	filename := strings.Split(attrKeys.credFile, "/")
	if strings.Contains(filename[len(filename)-1], ".") {
		viper.SetConfigName(strings.Split(filename[len(filename)-1], ".")[0])
	}
	path := strings.Join(filename[:len(filename)-1], "/")
	viper.SetConfigType("ini")
	viper.AddConfigPath(path)

	err := viper.ReadInConfig() // Find and read the config file
	if err != nil {             // Handle errors reading the config file
		panic(fmt.Errorf("fatal error config file: %w", err))
	}

	mysqlCredentials.user = viper.GetString("dba.dba_username")
	mysqlCredentials.pass = viper.GetString("dba.password")
}

func writeResult() {
	_, err := os.Stat(attrKeys.scvFile)
	if os.IsNotExist(err) {
		file, err := os.Create(attrKeys.scvFile)
		if err != nil {
			fmt.Println("Cant create file!")
			os.Exit(2)
		}
		defer func(file *os.File) {
			err := file.Close()
			if err != nil {
				fmt.Println("Cant close file!")
				os.Exit(2)
			}
		}(file)

		writer := csv.NewWriter(file)
		defer writer.Flush()

		for _, value := range mysqlQuery.queryResult {
			err := writer.Write(value)
			if err != nil {
				fmt.Println("Cant write to file!")
				os.Exit(2)
			}
		}

	} else {
		file, err := os.OpenFile(attrKeys.scvFile, os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			fmt.Println("Cant open file!")
			os.Exit(2)
		}
		defer func(file *os.File) {
			err := file.Close()
			if err != nil {
				fmt.Println("Cant close file!")
				os.Exit(2)
			}
		}(file)

		writer := csv.NewWriter(file)
		defer writer.Flush()
		for _, value := range mysqlQuery.queryResult {
			if !strings.Contains(value[0], "CHECK TIME") {
				err := writer.Write(value)
				if err != nil {
					fmt.Println("Cant write to file!")
					os.Exit(2)
				}
			}
		}
	}
}

func main() {
	getArguments()
	readCredentialsFile()
	readQueryFile()
	runQuery()

	if attrKeys.scvFile != "" {
		writeResult()
	} else {
		fmt.Println("+---------------------+--------+-----------------------+----------------------------------------+------------------+---------+------+-------+------+")
		fmt.Println("| TIME ON HOST        |ID      | USER                  | HOST                                   | DB               | COMMAND | TIME | STATE | INFO |")
		fmt.Println("+---------------------+--------+-----------------------+----------------------------------------+----------------- +---------+------+-------+------+")
		for _, value := range mysqlQuery.queryResult {
			if !strings.Contains(value[0], "ID") {
				fmt.Println(" " + strings.Join(strings.Split(value[0], ";"), "   "))
			}
		}
	}
}
