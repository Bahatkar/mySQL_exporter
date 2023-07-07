package main

import (
	"database/sql"
	"fmt"
	"log"
	"mysql_exporter/repository"
	"net/http"

	_ "github.com/go-sql-driver/mysql"
	"github.com/spf13/viper"
)

func main() {
	if err := initConfig(); err != nil {
		log.Fatalf("Error occured while reading configs: %s\n", err)
	}

	srvHost := viper.GetString("localServer.host")
	srvPort := viper.GetString("localServer.port")
	allDBNames := viper.GetStringSlice("db.dbname")

	dbc := repository.DBConfig{
		Host:     viper.GetString("db.host"),
		Port:     viper.GetString("db.port"),
		Username: viper.GetString("db.username"),
		Password: viper.GetString("db.password"),
	}

	srvr := func(w http.ResponseWriter, r *http.Request) {
		var prometheusResponse string

		for _, dbName := range allDBNames {
			dbc.DBName = dbName
			//Подключение к бд
			db, err := repository.OpenMySQL(dbc)
			if err != nil {
				log.Fatalf("Error occured while connecting to DB: %s\n", err)
			}
			defer db.Close()

			users, err := getMetrics(db)
			if err != nil {
				log.Printf("Error occured while collecting metrics: %s\n", err)
			}

			//Формирую ответ
			for user, userCount := range users {
				prometheusResponse += fmt.Sprintf("mysql_users_by_count{method=\"user\", user=\"%s\", database=\"%s\"} %d\n",
					user, dbc.DBName, userCount)
			}
		}
		log.Println(prometheusResponse)
		//Отдаю Прометеусу ответ
		fmt.Fprint(w, prometheusResponse)
	}

	http.HandleFunc("/metrics", srvr)
	log.Fatal(http.ListenAndServe(fmt.Sprintf("%s:%s", srvHost, srvPort), nil))
}

// Получение инфы из бд для формирования метрик
func getMetrics(db *sql.DB) (map[string]int, error) {
	var (
		user               string
		userCount, cycleID int
		usersByCount       = make(map[string]int)
	)
	//Достаю из базы айди актуального цикла
	rows, err := db.Query(`select max(pc.id) from parser_cycle pc`)
	if err != nil {
		return nil, err
	}
	rows.Scan()
	for rows.Next() {
		rows.Scan(&cycleID)
	}

	//Достаю из базы имена пользователей и кол-во проработанных ими объектов
	rows, err = db.Query(`select 
							concat(au.firstname, " ", au.lastname) as username,
							count(pcb.building_id) as cnt
						from
							parser_cycle_building pcb
						left join auth_user au on au.id = pcb.user_id
						left join parser_building pb on pb.id = pcb.building_id 
						where pcb.status = 1
							and pb.sale_status = 1
							and pcb.cycle_id = ?
						group by username`, cycleID)
	if err != nil {
		return nil, err
	}

	//Разбираю результат запроса в мапу
	rows.Scan()
	for rows.Next() {
		rows.Scan(&user, &userCount)
		usersByCount[user] = userCount
	}

	return usersByCount, nil
}

// Получение конфигов из .yaml файла
func initConfig() error {
	viper.AddConfigPath("./")
	viper.SetConfigName("config")
	return viper.ReadInConfig()
}
