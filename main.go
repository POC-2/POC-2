package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/olivere/elastic"
)

type Business struct {
	Business_address      string    `json:"business_address"`
	Business_city         string    `json:"business_city"`
	Business_id           string    `json:"business_id"`
	Business_name         string    `json:"business_name"`
	Business_longitude    string    `json:"business_longitude"`
	Business_postal_code  string    `json:"business_postal_code"`
	Business_state        string    `json:"business_state"`
	Inspection_date       time.Time `json:"inspection_date"`
	Inspection_id         string    `json:"inspection_id"`
	Inspection_score      int64     `json:"inspection_score"`
	Inspection_type       string    `json:"inspection_type"`
	Risk_category         string    `json:"risk_category"`
	Violation_description string    `json:"violation_description"`
	Violation_id          string    `json:"violation_id"`
}

func GetESClient() (*elastic.Client, error) {

	client, err := elastic.NewClient(elastic.SetURL("http://localhost:9200"),
		elastic.SetSniff(false),
		elastic.SetHealthcheck(false))

	fmt.Println("ES initialized...")

	return client, err

}

func homePage(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Home page")
	fmt.Println("endpoint hit: HomePage")
}

func insert() string {
	ctx := context.Background()
	esclient, err := GetESClient()
	if err != nil {
		fmt.Println("error initializing : ", err)
		panic("client fail")
	}
	newRecord := Business{
		Business_address:      "xyz",
		Business_city:         "pune",
		Business_id:           "123",
		Business_name:         "VedankB",
		Business_longitude:    "5",
		Business_postal_code:  "123",
		Business_state:        "MH",
		Inspection_date:       time.Now(),
		Inspection_id:         "1010",
		Inspection_score:      10,
		Inspection_type:       "strict",
		Risk_category:         "high",
		Violation_description: "none",
		Violation_id:          "113",
	}

	dataJson, err := json.Marshal(newRecord)
	js := string(dataJson)

	ind, err := esclient.Index().Index("poc_two").BodyJson(js).Do(ctx)

	fmt.Println(ind)

	if err != nil {
		panic(err)
	}

	fmt.Println("Insertion successful")

	return string(js)
}

func insert_api(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, insert())
	fmt.Println("endpoint hit: insert")

}

func handleRequests() {
	http.HandleFunc("/", homePage)
	http.HandleFunc("/insert", insert_api)
	log.Fatal(http.ListenAndServe(":8090", nil))
}

func main() {
	handleRequests()
}
