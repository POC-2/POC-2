package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"
	log "github.com/sirupsen/logrus"
	"github.com/olivere/elastic"
	"poc2.com/POC-2/elasticclient"
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

func PaginateService(from int, size int, w http.ResponseWriter){
	file, _ := os.OpenFile("logs.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	ctx := context.Background()
	esclient, err := elasticclient.GetESClient()
	if err != nil {
		log.SetOutput(file)
		log.Println("Error initializing : ", err)
		log.Fatal("Client fail ")
	}

	var businesses []Business

	searchSource := elastic.NewSearchSource()
	searchSource.Query(elastic.NewMatchAllQuery())

	queryStr, err1 := searchSource.Source()
	queryJs, err2 := json.Marshal(queryStr)

	if err1 != nil || err2 != nil {
		log.SetOutput(file)
		log.Println("[esclient][GetResponse]err during query marshal=", err1, err2)
	}
	log.Println("[esclient]Final ESQuery=\n", string(queryJs))

	searchService := esclient.Search().Index("poc_two").SearchSource(searchSource).From(from).Size(size).RestTotalHitsAsInt(true)

	searchResult, err := searchService.Do(ctx)
	if err != nil {
		log.SetOutput(file)
		log.Fatal("[ProductsES][GetPIds]Error=", err)
		return
	}
	// fmt.Print("hits: ", searchResult.Hits.Hits)
	for _, hit := range searchResult.Hits.Hits {
		var business Business
		// fmt.Println("Hit source: ", hit.InnerHits)
		err := json.Unmarshal(*hit.Source, &business)
		if err != nil {
			log.SetOutput(file)
			log.Println("[Getting Businesses][Unmarshal] Err=", err)
		}

		businesses = append(businesses, business)
		json.NewEncoder(w).Encode(hit)
	}

	if err != nil {
		log.SetOutput(file)
		log.Error("Fetching business fail: ", err)
	} else {
		for _, s := range businesses {
			fmt.Printf("Businesses found Name: %s, Ins_id: %s, Vio_id: %s \n", s.Business_name, s.Inspection_id, s.Violation_id)
		}
	}
}

func InsertDataService(reqBody []byte, w http.ResponseWriter){
	file, _ := os.OpenFile("logs.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	ctx := context.Background()
	esclient, err := elasticclient.GetESClient()
	if err != nil {
		log.SetOutput(file)
		log.Error("Error initializing : ", err)
		panic("Client fail ")
	}
	var business Business
	json.Unmarshal(reqBody, &business)
	fmt.Println("unmarshal hua")
	dataJSON, err := json.Marshal(business)
	js := string(dataJSON)
	_, err = esclient.Index().
		Index("poc_two").
		Type("_doc").
		BodyJson(js).
		Id(business.Inspection_id).
		Do(ctx)
	if err != nil {
		log.SetOutput(file)
		log.Error(err)
	}
	json.NewEncoder(w).Encode("Insertion Succesful!!")
	log.SetOutput(file)
	log.Println("[Elastic][InsertProduct]Insertion Successful")
}

func DeleteDataService(delval string, w http.ResponseWriter){
	file, _ := os.OpenFile("logs.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	ctx := context.Background()
	esclient, err := elasticclient.GetESClient()
	if err != nil {
		log.SetOutput(file)
		log.Error("Error initializing : ", err)
		log.Println("Client fail ")
	}
	boolQuery := elastic.NewBoolQuery().Must(elastic.NewTermQuery("inspection_id", delval))
	result2, err2 := elastic.NewDeleteByQueryService(esclient).
		Index("poc_two").
		Query(boolQuery).
		Do(ctx)
	log.Println("DELETE RESPONSE 2: \n", result2, err2)
	json.NewEncoder(w).Encode("Deletion Successful!!")
}

func GetDataService(idval string, w http.ResponseWriter){
	file, _ := os.OpenFile("logs.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	ctx := context.Background()
	esclient, err := elasticclient.GetESClient()
	if err != nil {
		log.SetOutput(file)
		log.Error("Error initializing : ", err)
		log.Println("Client fail ")
	}
	var businesses []Business
	searchService := esclient.Search().Index("poc_two").Query(elastic.NewMatchQuery("inspection_id", idval)).RestTotalHitsAsInt(true)
	searchResult, err := searchService.Do(ctx)
	if err != nil {
		log.SetOutput(file)
		log.Error("[ProductsES][GetPIds]Error=", err)
		return
	}
	if len(searchResult.Hits.Hits) == 0 {
		json.NewEncoder(w).Encode("No data found")
		log.SetOutput(file)
		log.Error("No data found!!")
		return
	}
	// fmt.Print("hits: ", len(searchResult.Hits.Hits))
	for _, hit := range searchResult.Hits.Hits {
		var business Business
		// fmt.Println("Hit source: ", hit.InnerHits)
		err := json.Unmarshal(*hit.Source, &business)
		if err != nil {
			log.SetOutput(file)
			log.Error("[Getting Businesses][Unmarshal] Err=", err)
		}
		businesses = append(businesses, business)
		json.NewEncoder(w).Encode(hit)
	}
	if err != nil {
		log.Println("Fetching business fail: ", err)
	} else {
		for _, s := range businesses {
			fmt.Printf("Businesses found Name: %s, Ins_id: %s, Vio_id: %s \n", s.Business_name, s.Inspection_id, s.Violation_id)
		}
	}
}

func SortService(fieldval []string, size int, typeval []string, w http.ResponseWriter){
	file, _ := os.OpenFile("logs.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	ctx := context.Background()
	esclient, err := elasticclient.GetESClient()
	if err != nil {
		log.SetOutput(file)
		log.Error("Error initializing : ", err)
		log.Panic("Client fail ")
	}
	var businesses []Business
	if fieldval[0] != "inspection_score" {
		fieldval[0] = fieldval[0] + ".keyword"
	}
	flag := true
	if typeval[0] == "asc" || typeval[0] == "ascending" {
		flag = true
	} else if typeval[0] == "desc" || typeval[0] == "descending" {
		flag = false
	}
	searchService := esclient.Search().Index("poc_two").Query(elastic.NewMatchAllQuery()).Size(size).Sort(fieldval[0], flag).RestTotalHitsAsInt(true)
	searchResult, err := searchService.Do(ctx)
	if err != nil {
		log.SetOutput(file)
		log.Error("[ProductsES][GetPIds]Error=", err)
		return
	}
	// fmt.Print("hits: ", searchResult.Hits.Hits)
	for _, hit := range searchResult.Hits.Hits {
		var business Business
		// fmt.Println("Hit source: ", hit.InnerHits)
		err := json.Unmarshal(*hit.Source, &business)
		if err != nil {
			log.SetOutput(file)
			log.Error("[Getting Businesses][Unmarshal] Err=", err)
		}

		businesses = append(businesses, business)
		json.NewEncoder(w).Encode(hit)
	}

	if err != nil {
		log.Println("Fetching business fail: ", err)
	} else {
		for _, s := range businesses {
			log.SetOutput(file)
			log.Printf("Businesses found Name: %s, Ins_id: %s, Vio_id: %s \n", s.Business_name, s.Inspection_id, s.Violation_id)
		}
	}
}