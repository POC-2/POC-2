package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/olivere/elastic"
	log "github.com/sirupsen/logrus"
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

func PaginateService(from int, size int, w http.ResponseWriter) {
	file, _ := os.OpenFile("logs.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	ctx := context.Background()
	esclient, err := elasticclient.GetESClient()
	if err != nil {
		w.WriteHeader(500)
		log.SetOutput(file)
		log.Println("Endpoint hit: Paginate (Service),  Output: Error initializing : ", err)
		log.Fatal("Endpoint hit: Paginate (Service),  Output: Client fail ")
	}

	var businesses []Business

	searchSource := elastic.NewSearchSource()
	searchSource.Query(elastic.NewMatchAllQuery())

	queryStr, err1 := searchSource.Source()
	queryJs, err2 := json.Marshal(queryStr)

	if err1 != nil || err2 != nil {
		w.WriteHeader(500)
		log.SetOutput(file)
		log.Error("Endpoint hit: Paginate (Service),  Output: (EsClient)err during query marshal=", err1, err2)
	}
	fmt.Println("[esclient]Final ESQuery=\n", string(queryJs))

	searchService := esclient.Search().Index("poc_two").SearchSource(searchSource).From(from).Size(size).RestTotalHitsAsInt(true)

	searchResult, err := searchService.Do(ctx)
	if err != nil {
		w.WriteHeader(500)
		log.SetOutput(file)
		log.Error("Endpoint hit: Paginate (Service),  Output: SearchResult Error=", err)
		return
	}
	if len(searchResult.Hits.Hits) == 0 {
		w.WriteHeader(404)
		json.NewEncoder(w).Encode("No data found")
		log.SetOutput(file)
		log.Error("Endpoint hit: Paginate (Service), Output: No data found!!")
		return
	}

	for _, hit := range searchResult.Hits.Hits {
		var business Business
		// fmt.Println("Hit source: ", hit.InnerHits)
		err := json.Unmarshal(*hit.Source, &business)
		if err != nil {
			w.WriteHeader(500)
			log.SetOutput(file)
			log.Error("Endpoint hit: Paginate (Service),  Output: [Getting Businesses][Unmarshal] Err=", err)
		}

		businesses = append(businesses, business)
		json.NewEncoder(w).Encode(hit)
	}

	if err != nil {
		w.WriteHeader(500)
		log.SetOutput(file)
		log.Error("Endpoint hit: Paginate (Service),  Output: Fetching business fail: ", err)
	} else {
		log.SetOutput(file)
		log.Printf("Endpoint hit: Paginate (Service),  Output: Pagination successful with values from = %d and size = %d", from, size)
		for _, s := range businesses {
			fmt.Printf("Businesses found Name: %s, Ins_id: %s, Vio_id: %s \n", s.Business_name, s.Inspection_id, s.Violation_id)
		}
	}
}

func InsertDataService(reqBody []byte, flag int, w http.ResponseWriter) {
	file, _ := os.OpenFile("logs.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	ctx := context.Background()
	esclient, err := elasticclient.GetESClient()
	if err != nil {
		w.WriteHeader(500)
		log.SetOutput(file)
		if flag == 0 {
			log.Error("Endpoint hit: Insert Data (Service), Output: (Client Fail)Error initializing : ", err)
		} else {
			log.Error("Endpoint hit: Insert Data (Service), Output: (Updating)(Client Fail)Error initializing : ", err)
		}
	}
	var business Business
	json.Unmarshal(reqBody, &business)
	dataJSON, err := json.Marshal(business)
	js := string(dataJSON)
	_, err = esclient.Index().
		Index("poc_two").
		Type("_doc").
		BodyJson(js).
		Id(business.Inspection_id).
		Do(ctx)
	if err != nil {
		w.WriteHeader(500)
		log.SetOutput(file)
		if flag == 0 {
			log.Error("Endpoint hit: Insert Data (Service), Output:", err)
		} else {
			log.Error("Endpoint hit: Insert Data (Service)(Updating), Output:", err)
		}

	}
	if flag == 0 {
		json.NewEncoder(w).Encode("Insertion Succesful!!")
	} else {
		json.NewEncoder(w).Encode("Updation Succesful!!")
	}
	log.SetOutput(file)
	if flag == 0 {
		log.Println("Endpoint hit: Insert Data (Service), Output: Insertion Successful")
	} else {
		log.Println("Endpoint hit: Insert Data (Service), Output: Updation Successful")
	}

}

func DeleteDataService(delval string, w http.ResponseWriter) {
	file, _ := os.OpenFile("logs.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	ctx := context.Background()
	esclient, err := elasticclient.GetESClient()
	if err != nil {
		w.WriteHeader(500)
		log.SetOutput(file)
		log.Error("Endpoint hit: Delete Data (Service), Output: (Client Fail)Error initializing : ", err)
	}
	boolQuery := elastic.NewBoolQuery().Must(elastic.NewTermQuery("inspection_id", delval))
	_, err2 := elastic.NewDeleteByQueryService(esclient).
		Index("poc_two").
		Query(boolQuery).
		Do(ctx)
	if err2 != nil {
		w.WriteHeader(500)
		log.SetOutput(file)
		log.Error("Endpoint hit: Delete Data (Service), Output: Error:", err2)
	}
	log.Println("Endpoint hit: Delete Data (Service), Output: Deletion Successful!!")
	json.NewEncoder(w).Encode("Deletion Successful!!")
}

func GetDataService(idval string, w http.ResponseWriter) {
	file, _ := os.OpenFile("logs.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	ctx := context.Background()
	esclient, err := elasticclient.GetESClient()
	if err != nil {
		w.WriteHeader(500)
		log.SetOutput(file)
		log.Error("Endpoint hit: Get Data (Service), Output: (Client Fail)Error initializing : ", err)
	}
	var businesses []Business
	searchService := esclient.Search().Index("poc_two").Query(elastic.NewMatchQuery("inspection_id", idval)).RestTotalHitsAsInt(true)
	searchResult, err := searchService.Do(ctx)
	if err != nil {
		w.WriteHeader(404)
		log.SetOutput(file)
		log.Error("Endpoint hit: Get Data (Service), Output: SearchResult Error=", err)
		return
	}
	if len(searchResult.Hits.Hits) == 0 {
		w.WriteHeader(404)
		json.NewEncoder(w).Encode("No data found")
		log.SetOutput(file)
		log.Error("Endpoint hit: Get Data (Service), Output: No data found!!")
		return
	}
	// fmt.Print("hits: ", len(searchResult.Hits.Hits))
	for _, hit := range searchResult.Hits.Hits {
		var business Business
		// fmt.Println("Hit source: ", hit.InnerHits)
		err := json.Unmarshal(*hit.Source, &business)
		if err != nil {
			w.WriteHeader(500)
			log.SetOutput(file)
			log.Error("Endpoint hit: Get Data (Service), Output: (Unmarshal) Err=", err)
		}
		businesses = append(businesses, business)
		json.NewEncoder(w).Encode(hit)
	}
	if err != nil {
		w.WriteHeader(500)
		log.Println("Endpoint hit: Get Data (Service), Output: Fetching business fail: ", err)
	} else {
		log.SetOutput(file)
		log.Println("Endpoint hit: Get Data (Service), Output: Data Retrieval Successful!!")
		for _, s := range businesses {
			fmt.Printf("Businesses found Name: %s, Ins_id: %s, Vio_id: %s \n", s.Business_name, s.Inspection_id, s.Violation_id)
		}
	}
}

func SortService(fieldval []string, size int, typeval []string, w http.ResponseWriter) {
	file, _ := os.OpenFile("logs.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	ctx := context.Background()
	esclient, err := elasticclient.GetESClient()
	if err != nil {
		w.WriteHeader(500)
		log.SetOutput(file)
		log.Error("Endpoint hit: Sort Data (Service), Output: (Client Fail)Error initializing : ", err)
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
		w.WriteHeader(500)
		log.SetOutput(file)
		log.Error("Endpoint hit: Sort Data (Service), Output: SearchResult Error=", err)
		return
	}
	if len(searchResult.Hits.Hits) == 0 {
		w.WriteHeader(404)
		json.NewEncoder(w).Encode("No data found")
		log.SetOutput(file)
		log.Error("Endpoint hit: Sort Data (Service), Output: No data found!!")
		return
	}

	for _, hit := range searchResult.Hits.Hits {
		var business Business
		// fmt.Println("Hit source: ", hit.InnerHits)
		err := json.Unmarshal(*hit.Source, &business)
		if err != nil {
			w.WriteHeader(500)
			log.SetOutput(file)
			log.Error("Endpoint hit: Sort Data (Service), Output: (Unmarshal) Err=", err)
		}

		businesses = append(businesses, business)
		json.NewEncoder(w).Encode(hit)
	}

	if err != nil {
		w.WriteHeader(500)
		log.Println("Endpoint hit: Sort Data (Service), Output: Fetching business fail: ", err)
	} else {
		log.SetOutput(file)
		log.Printf("Endpoint hit: Sort Data (Service), Output: Data Sorted Successfully with field = %s, size = %d, type = %s", fieldval[0], size, typeval[0])
		for _, s := range businesses {
			fmt.Printf("Businesses found Name: %s, Ins_id: %s, Vio_id: %s \n", s.Business_name, s.Inspection_id, s.Violation_id)
		}
	}
}
