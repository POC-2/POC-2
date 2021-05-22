package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
	"net/http"
	"github.com/olivere/elastic"
	"github.com/gorilla/mux"
	"strconv"
	"log"
	"io/ioutil"
	"poc2.com/POC_2/util"
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
	// Load Config variables
    config, err := util.LoadConfig(".")
    if err != nil {
        // log.Fatal("Cannot load config: ", err)
        fmt.Println("Cannot load config: ", err)
    }

	client, err := elastic.NewClient(elastic.SetURL(config.ELASTICSEARCH_URL),
		elastic.SetSniff(false),
		elastic.SetHealthcheck(false))

	fmt.Println("ES initialized...")

	return client, err
}

func paginateData(w http.ResponseWriter, r *http.Request) {
    vars := mux.Vars(r)
    
    fromval := vars["from"]
	sizeval := vars["size"]
	from, err1 := strconv.Atoi(fromval)
	size, err2 := strconv.Atoi(sizeval)
	if err1 != nil || err2 != nil{
		fmt.Println("Error initializing : ", err1)
		fmt.Println("Error initializing : ", err2)
		panic("Doesn't look like a number")
	}
	ctx := context.Background()
	esclient, err := GetESClient()
	if err != nil {
		fmt.Println("Error initializing : ", err)
		panic("Client fail ")
	}

	var businesses []Business

	searchSource := elastic.NewSearchSource()
	searchSource.Query(elastic.NewMatchAllQuery())

	queryStr, err1 := searchSource.Source()
	queryJs, err2 := json.Marshal(queryStr)

	if err1 != nil || err2 != nil {
		fmt.Println("[esclient][GetResponse]err during query marshal=", err1, err2)
	}
	fmt.Println("[esclient]Final ESQuery=\n", string(queryJs))

	searchService := esclient.Search().Index("poc_two").SearchSource(searchSource).From(from).Size(size)

	searchResult, err := searchService.Do(ctx)
	if err != nil {
		fmt.Println("[ProductsES][GetPIds]Error=", err)
		return
	}
	// fmt.Print("hits: ", searchResult.Hits.Hits)
	for _, hit := range searchResult.Hits.Hits {
		var business Business
		// fmt.Println("Hit source: ", hit.InnerHits)
		err := json.Unmarshal(*hit.Source, &business)
		if err != nil {
			fmt.Println("[Getting Businesses][Unmarshal] Err=", err)
		}

		businesses = append(businesses, business)
		json.NewEncoder(w).Encode(hit)
	}

	if err != nil {
		fmt.Println("Fetching business fail: ", err)
	} else {
		for _, s := range businesses {
			fmt.Printf("Businesses found Name: %s, Ins_id: %s, Vio_id: %s \n", s.Business_name, s.Inspection_id, s.Violation_id)
		}
	}

}

func insertNewBusiness(w http.ResponseWriter, r *http.Request) {

	ctx := context.Background()
	esclient, err := GetESClient()
	if err != nil {
		fmt.Println("Error initializing : ", err)
		panic("Client fail ")
	}
    reqBody, _ := ioutil.ReadAll(r.Body)
	fmt.Print("Reqbody: "+string(reqBody))
    var business Business 
    json.Unmarshal(reqBody, &business)

	dataJSON, err := json.Marshal(business)
	js := string(dataJSON)
	_, err = esclient.Index().
		Index("poc_two").
		BodyJson(js).
		Do(ctx)
	if err != nil {
		panic(err)
	}
	json.NewEncoder(w).Encode("Insertion Succesful!!")
	fmt.Println("[Elastic][InsertProduct]Insertion Successful")

    
}

func deleteBusiness(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)
    
    delval := vars["ins_id"]
	ctx := context.Background()
	esclient, err := GetESClient()
	if err != nil {
		fmt.Println("Error initializing : ", err)
		panic("Client fail ")
	}
    boolQuery := elastic.NewBoolQuery().Must(elastic.NewTermQuery("inspection_id", delval))

    result2, err2 := elastic.NewDeleteByQueryService(esclient).
        Index("poc_two").
        Query(boolQuery).
        Do(ctx)
    fmt.Println("DELETE RESPONSE 2: \n", result2, err2)
	json.NewEncoder(w).Encode("Deletion Successful!!")

}

// func sortData(w http.ResponseWriter, r *http.Request) {
//     vars := mux.Vars(r)
    
//     fieldval := vars["field"]

// 	ctx := context.Background()
// 	esclient, err := GetESClient()
// 	if err != nil {
// 		fmt.Println("Error initializing : ", err)
// 		panic("Client fail ")
// 	}

// 	var businesses []Business

// 	searchSource := elastic.NewSearchSource()
// 	searchSource.Query(elastic.NewMatchAllQuery())

// 	queryStr, err1 := searchSource.Source()
// 	queryJs, err2 := json.Marshal(queryStr)

// 	if err1 != nil || err2 != nil {
// 		fmt.Println("[esclient][GetResponse]err during query marshal=", err1, err2)
// 	}
// 	fmt.Println("[esclient]Final ESQuery=\n", string(queryJs))

// 	searchService := esclient.Search().Index("poc_two").SearchSource(searchSource).Sort(fieldval, true)

// 	searchResult, err := searchService.Do(ctx)
// 	if err != nil {
// 		fmt.Println("[ProductsES][GetPIds]Error=", err)
// 		return
// 	}
// 	// fmt.Print("hits: ", searchResult.Hits.Hits)
// 	for _, hit := range searchResult.Hits.Hits {
// 		var business Business
// 		// fmt.Println("Hit source: ", hit.InnerHits)
// 		err := json.Unmarshal(hit.Source, &business)
// 		if err != nil {
// 			fmt.Println("[Getting Businesses][Unmarshal] Err=", err)
// 		}

// 		businesses = append(businesses, business)
// 		json.NewEncoder(w).Encode(hit)
// 	}

// 	if err != nil {
// 		fmt.Println("Fetching business fail: ", err)
// 	} else {
// 		for _, s := range businesses {
// 			fmt.Printf("Businesses found Name: %s, Ins_id: %s, Vio_id: %s \n", s.Business_name, s.Inspection_id, s.Violation_id)
// 		}
// 	}

// }


func handleRequests() {
	myRouter := mux.NewRouter().StrictSlash(true)
	myRouter.HandleFunc("/paginate/{from}/{size}", paginateData)
	myRouter.HandleFunc("/insert", insertNewBusiness).Methods("POST")
	myRouter.HandleFunc("/delete/{ins_id}", deleteBusiness).Methods("DELETE")
	// myRouter.HandleFunc("/sort/{field}", sortData)
	log.Fatal(http.ListenAndServe(":8090", myRouter))
}

func main() {
	handleRequests()
}


