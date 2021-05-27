package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"time"
	"github.com/gorilla/mux"
	"github.com/olivere/elastic"
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
	vars := r.URL.Query()

	fromval, present := vars["from"]
	if !present || len(fromval)==0{
		fmt.Println("Field value not provided!")
		json.NewEncoder(w).Encode("Field value not provided!")
		w.WriteHeader(400)
		return
	}
	sizeval, present := vars["size"]
	if !present || len(sizeval)==0{
		fmt.Println("Size value not provided!")
		json.NewEncoder(w).Encode("Size value not provided!")
		return
	}
	
	from, err1 := strconv.Atoi(fromval[0])
	size, err2 := strconv.Atoi(sizeval[0])
	if err1 != nil || err2 != nil {
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

	searchService := esclient.Search().Index("poc_two").SearchSource(searchSource).From(from).Size(size).RestTotalHitsAsInt(true)

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


func operationsOnBusiness(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Galat Yaha aaya")
	vars := mux.Vars(r)
	ctx := context.Background()
	esclient, err := GetESClient()
	if err != nil {
		fmt.Println("Error initializing : ", err)
		panic("Client fail ")
	}
	if r.Method == "GET"{
		idval := vars["ins_id"]
		var businesses []Business
		searchService := esclient.Search().Index("poc_two").Query(elastic.NewMatchQuery("inspection_id", idval)).RestTotalHitsAsInt(true)
		searchResult, err := searchService.Do(ctx)
		if err != nil {
			fmt.Println("[ProductsES][GetPIds]Error=", err)
			return
		}
		if len(searchResult.Hits.Hits) == 0{
			json.NewEncoder(w).Encode("No data found")
			fmt.Println("No data found!!")
			return
		}
		// fmt.Print("hits: ", len(searchResult.Hits.Hits))
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

	}else if r.Method == "DELETE" {

		vars := mux.Vars(r)
		delval := vars["ins_id"]
		boolQuery := elastic.NewBoolQuery().Must(elastic.NewTermQuery("inspection_id", delval))
		result2, err2 := elastic.NewDeleteByQueryService(esclient).
			Index("poc_two").
			Query(boolQuery).
			Do(ctx)
		fmt.Println("DELETE RESPONSE 2: \n", result2, err2)
		json.NewEncoder(w).Encode("Deletion Successful!!")

	}else if r.Method == "POST" {

		reqBody, _ := ioutil.ReadAll(r.Body)
		fmt.Print("Reqbody: " + string(reqBody))
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
			panic(err)
		}
		json.NewEncoder(w).Encode("Insertion Succesful!!")
		fmt.Println("[Elastic][InsertProduct]Insertion Successful")

	}
}

func sortData(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Yaha aaya")
	vars := r.URL.Query()

	fieldval, present := vars["field"]
	if !present || len(fieldval)==0{
		fmt.Println("Field value not provided!")
		json.NewEncoder(w).Encode("Field value not provided!")
		w.WriteHeader(400)
		return
	}
	sizeval, present := vars["size"]
	if !present || len(sizeval)==0{
		fmt.Println("Size value not provided!")
		json.NewEncoder(w).Encode("Size value not provided!")
		return
	}
	typeval, present := vars["type"]
	if !present || len(typeval)==0{
		fmt.Println("Sorting type not provided!")
		json.NewEncoder(w).Encode("Sorting type not provided!")
		return
	}
	size, err1 := strconv.Atoi(sizeval[0])
	if err1!=nil {
		panic("Doesn't look like a number")
	}
	fmt.Print(fieldval)
	ctx := context.Background()
	esclient, err := GetESClient()
	if err != nil {
		fmt.Println("Error initializing : ", err)
		panic("Client fail ")
	}
	var businesses []Business
	if fieldval[0] != "inspection_score"{
		fieldval[0] = fieldval[0] + ".keyword"
	}
	flag := true
	if typeval[0] == "asc" || typeval[0] == "ascending" {
		flag = true
	}else if typeval[0] == "desc" || typeval[0] == "descending"{
		flag = false
	}
	searchService := esclient.Search().Index("poc_two").Query(elastic.NewMatchAllQuery()).Size(size).Sort(fieldval[0], flag).RestTotalHitsAsInt(true)
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
func handleRequests() {
	config, err := util.LoadConfig(".")
	if err != nil {
		// log.Fatal("Cannot load config: ", err)
		fmt.Println("Cannot load config: ", err)
	}
	myRouter := mux.NewRouter().StrictSlash(true)
	myRouter.HandleFunc("/POC2/business/paginate", paginateData)
	myRouter.HandleFunc("/POC2/business/sort", sortData)
	myRouter.HandleFunc("/POC2/business/{ins_id}", operationsOnBusiness).Methods("GET","POST","DELETE")
	log.Fatal(http.ListenAndServe(config.LOCALHOST_PORT, myRouter))
}

func main() {
	handleRequests()
}
