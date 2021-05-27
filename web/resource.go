package web

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"
	"github.com/gorilla/mux"
	"poc2.com/POC-2/service"
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

func PaginateData(w http.ResponseWriter, r *http.Request) {
	vars := r.URL.Query()

	fromval, present := vars["from"]
	if !present || len(fromval) == 0 {
		fmt.Println("Field value not provided!")
		json.NewEncoder(w).Encode("Field value not provided!")
		w.WriteHeader(400)
		return
	}
	sizeval, present := vars["size"]
	if !present || len(sizeval) == 0 {
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

	service.PaginateService(from, size, w)
}

func OperationsOnBusiness(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Galat Yaha aaya")
	vars := mux.Vars(r)
	if r.Method == "GET" {
		idval := vars["ins_id"]
		service.GetDataService(idval, w)

	} else if r.Method == "DELETE" {

		delval := vars["ins_id"]
		service.DeleteDataService(delval, w)

	} else if r.Method == "POST" {

		reqBody, _ := ioutil.ReadAll(r.Body)
		fmt.Print("Reqbody: " + string(reqBody))
		service.InsertDataService(reqBody, w)
	}
}

func SortData(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Yaha aaya")
	vars := r.URL.Query()
	fieldval, present := vars["field"]
	if !present || len(fieldval) == 0 {
		fmt.Println("Field value not provided!")
		json.NewEncoder(w).Encode("Field value not provided!")
		w.WriteHeader(400)
		return
	}
	sizeval, present := vars["size"]
	if !present || len(sizeval) == 0 {
		fmt.Println("Size value not provided!")
		json.NewEncoder(w).Encode("Size value not provided!")
		return
	}
	typeval, present := vars["type"]
	if !present || len(typeval) == 0 {
		fmt.Println("Sorting type not provided!")
		json.NewEncoder(w).Encode("Sorting type not provided!")
		return
	}
	size, err1 := strconv.Atoi(sizeval[0])
	if err1 != nil {
		panic("Doesn't look like a number")
	}
	fmt.Print(fieldval)
	service.SortService(fieldval, size, typeval, w)

}
