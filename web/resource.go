package web

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"

	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
	"poc2.com/POC-2/service"
)

func PaginateData(w http.ResponseWriter, r *http.Request) {

	// Creates a logs.txt file and opens it if already created.
	file, _ := os.OpenFile("logs.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)

	// To get all the query params
	vars := r.URL.Query()

	fromval, present := vars["from"]
	if !present || len(fromval) == 0 {

		// To write in the file
		log.SetOutput(file)
		w.WriteHeader(400)
		json.NewEncoder(w).Encode("Field value not provided!")
		log.Error("Endpoint hit: Paginate,  Output: Field value not provided!")
		return
	}
	// log.SetOutput(file)
	sizeval, present := vars["size"]
	if !present || len(sizeval) == 0 {
		w.WriteHeader(400)
		log.SetOutput(file)
		json.NewEncoder(w).Encode("Size value not provided!")
		log.Error("Endpoint hit: Paginate,  Output: Size value not provided!")
		return
	}

	from, err1 := strconv.Atoi(fromval[0])
	size, err2 := strconv.Atoi(sizeval[0])
	if err1 != nil || err2 != nil {
		w.WriteHeader(400)
		log.Println("Endpoint hit: Paginate,  Output: Error initializing : ", err1)
		log.Println("Endpoint hit: Paginate,  Output: Error initializing : ", err2)
		log.Error("Endpoint hit: Paginate,  Output: Doesn't look like a number!")
	}

	service.PaginateService(from, size, w)
}

func OperationsOnBusiness(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	if r.Method == "GET" {
		idval := vars["ins_id"]
		service.GetDataService(idval, w)

	} else if r.Method == "DELETE" {

		delval := vars["ins_id"]
		service.DeleteDataService(delval, w)

	} else if r.Method == "POST" {

		idval := vars["ins_id"]
		reqBody, _ := ioutil.ReadAll(r.Body)
		fmt.Print("Reqbody: " + string(reqBody))
		service.InsertDataService(idval, reqBody, 0, w)
	} else if r.Method == "PUT" {

		idval := vars["ins_id"]
		reqBody, _ := ioutil.ReadAll(r.Body)
		fmt.Print("Reqbody: " + string(reqBody))
		service.InsertDataService(idval, reqBody, 1, w)
	}
}

func SortData(w http.ResponseWriter, r *http.Request) {
	file, _ := os.OpenFile("logs.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	valid_fields := []string{"business_address", "business_city", "business_id", "business_name", "business_longitude", "business_postal_code", "business_state",
		"inspection_date", "inspection_id", "inspection_score", "inspection_type", "risk_category", "violation_description", "violation_id"}
	valid_type := map[string]bool{"asc": true, "ascending": true, "desc": true, "descending": true}
	flag := 0
	vars := r.URL.Query()
	fieldval, present := vars["field"]
	if !present || len(fieldval) == 0 {
		w.WriteHeader(400)
		log.SetOutput(file)
		json.NewEncoder(w).Encode("Field value not provided!")
		log.Error("Endpoint hit: Sort,  Output: Field value not provided!")
		return
	} else {
		for i := 0; i < len(valid_fields); i++ {
			if valid_fields[i] == fieldval[0] {
				flag = 1
			}
		}
		if flag == 0 {
			w.WriteHeader(400)
			log.SetOutput(file)
			json.NewEncoder(w).Encode("Invalid field value!")
			log.Error("Endpoint hit: Sort,  Output: Invalid Field Value!")
			return
		}
	}
	sizeval, present := vars["size"]
	if !present || len(sizeval) == 0 {
		w.WriteHeader(400)
		log.SetOutput(file)
		json.NewEncoder(w).Encode("Size value not provided!")
		log.Error("Endpoint hit: Sort,  Output: Size value not provided!")
		return
	}
	typeval, present := vars["type"]
	if !present || len(typeval) == 0 {
		w.WriteHeader(400)
		log.SetOutput(file)
		json.NewEncoder(w).Encode("Sorting type not provided!")
		log.Error("Endpoint hit: Sort,  Output: Sorting order not provided!")
		return
	} else {
		if !valid_type[typeval[0]] {
			w.WriteHeader(400)
			log.SetOutput(file)
			json.NewEncoder(w).Encode("Invalid Sorting Type! Kindly mention 'asc'or 'ascending' for Ascending and 'desc' or 'descending' for Descending Order")
			log.Error("Endpoint hit: Sort,  Output: Invalid Sorting Type provided!")
			return
		}
	}
	size, err1 := strconv.Atoi(sizeval[0])
	if err1 != nil {
		w.WriteHeader(400)
		log.SetOutput(file)
		log.Error("Endpoint hit: Sort,  Output: Doesn't look like a number!")
	}
	fmt.Print(fieldval)
	service.SortService(fieldval, size, typeval, w)

}
