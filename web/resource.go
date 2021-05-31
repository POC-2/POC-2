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
	file, _ := os.OpenFile("logs.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	vars := r.URL.Query()

	fromval, present := vars["from"]
	if !present || len(fromval) == 0 {
		log.SetOutput(file)
		// fmt.Println("Field value not provided!")
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
		service.InsertDataService(reqBody, 0, w)
	} else if r.Method == "PUT" {

		reqBody, _ := ioutil.ReadAll(r.Body)
		fmt.Print("Reqbody: " + string(reqBody))
		service.InsertDataService(reqBody, 1, w)
	}
}

func SortData(w http.ResponseWriter, r *http.Request) {
	file, _ := os.OpenFile("logs.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	fmt.Println("Yaha aaya")
	vars := r.URL.Query()
	fieldval, present := vars["field"]
	if !present || len(fieldval) == 0 {
		w.WriteHeader(400)
		log.SetOutput(file)
		json.NewEncoder(w).Encode("Field value not provided!")
		log.Error("Endpoint hit: Sort,  Output: Field value not provided!")
		return
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
