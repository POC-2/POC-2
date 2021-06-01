package web

import (
	"log"
	"net/http"

	"github.com/gorilla/mux"
	"poc2.com/POC-2/util"
)

// This functions lists all the endpoints and the functions to be called for each endpoint.
func Listen() {

	// Loading Config
	config, err := util.LoadConfig(".")
	if err != nil {
		// If there is any error then display the error and mention it in the log file.
		log.Fatal("Cannot load config: ", err)
	}
	// Used mux Router to define the endpoints.
	myRouter := mux.NewRouter().StrictSlash(true)

	// Sort endpoint accpets 3 query params "field", "size" and "type".
	myRouter.HandleFunc("/POC2/business/sort", SortData) // field: Field on basis of which sorting should be done, size: Number of records to be printed, type: Order of sorting

	// Paginate endpoint accepts 2 query params "from" and "size".
	myRouter.HandleFunc("/POC2/business/paginate", PaginateData) //from: Number of records to be skipped, size: Number of records to be printed.

	// 4 Operations can be performed on this endpoint.
	myRouter.HandleFunc("/POC2/business/{ins_id}", OperationsOnBusiness).Methods("GET", "POST", "DELETE", "PUT")

	log.Fatal(http.ListenAndServe(config.LOCALHOST_PORT, myRouter))
}
