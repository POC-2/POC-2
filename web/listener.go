package web

import (
	"fmt"
	"log"
	"net/http"
	"github.com/gorilla/mux"
	"poc2.com/POC-2/util"
)

func Listen() {
	config, err := util.LoadConfig(".")
	if err != nil {
		log.Fatal("Cannot load config: ", err)
	}
	myRouter := mux.NewRouter().StrictSlash(true)
	myRouter.HandleFunc("/POC2/business/paginate", PaginateData)
	myRouter.HandleFunc("/POC2/business/sort", SortData)
	myRouter.HandleFunc("/POC2/business/{ins_id}", OperationsOnBusiness).Methods("GET","POST","DELETE")
	log.Fatal(http.ListenAndServe(config.LOCALHOST_PORT, myRouter))
}
