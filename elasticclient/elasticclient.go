package elasticclient

import (
	"fmt"
	"github.com/olivere/elastic"
	"poc2.com/POC-2/util"
)

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
