package elasticclient

import (
	"fmt"

	"github.com/olivere/elastic"
	"poc2.com/POC-2/util"
)

func GetESClient() (*elastic.Client, error) {

	// Loading config
	config, err := util.LoadConfig(".")
	if err != nil {
		// log.Fatal("Cannot load config: ", err)
		fmt.Println("Cannot load config: ", err)
	}

	// Connecting to a New ElasticSearch Client by passing URL from Config File.
	client, err := elastic.NewClient(elastic.SetURL(config.ELASTICSEARCH_URL),
		elastic.SetSniff(false),
		elastic.SetHealthcheck(false))

	fmt.Println("ES initialized...")

	return client, err
}
