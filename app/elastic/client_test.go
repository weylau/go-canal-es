package elastic

import (
	"flag"
	"fmt"
	"testing"
)

type elasticTest struct {
	c *Client
}

var host = flag.String("host", "172.16.57.112", "Elasticsearch host")
var port = flag.Int("port", 9200, "Elasticsearch port")

var elasticTestClient *elasticTest

func TestMain(m *testing.M) {
	elasticTestClient = &elasticTest{}
	cfg := new(ClientConfig)
	cfg.Addr = fmt.Sprintf("%s:%d", *host, *port)
	cfg.User = ""
	cfg.Password = ""
	elasticTestClient.c = NewClient(cfg)
	m.Run()
}

func TestUpdate(t *testing.T) {
	fmt.Println(elasticTestClient)
}

func TestCreateMapping(t *testing.T) {
	index := "testindex002"
	docType := "testtype002"
	ParentType := "testparent002"

	mapping := map[string]interface{}{
		docType: map[string]interface{}{
			"_parent": map[string]string{"type": ParentType},
		},
	}
	err := elasticTestClient.c.CreateMapping(index, docType, mapping)
	if err != nil {
		t.Error(err)
	}
}

func TestGetMapping(t *testing.T) {
	index := "testindex003"
	docType := "testtype003"

	res, err := elasticTestClient.c.GetMapping(index, docType)
	if err != nil {
		t.Error(err)
	}
	fmt.Println(res)
}


func TestBulk(t *testing.T) {
	var bulk_res  []*BulkRequest
	index := "testindex003"
	docType := "testtype003"
	data := map[string]interface{}{}

	data["id"] = "2"
	data["username"] = "admin11333"
	data["password"] = "password11333"
	data["realname"] = "realname11333"
	data["mobile"] = "mobile"
	data["email"] = "email"
	data["intro"] = "intro"
	data["stars"] = "stars"
	data["newfield"] = "newfield"
	bulk_res = append(bulk_res, &BulkRequest{
		Action: ActionIndex,
		Index:  index,
		Type:   docType,
		ID:     "2",
		Data:   data,
	})

	res, err := elasticTestClient.c.Bulk(bulk_res)
	if err != nil {
		t.Error(err)
	}
	fmt.Println(res)
}


func TestGet(t *testing.T) {
	index := "testindex003"
	docType := "testtype003"
	id := "2"
	res, err := elasticTestClient.c.Get(index, docType, id)
	if err != nil {
		t.Error(err)
	}
	fmt.Println(res)
}
