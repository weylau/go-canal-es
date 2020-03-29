package elastic

import (
	"flag"
	"fmt"
	"go-canal-es/app"
	"testing"
)

type elasticTest struct {
	c *app.Client
}

var host = flag.String("host", "172.16.57.110", "Elasticsearch host")
var port = flag.Int("port", 9200, "Elasticsearch port")

var elasticTestClient *elasticTest

func TestMain(m *testing.M) {
	elasticTestClient = &elasticTest{}
	cfg := new(app.ClientConfig)
	cfg.Addr = fmt.Sprintf("%s:%d", *host, *port)
	cfg.User = ""
	cfg.Password = ""
	elasticTestClient.c = app.NewClient(cfg)
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
	index := "testindex002"
	docType := "testtype002"

	res, err := elasticTestClient.c.GetMapping(index, docType)
	if err != nil {
		t.Error(err)
	}
	fmt.Println(res)
}


func TestBulk(t *testing.T) {
	var bulk_res  []*app.BulkRequest
	index := "testindex002"
	docType := "testtype002"
	data := map[string]interface{}{}

	data["id"] = "1"
	data["username"] = "admin"
	data["password"] = "password"
	data["realname"] = "realname"
	data["mobile"] = "mobile"
	data["email"] = "email"
	data["intro"] = "intro"
	data["stars"] = "stars"
	bulk_res = append(bulk_res, &app.BulkRequest{
		Action: app.ActionCreate,
		Index:  index,
		Type:   docType,
		ID:     "1",
		Data:   data,
	})

	res, err := elasticTestClient.c.Bulk(bulk_res)
	if err != nil {
		t.Error(err)
	}
	fmt.Println(res)
}


func TestGet(t *testing.T) {
	index := "testindex002"
	docType := "testtype002"
	id := "1"
	res, err := elasticTestClient.c.Get(index, docType, id)
	if err != nil {
		t.Error(err)
	}
	fmt.Println(res)
}
