package main
import (
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"os"
	"strings"
	"sync"
	"io/ioutil"
	"net/http"
	"encoding/json"
	"time"
	"gopkg.in/yaml.v2"
	"bytes"
)

type falconType struct {
	Endpoint    string  `json:"endpoint"`
	Metric      string  `json:"metric"`
	Timestamp   int64   `json:"timestamp"`
	Step        int     `json:"step"`
	Value       float64 `json:"value"`
	CounterType string  `json:"counterType"`
	Tags        string  `json:"tags"`
}

type Metric struct {
	MetricsName      string
	MetricsValue     MetricsValue
	MetricsTimestamp string
	MetricsTags      MetricsTags
}

type MetricsValue struct {
	Value float64 `json:"value"`
}
type MetricsTags struct {
	Container_name string `json:"container_name"`
	Host_id        string `json:"host_id"`
	Hostname      string `json:"hostname"`
	Nodename       string `json:"nodename"`
	Resource_id    string `json:"resource_id"`
	Types          string `json:"types"`
}

type conf struct {
	KafkaHost string   `yaml:"kafkahost"`
	KafkaTopic    string `yaml:"kafkatopic"`
	Openfalcon_host    string `yaml:"openfalconhost"`
}

var (
	wg     sync.WaitGroup
	logger = log.New(os.Stderr, "[srama]", log.LstdFlags)
)

func main() {

	var c conf
	c.getConf()
	fmt.Println(c)

	sarama.Logger = logger

	consumer, err := sarama.NewConsumer(strings.Split(c.KafkaHost, ","), nil)
	if err != nil {
		logger.Println("Failed to start consumer: %s", err)
	}

	partitionList, err := consumer.Partitions(c.KafkaTopic)
	if err != nil {
		logger.Println("Failed to get the list of partitions: ", err)
	}

	for partition := range partitionList {
		pc, err := consumer.ConsumePartition(c.KafkaTopic, int32(partition), sarama.OffsetNewest)
		if err != nil {
			logger.Printf("Failed to start consumer for partition %d: %s\n", partition, err)
		}
		defer pc.AsyncClose()

		wg.Add(1)

//		var jsonArr []string

		go func(sarama.PartitionConsumer) {
			defer wg.Done()
//			var jsonStr string
			fts := make([]falconType, 0, 15)
			for msg := range pc.Messages() {
//				jsonStr = jsonStr + setData(string(msg.Value))
				m := &Metric{}
				//	jsonStr := `{"MetricsName":"disk/io_read_bytes_rate","MetricsValue":{"value":81437.125},"MetricsTimestamp":"2018-04-18T06:35:00Z","MetricsTags":{"container_name":"system.slice/rsyslog.service","host_id":"k8s-master","hostname":"k8s-master","nodename":"k8s-master","resource_id":"8:0","type":"sys_container"}}`
				json.Unmarshal(msg.Value, m)

				ft := &falconType{Endpoint: m.MetricsTags.Hostname, Metric: m.MetricsName, Timestamp: time.Now().UTC().Unix(), Step: 60, Value: m.MetricsValue.Value, CounterType: "GAUGE", Tags: ""}

				fts = append(fts, *ft)
//				fmt.Println(*ft)
				fmt.Println(fts)
//				jsonArr = append(jsonArr,setData(string(msg.Value)))
				if(len(fts)>10)  {
					httpPost(fts,c.Openfalcon_host)
//					httpPost(jsonStr,c.Openfalcon_host)
//					jsonStr = ""
					fts=make([]falconType,0,15);
				}

//				fmt.Printf("Value:%s", jsonArr)
				fmt.Println()
			}
		}(pc)
	}

	wg.Wait()

	logger.Println("Done consuming topic ")
	consumer.Close()
}


func httpPost(fts []falconType,host string) {
//	var jsonArrStr string
//	fmt.Println(fts)
	jsonArrStr, err := json.Marshal(fts)
	if err == nil {
//		fmt.Println(string(jsonArrStr))
	}
	//s := string(jsonArrStr)
	//s = strings.Replace(s, "\"{", "{", -1)
	//s = strings.Replace(s, "}\"", "}", -1)

	fmt.Printf("jsonArrStr %s",jsonArrStr)

	resp, err := http.Post(host+"/v1/push",
		"application/json",
		bytes.NewReader(jsonArrStr))
	if err != nil {
		fmt.Println(err)
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		// handle error
	}

	fmt.Println(string(body))
}

/**
func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}
 */
func (c *conf) getConf() *conf {

	yamlFile, err := ioutil.ReadFile("conf.yml")
	if err != nil {
		log.Printf("yamlFile.Get err   #%v ", err)
	}
	err = yaml.Unmarshal(yamlFile, c)
	if err != nil {
		log.Fatalf("Unmarshal: %v", err)
	}
	return c
}



