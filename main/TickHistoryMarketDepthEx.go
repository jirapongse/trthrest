package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httputil"
	"os"
	"strconv"
	"strings"
	"thomsonreuters/trthrest"
	"time"
)

//Enter usrname and password here
var dssUserName = ""
var dssPassword = ""
var trthURL = "https://hosted.datascopeapi.reuters.com/RestApi/v1/"

//HTTPGet : The function that wraps HTTP GET request. It adds the authorization token if token isn't nil
func HTTPGet(client *http.Client, url string, token *string, trace bool) (*http.Response, error) {
	req, _ := http.NewRequest("GET", url, nil)

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Prefer", "respond-async")
	if token != nil {
		req.Header.Add("Authorization", "Token "+*token)
	}
	if trace == true {
		dump, _ := httputil.DumpRequestOut(req, true)
		log.Println(string(dump))
	}

	resp, err := client.Do(req)

	if trace == true {
		dumpBody := true
		contentLength, _ := strconv.Atoi(resp.Header.Get("Content-Length"))
		if contentLength > 100 {
			dumpBody = false
		}

		dump, _ := httputil.DumpResponse(resp, dumpBody)
		fmt.Println(string(dump))
	}

	return resp, err

}

//PrintDownloadPercent : This function shows the download progress
func PrintDownloadPercent(done chan int64, path string, total int64) {

	var stop = false

	for {
		select {
		case <-done:
			stop = true
		default:

			file, err := os.Open(path)
			if err != nil {
				log.Fatal(err)
			}

			fi, err := file.Stat()
			if err != nil {
				log.Fatal(err)
			}

			size := fi.Size()

			if size == 0 {
				size = 1
			}

			var percent float64
			percent = float64(size) / float64(total) * 100

			log.Printf("%s, Bytes: %d/Total: %d (%.0f%%)", path, size, total, percent)

		}

		if stop {
			break
		}

		time.Sleep(time.Second * 5)
	}
}

//HTTPPost : The function that wraps HTTP POST request. It adds the authorization token if token isn't nil
func HTTPPost(client *http.Client, url string, token *string, body *bytes.Buffer, trace bool) (*http.Response, error) {

	req, _ := http.NewRequest("POST", url, body)

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Prefer", "respond-async")
	if token != nil {
		req.Header.Add("Authorization", "Token "+*token)
	}
	if trace == true {
		dump, _ := httputil.DumpRequestOut(req, true)
		fmt.Println(string(dump))
	}

	resp, err := client.Do(req)

	if trace == true {

		dumpBody := true
		contentLength, _ := strconv.Atoi(resp.Header.Get("Content-Length"))
		if contentLength > 100 {
			dumpBody = false
		}

		dump, _ := httputil.DumpResponse(resp, dumpBody)
		fmt.Println(string(dump))
	}

	return resp, err
}
func main() {

	//var jsonStr = []byte(`{"Credentials":{"Username":"9008895", "Password":"Reuters123"}}`)
	request := new(trthrest.TickHistoryMarketDepthExtractionRequest)

	request.Condition.View = trthrest.ViewOptionsNormalizedLL2Enum
	request.Condition.SortBy = trthrest.SortSingleByRicEnum
	request.Condition.NumberOfLevels = 10
	request.Condition.MessageTimeStampIn = trthrest.TimeOptionsGmtUtcEnum
	request.Condition.DisplaySourceRIC = true
	request.Condition.ReportDateRangeType = trthrest.ReportDateRangeTypeRangeEnum
	startdate := time.Date(2016, 8, 29, 9, 0, 0, 0, time.UTC)
	request.Condition.QueryStartDate = &startdate
	enddate := time.Date(2016, 9, 29, 12, 0, 0, 0, time.UTC)
	request.Condition.QueryEndDate = &enddate
	//request.Condition.QueryEndDate = nil
	request.ContentFieldNames = []string{
		"Ask Price",
		"Ask Size",
		"Bid Price",
		"Bid Size",
		"Domain",
		"History End",
		"History Start",
		"Instrument ID",
		"Instrument ID Type",
		"Number of Buyers",
		"Number of Sellers",
		"Sample Data",
	}

	//request.ContentFieldNames = append(request.ContentFieldNames, "Ask Size")

	request.IdentifierList.InstrumentIdentifiers = append(request.IdentifierList.InstrumentIdentifiers, trthrest.InstrumentIdentifier{Identifier: "IBM.N", IdentifierType: "Ric"})
	request.IdentifierList.ValidationOptions = &trthrest.InstrumentValidationOptions{AllowHistoricalInstruments: true}
	//request.IdentifierList.ValidationOptions.AllowHistoricalInstruments = true
	/*
		reqxx := struct {
			ExtractRequest *TickHistoryMarketDepthExtractionRequest
		}{
			ExtractRequest: request,
		}*/

	//req1, _ := json.Marshal(reqxx)

	//req1, _ := json.Marshal(ExtractRequest{ExtractRequest: request})

	//fmt.Println(string(req1))
	/*
		map1 := map[string]interface{}{
			"ExtractRequest": map[string]interface{}{
				"@odata.type":       "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.TickHistoryMarketDepthExtractionRequest",
				"ContentFieldNames": [2]string{"BID", "ASK"},
				"IdentifierList": map[string]interface{}{
					"@odata.type": "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.InstrumentIdentifierList",
					"InstrumentIdentifiers": [2]map[string]string{
						{
							"Identifier":     "IBM.N",
							"IdentifierType": "Ric",
						}, {
							"Identifier":     "PTT.BK",
							"IdentifierType": "Ric",
						},
					},
					"ValidationOptions": map[string]interface{}{
						"AllowHistoricalInstruments": true,
					},
				},
				"Condition": map[string]interface{}{
					"View":             "RawMarketPricePrice",
					"SortBy":           "SingleByRic",
					"QueryStartDate":   time.Date(2016, 5, 11, 19, 0, 0, 0, time.UTC),
					"DisplaySourceRic": false,
				},
			},
		}
	*/
	//map1 = make(map[string]interface{})
	//req2, _ := json.Marshal(map1)
	//fmt.Println(string(req2))
	tr := &http.Transport{
		DisableCompression: true,
	}
	client := &http.Client{Transport: tr}

	/*
		message := &RequestTokenMsg{
			Credentials: Credential{
				"9008895",
				"Reuters123",
			},
		}
	*/
	//b, err := json.Marshal(message)

	if dssUserName == "" {
		fmt.Print("Enter DSS Username: ")
		fmt.Scanln(&dssUserName)
	}
	if dssPassword == "" {
		fmt.Print("Enter DSS Password: ")
		fmt.Scanln(&dssPassword)
	}
	b, err := json.Marshal(struct {
		Credentials trthrest.Credential
	}{
		Credentials: trthrest.Credential{
			Username: dssUserName,
			Password: dssPassword,
		},
	})
	resp, err := HTTPPost(client, trthURL+"Authentication/RequestToken", nil, bytes.NewBuffer(b), true)

	if err != nil {
		log.Printf("Error: %s\n", err.Error())
		panic(err)
	}

	body, _ := ioutil.ReadAll(resp.Body)

	if resp.StatusCode != 200 {
		//var objmap map[string]interface{}
		//err = json.Unmarshal(body, &objmap)
		//errorMessage := objmap["error"].(map[string]interface{})
		log.Fatalf("Status Code: %s\n%s ", resp.Status, string(body))
		//panic(errorMessage["message"])
	}

	var respMsg = &trthrest.RequestTokenResponse{}
	//fmt.Println("response body:", string(sampleData))
	err = json.Unmarshal(body, respMsg)
	resp.Body.Close()
	//err = json.Unmarshal(sampleData, respMsg)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Context: %s\n", respMsg.Metadata)
	fmt.Printf("Token: %s\n", respMsg.Value)
	token := respMsg.Value

	req1, _ := json.Marshal(struct {
		ExtractionRequest *trthrest.TickHistoryMarketDepthExtractionRequest
	}{
		ExtractionRequest: request,
	})

	resp, err = HTTPPost(client, trthURL+"Extractions/ExtractRaw", &token, bytes.NewBuffer(req1), true)

	if err != nil {
		log.Fatal(err)
	}

	for resp.StatusCode == 202 {
		time.Sleep(3000 * time.Millisecond)
		location := resp.Header.Get("Location")
		location = strings.Replace(location, "http:", "https:", 1)
		resp, err = HTTPGet(client, location, &token, true)
	}

	body, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatal(err)
	}
	if resp.StatusCode != 200 {
		log.Fatalf("Status Code: %s\n%s ", resp.Status, string(body))
	}

	extractRawResult := &trthrest.RawExtractionResult{}
	err = json.Unmarshal(body, extractRawResult)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(extractRawResult.Metadata)
	fmt.Println(extractRawResult.JobID)
	fmt.Println(extractRawResult.Notes)
	resp.Body.Close()

	jobIDURL := trthURL + "Extractions/RawExtractionResults('" + extractRawResult.JobID + "')" + "/$value"
	//jobIDURL := trthURL + "StandardExtractions/UserPackageDeliveries('0x05d4d06c151b2f86')/$value"
	resp, err = HTTPGet(client, jobIDURL, &token, true)

	if err != nil {
		log.Fatal(err)
	}

	//fmt.Println(string(body))
	if resp.StatusCode != 200 {
		body, _ = ioutil.ReadAll(resp.Body)
		log.Fatalf("Status Code: %s\n%s ", resp.Status, string(body))
	}

	size, err := strconv.Atoi(resp.Header.Get("Content-Length"))

	if err != nil {
		log.Fatal(err)
	}

	done := make(chan int64)
	outputFileName := "output_" + strconv.Itoa(os.Getpid()) + ".csv.gz"

	out, err := os.Create(outputFileName)
	if err != nil {
		log.Fatal(err)
	}

	go PrintDownloadPercent(done, outputFileName, int64(size))

	n, err := io.Copy(out, resp.Body)
	if err != nil {
		log.Fatal(err)
	}
	done <- n
	resp.Body.Close()
	log.Println("Download Completed!")
}
