package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"regexp"
	"strings"
	"time"
	"github.com/howeyc/gopass"
	"github.com/jirapongse/trthrest"
)

//Enter username and password here
var dssUserName = ""
var dssPassword = ""
var trthURL = "https://hosted.datascopeapi.reuters.com/RestApi/v1/"



//GetExtractionIDFromNote : Get Extraction ID number from note in the response
func GetExtractionIDFromNote(note string) string {
	extractionIDReg := regexp.MustCompile("Extraction ID: ([0-9]+)")
	IDReg := regexp.MustCompile("[0-9]+")
	return IDReg.FindString(extractionIDReg.FindString(note))

}


/*
func RequestToken(client *http.Client, trthapiurl string, body *bytes.Buffer, headers map[string]string, trace bool) (*http.Response, error) {
	return trthrest.HTTPPost(client, trthapiurl+"Authentication/RequestToken", body, headers, trace)	
}

func ExtractRaw(client *http.Client, trthapiurl string, body *bytes.Buffer, headers map[string]string, trace bool) (*http.Response, error) {
	return trthrest.HTTPPost(client, trthapiurl+"Extractions/ExtractRaw", body, headers, trace)	
}

func ReportExtractionFullFile(client *http.Client, trthapiurl string, extractionId string, headers map[string]string, trace bool) (*http.Response, error) {
	reportExtractionURL := trthapiurl + "Extractions/ReportExtractions('" + extractionId + "')/FullFile"
	return trthrest.HTTPGet(client, reportExtractionURL, headers, trace)
}
func RawExtractionResultGetDefaultStream(client *http.Client, trthapiurl string, jobId string, headers map[string]string, trace bool) (*http.Response, error) {
	rawExtractionResultURL := trthapiurl + "Extractions/RawExtractionResults('" + jobId + "')" + "/$value"
	return trthrest.HTTPGet(client, rawExtractionResultURL, headers, trace)
}
*/
func main() {

	//var concurrentDownload = true
	//var NumOfDownloadConnections int
	var headers map[string]string
	var outputFilename string
	var fileSize int64

	directDownloadFlag := flag.Bool("aws", false, "Download from AWS (false)")
	numOfConnection := flag.Int("n", 1, "Number of concurent download channels")
	traceFlag := flag.Bool("X", false, "Enable HTTP tracing (false)")
	username := flag.String("u", "", "DSS Username ('')")
	password := flag.String("p", "", "DSS Password ('')")
	flag.Parse()
//	NumOfDownloadConnections = *numOfConnection
	dssUserName = *username
	dssPassword = *password

	if *directDownloadFlag == true {
		log.Printf("X-Direct-Download Flag: true \n")
	}
	if *traceFlag == true {
		log.Printf("tracing Flag: true \n")
	}
	log.Printf("Number of concurent download: %d\n", *numOfConnection)
	/*
	if NumOfDownloadConnections == 1{
		concurrentDownload = false
		fmt.Printf("Concurent Download is false\n")
	}*/

	//var jsonStr = []byte(`{"Credentials":{"Username":"9008895", "Password":"Reuters123"}}`)
	request := new(trthrest.TickHistoryMarketDepthExtractionRequest)
	headers = make(map[string]string)

	headers["Content-Type"] = "application/json"
	headers["Prefer"] = "respond-async"

	request.Condition.View = trthrest.ViewOptionsNormalizedLL2Enum
	request.Condition.SortBy = trthrest.SortSingleByRicEnum
	request.Condition.NumberOfLevels = 10
	request.Condition.MessageTimeStampIn = trthrest.TimeOptionsGmtUtcEnum
	request.Condition.DisplaySourceRIC = true
	request.Condition.ReportDateRangeType = trthrest.ReportDateRangeTypeRangeEnum
	startdate := time.Date(2017, 8, 1, 0, 0, 0, 0, time.UTC)
	request.Condition.QueryStartDate = &startdate
	enddate := time.Date(2017, 8, 23, 0, 0, 0, 0, time.UTC)
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
	client := &http.Client{
		Transport: tr,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

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
		b, _ := gopass.GetPasswdMasked()
		dssPassword = string(b)
		//fmt.Scanln(&dssPassword)
	}
	b, err := json.Marshal(struct {
		Credentials trthrest.Credential
	}{
		Credentials: trthrest.Credential{
			Username: dssUserName,
			Password: dssPassword,
		},
	})
	resp, err := trthrest.HTTPPost(client, trthrest.GetRequestTokenURL(trthURL), bytes.NewBuffer(b), headers, *traceFlag)

	if err != nil {
		log.Printf("Error: %s\n", err.Error())
		log.Fatal(err)
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
	//fmt.Printf("Context: %s\n", respMsg.Metadata)
	//fmt.Printf("Token: %s\n", respMsg.Value)
	token := respMsg.Value
	headers["Authorization"] = "Token " + token

	req1, _ := json.Marshal(struct {
		ExtractionRequest *trthrest.TickHistoryMarketDepthExtractionRequest
	}{
		ExtractionRequest: request,
	})

	resp, err = trthrest.HTTPPost(client, trthrest.GetExtractRawURL(trthURL), bytes.NewBuffer(req1), headers, *traceFlag)
	//resp, err = ExtractRaw(client, trthURL, bytes.NewBuffer(req1), headers, *traceFlag)

	if err != nil {
		log.Fatal(err)
	}

	for resp.StatusCode == 202 {
		time.Sleep(3000 * time.Millisecond)
		location := resp.Header.Get("Location")
		location = strings.Replace(location, "http:", "https:", 1)
		resp, err = trthrest.HTTPGet(client, location, headers, *traceFlag)
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
	//fmt.Println(extractRawResult.Metadata)
	//fmt.Println(extractRawResult.JobID)
	//fmt.Println(extractRawResult.Notes)
	//note := extractRawResult.Notes[0]
	resp.Body.Close()
	if (*numOfConnection > 1 ){
	extractionID := GetExtractionIDFromNote(extractRawResult.Notes[0])
	//extractionID := GetExtractionIDFromNote("Hello World")
	fmt.Printf("**************\nExtractionID: %q\n**************\n", extractionID)
	if extractionID == "" {
		log.Println("ExtractionID is nil: Disable Concurrent Download")
		*numOfConnection = 1
		outputFilename = fmt.Sprintf("output_%s.csv.gz",extractRawResult.JobID )
		fileSize = 0
		//concurrentDownload = false
	}

	//resp, err = ReportExtractionFullFile(client, trthURL, extractionID, headers, *traceFlag)
	//reportExtractionURL := trthURL + "Extractions/ReportExtractions('" + extractionID + "')/FullFile"
	if extractionID != ""{

		resp, err = trthrest.HTTPGet(client, trthrest.GetReportExtractionFullFileURL(trthURL, extractionID), headers, *traceFlag)
		if err != nil {	
			log.Fatal(err)
		}
		body, err = ioutil.ReadAll(resp.Body)

		if err != nil {
			log.Fatal(err)
		}
		if resp.StatusCode != 200 {

			log.Fatalf("Status Code: %s\n%s ", resp.Status, string(body))
		}
		extractedFile := &trthrest.ExtractedFile{}
		err = json.Unmarshal(body, extractedFile)
		if err != nil {
			log.Fatal(err)
		}
		outputFilename = extractedFile.ExtractedFileName
		fileSize = extractedFile.Size
	}
	}else{
		outputFilename = fmt.Sprintf("output_%s.csv.gz",extractRawResult.JobID )
		fileSize = 0
	}
	//fmt.Println(extractedFile.Metadata)
	//fmt.Println(extractedFile.ReportExtractionId)
	//fmt.Println(extractedFile.ExtractedFileId)
	//fmt.Println(extractedFile.ScheduleId)
	//fmt.Println(extractedFile.ExtractedFileName)
	//fmt.Println(extractedFile.Size)
	//fmt.Println(extractedFile.FileType)
	//fmt.Println(extractedFile.LastWriteTimeUtc.String())
	//fmt.Println(extractedFile.ReceivedDateUtc.String())

	//extractionIDReg := regexp.MustCompile("Extraction ID: ([0-9]+)")
	//IDReg := regexp.MustCompile("[0-9]+")
	//fmt.Printf("**************\n%q\n**************\n", IDReg.FindString(extractionIDReg.FindString(note)))
	downloadURL := trthrest.GetRawExtractionResultGetDefaultStreamURL(trthURL, extractRawResult.JobID)
	//jobIDURL := trthURL + "StandardExtractions/UserPackageDeliveries('0x05d4d06c151b2f86')/$value"
	start := time.Now()
	if *directDownloadFlag == true {

		//Clone the TRTH headers to newHeaders and then add X-Direct-Download to the new header
		newHeaders := make(map[string]string)
		for k, v := range headers {
			newHeaders[k] = v
		}
		newHeaders["X-Direct-Download"] = "true"
		resp, err = trthrest.HTTPGet(client, downloadURL, newHeaders, *traceFlag)
		//resp, err = RawExtractionResultGetDefaultStream(client, trthURL, extractRawResult.JobID, newHeaders, *traceFlag)
		if err != nil {
			log.Fatal(err)
		}
		if resp.StatusCode == 302 {
			//GET AWS URL used to download a file and change the URL in downloadURL variable
			downloadURL = resp.Header.Get("Location")
			//Clear all headers before sending GET request to AWS. Otherwise, it will return an error
			for k := range headers {
				delete(headers, k)
			}
		} 

	}

	if *numOfConnection > 1 {
		//if we get the filename and filesize from Extractions/ReportExtractions, it will use the concurrent download
		trthrest.ConcurrentDownload(client, headers, downloadURL, outputFilename, *numOfConnection, fileSize, *traceFlag)
	} else {
		//if we can't get the filename and filesize from Extractions/ReportExtractions, it will download with one connection
		trthrest.DownloadFile(client, headers, downloadURL, outputFilename, -1, -1, *traceFlag)
	}
	elapsed := time.Since(start)
	log.Printf("Download Time: %s\n", elapsed)
}
