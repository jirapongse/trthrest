package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httputil"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jirapongse/trthrest"
)

//Enter usrname and password here
var dssUserName = ""
var dssPassword = ""
var trthURL = "https://hosted.datascopeapi.reuters.com/RestApi/v1/"

//HTTPGet : The function that wraps HTTP GET request. It adds the authorization token if token isn't nil
func HTTPGet(client *http.Client, url string, headers map[string]string, trace bool) (*http.Response, error) {
	req, _ := http.NewRequest("GET", url, nil)
	/*
		req.Header.Add("Content-Type", "application/json")
		req.Header.Add("Prefer", "respond-async")
		if token != nil {
			req.Header.Add("Authorization", "Token "+*token)

	*/
	for key, value := range headers {
		fmt.Printf("%s: %s\n", key, value)
		req.Header.Add(key, value)
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

		time.Sleep(time.Second * 1)
	}
}

func MergeFile(numberOfParts int, outFileName string) {
	b := make([]byte, 5000)
	destFile, _ := os.Create(outFileName)
	writer := bufio.NewWriter(destFile)
	for i := 1; i <= numberOfParts; i++ {
		filename := fmt.Sprintf("part%d", i)
		srcFile, _ := os.Open(filename)
		//fmt.Printf("Open File: %s\n", fmt.Sprintf("part%d", i))
		reader := bufio.NewReader(srcFile)
		readByte, err := reader.Read(b)
		//fmt.Printf("Read: %d bytes\n", readByte)
		for err != io.EOF || readByte > 0 {
			writer.Write(b[:readByte])
			//fmt.Printf("Write: %d bytes\n", writeByte)
			readByte, err = reader.Read(b)
			//fmt.Printf("Read: %d bytes\n", readByte)

		}
		srcFile.Close()
	}
	writer.Flush()
	destFile.Close()
}

func DownloadFile(client *http.Client, headers map[string]string, url string, outFileName string, start int64, stop int64) {

	log.Printf("Download File: %s, %d, %d\n", outFileName, start, stop)
	var newHeaders map[string]string
	newHeaders = make(map[string]string)
	for k, v := range headers {
		newHeaders[k] = v
	}
	if start != -1 {
		//start == -1 means download full file
		if stop != -1 {
			newHeaders["Range"] = fmt.Sprintf("bytes=%d-%d", start, stop)

		} else {
			newHeaders["Range"] = fmt.Sprintf("bytes=%d-", start)
		}

	}
	resp, err := HTTPGet(client, url, newHeaders, true)

	if err != nil {
		log.Fatal(err)
	}

	if resp.StatusCode != 200 && resp.StatusCode != 206 {
		body, _ := ioutil.ReadAll(resp.Body)
		log.Fatalf("Status Code: %s\n%s ", resp.Status, string(body))
		//log.Fatalf("Status Code: %s\n ", resp.Status)
	}

	size, err := strconv.Atoi(resp.Header.Get("Content-Length"))

	if err != nil {
		log.Fatal(err)
	}

	done := make(chan int64)
	//outputFileName := "output_" + strconv.Itoa(os.Getpid()) + ".csv.gz"

	out, err := os.Create(outFileName)
	if err != nil {
		log.Fatal(err)
	}

	go PrintDownloadPercent(done, outFileName, int64(size))

	n, err := io.Copy(out, resp.Body)
	if err != nil {
		log.Fatal(err)
	}
	done <- n
	resp.Body.Close()
	log.Println(outFileName + ": Download Completed!")
}

func ConcurrentDownload(client *http.Client, headers map[string]string, url string, outFileName string, numOfConn int, fileSize int64) {
	var partSize, fileOffset int64
	partSize = fileSize / int64(numOfConn)
	fileOffset = 0

	var wg sync.WaitGroup

	for i := 1; i <= numOfConn; i++ {
		wg.Add(1)
		if i == numOfConn {
			fmt.Printf("Part %d: %d- \n", i, fileOffset)

			go func(filename string, start int64, stop int64) {
				defer wg.Done()
				DownloadFile(client, headers, url, filename, start, stop)
			}(fmt.Sprintf("part%d", i), fileOffset, -1)
		} else {
			fmt.Printf("Part %d: %d - %d\n", i, fileOffset, fileOffset+partSize-1)

			go func(filename string, start int64, stop int64) {
				defer wg.Done()
				DownloadFile(client, headers, url, filename, start, stop)
			}(fmt.Sprintf("part%d", i), fileOffset, fileOffset+partSize-1)
			fileOffset = fileOffset + partSize
		}
	}
	wg.Wait()
	MergeFile(numOfConn, outFileName)
}

//GetExtractionIDFromNote : Get Extraction ID number from note in the response
func GetExtractionIDFromNote(note string) string {
	extractionIDReg := regexp.MustCompile("Extraction ID: ([0-9]+)")
	IDReg := regexp.MustCompile("[0-9]+")
	return IDReg.FindString(extractionIDReg.FindString(note))

}

//HTTPPost : The function that wraps HTTP POST request. It adds the authorization token if token isn't nil
func HTTPPost(client *http.Client, url string, body *bytes.Buffer, headers map[string]string, trace bool) (*http.Response, error) {

	req, _ := http.NewRequest("POST", url, body)

	/*req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Prefer", "respond-async")
	if token != nil {
		req.Header.Add("Authorization", "Token "+*token)
	*/

	for key, value := range headers {
		fmt.Printf("%s: %s\n", key, value)
		req.Header.Add(key, value)
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

	var concurrentDownload = true
	var NumOfDownloadConnections int
	var headers map[string]string

	directDownloadFlag := flag.Bool("X", false, "Download from AWS (false)")
	numOfConnection := flag.Int("c", 1, "Number of concurent download channels")
	flag.Parse()
	NumOfDownloadConnections = *numOfConnection

	if *directDownloadFlag == true {
		fmt.Printf("X-Direct-Download Flag: true \n")
	}

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
	startdate := time.Date(2017, 7, 1, 0, 0, 0, 0, time.UTC)
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
	resp, err := HTTPPost(client, trthURL+"Authentication/RequestToken", bytes.NewBuffer(b), headers, true)

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
	headers["Authorization"] = "Token " + token

	req1, _ := json.Marshal(struct {
		ExtractionRequest *trthrest.TickHistoryMarketDepthExtractionRequest
	}{
		ExtractionRequest: request,
	})

	resp, err = HTTPPost(client, trthURL+"Extractions/ExtractRaw", bytes.NewBuffer(req1), headers, true)

	if err != nil {
		log.Fatal(err)
	}

	for resp.StatusCode == 202 {
		time.Sleep(3000 * time.Millisecond)
		location := resp.Header.Get("Location")
		location = strings.Replace(location, "http:", "https:", 1)
		resp, err = HTTPGet(client, location, headers, true)
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
	//note := extractRawResult.Notes[0]
	resp.Body.Close()
	extractionID := GetExtractionIDFromNote(extractRawResult.Notes[0])
	//extractionID := GetExtractionIDFromNote("Hello World")
	fmt.Printf("**************\nExtractionID: %q\n**************\n", extractionID)
	if extractionID == "" {
		log.Println("ExtractionID is nil: Disable Concorrent Download")
		concurrentDownload = false
	}

	reportExtractionURL := trthURL + "Extractions/ReportExtractions('" + extractionID + "')/FullFile"
	resp, err = HTTPGet(client, reportExtractionURL, headers, true)

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
	fmt.Println(extractedFile.Metadata)
	fmt.Println(extractedFile.ReportExtractionId)
	fmt.Println(extractedFile.ExtractedFileId)
	fmt.Println(extractedFile.ScheduleId)
	fmt.Println(extractedFile.ExtractedFileName)
	fmt.Println(extractedFile.Size)
	fmt.Println(extractedFile.FileType)
	fmt.Println(extractedFile.LastWriteTimeUtc.String())
	//fmt.Println(extractedFile.ReceivedDateUtc.String())

	//extractionIDReg := regexp.MustCompile("Extraction ID: ([0-9]+)")
	//IDReg := regexp.MustCompile("[0-9]+")
	//fmt.Printf("**************\n%q\n**************\n", IDReg.FindString(extractionIDReg.FindString(note)))
	downloadURL := trthURL + "Extractions/RawExtractionResults('" + extractRawResult.JobID + "')" + "/$value"
	//jobIDURL := trthURL + "StandardExtractions/UserPackageDeliveries('0x05d4d06c151b2f86')/$value"
	start := time.Now()
	if *directDownloadFlag == true {
		newHeaders := make(map[string]string)
		for k, v := range headers {
			newHeaders[k] = v
		}
		newHeaders["X-Direct-Download"] = "true"
		resp, err = HTTPGet(client, downloadURL, newHeaders, true)
		if err != nil {
			log.Fatal(err)
		}
		if resp.StatusCode == 302 {
			downloadURL = resp.Header.Get("Location")
			for k := range headers {
				delete(headers, k)
			}
		} else {
			delete(headers, "X-Direct-Download")
		}

	}

	if concurrentDownload == true {
		ConcurrentDownload(client, headers, downloadURL, extractedFile.ExtractedFileName, NumOfDownloadConnections, extractedFile.Size)
	} else {
		DownloadFile(client, headers, downloadURL, extractedFile.ExtractedFileName, -1, -1)
	}
	elapsed := time.Since(start)
	log.Printf("Download Time: %s\n", elapsed)
}
