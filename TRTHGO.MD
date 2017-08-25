# Using Tick History V2 REST API with Go Programming Language

## Introduction

Thomson Reuters Tick History (TRTH) is an Internet-hosted product on the DataScope Select platform that provides SOAP-based and a REST API for unparalleled access to historical high frequency data across global asset classes dating to 1996. However a legacy SOAP-based API is also available, and is scheduled to be sunset. Therefore client who still uses SOAP-based API may need to migrate their application to use REST API instead.

This article demonstrates problems and solutions that developers should aware when using TRTH V2 On Demand data extraction with Go programming language. It uses Tick History Market Depth On-Demand data extraction as an example to demonstrate the usage and solutions. However, the methods mentioned in this article can be applied to other types of data extractions.

## Prerequisite

The following knowledges are required to follow this article.

* You must know how to use On-Demand extraction in TRTH V2. This article doesn't explain TRTH V2 REST API On Demand data extraction request in detail. Fortunately, there is a [REST API Tutorial 3: On Demand Data extraction workflow](https://developers.thomsonreuters.com/thomson-reuters-tick-history-trth/thomson-reuters-tick-history-trth-rest-api/learning?content=11307&type=learning_material_item) tutorial available in the Developer Community which thoroughly explains On Demand data extraction

* You must have basic knowledge of Go programing language. This article doesn't cover the installation, settings, and usage of Go programming language. You can refer to the official [Go Programming Language Website](https://golang.org/) for more information

## Overview

Go is an open source project under a BSD-style license developed by a team at Google in 2007 and many contributors from the open source community. Its binary distributions are available for Linux, Max OS X, Windows, and more. Go is a statically typed and compiled language with a simple syntax. It features garbage collection, concurrency, type safety and large standard library.

Developers can use Go programing language to consume Tick History data via TRTH V2 REST API. This article lists several problems and solutions which developers may find during development. The list of problems are:

* Prepare JSON data, such as **Credential**, and **TickHistoryMarketDepthExtractionRequest** for the HTTP POST  request
* Process the JSON response from TRTH V2 REST API
* Concurrently download the GZIP file from the raw extraction result






## References

* [Go Programming Language Website](https://golang.org/)
* [Thomson Reuters Tick History (TRTH) - REST API](https://developers.thomsonreuters.com/thomson-reuters-tick-history-trth/thomson-reuters-tick-history-trth-rest-api)