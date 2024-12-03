
### Build Status
[![Build Status](https://ci.walmart.com/buildStatus/icon?job=sams-product-recommendations)](https://ci.falcon2.walmart.com/job/sams-product-recommendations/)

### Quality Gate
[![Quality Gate](https://sonar.looper.prod.walmartlabs.com/api/project_badges/quality_gate?project=com.sams.personalization.products%3Arecommendation-processor)](https://sonar.prod.walmart.com/dashboard?id=com.sams.personalization.products%3Arecommendation-processor)


```

 ______                                                    __         __   __               
|   __ \.-----.----.-----.--------.--------.-----.-----.--|  |.---.-.|  |_|__|.-----.-----. 
|      <|  -__|  __|  _  |        |        |  -__|     |  _  ||  _  ||   _|  ||  _  |     | 
|___|__||_____|____|_____|__|__|__|__|__|__|_____|__|__|_____||___._||____|__||_____|__|__| 
 ______                                               
|   __ \.----.-----.----.-----.-----.-----.-----.----.
|    __/|   _|  _  |  __|  -__|__ --|__ --|  _  |   _|
|___|   |__| |_____|____|_____|_____|_____|_____|__|  
                                                      
```



### Recommendation Processor

This App Builds and Runs A Data Pipeline To Read a list of recommenation files and trasform them.


### Required Dev Instatllations
1. Install gcloud CLI and connect to GCP Project To Your Terminal. (https://formulae.brew.sh/cask/google-cloud-sdk)
2. Setup Auth Creds Based on service account in you local. (https://cloud.google.com/docs/authentication/provide-credentials-adc) (Follow Service Account Credentials Section).
3. Java 17
4. Maven 


### Run App As a Dataflow Job in GCP

| AdditionalParameters | Value  |
| -------------------- | ------- |
| --runner               | DataFlowRunner |




### Run Application Locally:

| RuntimeParameters   | PossibleValues |
| -----------------   | -------------- |
| writeToCosmos       |     Y/N        |
| writeToCloudStorage |     Y/N        |
| writeToBigQuery     |     Y/N        |
| feedType            |     warm/cold  |
| module              |   savings/complimentary/persist/landing/rye |


```

mvn compile exec:java \
-Dexec.mainClass=com.sams.productrecommendations.batch.RecommendationProcessor \
-Dexec.cleanupDaemonThreads=false \
-Dexec.args=" \
--project=dev-sams-data-generator \
--region=us-central1 \
--subnetwork=https://www.googleapis.com/compute/v1/projects/shared-vpc-admin/regions/us-central1/subnetworks/priv-svc-access-01 \
--usePublicIps=false \
--stagingLocation=gs://poc-sample-parquet/stage \
--gcpTempLocation=gs://poc-sample-parquet/temp \
--tempLocation=gs://datageneratorbigquerytemp \
--autoscalingAlgorithm=THROUGHPUT_BASED \
--workerMachineType=n1-standard-8  \
--module=${recommendationModule} \
--feedType=${recommendationFeedType} \
--writeToCosmos=${Y/N} \
--writeToCloudStorage=${Y/N} \
--writeToBigQuery=${Y/N} \
--uuidToAudit=testing \
--outPutFilePath=${Add Gcs Output bucket Path} \
--inputFile=${File Gcs Path to be Read} \
--serviceAccount=${Get Service Account Details From Dev Team}"

```

### Confluence Page Links
1. (Pipeline Design): https://confluence.walmart.com/display/GPMSC/Generic+Recommendation+Pipeline
2. (App Doc): https://confluence.walmart.com/display/GPMSC/Recommendation+Processor+App



   
