# Project Instructions
## Project Instructions
Using AWS Glue, AWS S3, Python, and Spark, create or generate Python scripts to build a lakehouse solution in AWS that satisfies these requirements from the STEDI data scientists.

Refer to the flowchart below to better understand the workflow.

A flowchart displaying the workflow.
A flowchart displaying the workflow.

## Requirements
To simulate the data coming from the various sources, you will need to create your own S3 directories for customer_landing, step_trainer_landing, and accelerometer_landing zones, and copy the data there as a starting point.

* You have decided you want to get a feel for the data you are dealing with in a semi-structured format, so you decide to create two Glue tables for the two landing zones. Share your customer_landing.sql and your accelerometer_landing.sql script in git.
* Query those tables using Athena, and take a screenshot of each one showing the resulting data. Name the screenshots customer_landing(.png,.jpeg, etc.) and accelerometer_landing(.png,.jpeg, etc.).
The Data Science team has done some preliminary data analysis and determined that the Accelerometer Records each match one of the Customer Records. They would like you to create 2 AWS Glue Jobs that do the following:

1. Sanitize the Customer data from the Website (Landing Zone) and only store the Customer Records who agreed to share their data for research purposes (Trusted Zone) - creating a Glue Table called customer_trusted.

2. Sanitize the Accelerometer data from the Mobile App (Landing Zone) - and only store Accelerometer Readings from customers who agreed to share their data for research purposes (Trusted Zone) - creating a Glue Table called accelerometer_trusted.

3. You need to verify your Glue job is successful and only contains Customer Records from people who agreed to share their data. Query your Glue customer_trusted table with Athena and take a screenshot of the data. Name the screenshot customer_trusted(.png,.jpeg, etc.).


Data Scientists have discovered a data quality issue with the Customer Data. The serial number should be a unique identifier for the STEDI Step Trainer they purchased. However, there was a defect in the fulfillment website, and it used the same 30 serial numbers over and over again for millions of customers! Most customers have not received their Step Trainers yet, but those who have, are submitting Step Trainer data over the IoT network (Landing Zone). The data from the Step Trainer Records has the correct serial numbers.

The problem is that because of this serial number bug in the fulfillment data (Landing Zone), we don’t know which customer the Step Trainer Records data belongs to.

The Data Science team would like you to write a Glue job that does the following:

1. Sanitize the Customer data (Trusted Zone) and create a Glue Table (Curated Zone) that only includes customers who have accelerometer data and have agreed to share their data for research called customers_curated.
Finally, you need to create two Glue Studio jobs that do the following tasks:

1. Read the Step Trainer IoT data stream (S3) and populate a Trusted Zone Glue Table called step_trainer_trusted that contains the Step Trainer Records data for customers who have accelerometer data and have agreed to share their data for research (customers_curated).
2. Create an aggregated table that has each of the Step Trainer Readings, and the associated accelerometer reading data for the same timestamp, but only for customers who have agreed to share their data, and make a glue table called machine_learning_curated.

## Project Summary
### Landing Zone

* [customer_landing.sql](scripts/customer_landing.sql)
* [accelerometer_landing.sql](scripts/accelerometer_landing.sql)
* [step_trainer_landing.sql](scripts/step_trainer_landing.sql)

* count check for landing:
* ![image](https://github.com/ronger4242/udacity_nanodegree_dataEngineeringAWS-project3_STEDI_Human_Balance_Analytics/assets/53929071/f9b9f84e-04b9-41c6-bba2-97e651c42dbc)
* count check for customer_landing with blank sharewithresearchasofdate:
* ![image](https://github.com/ronger4242/udacity_nanodegree_dataEngineeringAWS-project3_STEDI_Human_Balance_Analytics/assets/53929071/0dac5736-fe94-4f12-8d8c-1390a290432a)
* 



### Trusted Zone
* [customer_trusted.py](scripts/customer_trusted.py)
* [accelerometer_trusted.py](scripts/accelerometer_trusted.py)
* [step_trainer_trusted.py](scripts/step_trainer_trusted.py)

* count check for trusted:
  ![image](https://github.com/ronger4242/udacity_nanodegree_dataEngineeringAWS-project3_STEDI_Human_Balance_Analytics/assets/53929071/4707c647-c5b5-4d11-b48c-f3a65a0d4db0)
* customer trusted check:
  ![image](https://github.com/ronger4242/udacity_nanodegree_dataEngineeringAWS-project3_STEDI_Human_Balance_Analytics/assets/53929071/7c5098e4-7574-4a24-b64d-f2abdc4d11dd)
  ![image](https://github.com/ronger4242/udacity_nanodegree_dataEngineeringAWS-project3_STEDI_Human_Balance_Analytics/assets/53929071/61d96f9f-1a19-49da-90e4-34d6efe6ebcf)




### Curated Zone
* [customers_curated.py](scripts/customers_curated.py)
* [machine_learning_curated.py](scripts/machine_learning_curated.py)
* count check for curated:
  ![image](https://github.com/ronger4242/udacity_nanodegree_dataEngineeringAWS-project3_STEDI_Human_Balance_Analytics/assets/53929071/8ab3cb85-ab1c-4acc-8235-15ab3a0a94d8)
  
* all glue tables
  
  ![image](https://github.com/ronger4242/udacity_nanodegree_dataEngineeringAWS-project3_STEDI_Human_Balance_Analytics/assets/53929071/d8533128-6f8f-4439-afbb-28b2e4585e77)

  




