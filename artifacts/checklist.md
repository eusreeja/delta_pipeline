

Technical challenge for the data engineer role at BayWar.e Data Services GmbH

1. Build the following ingestion data pipeline
    Source data: Energy-Charts API - Swagger UI
    Public power data 15minutes interval: daily ingestion
    Price data: daily ingestion
    Installed power: monthly ingestion

    We should be able to change at least:
    Ingestion frequency - 
    Start and end time for backfilling - 

    Restrict the data volume as you think fits a local execution of the pipeline. -  

2. Create and document a data model that allows the following BI/ML use  - 

        1. Trend of daily public net electricity production in Germany for each production type.
        2. Prediction of underperformance of public net electricity on 30min intervals.
        3. Analysis of daily price against the net power for offshore and onshore Wind

3. Store the data ingested into relevant tables fitting the use cases -  
4. Data should be able to flow from the source to the data model tables each time we run the pipeline. -  
5. Create SQL queries that would be the basis for each report. - wip

6. Preferred Stack
Storage type: Delta Table - not working compatability issues
Data Processing: pySpark - 


3. Monitor the pipeline run and data quality
Some basic pipelines run monitoring is required - wip
basic data quality measure can be checked. - wip
Any kind of method or framework is accepted for local use.


Deliverable
1. The code should be hosted on a git repository and shared with us.
2. The solution can be run locally by anyone, code should have basic comments.
3. A read README explaining your approach, any assumptions and decision made, and how to set up and test the solution.
4. Document a cloud production grade data ingestion, data warehouse and monitoring implementation of it. Diagrams of basic cloud components and data flow are enough. - 
5. Prepare for discussing the chosen solution at the next interview stage.