# Insight Data Engineering Fellowship NYC (Jan 2018)
# Customer PathRank
## Improving self-service for online customer support

This project has been developed in three weeks during my Insight Data Engineering fellowship in NYC in January 2018. This tool can be used to measure and improve self-service for online customer support. It ingests clickstream data into Kafka then advanced Spark analytics compute customer browsing path to success.

### Business Case
Starting from two observations regarding customer support:
- Navigating a customer support website to solve an issue can be a daunting experience for users
- On the over side operating customer support is a big part of a company cost structure (especially custom tickets or calls)

My project is a data pipeline:
- to experiment and find more performant compositions (shortest path to success) of a self-service customer support website
- according to real-time click streams collected on company web servers
- the output is intended for Data Science and Data Analyst teams
Generalizable to other kinds of programmatic customer experience (not only websites: chatbot, etc)

### What do we call a clickstream here?
Let's model each webpage of the customer support website in a very simple way as:
- a webpage unique ID
- a list of links to other webpages within the customer support website

|MyCustomerWebpage|PageID: 11|
|---|---|
|Link 1|PageID: 28|
|Link 2|PageID: 35|
|Link 3|PageID: 42|

If a user clicks on link number one on the above page then a new clickstream record is generated using the following structure:

|TimeStamp|CustomerID|NextPageID|IsResolved|
|---|---|---|---|
|1515764691|1|28|False|

The last column `IsResolved` is a flag that equals `True` when the customer acknowledges that the webpage he read helped him solve his support case.

![Example of Customer Support Website Acknowledgment](/images/customer_support_website_acknowledgment_cropped.png "Example  of customer support website acknowledgment")

### Sample scenario
To illustrate how the data is processed here is a sample scenario.

|TimeStamp|CustomerID|NextPageID|IsResolved|
|---|---|---|---|
|1515764691|1|28|False|
|1515764752|2|35|False|
|1515764876|2|42|True|
|1515764997|1|35|False|
|1515765000|1|42|True|

![Sample scenario](/images/sample_scenario.png "Sample scenario")

The first data processing job is to resolve the customer paths:

|PathStartTimestamp|CustomerID|Path|Pathlength|
|---|---|---|---|
|1515764691|1|[28,35,42]|3|
|1515764752|2|[35,42]|2|

Then useful metrics can be computed such as:
- a flag that is raised whenever a path shows a **turnaround**. Like a GPS system, locations that have a significant and unexpected rate of turn-arounds should be inspected with higher priority.
- the global average of path lengths: overall customer support percolation efficiency
- the global variance of path lengths: no customer very unhappy
