# Insight Data Engineering Fellowship NYC (Jan 2018)
# Customer Support Percolation

This project has been developed during my Insight Data Engineering NYC fellowship in January 2018.

## Business Case
Starting from two observations regarding customer support:
- Navigating a customer support website to solve an issue can be a daunting experience for users
- On the over side operating customer support is a big part of a company cost structure (especially custom tickets or calls)

My project is a data pipeline:
- to experiment and find more performant compositions of a self-service customer support website
- according to real-time click streams collected on company web servers
Generalizable to other kinds of programmatic customer experience (not only websites)

## What do we call a clickstream here?
Let's model each webpage of the customer support website in a very simple way as:
- a webpage unique ID
- a list of links to other webpage in the customer support website

|MyCustomerWebpage|PageID: 11|
|---|---|
|Link 1|PageID: 28|
|Link 2|PageID: 35|
|Link 3|PageID: 42|

If a user clicks on link number one on the above page then a new clickstream record is generated using the following structure:

|TimeStamp|CustomerID|SourcePageID|TargetPageID|IsResolved|
|---|---|---|---|---|
|1515764691|1|11|28|False|

The last column `IsResolved` is a flag that equals `True` when the customer acknowledges that the webpage he read helped him solve his support case.

![Example of Customer Support Website Acknowledgment](/images/customer_support_website_acknowledgment_cropped.png "Example  of customer support website acknowledgment")

## Sample scenario
To illustrate how the data is processed here is a sample scenario.

|TimeStamp|CustomerID|SourcePageID|TargetPageID|IsResolved|
|---|---|---|---|---|
|1515764691|1|11|28|False|
|1515764752|2|11|35|False|
|1515764876|2|35|42|True|
|1515764997|1|28|35|False|
|1515765000|1|35|42|True|

![Sample scenario](/images/sample_scenario.png "Sample scenario")

One of the first data processing job is to compute the customer paths:

|PathStartTimestamp|CustomerID|Path|Pathlength|
|---|---|---|---|
|1515764691|1|[11,28,35,42]|4|
|1515764752|2|[11,35,42]|3|

Then useful computations are:
- a flag that is raised whenever a path shows a **turnaround**. Like a GPS system, locations that have a significant and unexpected rate of turn-arounds should be inspected with higher priority.
- global webserver metrics such as:
  - the global average of path lengths: overall customer support percolation efficiency
  - the global variance of path lengths: no customer very unhappy
