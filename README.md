# popularioty-analytics-aggregator

## Overall view

This is the last (heavy-processing) step of the batch processes to process the reputation data.
The main responsibilities for this component are:

* Merge reputation values, e.g. reputation of applications, or service objects affect the reputation of the developer who wrote them, or also feedback about an app or a service object affects the entity but also the developer who created it...
* Calculate the up-to-date value for the reputation dimension (feedback, popularity, activity) or for the final reputation value. For this, this component needs a connection (through popularioty-commons) to the reputation database to be able to query the previous values and apply the math calculation to create the new values from them and the result of the batch processing.

## The input


## The output 


