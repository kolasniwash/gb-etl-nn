# Simple data pipeline.

 This project was made to automate the capture of click stream data that was previously being imported manually into Tableau.

 Summary of the project's functions.
 1. Connects to adjust API and retreves data from the last 30 days.
 2. Pre-formats and cleans data (specifically headers)into the stye currently used in our Tableau dashboards.
 3. Appends the most recent 30 days data to the current click data file found in google big query. This was done because adjust only holds data for approx 45 days. So any data requested from the API beyond that point would be lost. With this method we are able to keep all relevant data in the google big query table.


