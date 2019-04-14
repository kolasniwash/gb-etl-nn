
if __name__ == "__main__":
	import pandas as pd
	import requests as rq
	import json
	import io
	import datetime


	today = datetime.date.today()
	last_yr = today - datetime.timedelta(days=365)
	today = str(today)
	last_yr = str(last_yr)

	#Adjust access tokens and payload
	user_token = {"Authorization" : "Token 6kkwgMVq7wEgzXFwckTB"}
	app_token = "x38xxb8m41ds"
	payload = {"start_date" : last_yr, "end_date" : today,  "kpis" : "sessions,clicks,installs",
        	  "grouping" : "day, networks, campaigns, adgroups, creatives, country, os_name",
            	}

	#get requests for sessions, clicks, installs
	response = rq.get("https://api.adjust.com/kpis/v1/" + app_token + ".csv", headers = user_token, params = payload)
	status = response.status_code
	print(status)

	#creates a datframe the get responces for sessions, clicks, installs
	data = pd.read_csv(io.StringIO(response.text))
	data_adjust = pd.DataFrame(data)

	#add click and installs data to the sessions table
	#df_sessions["clicks"]
	data_adjust.columns = ['date', 'tracker_token', 'network', 'campaign', 'adgroup',
	       'creative', 'country', 'os_name', 'sessions', 'clicks', 'installs']

	#prints dataframe to a csv and exports it.
	print("Today's date " + today)
	print("Csv file path is /home/nick/adjust/data/ses_clk_by_day_" + today + ".csv")
	print(data_adjust.head())
	data_adjust.to_csv("/home/nick/adjust/data/ses_clk_by_day_" + today + ".csv")

