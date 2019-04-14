if __name__ == "__main__":

	from google.cloud import bigquery
	from google.oauth2 import service_account
	import pandas as pd
	import datetime
	import adjust_nn_deliverables_get

	#consruct client and login to goolg api
	credentials = service_account.Credentials.from_service_account_file('/home/nick/adjust/keys/tableau-neuronation-40af18a1a4ed.json')
	project_id = 'tableau-neuronation'
	client = bigquery.Client(credentials = credentials, project = project_id)

	#create two date strings. one for today, one for 30 days ago.
	today = datetime.date.today()
	last30 = today - datetime.timedelta(days=30)
	today = str(today)
	#last30 = str(last30)

	#naming tables
	dataset_id = 'Adjust'
	table_name = 'nn_deliverables'
	table_name_bigquery = "nn_deliverables"
	local_path = "/home/nick/adjust/data/" + table_name
	print("Local path: " + local_path)

	#download old data as dataframe from google big query
	dataset_ref = client.dataset(dataset_id).table(table_name)
	table = client.get_table(dataset_ref)
	data = client.list_rows(table).to_dataframe()
	print("Downloading adjust data from last 30 days...")

	#boolean mask to extract any dates that are older than 30 days
	data = data.iloc[:,1:]
	data_prev30 = data[data['date'] < last30]
#	print(data.dtypes)
#	print(data_prev30.head())
#
#	#filter current df to match the simple version used in testing
#	data_new = data_new.loc[:,['date', 'tracker_token', 'network', 'campaign',
#	       'adgroup', 'creative', 'country', 'os_name', 'sessions', 'clicks', 'installs']]
#	data_new.tail(10)
#
	#pull recent 30 days of data from adjust and combine into one dataframe
	data_new30 = adjust_nn_deliverables_get.get_data()
	data_full = data_prev30.append(data_new30, ignore_index = True)

	#save the dataframe as local file
	data_full.to_csv(local_path)

	#try to delete previous table. if failed catch the fail and notify
	try:
		print("Trying to delete..." + table_name_bigquery)
		table_ref = client.dataset(dataset_id).table(table_name_bigquery)
		client.delete_table(table_ref)  # API request
		print("Deleted sucessfully")
	except:
		print("No table named: " + table_name_bigquery)

	#recreate the table with the passed csv
	dataset_ref = client.dataset(dataset_id)
	job_config = bigquery.LoadJobConfig()
	job_config.autodetect = True
	job_config.skip_leading_rows = 1

	with open(local_path, 'rb') as source_file:
    		job = client.load_table_from_file(
        	source_file,
        	table_ref,
        	location='EU',  # Must match the destination dataset location.
        	job_config=job_config)  # API request

	job.result()  # Waits for table load to complete.

	print('Loaded {} rows into project: {} dataset: {} table: {}.'.format(job.output_rows, project_id, dataset_id, table_name_bigquery))
