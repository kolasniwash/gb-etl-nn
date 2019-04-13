if __name__ == "__main__":

	from google.cloud import bigquery
	#from google.cloud import storage
	from google.oauth2 import service_account

	credentials = service_account.Credentials.from_service_account_file('/home/nick/adjust/keys/tableau-neuronation-40af18a1a4ed.json')
	project_id = 'tableau-neuronation'

	#client access for google big query
	client = bigquery.Client(credentials = credentials, project = project_id)

	#print(tables)


	#construct the current file name for referencing
	import datetime
	today = datetime.date.today()
	today = str(today)
	table_name_local = 'ses_clk_by_day_' + today + ".csv"
	table_name_bigquery = "adjust_ses_clk_by_day"
	print("Table to update: " + table_name_local)
	local_path = "/home/nick/adjust/data/" + table_name_local
	print("Local path: " + local_path)
	dataset_id = "Adjust"

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

#	# The source format defaults to CSV, so the line below is optional.
#	job_config.source_format = bigquery.SourceFormat.CSV
#
#	uri = 'gs://adjust_csvs/data.csv'
#	uri2 = "file:///home/nick/adjust/data/ses_clk_by_day_" + today + ".csv"
#	load_job = client_gbq.load_table_from_uri(uri2,
#		dataset_ref.table(table_name_bigquery),
#    		job_config=job_config)  # API request
#
#	print('Starting job {}'.format(load_job.job_id))
#
#	load_job.result()  # Waits for table load to complete.
#	print('Job finished.')
#
#	destination_table = client_gbq.get_table(dataset_ref.table(table_name_bigquery))
#	print('Loaded {} rows.'.format(destination_table.num_rows))
