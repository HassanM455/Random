''

import os

from google.cloud import storage
from google.cloud import bigtable 
from google.cloud.bigtable import column_family
from google.cloud.bigtable import row_filters
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow_plugins.operators.zip import UnzipOperator
from apiCallScript import API_PythonCallable # import mode from apiCallScript.py
from airflow.contrib.operators.file_to_gcs import FileToGoogleCloudStorageOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryCreateExternalTableOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

from airflow.contrib.operators.bigquery_operator import BigQueryDeleteDatasetOperator

from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook

from bigTableUploader import uploadToBigTable # script to upload to bigtable

bucket_name = os.environ['bucket']
project_id = os.environ['projectID']
dataset_id = os.environ['datasetID']
table_id = os.environ['tableID']
gcs_client = storage.Client()
bucket = gcs_client.get_bucket(bucket_name)
qblob1, qblob2, qblob3 = bucket.get_blob("queries/query1.txt"), bucket.get_blob("queries/query2.txt") , bucket.get_blob("queries/query3.txt")
query1, query2, query3 = qblob1.download_as_string().decode('utf-8').strip(), qblob2.download_as_string().decode('utf-8').strip(), qblob3.download_as_string().decode('utf-8').strip()
listOfQueries = [query1,query2,query3]

'''
bigTable_id, bigTableInstance_id = os.environ['tableBigTableId'],os.environ['InstanceBigTableId']
bigTable_client = bigtable.Client(project=project_id, admin=True)
instance = client.instance(bigTableInstance_id)
table_BigTable = instance.table(bigTable_id)
max_version_rule = columns_family.MaxVersionGCRule(3)

def GCStoBigTable():
	count += 1
	columnFamily_list, column_families = [] , {}
	for agg_task in AggbigQueryToGCS_tasks: 
		cf_id = "aggQuery_{}".format(str(count))
		columnFamily_list.append(cf_id)
		column_families[cf_id] = max_version_rule
	else : 
		if not table_BigTable.exist():
			table_BigTable.create(column_families=column_families)


'''
'''
Assuming pip install airflow-plugins
'''


sample_cc = ['US','CA','MX','BR','GB','CH','JP','SK']
numOfcc = len(sample_cc)

default_args = {"owner":"Hassan",
               "start_date": datetime.now(),
               "retries":1,  # unnecessary since using API every 5 minutes
               "retry_delay" : timedelta(minutes=2),
               "email":["hassan.mahmood@slalom.com"] }

dag = DAG("Sample_dag",
	default_args=default_args,
	schedule_interval=timedelta(minutes=5),
	concurrency=numOfcc)


# t1
exportCCcount_task = BashOperator(task_id='COmmenceANDexport_lenCC',
	bash_command="echo 'Commencing pipeline on $(date)' && export numOfcc='{}' ".format(str(numOfcc)),
	dag=dag)

# t2,i for i in sample_cc
dataIngestionTask, filePaths_t2, decompressionTask, filePaths_t3  = [] , [] , [] , []

'''
Python callable API_PythonCallable assumptions : 
	- accepts keyword argument country_code to request API for specific country code data
	- accepts keyword argument file_out_path to write outputfile
'''



for cc in sample_cc:
	current_time = datetime.today().strftime("%Y%m%d_%H_%M")
	output_t2 = os.path.join(os.path.realpath(__file__),'API_Download',cc,current_time)
	t2 = PythonOperator(task_id="dataPull_{}_{}".format(cc,current_time),
		                python_callable=API_PythonCallable,
		                op_kwargs={"country_code":cc,
		                         "output_path": output_t2},
		                dag=dag)
	# t1 >> [t2_US,t2_CA,...]
    # exportCCcount_task >> dataIngestionTask
    t2.set_upstream(exportCCcount_task)
	dataIngestionTask.append(t2)
	filePaths_t2.append(output_t2) 

'''
Python script decompression.py assumptions:
	-- take input path as flag arguments and outpath as flag argument

NOTE : 
(1) maybe better to import Python callable and use PythonOperator
'''



for cc , input_t3 , t2  in zip(sample_cc, filePaths_t2, dataIngestionTask):
	current_time = datetime.today().strftime("%Y%m%d_%H_%M")
	output_t3 = os.path.join(os.path.realpath(__file__),'Decompressed',cc,current_time) 
	t3 = BashOperator(task_id='decompress_ApiData_{}'.format(cc),
	bash_command="python decompression.py --inputPath {} --outputPath{}".format(input_t3, output_t3),
	dag=dag)
	# t2_US >> t3_US , t2_CA >> t3_CA
	t3.set_upstream(t2)
	decompressionTask.append(t3)
	filePaths_t3.append(output_t3)


writeToGCS_task, tempGCS_dir_paths = [] , [] 

for cc , input_t4 , t3 in zip(sample_cc,filePaths_t3, decompressionTask):
	current_time = datetime.today().strftime("%Y%m%d_%H_%M") 
	tempGCS_dir_temp="gs://{}/temp/{}".format(bucket_name,current_time)
	GCS_dir_archive = "gs://{}/archive/{}/{}".format(bucket_name,cc,current_time)
	globals()['GCS_tempDataPath'] = tempGCS_dir_temp  # setting path variable to delete at the end of DAG
	tempGCS_filepath = os.path.join(tempGCS_dir_temp,cc)
	t4 = FileToGoogleCloudStorageOperator(task_id='uploadToGCS_{}'.format(cc),
		src=input_t4,
		dst=tempGCS_filepath,
		google_cloud_storage_conn_id = storage_connection_id, 
		gzip = False,
		dag=dag)
	t4_archive = FileToGoogleCloudStorageOperator(task_id='uploadToGCS_archive_{}'.format(cc),
		src=input_t4,
		dst=GCS_dir_archive,
		google_cloud_storage_conn_id = storage_connection_id, 
		gzip = True,
		dag=dag)
	t4.set_upstream(t3)
	t4_archive.set_upstream(t3)
	writeToGCS_task.append(t4)
	tempGCS_dir_paths.append(tempGCS_filepath)

schema = None  # remember to enter schema!

dummy_task = DummyOperator(task_id="forkMerge",
	dag=dag)

try : 
	t5_prime_tableCheck=BigQueryCheckOperator(task_id='checkForTable',
		sql="SELECT COUNT(*) FROM `{}.{}.{}`".format(project_id,dataset_id,table_id),
		bigquery_conn_id=bigquery_conn_id,
		use_legacy_sql=False,
		dag=dag)
	t5_prime_tableCheck.set_upstream(writeToGCS_task)
	storageToBQ_task = GoogleCloudStorageToBigQueryOperator(task_id='uploadtoBQ_{}'.format(datetime.now().strptime('%Y%m%d_%H%M')),
	bucket=bucket_name,
	source_objects=tempGCS_dir_paths,
	destination_project_dataset="`{}.{}.{}`".format(project_id,dataset_id,table_id),
	schema_fields=schema,
	create_disposition='WRITE_TRUNCATE',
	dag=dag)
	storageToBQ_task.set_upstream(t5_prime_tableCheck)
	dummy_task.set_upstream(storageToBQ_task)
except Exception as e : 
	print("BigQueryCheck error = {}".format(e))
	t5_gamme_tableCreate = BigQueryCreateExternalTableOperator(task_id='CreateBQtable',
		bucket=bucket_name,
		source_objects=tempGCS_dir_paths,
		destination_project_dataset_table="{}.{}.{}"format(project_id,dataset_id,dataset_id),
		schema=schema,
		dag=dag)
	dummy_task.set_upstream(t5_gamme_tableCreate)



query_tasks, tempAggtables_list = [] , []
count = 1
for query in listOfQueries:
	current_time = datetime.today().strftime("%Y%m%d_%H_%M")
	tempTable = "tempAggtable{}".format(str(count)) 
	tablePointer_str = "{}.{}.{}".format(project_id,dataset_id,tempTable)
	tempAggtables_list.append(tablePointer_str)
	aggregationQuery_task = BigQueryOperator(task_id="queryJOb_{}_{}".format(str(count),current_time),
		sql=query,
		destination_dataset_table=tempTable,
		write_disposition="WRITE_TRUNCATE",
		create_disposition="CREATE_IF_NEEDED",
		allow_large_results=True,
		dag=dag)
	aggregationQuery_task.set_upstream(dummy_task)
	query_tasks.append(aggregationQuery_task)
	count += 1


AggbigQueryToGCS_tasks , bigTableInputs = [] , [] 
count=1
for aggQuery,table in zip(query_tasks,tempAggtables_list): 
	current_time = datetime.today().strftime("%Y%m%d_%H_%M")
	gcsDestURI = "gs://{}/aggQuery/{}/current_time".format(bucket_name,str(count))
	bqToGCS = BigQueryToCloudStorageOperator(task_id = "aggWriteToGCS_{}_{}".format(str(count),current_time),
		source_project_dataset_table=table,
		destination_cloud_storage=gcsDestURI ,
		bigquery_conn_id=bigquery_conn_id,
		dag=dag)
	bqToGCS.set_upstream(aggQuery)
	AggbigQueryToGCS_tasks.append(bqToGCS)
	bigTableInputs.append(gcsDestURI)
	count+=1



storageToBigTable_task = PythonOperator(task_id='UploadTObigTable_{}'.format(datetime.today().strftime("%Y%m%d_%H_%M")),
	python_callable=uploadToBigTable,
	op_args=*bigTableInputs,
	dag=dag)




BigQueryCLeanUp_task = BigQueryDeleteDatasetOperator(task_id='DeleteHostingBQDataset_{}'.format(datetime.today().strftime("%Y%m%d_%H_%M")),
	dataset_id=dataset_id,
	project_id=project_id,
	bigquery_conn_id=bigquery_conn_id,
	dag=dag)



def deleleGCSdata(tempGCS_dir_temp):
	global bucket
	blob = bucket.get_blob(tempGCS_dir_temp)
	blob.delete()

GCSCleanUp_task = PythonOperator(task_id="deleteGCStempData_{}".format(datetime.today().strftime("%Y%m%d_%H_%M")),
	python_callable=deleleGCSdata,
	op_args=tempGCS_dir_paths,
	dag=dag)


Finish_task = BashOperator(task_id='Finish_task',
	bash_command="echo 'Finished pipeline on $(date)' ".format(str(numOfcc)),
	dag=dag)


AggbigQueryToGCS_tasks >> storageToBigTable_task >> [GCSCleanUp_task, BigQueryCLeanUp_task ] >> Finish_task



	

































