from datetime import datetime
from airflow import DAG
from operators.oci_object_storage import MakeBucket,  CopyFileToOCIObjectStorageOperator
from operators.oci_data_flow import OCIDataFlowRun, OCIDataFlowCreateApplication

default_args = {'owner': 'airflow',
                'start_date': datetime(2020, 5, 26),
                'email': ['your_email@somecompany.com'],
                'email_on_failure': False,
                'email_on_retry': False
                }

dag = DAG('dataflow_example2',
          default_args=default_args,
          schedule_interval='@hourly',
          catchup=False
          )

oci_conn_id = "oci_default"
bucketname = "bas-airflow-test2"
compartment_ocid = "ocid1.compartment.oc1..aaaaaaaa64t4n5eposuegupgdtggjah2lyp7zbqmbpssvwf55q6gnnsnwpyq"
dataflow_file = "some_file.jar"
dataflow_appname = "bas_test"

with dag:
    t1 = OCIDataFlowCreateApplication(task_id='Create_Dataflow_Application_{0}'.format(dataflow_appname),
                                      bucket_name=bucketname,
                                      display_name=dataflow_appname,
                                      compartment_ocid=compartment_ocid,
                                      oci_conn_id=oci_conn_id,
                                      object_name=dataflow_file,
                                      language='PYTHON',
                                      )
    t2 = OCIDataFlowRun(task_id='Run_Dataflow_Application_{0}'.format(dataflow_appname),
                        compartment_ocid=compartment_ocid,
                        display_name=dataflow_appname,
                        oci_conn_id=oci_conn_id,
                        bucket_name=bucketname
                        )
    t1 >> t2
