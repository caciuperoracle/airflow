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

dag = DAG('dataflow_example',
          default_args=default_args,
          schedule_interval='@hourly',
          catchup=False
          )

oci_conn_id = "oci_default"
bucketname = "bas-airflow-test2"
compartment_ocid = "ocid1.compartment.oc1..aaaaaaaa64t4n5eposuegupgdtggjah2lyp7zbqmbpssvwf55q6gnnsnwpyq"
dataflow_file = "oci://bas_test_bucket@osvcstage/bas-sparktest.jar"
dataflow_appname = "bas_test"


with dag:
    t1 = MakeBucket(task_id='Make_Bucket',
                    bucket_name=bucketname,
                    oci_conn_id=oci_conn_id,
                    compartment_ocid=compartment_ocid)
    t2 = CopyFileToOCIObjectStorageOperator(task_id='Copy_{0}_to_Bucket'.format(dataflow_file),
                                            bucket_name=bucketname,
                                            compartment_ocid=compartment_ocid,
                                            oci_conn_id=oci_conn_id,
                                            object_name=dataflow_file,
                                            local_file_path='/home/airflow/')
    t3 = OCIDataFlowCreateApplication(task_id='Create_Dataflow_Application_{0}'.format(dataflow_appname),
                                      bucket_name=bucketname,
                                      display_name=dataflow_appname,
                                      compartment_ocid=compartment_ocid,
                                      oci_conn_id=oci_conn_id,
                                      object_name=dataflow_file,
                                      language='PYTHON',
                                      )
    t4 = OCIDataFlowRun(task_id='Run_Dataflow_Application_{0}'.format(dataflow_appname),
                        compartment_ocid=compartment_ocid,
                        display_name=dataflow_appname,
                        oci_conn_id=oci_conn_id,
                        bucket_name=bucketname
                        )
    t1 >> t2 >> t3 >> t4
