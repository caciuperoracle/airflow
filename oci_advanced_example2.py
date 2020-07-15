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

dag = DAG('oci_advanced_example2',
          default_args=default_args,
          schedule_interval='@hourly',
          catchup=False
          )

print("oci ex1 dag")

oci_conn_id = "oci_default"
bucketname = "acs"
compartment_ocid = "ocid1.compartment.oc1..aaaaaaaa64t4n5eposuegupgdtggjah2lyp7zbqmbpssvwf55q6gnnsnwpyq"
dataflow_file = "some_local_file"
dataflow_appname = "some_app_name"


with dag:
    t1 = OCIDataFlowCreateApplication(task_id='Create_Dataflow_Application_{0}'.format(dataflow_appname),
                                      compartment_id="ocid1.compartment.oc1..aaaaaaaa64t4n5eposuegupgdtggjah2lyp7zbqmbpssvwf55q6gnnsnwpyq",
                                      display_name="Data Flow Test App",
                                      driver_shape="VM.Standard2.1",
                                      executor_shape="VM.Standard2.1",
                                      num_executors=1,
                                      spark_version="2.4.4",
                                      file_uri="oci://bas_test_bucket@osvcstage/bas-sparktest.jar",
                                      language="SCALA",
                                      class_name="HelloWorld",
                                      )
    t2 = OCIDataFlowRun(task_id='Run_Dataflow_Application_{0}'.format(dataflow_appname),
                        compartment_ocid="ocid1.compartment.oc1..aaaaaaaa64t4n5eposuegupgdtggjah2lyp7zbqmbpssvwf55q6gnnsnwpyq",
                        display_name=dataflow_appname,
                        oci_conn_id=oci_conn_id,
                        bucket_name=bucketname
                        )
    t1 >> t2
