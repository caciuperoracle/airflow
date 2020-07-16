from datetime import datetime
from airflow import DAG
from operators.oci_object_storage import MakeBucket,  CopyFileToOCIObjectStorageOperator
from operators.oci_data_flow import OCIDataFlowRun, OCIDataFlowCreateApplication

default_args = {'owner': 'airflow',
                'start_date': datetime(2020, 5, 26),
                'email': ['your_email@somecompany.com'],
                'email_on_failure': False,
                'email_on_retry': False,
                'bucket_name': 'acs',
                'oci_conn_id': 'oci_default',
                'compartment_ocid': 'ocid1.compartment.oc1..aaaaaaaa64t4n5eposuegupgdtggjah2lyp7zbqmbpssvwf55q6gnnsnwpyq',
                'object_name': 'oci://bas_test_bucket@osvcstage/bas-sparktest.jar'
                }

dag = DAG('oci_advanced_example2',
          default_args=default_args,
          schedule_interval='@hourly',
          catchup=False
          )

oci_conn_id = "oci_default"
bucketname = "acs"
compartment_ocid = "ocid1.compartment.oc1..aaaaaaaa64t4n5eposuegupgdtggjah2lyp7zbqmbpssvwf55q6gnnsnwpyq"
dataflow_file = "some_local_file"
dataflow_appname = "some_app_name"


with dag:
    t1 = OCIDataFlowCreateApplication(task_id='Create_Dataflow_Application_BAS',
                                      bucket_name="acs",
                                      oci_conn_id="oci_default",
                                      compartment_ocid="ocid1.compartment.oc1..aaaaaaaa64t4n5eposuegupgdtggjah2lyp7zbqmbpssvwf55q6gnnsnwpyq",
                                      display_name="Data Flow Test App",
                                      driver_shape="VM.Standard2.1",
                                      executor_shape="VM.Standard2.1",
                                      num_executors=1,
                                      spark_version="2.4.4",
                                      file_uri="oci://bas_test_bucket@osvcstage/bas-sparktest.jar",
                                      object_name="oci://bas_test_bucket@osvcstage/bas-sparktest.jar",
                                      language="SCALA",
                                      class_name="HelloWorld",
                                      )
    t2 = OCIDataFlowRun(task_id='Run_Dataflow_Application_BAS',
                        compartment_ocid="ocid1.compartment.oc1..aaaaaaaa64t4n5eposuegupgdtggjah2lyp7zbqmbpssvwf55q6gnnsnwpyq",
                        display_name="bas_test_app",
                        oci_conn_id="oci_default",
                        bucket_name="acs"
                        )
    t1 >> t2
# Broken DAG: [/opt/airflow/dags/oci_advanced_example2.py] Argument ['bucket_name', 'oci_conn_id', 'compartment_ocid', 'object_name'] is required