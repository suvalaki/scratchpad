import kfp
import logging

from kfp.v2 import dsl
from kfp.v2 import compiler

from google.cloud import aiplatform


@dsl.component(
    base_image="python:3.10",
    packages_to_install=["scikit-learn", "pandas", "numpy"],
)
def save_data(output: dsl.Output[dsl.Dataset]):

    import os
    from sklearn.datasets import load_breast_cancer

    data = load_breast_cancer(as_frame=True)

    # Because this is an output this will be saved
    # Somewhere under the PIPELINE_ROOT
    os.mkdir(output.path)
    pathX = output.path + "/X.csv"
    pathY = output.path + "/y.csv"

    data.data.to_csv(pathX, index=False)
    data.target.to_csv(pathY, index=False)


@dsl.pipeline(name="simplepipeline")
def simple_pipeline():

    save_data_task = save_data()


from pathlib import Path


def create_and_submit_pipeline(storage_path: Path, credentials):

    # Create the specification for the pipeline
    template_path = str(storage_path / "pipeline.yaml")
    # compiler.Compiler(mode=kfp.dsl.PipelineExecutionMode.V2_COMPATIBLE).compile(
    #     pipeline_func=simple_pipeline, package_path=template_path
    # )

    compiler.Compiler().compile(
        pipeline_func=simple_pipeline,
        package_path="pipeline.json",
    )

    # Run the pipeline
    job = aiplatform.PipelineJob(
        display_name="simplepipeline",
        template_path="./pipeline.json",
        pipeline_root="gs://" + str(storage_path),
        credentials=credentials,
    )

    try:
        job.run()
        job.wait()
    except Exception as e:
        print(e)


TEMPORARY_BUCKET_NAME = "suvalaki-temporary-bucket"
FOLDER_NAME = "simple-pipeline"

if __name__ == "__main__":

    # Prior to running you must enable your service account
    # gcloud auth application-default login
    # You need to create keys from iam > service accounts > burger > keys
    from google.oauth2 import service_account
    from pathlib import Path

    service_account_path = str(
        Path("~/.config/gcloud/testingpipelines-383506-48d266ca3bcd.json").expanduser()
    )
    credentials = service_account.Credentials.from_service_account_file(
        service_account_path,
        # scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

    # Alternative credentials
    import google.auth

    credentials, project_id = google.auth.default()

    from google.cloud import storage
    from google.cloud import aiplatform
    from utils.context_managers import TemporaryBucket

    client = storage.Client(credentials=credentials)
    # client = storage.Client.from_service_account_json(service_account_path)
    ai = aiplatform.init(credentials=credentials)
    with TemporaryBucket(credentials, client, TEMPORARY_BUCKET_NAME) as temp_bucket:
        storage_path = Path(TEMPORARY_BUCKET_NAME) / FOLDER_NAME

        create_and_submit_pipeline(storage_path, credentials)
        print(temp_bucket._bucket_name)

        # BY default pipeline jobs use the default service account for a project
        # {PROJECT_NUMBER}-compute@developer.gserviceaccount.com
        # In order to give the default credentials access to our new bucket we need
        # to run the script over it
