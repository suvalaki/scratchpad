import logging

from google.cloud import storage
import subprocess


class TemporaryBucket:
    def __init__(self, credentials, client: storage.Client, bucket_name: str, **kwargs):
        self._credentials = credentials
        self._client = client
        self._bucket_name = bucket_name
        self._kwargs = kwargs

    def update_permissions(self):
        # https://github.com/GoogleCloudPlatform/vertex-ai-samples/blob/main/notebooks/official/pipelines/google_cloud_pipeline_components_model_upload_predict_evaluate.ipynb
        # Give service account access to bucket
        # gsutil iam ch \
        #   serviceAccount:my-service-account@project.iam.gserviceaccount.com:objectAdmin \
        #   gs://my-project/my-bucket

        # ! gsutil iam ch serviceAccount:{SERVICE_ACCOUNT}:roles/storage.objectCreator $BUCKET_URI
        # ! gsutil iam ch serviceAccount:{SERVICE_ACCOUNT}:roles/storage.objectViewer $BUCKET_URI

        # email = self._credentials.service_account_email
        # roles = "roles/storage.objectAdmin"
        # update_role_str = f"gsutil iam ch {email}:{roles} gs://{self._bucket_name}"

        service_account = self._credentials.service_account_email
        # service_account = self._client.get_service_account_email()
        bucket_uri = f"gs://{self._bucket_name}"
        roles = ["roles/storage.objectCreator", "roles/storage.objectViewer"]

        for r in roles:
            script = f"gsutil iam ch serviceAccount:{service_account}:{r} {bucket_uri}"
            subprocess.run(script, shell=True)

    def __enter__(self):
        print("creating bucket")
        self.bucket = self._client.create_bucket(self._bucket_name, **self._kwargs)
        self.update_permissions()

        return self

    def __exit__(self, type, value, traceback):
        print("deleting bucket")
        self.bucket.delete(force=True)
        return True
