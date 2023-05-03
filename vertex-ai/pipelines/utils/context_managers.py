import logging

from google.cloud import storage
import subprocess


class TemporaryBucket:
    def __init__(
        self,
        credentials,
        client: storage.Client,
        bucket_name: str,
        **kwargs,
    ):
        self._credentials = credentials
        self._client = client
        self._bucket_name = bucket_name
        self._project_id = client.project
        self._kwargs = kwargs

    def _get_project_number(self):
        call = subprocess.run(
            f"gcloud projects describe {self._project_id}",
            shell=True,
            capture_output=True,
        )
        shell_output = call.stdout.decode("utf-8")
        project_number = shell_output.strip().split(": ")[-1].replace("'", "")
        return project_number

    def _get_default_aiplatform_service_account(self):
        # Ever vertex ai service has a default account
        project_number = self._get_project_number()
        service_account = f"{project_number}-compute@developer.gserviceaccount.com"
        return service_account

    def _is_service_account(self):
        return hasattr(self._credentials, "service_account_email")

    def update_permissions(self, service_account):
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

        # service_account = self._client.get_service_account_email()
        bucket_uri = f"gs://{self._bucket_name}"
        roles = ["roles/storage.objectCreator", "roles/storage.objectViewer"]

        for r in roles:
            script = f"gsutil iam ch serviceAccount:{service_account}:{r} {bucket_uri}"
            subprocess.run(script, shell=True)

    def __enter__(self):
        print("creating bucket")
        self.bucket = self._client.create_bucket(self._bucket_name, **self._kwargs)

        if self._is_service_account():
            service_account = self._credentials.service_account_email
            self.update_permissions(service_account)

        # Provide credentials to the projects default service account to edit bucket
        if self._project_id:
            service_account = self._get_default_aiplatform_service_account()
            self.update_permissions(service_account)

        return self

    def __exit__(self, type, value, traceback):
        from traceback import print_tb

        print_tb(traceback)
        print("deleting bucket")
        self.bucket.delete(force=True)
        return True
