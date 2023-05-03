if __name__ == "__main__":

    from google.cloud import storage
    from google.cloud import aiplatform

    from simple_pipeline import *
    from utils.context_managers import TemporaryBucket

    # Initialise the creds
    aiplatform.init()
    credentials = aiplatform.initializer.global_config.credentials
    client = storage.Client(credentials=credentials)

    with TemporaryBucket(credentials, client, TEMPORARY_BUCKET_NAME) as temp_bucket:
        storage_path = Path(TEMPORARY_BUCKET_NAME) / FOLDER_NAME
        create_and_submit_pipeline(storage_path, credentials)
        print(temp_bucket._bucket_name)
