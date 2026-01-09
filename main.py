import os
import csv
import io
import pymysql
from google.cloud import storage
from google.cloud import secretmanager


def get_secret(secret_id: str) -> str:
    """
    Reads secret value from Secret Manager
    """
    project_id = os.environ["GOOGLE_CLOUD_PROJECT"]

    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"

    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")


def main():
    # ---------- Non-sensitive config ----------
    bucket_name = os.environ["BUCKET_NAME"]
    output_file_name = os.environ["OUTPUT_FILE_NAME"]
    db_name = os.environ["DB_NAME"]
    instance_connection_name = os.environ["INSTANCE_CONNECTION_NAME"]

    # ---------- Secrets ----------
    db_user = get_secret("db-user")
    db_password = get_secret("db-password")

    # ---------- Connect to Cloud SQL ----------
    connection = pymysql.connect(
        user=db_user,
        password=db_password,
        unix_socket=f"/cloudsql/{instance_connection_name}",
        database=db_name
    )

    cursor = connection.cursor()
    cursor.execute("SELECT id, name, email FROM users")
    rows = cursor.fetchall()

    # ---------- Create CSV in memory ----------
    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)

    # header
    writer.writerow(["id", "name", "email"])

    # rows
    for row in rows:
        writer.writerow(row)

    # ---------- Upload CSV to GCS ----------
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(output_file_name)

    blob.upload_from_string(
        csv_buffer.getvalue(),
        content_type="text/csv"
    )

    cursor.close()
    connection.close()

    print(f"Export completed. Rows={len(rows)}")
    print(f"File written to gs://{bucket_name}/{output_file_name}")


if __name__ == "__main__":
    main()
