# Import Export Service
The Import Export Service is a client application that interacts with the client's database to perform data imports and exports. It utilizes multiple threads to handle concurrent operations. The service includes three export threads and three import threads.

## Functionality
### Export Threads
The export threads retrieve data from the client's database and export it to CSV files. The following features are supported:

Exported files are saved in the specified storage path.
The database record is updated to indicate that it has been exported.
If enabled, the service can send the exported file as an email attachment.
File logging is implemented for debugging purposes.
### Import Threads
The import threads check for available files in the specified import path. The service supports importing files in CSV, XLS, and XLSX formats. The following features are supported:

The service checks for the presence of files in the import path.
If files are found, they are processed and imported into the client's database.
Imported files are moved to the backup path for archival purposes.
SQL statements are executed to insert the imported data into the appropriate database tables.
## Configuration
The service's configuration is specified in the provided appSettings section. Below is an overview of the available configuration options:

### Export Configuration
The configuration for each export view consists of the following settings:

MailFile_ViewX: Specifies whether to send an email with the exported file as an attachment.
ToMailAddresses_ViewX: Email addresses to which the exported file should be sent.
Subject_ViewX: Subject line of the email.
Body_ViewX: Body content of the email.
storagePathExport_ViewX: Storage path where exported files will be saved.
name_ViewX: Name of the database table/view from which data will be exported.
query_ViewX: SQL query to retrieve data for export.
Export_ViewX: Specifies whether the export is enabled.
IncludeHeader_ViewX: Specifies whether to include headers in the exported file.
IntervalInMinutes_ViewX: Interval in minutes between export operations.
Separator_ViewX: Separator character used in the exported CSV file.
FilePrefix_ViewX: Prefix added to the exported file name.
FinishStatus_ViewX: Status value used to update the exported database record.
### Mail Configuration
The mail configuration includes the following settings:

SMTPServer: SMTP server address for sending emails.
SMTPServerPort: SMTP server port.
FromMailAddress: Email address used as the sender.
FromMailAddressPassword: Password for the sender's email account.
### Import Configuration
The configuration for each import includes the following settings:

Do_ImportX: Specifies whether the import is enabled.
HeadersIncluded_ImportX: Specifies whether the imported file includes headers.
FilePath_ImportX: Path from which the service retrieves import files.
Interval_ImportX: Interval in minutes between import operations.
BackupPath_ImportX: Path to which imported files are moved for backup (optional).
SQLStatement_ImportX: SQL statement used to insert imported data into the database.
## Dependencies
The Import Export Service relies on the following dependencies:

.NET Framework or compatible runtime environment.
Database server with appropriate access credentials.
SMTP server for sending email notifications.
## License

This project is licensed under the **Proprietary** license.

## Contact

For any inquiries or further information, please reach out to **Muneeb Ur Rehman** at muneeb110@live.com.
