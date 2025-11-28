CREATE SCHEMA IF NOT EXISTSevents
    OPTIONS (
        description = "Dataset containing event tables",
        location = 'us-east1',
        default_table_expiration_days = 7 -- save costs
    );