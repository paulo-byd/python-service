## Running Options

1. **Local Environment (default):**

   ```bash
   python main.py
   ```

2. **UAT Environment:**

   ```bash
   python main.py uat
   ```

3. **Production Environment:**

   ```bash
   python main.py prod
   ```

The environment parameter determines which database configuration to use (local, UAT, or production). If no parameter is provided, it defaults to "local".


## Documentation

### Main.py

1. Validate config before starting
2. **Configure Scheduler**: This will schedule the following 'jobs' to run automatically and periodically:
   1. Download Job
   2. Batch PDF Processing Job

#### i) Download Job
1. Get new files to download
   1. Get list of claims that needs download.
      1. Get DMS Claim Data into df
      2. Get BGate Claim Data into df
      3. Merge both and check:
         1. Claim is not on BGate
         2. Claim DMS Update_date > Stored Last_DMS_Update_Date
         3. Claim with ATTACHMENT_STATUS != 'COMPLETE'
      4. If any if these affirmations is true, then this claim is added to the list of claims that need download.
2. Mark all of the files in these claims as obsolete (They will be donwloaded again)
3. Gets a df with all the individual files that need download.
4. Iterate over files df and download them and log download status as SUCCESS or FAILED and new last modified date in the tracking table.

#### ii) PDF Processing Job
1. Get all the files sucessfully downloaded in the last 24 hours.
2. Create a list with each file path and processes these files.
3. Return a JSON

