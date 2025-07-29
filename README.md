# How to Run the Service

  Fill **config.yaml**: Add your real database credentials, the server path for PDFs, and adjust the schedule.

## Install Dependencies

Run in your terminal:

```Bash
pip install -r requirements.txt
```

## Start the Service

  Run the main script from your terminal

```Bash
python main.py
```

The service will perform an initial run immediately and then repeat the process according to the *periodicity_hours* you set in the configuration file.
