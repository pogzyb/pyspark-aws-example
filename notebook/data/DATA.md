Data for this project
--

### rides/*
The csv files for this example are fairly large (100-200MB each).
Instead of bogging down this repository with these files,
I will leave it up you to either:

- Download and extract a reason sample for local EDA with Jupyter Notebook
- (or) Run `make_sample_set.py` to generate `data/rides/sample_set.csv` (~10 million rows)

These csv's contains individual ride history.

- Duration – Duration of trip *(target feature)
- Start Date – Includes start date and time
- End Date – Includes end date and time
- Start Station – Includes starting station name and number
- End Station – Includes ending station name and number
- Bike Number – Includes ID number of bike used for the trip
- Member Type – Indicates whether user was a "registered" member (Annual Member, 30-Day Member or Day Key Member) or a "casual" rider (Single Trip, 24-Hour Pass, 3-Day Pass or 5-Day Pass)

sources:
- https://www.capitalbikeshare.com/system-data
- https://s3.amazonaws.com/capitalbikeshare-data/index.html 

### stations/*
The single csv file in this contains information about "Bike Stations"

source:
- https://opendata.dc.gov/datasets/capital-bike-share-locations