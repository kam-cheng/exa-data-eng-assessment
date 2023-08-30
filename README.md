# FHIR Data Processor 

The FHIR Data Processor is a service which aims to make it easier for users to work with FHIR data, storing it inside SQL tables so that it can be queried.

## Features
- FHIR data entries are stored in a PostgreSQL database, allowing for relational queries to be performed on patients and other resource types.
- Instead of a pre-made set of tables, the processor will create a new table for each new resource type it encounters. This enables the processor to be used with any FHIR resource type, without needing to modify the database schema.
- FHIR entry resources are stored in JSONB format, allowing the user to query the data regardless of the nested structure of the resource.
- Each table's 'resource' column will be indexed using GIN, allowing for faster queries on the resource data, and mitigating the disadvantages of using JSONB.


## Installation
### Requirements
- [Python 3.11](https://www.python.org/downloads/)
- [PostgreSQL 14](https://www.postgresql.org/download/)

### Setup

1. Clone the repository

```
git clone https://github.com/kam-cheng/exa-data-eng-assessment.git
```
2. Change directory into the repository

```
cd exa-data-eng-assessment
```
3. Create a virtual environment

```
python -m venv venv
```
4. Activate the virtual environment

```
. venv/bin/activate
```
5. Install the required packages

```
pip install -r requirements.txt
```
6. Seed the database. 

**Note: This deletes any existing databases named 'dev_patient_db' and 'test_patient_db'.**
```
python db/seed_db.py

2023-08-31 00:01:44,721 - INFO - Database 'dev_patient_db' deleted successfully.
2023-08-31 00:01:44,721 - INFO - Database 'test_patient_db' deleted successfully.
2023-08-31 00:01:44,799 - INFO - Database 'dev_patient_db' created successfully.
2023-08-31 00:01:44,843 - INFO - Database 'test_patient_db' created successfully.
```
7. Run the processor script
```
python main.py
```

## Usage

Once the processor script has been completed, the database will contain table 'patient', and any other table for each of the resource types identified. 

### Tables: 

Table `'patient'` stores FHIR entries with the resource type `'Patient'`: 

| `Column:` | `Data Type:`| `FHIR Key:`     |               
| --------- | ----------- | -------------   | 
| pid       | int         | N/A             |  
| full_url  | text        | fullUrl         |           
| resource  | jsonb       | resource        |           
| request   | jsonb       | request         |           


All other tables are created dynamically based on the resource type the entry, and will contain the following columns:

| `Column:` | `Data Type:`| `FHIR Key:`     | 
| --------- | ----------- | -------------   | 
| id        | int         | N/A             |           
| full_url  | text        | fullUrl         |           
| resource  | jsonb       | resource        |           
| request   | jsonb       | request         |     
| patient_id| int         | N/A             |     

The `patient.pid` value is used as the `patient_id` value in all other tables, allowing for relational queries to be performed on the data.

### Querying the data

A useful guide on querying FHIR data, and jsonb data can be found here: https://fhirbase.aidbox.app/writing-queries. 

These queries are performed after the processor script has been run, and the database has been populated with data from the `/data/` directory.

Example: Querying the patient and observation table for male smokers over the age of 45

```
SELECT DISTINCT(p.pid) as patient_id, p.resource#>>'{name,0,given,0}' as name
FROM patient p 
JOIN observation o on o.patient_id = p.pid
WHERE (o.resource @> '{"code": {"coding": [{"code":"72166-2"}] }}'::jsonb)
	AND (o.resource @> '{"valueCodeableConcept": {"text": "Former smoker"}}')
	  AND (extract(year from age(now(), (p.resource->>'birthDate')::date)) > 45) 
	    AND (p.resource->>'gender' = 'male');
```

Result: 
|"patient_id" |	"name"      |
|-------------|-------------|
|1	          |"Everett935" |
|4	          |"Leonardo412"|
|7	          |"Gonzalo160" |
|39	          |"Luciano237" |
|40	          |"Loyd638"    |
|43	          |"Zane918"    |
|58	          |"Aaron697"   |
|76           |	"Les282"    |

## Testing

Tests have been created using pytest, and are run against separate test data, on a separate test database to prevent any risk of interfering with live data.

Tests can be run using the following command:
```
pytest
```
