# encrypted-bigquery-client tutorial

This tutorial provides a step by step guide to using the encrypted BigQuery
client (ebq).

ebq enables storing of private data in encrypted form on BigQuery
while supporting a meaningful subset of the client query types that are
currently supported by these tools, maintaining scalable performance, and
keeping the client data and the content of the queries as hidden as possible
from the server.

The command line tool "bq" is extended and named "ebq" (i.e. encrypted bq)
which encrypts data before loading and transforms query to work over
encrypted data.

## Preliminary Requirements

* Since ebq is a replacement for the [bq command line tool](https://developers.google.com/bigquery/bq-command-line-tool), it also
needs similar access to a [BigQuery service and project setup](https://developers.google.com/bigquery/bq-command-line-tool-quickstart).
Please set up a project before proceeding further.
  * When bq or ebq is run the first time, you will be prompted for a default
  project ID which will be stored in your .bigqueryrc file ([see here](https://developers.google.com/bigquery/bq-command-line-tool) for bq
  user guide).
* Depending on where you install ebq you may wish to set an alias to it for
easier access, e.g. `alias ebq="$HOME/.local/bin/ebq"`

## Dataset Management

### Create a test dataset

```
$ ebq mk testdataset
Dataset 'google.com:bigquerytestproject:testdataset' successfully created.
```

Makes a dataset named `testdataset` in the default project.

### List datasets

```
$ ebq ls
      datasetId
   --------------
    testdataset
    other_dataset
```

Returns a list of datasets in the default project.

### Loading (importing) a file of data

#### Figure 1: "cars.schema", an example extended schema

```
[
  {"name": "Year", "type": "integer", "mode": "required", "encrypt": "none"},
  {"name": "Make", "type": "string", "mode": "required", "encrypt": "pseudonym"},
  {"name": "Model", "type": "string", "mode": "required", "encrypt": "probabilistic_searchwords"},
  {"name": "Description", "type": "string", "mode": "nullable", "encrypt": "searchwords"},
  {"name": "Website", "type": "string", "mode": "nullable", "encrypt": "searchwords", "searchwords_separator": "/"},
  {"name": "Price", "type": "float", "mode": "required", "encrypt": "probabilistic"},
  {"name": "Invoice_Price", "type": "integer", "mode": "required", "encrypt": "homomorphic"},
  {"name": "Holdback_Percentage", "type": "float", "mode": "required", "encrypt": "homomorphic"}
]
```

#### Figure 2: "cars.csv", an example of csv data

```
1997,Ford,E350,"ac, abs","www.ford.com",3000.00,2000,1.2
1999,Chevy,"Venture ""Extended Edition""","","www.cheverolet.com",4900.00,3800,2.3
1999,Chevy,"Venture ""Extended Edition, Very Large""","","www.chevrolet.com",5000.00,4300,1.9
1996,Jeep,Grand Cherokee,"MUST SELL! air, moon roof, loaded","www.chrysler.com/jeep/grand-cherokee",4799.00,3950,2.4
```

In the local directory, copy data in figure 1 and figure 2 to files
`cars.schema` and `cars.csv`, respectively. `cars.schema` is an example of
extended schema which along with standard bq fields also includes an
"encrypt" field. `cars.csv` contains example data that may be partially
encrypted and uploaded to BigQuery.

```
$ ebq load --master_key_filename="key_file" testdataset.cars cars.csv cars.schema

Waiting on bqjob_r14826b06_0000014030f49144_1 ... (85s) Current status: DONE
```

This will upload the data in `cars.csv` to BigQuery and create a table `cars`
in `testdataset` dataset by first encrypting parts of the data in `cars.csv`
file as specified by the extended schema file `cars.schema`. It will use the
key in `key_file` to encrypt the data, however, if the key does not exist
then it will create a key and store it in the same `key_file` path that is
specified. Loading can take over a minute sometimes.

### Show the schema

```
$ ebq show testdataset.cars

Table google.com:bigquerytestproject:testdataset.cars

   Last modified                       Schema                       Total Rows   Total Bytes   Expiration
 ----------------- ----------------------------------------------- ------------ ------------- ------------
  28 Jul 22:26:49   |- Year: integer (required)                     4            4428
                               |- Make: ciphertext (required)
                               |- Model: ciphertext (required)
                               |- Model: ciphertext (required)
                               |- Description: ciphertext
                               |- Website: ciphertext
                               |- Price: ciphertext (required)
                               |- Invoice_Price: ciphertext (required)
                               |- Holdback_Percentage: ciphertext (required)
```

This returns the schema of the `cars` table indicating field name and their
type. Note that for encrypted fields the type returned is `ciphertext`.

### List the top n rows of the table

```
$ ebq head testdataset.cars
| Year | p698000442118338_PSEUDONYM_Make |    p698000442118338_SEARCHWORDS_Model . . .
| 1997 | 5KZwDE4tCljnuz0NfV/8Lw==        | J7c6nUNZIhOPtDkHr0XBcQ== +Hlh0QmgWC0= . . .
| 1999 | 7w6zkF1hoAOei294HZ9EVQ==        | NUl6uZ581LODAbTYceiplg==  . . .
...
```

This acts like `head(1)` and returns the top rows of the data directly from
BigQuery without handling decryption. We can see that certain fields
have values that are clear (e.g. Year) while other fields have encrypted,
base64 encoded values.

### Example Queries

#### Basic query

```
$ ebq query --master_key_filename=key_file "SELECT Year, Model, Price, Invoice_Price FROM testdataset.cars"

+------+---------------------------------------------------+---------+------------------+
| Year |                 Model                             | Price   | Invoice_Price    |
+------+---------------------------------------------------+---------+------------------+
| 1997 |                                              E350 |  3000.0 |             2000 |
| 1999 |                        Venture "Extended Edition" |  4900.0 |             3800 |
| 1999 |            Venture "Extended Edition, Very Large" |  5000.0 |             4300 |
| 1996 |                                    Grand Cherokee |  4799.0 |             3950 |
+------+---------------------------------------------------+---------+------------------+
```

This query returns selected fields from the `cars` table. The local `key_file`
is used by ebq to decrypt columns on the client before printing.

#### Query with WHERE condition

```
$ ebq query --master_key_filename=key_file "SELECT Year, Model, Price, Invoice_Price FROM testdataset.cars WHERE Year > 1997"
+------+-----------------------------------------------------+-----------+------------------+
| Year |                                           Model     |  Price    | Invoice_Price    |
+------+-----------------------------------------------------+-----------+------------------+
| 1999 |                          Venture "Extended Edition" |   4900.0  |            3800  |
| 1999 |             Venture "Extended Edition, Very Large"  |   5000.0  |            4300  |
+------+-----------------------------------------------------+-----------+------------------+
```

This query returns only the rows where the year values are greater than 1997.

#### Query with private WHERE condition using CONTAINS

```
$ ebq query --master_key_filename=key_file "SELECT Year, Model, Price, Invoice_Price FROM testdataset.cars WHERE Description CONTAINS 'moon roof'"

+------+-----------------------+----------+------------------+
| Year |              Model    |   Price  | Invoice_Price    |
+------+-----------------------+----------+------------------+
| 1996 |        Grand Cherokee |  4799.0  |            3950  |
+------+-----------------------+----------+------------------+

```

This query returns the single row where the `Description` field contains the
`moon roof` sequence of words. Note the case insensitivty of the CONTAINS
query: `CONTAINS 'Moon Roof'` would also return the same result.

#### Query with private WHERE condition using (equality)

```
$ ebq query --master_key_filename=key_file "SELECT Year, Model, Price, Invoice_Price FROM testdataset.cars WHERE Make == 'Chevy' "
+------+-----------------------------------------------------+-----------+------------------+
| Year |                                           Model     |  Price    | Invoice_Price    |
+------+-----------------------------------------------------+-----------+------------------+
| 1999 |                          Venture "Extended Edition" |  4900.0   |            3800  |
| 1999 |             Venture "Extended Edition, Very Large"  |  5000.0   |            4300  |
+------+-----------------------------------------------------+-----------+------------------+
```

This query returns the two rows where the `Make` field equals `Chevy`. The
`Make` field is a pseudonymn and thus is case sensitive.

#### Query with expression on (encrypted) fields

```
$ ebq query --master_key_filename=key_file "SELECT .9 * Price - 1000, .8 * Price FROM testdataset.cars WHERE Year > 1997"
+----------------------------+-----------------+
|    ((0.9 * Price) - 1000)  |  (0.8 * Price)  |
+----------------------------+-----------------+
|                     3410.0 |          3920.0 |
|                     3500.0 |          4000.0 |
+----------------------------+-----------------+
```

Even though `Price` is stored encrypted on BigQuery, ebq first decrypts the
response and then evaluates the expression before displaying the result.

#### Query with aggregation of (privately) selected records

```
$ ebq query --master_key_filename=key_file "SELECT SUM(Invoice_Price) FROM testdataset.cars"
+---------------------------+
|     SUM(Invoice_Price)    |
+---------------------------+
|                   14050.0 |
+---------------------------+
```

This query returns the sum of all the values in the `Invoice_Price` field,
which is 14050.0 for our test data.  The sum is calculated securely on the
server side using the special type of encryption, homomorphic, and the new
summation value is returned while still encrypted. The client decrypts it
with `key_file`.

#### Query with aggregation of (privately) selected records using expressions

```
$ ebq query --master_key_filename=key_file "SELECT sum(.9 * Invoice_Price), avg(.3 * Holdback_Percentage) From testdataset.cars WHERE Make == `Chevy'"
+------------------------------------+---------------- ------------------------------+
|        SUM((0.9 * Invoice_Price))  |  AVG((0.3 * Holdback_Percentage))             |
+------------------------------------+-----------------------------------------------+
|                             7290.0 |                                          0.63 |
+------------------------------------+-----------------------------------------------+
```

This query returns the sum and average of `Invoice_Price` (an int value) and
`Holdback_Percentage` (a float value) appropriately modified based on the
expression, with return values of 7290.0 and 0.63, respectively. Note that
ebq can accept simple linear expressions with the aggregation functions
`AVG` and `SUM`.

### Delete table and dataset

```
$ ebq rm testdataset.cars
rm: remove table 'google.com:bigquerytestproject:testdataset.cars'? (y/N) y
```

This command removes the `cars` table.

```
$ ebq rm testdataset
rm: remove dataset 'google.com:bigquerytestproject:testdataset'? (y/N) y
```

This command removes the entire dataset named `testdataset` from the
default project.
