import apache_beam as beam
from apache_beam.dataframe.io import read_csv
import hashlib

with beam.Pipeline() as p:
    df = p | 'Read CSV' >> beam.dataframe.io.read_csv("pp-monthly-update-new-version.csv",names=["Transaction unique identifier","Price","Date of Transfer","Postcode","Property Type","Old/New","Duration","PAON","SAON","Street","Locality","Town/City","District","County","PPD Category Type","Record Status - monthly file only"])
    df.to_json(path='out',orient='records',lines=True)

with beam.Pipeline() as p2:
    df2 = p2 | 'Read CSV' >> beam.dataframe.io.read_csv("pp-monthly-update-new-version.csv",names=["Transaction unique identifier","Price","Date of Transfer","Postcode","Property Type","Old/New","Duration","PAON","SAON","Street","Locality","Town/City","District","County","PPD Category Type","Record Status - monthly file only"])
    
    agg = df2[['Property Type']].groupby('Property Type').count()
    agg.to_csv('output')

1==1