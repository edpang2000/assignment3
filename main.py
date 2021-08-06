import apache_beam as beam
from apache_beam.dataframe.io import read_csv
from apache_beam.io.textio import ReadFromText
from apache_beam.io.textio import WriteToText
import csv
import json
import hashlib

"""

There are 2 methods of creating an ndjson output here:
1) Outputs a file called 'out'. For this 1st Beam pipeline, it uses the 'traditional' way, is slower,
    but is complete - it generates a uid column/value by hashing all the other values concatenated, with md5.

2) Outputs a file called 'out2'. For this 2nd Beam pipeline, it uses the newer Dataframe API, is a lot faster,
    but I'm not able to reference the other existing values (per JSON object) to generate the hash, without hitting
    an error of some kind.

You may wish to comment out the pipelines to see how each pipeline performs.

"""

fieldnames = [
        "TransactionUniqueIdentifier"
        ,"Price"
        ,"DateOfTransfer"
        ,"Postcode"
        ,"PropertyType"
        ,"OldNew"
        ,"Duration"
        ,"PAON"
        ,"SAON"
        ,"Street"
        ,"Locality"
        ,"TownCity"
        ,"District"
        ,"County"
        ,"PPDCategoryType"
        ,"RecordStatusMonthlyFileOnly"
]

class SplitLines(beam.DoFn):
    def process(self, elem):
        for line in csv.DictReader([elem], fieldnames=fieldnames, quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True):

            # create hash
            line.update({'uid':hashlib.md5('|'.join(line.values()).encode()).hexdigest()})

            # shift 'uid' to the beginning of the dict
            line.move_to_end('uid',last=False)

            yield json.dumps(line)

def run():
    # 1st pipeline - slower, but is complete
    with beam.Pipeline() as p:
        df = (
            p
            | 'Read' >> ReadFromText("pp-monthly-update-new-version.csv")
            | 'Parse' >> beam.ParDo(SplitLines())
            | 'Write' >> WriteToText("out")
        )

    # 2nd pipeline - faster, but it doesn't generate the uid key
    with beam.Pipeline() as p2:
        df2 = (
            p2
            | 'Read' >> read_csv("pp-monthly-update-new-version.csv",names=fieldnames)
        )

        df2.to_json(path='out2',orient='records',lines=True)

if __name__ == '__main__':
    run()