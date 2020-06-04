import argparse
import logging
import re
import datetime

from past.builtins import unicode

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io.mongodbio import ReadFromMongoDB, WriteToMongoDB

def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    with beam.Pipeline(options=pipeline_options) as p:
    
        input = p | 'Read from Mongo' >> ReadFromMongoDB(uri='mongodb://localhost:27017',
            db='demo',
            coll='demo')

        def convert_doc(doc):
            doc['fullname'] = "{} {}".format(
                doc['name']['first'],
                doc['name']['last']
                ).upper()
            doc['migrate_ts'] = datetime.datetime.utcnow()
            return doc

        converted = input | 'Convert' >> beam.Map(convert_doc)
        written = converted |  'Write to Mongo' >> WriteToMongoDB(uri='mongodb://localhost:27017',
            db='demo',
            coll='demo')


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
