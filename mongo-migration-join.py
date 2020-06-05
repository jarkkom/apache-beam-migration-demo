import argparse
import logging
import re
import datetime
from pprint import pprint

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

        def getUserCompanyKey(element):
            return element['company']

        def getCompanyKey(element):
            return element['name']

        users = p | 'Read users from Mongo' >> ReadFromMongoDB(uri='mongodb://localhost:27017',
            db='demo',
            coll='users')

        companies = p | 'Read companies from Mongo' >> ReadFromMongoDB(uri='mongodb://localhost:27017',
            db='demo',
            coll='companies')

        users_with_company_keys = users | 'Transform user' >> beam.WithKeys(getUserCompanyKey)
        users_grouped_by_company = users_with_company_keys | 'Group users by company' >> beam.GroupByKey()

        companies_with_keys = companies | 'Transform company' >> beam.WithKeys(getCompanyKey)

        combine = (
            {
                'companies': companies_with_keys,
                'users': users_grouped_by_company,
            } | 'Group users under companies' >> beam.CoGroupByKey()
        )

        def enrichUsersWithCompany(element):
            (key, value) = element
            for v in value['users'][0]:
                v['companyDescription'] = value['companies'][0]['description']

            return value['users'][0]

        converted_users = combine | 'Enrich users with company data' >> beam.FlatMap(enrichUsersWithCompany)

        written = converted_users |  'Write to Mongo' >> WriteToMongoDB(uri='mongodb://localhost:27017',
            db='demo',
            coll='users')


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
