# Demo code from using Dataflow & Apache Beam for migrations presentation

## Usage

Set up Python virtualenv and packages

`virtualenv venv`
`. venv/bin/activate`
`pip install -r requirements.txt`

Start Mongo in docker and seed database

`docker-compose up -d`

Check that Mongo is up

`mongo localhost:27017/demo --eval 'db.users.find();'`

Run sample migration job

`python mongo-migration-simple.py`
or
`python mongo-migration-join.py`

See if migration was successful

`mongo localhost:27017/demo --eval 'db.users.find();'`
