FROM mongo:4.0

COPY seed.json /seed.json

CMD mongoimport --host mongo --db demo --collection demo --drop --file /seed.json --jsonArray
