FROM mongo:4.0

COPY seed.json /seed.json

CMD mongoimport --host mongo --db demo --collection users --drop --file /users.json --jsonArray
