FROM mongo:4.0

COPY users.json /users.json
COPY companies.json /companies.json

CMD mongoimport --host mongo --db demo --collection users --drop --file /users.json --jsonArray ; mongoimport --host mongo --db demo --collection companies --drop --file /companies.json --jsonArray
