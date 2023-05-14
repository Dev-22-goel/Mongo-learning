from dotenv import load_dotenv, find_dotenv
import os
import pprint
from pymongo import MongoClient

load_dotenv(find_dotenv())
password=os.environ.get("MONGODB_PWD")

connection_string=f"mongodb+srv://devansh:{password}@cluster0.jahsqst.mongodb.net/?retryWrites=true&w=majority"

client=MongoClient(connection_string)


dbs=client.list_database_names()
test_db=client.test
collections=test_db.list_collection_names()
# print(collections)

#########################
# Inserting Documents
#########################

def insert_test_doc():
    collection=test_db.test
    test_document= {
        "name": "Dev",
        "type": "test"
    }

    inserted_id= collection.insert_one(test_document).inserted_id
    print(inserted_id)

# insert_test_doc()

production= client.production
person_collection=production.person_collection

def create_documents():
    first_names= ["Dev", "Pratik", "Rahul", "Joe", "Donald", "Narendra"]
    last_names= ["Goel", "Biswal", "Jaiswal", "Biden", "Trump", "Modi"]
    ages=[21, 20, 20, 64, 54, 52]

    docs=[]
    for first_name, last_name, age in zip(first_names, last_names, ages):
        doc= {"first_name": first_name, "last_name": last_name, "age" : age}
        docs.append(doc)

        # person_collection.insert_one(doc)

    person_collection.insert_many(docs)

# create_documents()

#########################
# Reading Documents
#########################
printer= pprint.PrettyPrinter()

def find_all_people():
    people= person_collection.find()
    
    #people is the cursor in Mongo
    # print(list(people))

    for person in people:
        printer.pprint(person)

# find_all_people()

def find_dev():
    # dev= person_collection.find_one({"first_name": "Dev"})
    dev= person_collection.find_one({"first_name": "Dev", "last_name": "goel"}) #and based structure
    printer.pprint(dev)

# find_dev()

def count_all_people():
    count= person_collection.count_documents(filter={})
    # count = person_collection.find().count()
    print("Number of people", count)

# count_all_people()

def get_person_by_id(person_id):
    from bson.objectid import ObjectId

    _id=ObjectId(person_id)
    person= person_collection.find_one({"_id": _id})
    printer.pprint(person)

# get_person_by_id("6373f85b0833e0efc0be5087")

def get_age_range(min_age, max_age):
    query= {"$and": [
            {"age": {"$gte": min_age}},
            {"age": {"$lte": max_age}}
        ]}
    
    people= person_collection.find(query).sort("age")

    for person in people:
        printer.pprint(person)
    
# get_age_range(10, 70)

def project_columns():
    columns= {"_id": 0, "first_name": 1, "last_name": 1}
    people= person_collection.find({}, columns)

    for person in people:
        printer.pprint(person)
    
# project_columns()

#########################
# Updating Documents
#########################

def update_person_by_id(person_id):
    from bson.objectid import ObjectId

    _id= ObjectId(person_id)

    # all_updates= {
    #     "$set": {"new_field": True},
    #     "$inc": {"age": 1},
    #     "$rename": {"first_name": "first", "last_name": "last"}
    # }
    # person_collection.update_one({"_id": _id}, all_updates)

    #updating a person
    # person_collection.update_one({"_id": _id}, {"$unset": {"new_field": ""}})

# update_person_by_id("6373f85b0833e0efc0be5084")

def replace_one(person_id):
    from bson.objectid import ObjectId

    _id= ObjectId(person_id)

    new_doc={
        "first_name": "new first name",
        "last_name" : "new last name",
        "age":100
    }

    person_collection.replace_one({"_id": _id}, new_doc)

# replace_one("6373f85b0833e0efc0be5084")

#########################
# Updating Documents
#########################

def delete_doc_by_id(person_id):
    from bson.objectid import ObjectId
    _id= ObjectId(person_id)

    person_collection.delete_one({"_id": _id})
    
    #deletes everything
    # person_collection.delete_many({})



# delete_doc_by_id("6373f85b0833e0efc0be5084")


#########################
# Relationships
#########################

address= {
    "_id": "5243847987532978s487vds978942",
    "street": "Bay Street",
    "number": 2706,
    "city": "United States",
    "zip": "940123"
}

def add_address_embed(person_id, address):
    from bson.objectid import ObjectId
    _id= ObjectId(person_id)

    person_collection.update_one({"_id": _id}, {"$addToSet": {'addresses': address}})

# add_address_embed("6373f85b0833e0efc0be5085", address)

def add_address_relationship(person_id, address):
    from bson.objectid import ObjectId
    _id = ObjectId(person_id)

    address=address.copy()
    address["owner_id"]= person_id


    address_collection= production.address
    address_collection.insert_one(address)

add_address_relationship("6373f85b0833e0efc0be5088", address)