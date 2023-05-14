from dotenv import load_dotenv, find_dotenv
import os
import pprint
from pymongo import MongoClient
from datetime import datetime as dt

load_dotenv(find_dotenv())
password=os.environ.get("MONGODB_PWD")

#normal connection
# connection_string=f"mongodb+srv://devansh:{password}@cluster0.jahsqst.mongodb.net/?retryWrites=true&w=majority"

#admin connection
connection_string=f"mongodb+srv://devansh:{password}@cluster0.jahsqst.mongodb.net/?retryWrites=true&w=majority&authSource=admin"

client=MongoClient(connection_string)

dbs=client.list_database_names()

production= client.production

def create_book_collection():
    book_validator= {
        "jsonSchema": {
            "bsonType": "object",
            "required": ["title", "authors", "publish_date", "type", "copies"],
            "properties": {
                "title": {
                    "bsonType": "string",
                    "description": "must be a string and is required"
                },
                "authors": {
                    "bsonType": "array",
                    "items": {
                        "bsonType": "objectId",
                        "description": "must be a objectId and is required"
                    }
                },
                "publish_date": {
                    "bsonType": "date",
                    "description": "must be a date and is required"
                },
                "type": {
                    "enum": ["Fiction", "Non-Fiction"],
                    "description": "can only be one of the enum values and is required"
                },
                "copies": {
                    "bsonType": "int",
                    "minimum": 0,
                    "description": "must be an integer greater than  0 and is required"
                },
            }
        }
    }

    try:
        production.create_collection("book")

    except Exception as e:
        print(e)

    production.command("collMod", "book", validator=book_validator) 

 
def create_author_collection():
    author_validator= {
        "jsonSchema": {
            "bsonType": "object",
            "required": ["first_name", "last_name", "date_of_birth"],
            "properties": {
                "first_name": {
                    "bsonType": "string",
                    "description": "must be a string and is required"
                },
                "last_name": {
                    "bsonType": "string",
                    "description": "must be a string and is required"
                },
                "date_of_birth": {
                    "bsonType": "date",
                    "description": "must be a date and is required"
                },
                

            }
        }
    }

    try:
        production.create_collection("author")

    except Exception as e:
        print(e)

    production.command("collMod", "author", validator=author_validator) 

# create_author_collection()

def create_data():
    authors= [
        {
            "first_name": "Tim",
            "last_name": "Ruscica",
            "date_of_birth": dt(2000,7,20)
        },
        {
            "first_name":"George",
            "last_name":"Owel",
            "date_of_birth": dt(1903,6,25)
        },
        {
            "first_name": "Herman",
            "last_name": "Melville",
            "date_of_birth": dt(1819,8,1)
        },
        {
            "first_name": "F. Scott",
            "last_name": "Fitzgerald",
            "date_of_birth": dt(1896,9,26)
        }
    ]
    author_collection= production.author
    author_ids=author_collection.insert_many(authors).inserted_ids

    books =[
        {
            "title": "MongoDB Advance Tutorial",
            "authors": [authors[0]],
            "publish_date": dt.today(),
            "type": "Non-Fiction",
            "copies": 5
        },
        {
            "title": "Python For Dummies",
            "authors": [authors[0]],
            "publish_date": dt(2022,1,17),
            "type": "Non-Fiction",
            "copies": 5
        },
        {
            "title": "Nineteen Eighty-Four",
            "authors": [authors[1]],
            "publish_date": dt(1949,6,8),
            "type": "Fiction",
            "copies": 5
        },
        {
            "title": "The Great Gatsby",
            "authors": [authors[3]],
            "publish_date": dt(2014,5,23),
            "type": "Fiction",
            "copies": 5
        },
        {
            "title": "Moby Dick",
            "authors": [authors[2]],
            "publish_date": dt(1851,9,24),
            "type": "Non-Fiction",
            "copies": 5
        },
    ]
    book_collection=production.book
    book_collection.update_many(books)

create_data()