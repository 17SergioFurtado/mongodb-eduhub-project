#!/usr/bin/env python
# coding: utf-8

# In[162]:


# This is used to connect to a MongoDB database server
from pymongo import MongoClient
from datetime import datetime, timedelta
import time
import json

# Create a connection to the local MongoDB server running on the default port 27017
client = MongoClient("mongodb://localhost:27017/")

# Access or create if it doesn’t exist the database named 'eduhub_db'
db = client["eduhub_db"]



# In[163]:


#### UTILS FUNCTIONS
# This section contains helper functions that perform
# common reusable tasks such as generating id, loading JSON data,
# and creating collections with schema validation.


def get_next_id(db, collection_name, id_field):
    """
    Generate the next available ID for a given MongoDB collection.

    This function retrieves the document with the highest ID value
    and returns that value + 1. If the collection is empty, it returns 1.

    Parameters:
        db (Database): The MongoDB database object.
        collection_name (str): Name of the collection.
        id_field (str): Field name used as the ID.

    Returns:
        int: The next available ID.
        None: If an exception occurs.
    """
    try:
        # Access the specified collection
        collection = db[collection_name]

        # Find the document with the highest ID (descending order)
        last_doc = collection.find_one(sort=[(id_field, -1)])

        # If a document exists and contains the ID field, increment by 1
        if last_doc and id_field in last_doc:
            return last_doc[id_field] + 1

        # Otherwise, return 1 as the first ID
        return 1

    except Exception as e:
        # Print error message and return None if any error occurs
        print(f"Error while fetching id: {e}")
        return None


def load_json_file(file_path):
    """
    Load and parse data from a JSON file.

    Parameters:
        file_path (str): Path to the JSON file.

    Returns:
        dict or list: Parsed JSON data if successful.
        None: If the file is not found or an error occurs.
    """
    try:
        # Open the file in read mode
        with open(file_path, "r") as file:
            # Load and parse the JSON data
            data = json.load(file)
        return data

    except FileNotFoundError:
        # Print message if the file does not exist
        print(f"File not found")
        return None


def create_collection_with_schema(db, collection_name, schema):
    """
    Create a MongoDB collection with a specified JSON schema validator.

    Parameters:
        db (Database): The MongoDB database object.
        collection_name (str): Name of the collection to create.
        schema (dict): JSON schema for validation.

    Returns:
        bool: True if the collection is created successfully.
              False if an exception occurs.
    """
    try:
        # Create the collection with schema validation
        db.create_collection(
            collection_name,
            validator={"$jsonSchema": schema},   #Enforce JSON schema
            validationLevel="strict"             #Apply strict validation
        )

        # Inform the user that the collection was created successfully
        print(f"Collection '{collection_name}' created successfully with schema validation.")
        return True

    except Exception as e:
        # Print error message if creation fails
        print(f"Error creating collection '{collection_name}': {e}")
        return False


# In[165]:


### CRUD OPERATIONS FUNCTIONS
# This section defines the basic CRUD (Create, Read, Update, Delete)
#functions used to interact with MongoDB collections.
# Each function handles database operations with error handling
#and returns a meaningful result.


def read_document(db, collection_name, filter_query):
    """
    Read (find) documents that match a given filter.

    Returns:
        list: List of matching documents.
              Returns an empty list if no results or an error occurs.
    """
    try:
        # Access the target collection
        collection = db[collection_name]

        # Execute the query to find all matching documents
        result = collection.find(filter_query)

        # Convert the cursor result to a list of documents
        return list(result)
    except Exception as e:
        # Print error and return an empty list on failure
        print(f"Error reading documents: {e}")
        return []


def create_document(db, collection_name, document):
    """
    Create (insert) a single document into a MongoDB collection.

    Returns:
        str: The inserted document’s ObjectId as a string.
        None: If an error occurs.
    """
    try:
        # Access the target collection
        collection = db[collection_name]

        # Insert the document and store the result
        result = collection.insert_one(document)

        # Return the inserted document ID
        return str(result.inserted_id)
    except Exception as e:
        # Print error and return None if insertion fails
        print(f"Error inserting document: {e}")
        return None


def create_documents(db, collection_name, documents, id_field):
    """
    Create (insert) multiple documents at once into a MongoDB collection.

    Returns:
        list: List of inserted document IDs as strings.
              Returns an empty list if insertion fails.
    """
    try:
        # Access the collection
        collection = db[collection_name]

        # Insert multiple documents and retrieve result
        result = collection.insert_many(documents)

        # Return list of inserted document IDs
        return [str(id_field) for id_field in result.inserted_ids]
    except Exception as e:
        # Print error and return empty list on failure
        print(f"Error inserting documents: {e}")
        return []


def update_document(db, collection_name, filter_query, update_data):
    """
    Update one or more documents matching a filter with new data.

    Returns:
        int: Number of documents modified.
             Returns 0 if no documents were updated or if an error occurs.
    """
    try:
        # Access the collection
        collection = db[collection_name]

        # Update all matching documents
        result = collection.update_many(filter_query, update_data)

        # Return count of modified documents
        return result.modified_count
    except Exception as e:
        # Print error and return 0 on failure
        print(f"Error updating document: {e}")
        return 0


def delete_document(db, collection_name, document_query):
    """
    Perform a soft delete by setting 'isActive' to False
    instead of physically deleting the document.

    Returns:
        int: Number of documents updated (soft-deleted).
             Returns 0 if an error occurs.
    """
    try:
        # Access the collection
        collection = db[collection_name]

        # Perform soft delete: mark as inactive and update timestamp
        result = collection.update_many(
            document_query,
            {"$set": {
                "isActive": False,
                "updatedAt": datetime.now()
            }}
        )

        # Return number of modified documents
        return result.modified_count
    except Exception as e:
        # Print error and return 0 if operation fails
        print(f"Error performing soft delete: {e}")
        return 0


# In[166]:


# 1. Load JSON Schemas for Each Collection
# ------------------------------------------------------------
# Each schema defines the structure, field types, and validation
# rules for documents in its respective MongoDB collection.
# The load_json_file() function reads the schema

user_schema = load_json_file("user_collection_schema.json")          
course_schema = load_json_file("course_collection_schema.json")      
lesson_schema = load_json_file("lesson_collection_schema.json")       
assignment_schema = load_json_file("assignment_collection_schema.json")  
submission_schema = load_json_file("submission_collection_schema.json")  
enrollment_schema = load_json_file("enrollment_collection_schema.json") 


# 2. Assign Collection Names to Variables
# ------------------------------------------------------------
# These variables store the names of the collections used

user_collection = "user"              
course_collection = "course"           
lesson_collection = "lesson"           
assignment_collection = "assignment"   
submission_collection = "submission" 
enrollment_collection = "enrollment"  



# In[177]:


# 2. Create Collections with Schema Validation
# ------------------------------------------------------------
# Each call creates a MongoDB collection (if it doesn't already exist) and applies a JSON schema validator.
#Enforces data consistency and integrity by validating all inserted or updated documents against the specified schema.

#Create 'user' collection with its validation schema
create_collection_with_schema(db, user_collection, user_schema)

#Create 'course' collection with its validation schema
create_collection_with_schema(db, course_collection, course_schema)

#Create  'enrollment' collection with its validation schema
create_collection_with_schema(db, enrollment_collection, enrollment_schema)

#Create 'lesson' collection with its validation schema
create_collection_with_schema(db, lesson_collection, lesson_schema)

#Create 'assignment' collection with its validation schema
create_collection_with_schema(db, assignment_collection, assignment_schema)

#Create 'submission' collection with its validation schema
create_collection_with_schema(db, submission_collection, submission_schema)



# In[168]:


user = db[user_collection]
course = db[course_collection]
enrollment = db[enrollment_collection]
lesson = db[lesson_collection]
assignment = db[assignment_collection]
submission = db[submission_collection]

#create indexes for unique identifiers

#each user has a unique userId
#prevents inserting two users with the same ID
user.create_index("userId", unique=True)

#each enrollment has a unique enrollmentId
#prevents duplicate enrollment records with the same ID
enrollment.create_index("enrollmentId", unique=True)

#create a unique compound index to prevent the same student from enrolling in the same course more than once
#combination of courseId + studentId must be unique
enrollment.create_index([("courseId", 1), ("studentId", 1)], unique=True)

#each lesson has a unique lessonId
#prevents duplicate lessons with the same ID
lesson.create_index("lessonId", unique=True)

#each assignment has a unique assignmentId
#prevents duplicate assignments with the same ID
assignment.create_index("assignmentId", unique=True)

#each submission has a unique submissionId
#prevents duplicate submissions with the same ID
submission.create_index("submissionId", unique=True)


# In[169]:


users_data = [
    {"userId": 1, "email": "alice.student@example.com", "firstName": "Alice", "lastName": "Johnson", "role": "student",
     "joinedAt": datetime(2023, 1, 15, 10, 0, 0), "profile": {"bio": "Loves learning Python", "avatar": "", "skills": ["Python", "Data Analysis"]},
     "isActive": True, "updatedAt": datetime(2023, 9, 20, 12, 0, 0)},
    {"userId": 2, "email": "bob.student@example.com", "firstName": "Bob", "lastName": "Smith", "role": "student",
     "joinedAt": datetime(2023, 2, 10, 9, 30, 0), "profile": {"bio": "Front-end enthusiast", "avatar": "", "skills": ["HTML", "CSS", "JavaScript"]},
     "isActive": True, "updatedAt": datetime(2023, 9, 18, 11, 0, 0)},
    {"userId": 3, "email": "carol.student@example.com", "firstName": "Carol", "lastName": "Davis", "role": "student",
     "joinedAt": datetime(2023, 3, 1, 8, 45, 0), "profile": {"bio": "Data Science beginner", "avatar": "", "skills": ["Python", "Statistics"]},
     "isActive": True, "updatedAt": datetime(2023, 9, 15, 10, 0, 0)},
    {"userId": 4, "email": "david.student@example.com", "firstName": "David", "lastName": "Wilson", "role": "student",
     "joinedAt": datetime(2023, 1, 20, 10, 15, 0), "profile": {"bio": "AI hobbyist", "avatar": "", "skills": ["Python", "Machine Learning"]},
     "isActive": True, "updatedAt": datetime(2023, 9, 19, 9, 30, 0)},
    {"userId": 5, "email": "emma.student@example.com", "firstName": "Emma", "lastName": "Taylor", "role": "student",
     "joinedAt": datetime(2023, 2, 25, 11, 0, 0), "profile": {"bio": "Web developer in training", "avatar": "", "skills": ["HTML", "CSS"]},
     "isActive": True, "updatedAt": datetime(2023, 9, 21, 12, 15, 0)},
    {"userId": 6, "email": "frank.student@example.com", "firstName": "Frank", "lastName": "Anderson", "role": "student",
     "joinedAt": datetime(2023, 3, 10, 9, 30, 0), "profile": {"bio": "Interested in DevOps", "avatar": "", "skills": ["Docker", "Kubernetes"]},
     "isActive": True, "updatedAt": datetime(2023, 9, 20, 11, 45, 0)},
    {"userId": 7, "email": "grace.student@example.com", "firstName": "Grace", "lastName": "Thomas", "role": "student",
     "joinedAt": datetime(2023, 1, 28, 10, 0, 0), "profile": {"bio": "Learning backend development", "avatar": "", "skills": ["Node.js", "Express"]},
     "isActive": True, "updatedAt": datetime(2023, 9, 19, 10, 30, 0)},
    {"userId": 8, "email": "henry.student@example.com", "firstName": "Henry", "lastName": "Moore", "role": "student",
     "joinedAt": datetime(2023, 2, 5, 9, 0, 0), "profile": {"bio": "Database enthusiast", "avatar": "", "skills": ["SQL", "PostgreSQL"]},
     "isActive": True, "updatedAt": datetime(2023, 9, 18, 11, 15, 0)},
    {"userId": 9, "email": "isabel.student@example.com", "firstName": "Isabel", "lastName": "Martin", "role": "student",
     "joinedAt": datetime(2023, 3, 12, 8, 30, 0), "profile": {"bio": "Learning AI", "avatar": "", "skills": ["Python", "TensorFlow"]},
     "isActive": True, "updatedAt": datetime(2023, 9, 21, 9, 50, 0)},
    {"userId": 10, "email": "jack.student@example.com", "firstName": "Jack", "lastName": "Lee", "role": "student",
     "joinedAt": datetime(2023, 1, 18, 10, 20, 0), "profile": {"bio": "Cloud computing beginner", "avatar": "", "skills": ["AWS", "Azure"]},
     "isActive": True, "updatedAt": datetime(2023, 9, 20, 10, 40, 0)},
    {"userId": 11, "email": "kate.student@example.com", "firstName": "Kate", "lastName": "Perez", "role": "student",
     "joinedAt": datetime(2023, 2, 22, 11, 10, 0), "profile": {"bio": "Cybersecurity enthusiast", "avatar": "", "skills": ["Network Security", "Linux"]},
     "isActive": True, "updatedAt": datetime(2023, 9, 19, 12, 0, 0)},
    {"userId": 12, "email": "leo.student@example.com", "firstName": "Leo", "lastName": "Harris", "role": "student",
     "joinedAt": datetime(2023, 3, 5, 9, 50, 0), "profile": {"bio": "Loves Python scripting", "avatar": "", "skills": ["Python", "Automation"]},
     "isActive": True, "updatedAt": datetime(2023, 9, 18, 10, 20, 0)},
    {"userId": 13, "email": "mia.student@example.com", "firstName": "Mia", "lastName": "Clark", "role": "student",
     "joinedAt": datetime(2023, 1, 30, 10, 5, 0), "profile": {"bio": "Full-stack web developer", "avatar": "", "skills": ["React", "Node.js"]},
     "isActive": True, "updatedAt": datetime(2023, 9, 21, 11, 10, 0)},
    {"userId": 14, "email": "nick.student@example.com", "firstName": "Nick", "lastName": "Lewis", "role": "student",
     "joinedAt": datetime(2023, 2, 12, 9, 40, 0), "profile": {"bio": "Interested in DevOps", "avatar": "", "skills": ["Docker", "CI/CD"]},
     "isActive": True, "updatedAt": datetime(2023, 9, 20, 12, 30, 0)},
    {"userId": 15, "email": "olivia.student@example.com", "firstName": "Olivia", "lastName": "Walker", "role": "student",
     "joinedAt": datetime(2023, 3, 7, 8, 50, 0), "profile": {"bio": "Learning Data Science", "avatar": "", "skills": ["Python", "Pandas"]},
     "isActive": True, "updatedAt": datetime(2023, 9, 19, 10, 50, 0)},
    {"userId": 16, "email": "paul.student@example.com", "firstName": "Paul", "lastName": "Hall", "role": "student",
     "joinedAt": datetime(2023, 1, 25, 10, 30, 0), "profile": {"bio": "Interested in AI", "avatar": "", "skills": ["Python", "Keras"]},
     "isActive": True, "updatedAt": datetime(2023, 9, 21, 12, 10, 0)},
    {"userId": 17, "email": "queen.instructor@example.com", "firstName": "Queen", "lastName": "Allen", "role": "instructor",
     "joinedAt": datetime(2022, 12, 10, 9, 0, 0), "profile": {"bio": "Expert in Python", "avatar": "", "skills": ["Python", "Data Science"]},
     "isActive": True, "updatedAt": datetime(2023, 9, 15, 12, 0, 0)},
    {"userId": 18, "email": "roger.instructor@example.com", "firstName": "Roger", "lastName": "Young", "role": "instructor",
     "joinedAt": datetime(2022, 11, 5, 8, 30, 0), "profile": {"bio": "Web development instructor", "avatar": "", "skills": ["HTML", "CSS", "JavaScript"]},
     "isActive": True, "updatedAt": datetime(2023, 9, 18, 11, 40, 0)},
    {"userId": 19, "email": "sophia.instructor@example.com", "firstName": "Sophia", "lastName": "King", "role": "instructor",
     "joinedAt": datetime(2022, 12, 15, 9, 15, 0), "profile": {"bio": "AI and ML instructor", "avatar": "", "skills": ["Python", "Machine Learning"]},
     "isActive": True, "updatedAt": datetime(2023, 9, 20, 10, 30, 0)},
    {"userId": 20, "email": "tom.instructor@example.com", "firstName": "Tom", "lastName": "Scott", "role": "instructor",
     "joinedAt": datetime(2022, 12, 20, 8, 45, 0), "profile": {"bio": "Cloud computing instructor", "avatar": "", "skills": ["AWS", "Azure"]},
     "isActive": True, "updatedAt": datetime(2023, 9, 21, 11, 50, 0)},
    {"userId": 21, "email": "uma.student@example.com", "firstName": "Uma", "lastName": "Adams", "role": "student",
     "joinedAt": datetime(2023, 2, 1, 10, 10, 0), "profile": {"bio": "Learning front-end", "avatar": "", "skills": ["React", "CSS"]},
     "isActive": True, "updatedAt": datetime(2023, 9, 20, 10, 20, 0)},
    {"userId": 22, "email": "victor.student@example.com", "firstName": "Victor", "lastName": "Baker", "role": "student",
     "joinedAt": datetime(2023, 2, 18, 9, 35, 0), "profile": {"bio": "Backend beginner", "avatar": "", "skills": ["Node.js", "Express"]},
     "isActive": True, "updatedAt": datetime(2023, 9, 19, 11, 15, 0)},
    {"userId": 23, "email": "wendy.student@example.com", "firstName": "Wendy", "lastName": "Carter", "role": "student",
     "joinedAt": datetime(2023, 3, 3, 8, 55, 0), "profile": {"bio": "Learning DevOps", "avatar": "", "skills": ["Docker", "Kubernetes"]},
     "isActive": True, "updatedAt": datetime(2023, 9, 21, 12, 40, 0)},
    {"userId": 24, "email": "xander.student@example.com", "firstName": "Xander", "lastName": "Evans", "role": "student",
     "joinedAt": datetime(2023, 1, 27, 10, 20, 0), "profile": {"bio": "Database learner", "avatar": "", "skills": ["SQL", "PostgreSQL"]},
     "isActive": True, "updatedAt": datetime(2023, 9, 18, 10, 50, 0)},
    {"userId": 25, "email": "yara.student@example.com", "firstName": "Yara", "lastName": "Foster", "role": "student",
     "joinedAt": datetime(2023, 2, 8, 9, 50, 0), "profile": {"bio": "Learning AI", "avatar": "", "skills": ["Python", "TensorFlow"]},
     "isActive": True, "updatedAt": datetime(2023, 9, 20, 11, 25, 0)},
    {"userId": 26, "email": "zane.student@example.com", "firstName": "Zane", "lastName": "Green", "role": "student",
     "joinedAt": datetime(2023, 3, 11, 8, 40, 0), "profile": {"bio": "Python and ML beginner", "avatar": "", "skills": ["Python", "ML"]},
     "isActive": True, "updatedAt": datetime(2023, 9, 21, 12, 5, 0)},
    {"userId": 27, "email": "amy.student@example.com", "firstName": "Amy", "lastName": "Hughes", "role": "student",
     "joinedAt": datetime(2023, 1, 19, 10, 5, 0), "profile": {"bio": "Interested in cybersecurity", "avatar": "", "skills": ["Network Security", "Linux"]},
     "isActive": True, "updatedAt": datetime(2023, 9, 20, 10, 35, 0)},
    {"userId": 28, "email": "brian.student@example.com", "firstName": "Brian", "lastName": "Irwin", "role": "student",
     "joinedAt": datetime(2023, 2, 28, 9, 20, 0), "profile": {"bio": "Learning cloud computing", "avatar": "", "skills": ["AWS", "Azure"]},
     "isActive": True, "updatedAt": datetime(2023, 9, 19, 11, 55, 0)},
    {"userId": 29, "email": "chloe.student@example.com", "firstName": "Chloe", "lastName": "Jones", "role": "student",
     "joinedAt": datetime(2023, 3, 6, 8, 50, 0), "profile": {"bio": "Data science enthusiast", "avatar": "", "skills": ["Python", "Pandas"]},
     "isActive": True, "updatedAt": datetime(2023, 9, 21, 12, 30, 0)},
    {"userId": 30, "email": "daniel.student@example.com", "firstName": "Daniel", "lastName": "Kelly", "role": "student",
     "joinedAt": datetime(2023, 1, 23, 10, 25, 0), "profile": {"bio": "Interested in AI and ML", "avatar": "", "skills": ["Python", "Keras"]},
     "isActive": True, "updatedAt": datetime(2023, 9, 20, 10, 55, 0)},
    {"userId": 31, "email": "ella.student@example.com", "firstName": "Ella", "lastName": "Lopez", "role": "student",
     "joinedAt": datetime(2023, 2, 14, 9, 40, 0), "profile": {"bio": "Front-end learner", "avatar": "", "skills": ["HTML", "CSS", "React"]},
     "isActive": True, "updatedAt": datetime(2023, 9, 19, 11, 10, 0)},
    {"userId": 32, "email": "fred.student@example.com", "firstName": "Fred", "lastName": "Morris", "role": "student",
     "joinedAt": datetime(2023, 3, 9, 8, 35, 0), "profile": {"bio": "Learning backend development", "avatar": "", "skills": ["Node.js", "Express"]},
     "isActive": True, "updatedAt": datetime(2023, 9, 21, 12, 20, 0)}
]


# In[170]:


courses_data = [
    {"courseId": 1, "title": "Introduction to Python", "description": "Learn the basics of Python programming.",
     "instructorId": 17, "category": "Programming", "difficultyLevel": "beginner", "duration": 20, "price": 49.99,
     "tags": ["python", "programming", "basics"], "isPublished": True, "isActive": True, "reviewRate": 4.5,
     "createdAt": datetime(2023, 1, 10, 9, 0, 0), "updatedAt": datetime(2023, 5, 1, 12, 0, 0)},
    {"courseId": 2, "title": "Advanced Python", "description": "Master advanced Python concepts and best practices.",
     "instructorId": 18, "category": "Programming", "difficultyLevel": "advanced", "duration": 35, "price": 99.99,
     "tags": ["python", "advanced", "OOP"], "isPublished": True, "isActive": True, "reviewRate": 4.8,
     "createdAt": datetime(2023, 2, 15, 10, 0, 0), "updatedAt": datetime(2023, 6, 20, 14, 0, 0)},
    {"courseId": 3, "title": "Data Science Basics", "description": "Introduction to data science, statistics, and visualization.",
     "instructorId": 19, "category": "Data Science", "difficultyLevel": "beginner", "duration": 25, "price": 59.99,
     "tags": ["data science", "statistics", "visualization"], "isPublished": True, "isActive": True, "reviewRate": 4.6,
     "createdAt": datetime(2023, 3, 1, 11, 0, 0), "updatedAt": datetime(2023, 6, 15, 12, 0, 0)},
    {"courseId": 4, "title": "Machine Learning A-Z", "description": "Learn machine learning algorithms from scratch.",
     "instructorId": 20, "category": "Data Science", "difficultyLevel": "intermidiate", "duration": 40, "price": 129.99,
     "tags": ["machine learning", "python", "AI"], "isPublished": True, "isActive": True, "reviewRate": 4.7,
     "createdAt": datetime(2023, 3, 10, 9, 0, 0), "updatedAt": datetime(2023, 7, 1, 15, 0, 0)},
    {"courseId": 5, "title": "Web Development with HTML, CSS, JS",
     "description": "Build interactive websites using HTML, CSS, and JavaScript.",
     "instructorId": 17, "category": "Web Development", "difficultyLevel": "beginner", "duration": 30, "price": 79.99,
     "tags": ["web", "html", "css", "javascript"], "isPublished": True, "isActive": True, "reviewRate": 4.3,
     "createdAt": datetime(2023, 1, 20, 8, 0, 0), "updatedAt": datetime(2023, 5, 25, 13, 0, 0)},
    {"courseId": 6, "title": "React for Beginners", "description": "Learn how to build dynamic front-end applications using React.",
     "instructorId": 18, "category": "Web Development", "difficultyLevel": "beginner", "duration": 28, "price": 89.99,
     "tags": ["react", "javascript", "frontend"], "isPublished": True, "isActive": True, "reviewRate": 4.4,
     "createdAt": datetime(2023, 2, 5, 10, 30, 0), "updatedAt": datetime(2023, 6, 5, 12, 30, 0)},
    {"courseId": 7, "title": "Node.js and Express", "description": "Server-side development with Node.js and Express.",
     "instructorId": 19, "category": "Backend Development", "difficultyLevel": "intermidiate", "duration": 32, "price": 99.99,
     "tags": ["nodejs", "express", "backend"], "isPublished": True, "isActive": True, "reviewRate": 4.5,
     "createdAt": datetime(2023, 3, 1, 9, 30, 0), "updatedAt": datetime(2023, 7, 10, 14, 0, 0)},
    {"courseId": 8, "title": "Docker Essentials", "description": "Learn containerization concepts and Docker fundamentals.",
     "instructorId": 20, "category": "DevOps", "difficultyLevel": "beginner", "duration": 18, "price": 59.99,
     "tags": ["docker", "containers", "devops"], "isPublished": True, "isActive": True, "reviewRate": 4.2,
     "createdAt": datetime(2023, 1, 25, 11, 0, 0), "updatedAt": datetime(2023, 5, 30, 13, 30, 0)},
    {"courseId": 9, "title": "Kubernetes for Beginners", "description": "Introduction to Kubernetes and container orchestration.",
     "instructorId": 17, "category": "DevOps", "difficultyLevel": "intermidiate", "duration": 25, "price": 79.99,
     "tags": ["kubernetes", "containers", "orchestration"], "isPublished": True, "isActive": True, "reviewRate": 4.3,
     "createdAt": datetime(2023, 2, 15, 9, 45, 0), "updatedAt": datetime(2023, 6, 20, 12, 30, 0)},
    {"courseId": 10, "title": "SQL for Beginners", "description": "Learn to query databases using SQL.",
     "instructorId": 18, "category": "Database", "difficultyLevel": "beginner", "duration": 20, "price": 49.99,
     "tags": ["sql", "database", "query"], "isPublished": True, "isActive": True, "reviewRate": 4.4,
     "createdAt": datetime(2023, 1, 12, 8, 30, 0), "updatedAt": datetime(2023, 5, 15, 11, 0, 0)},
    {"courseId": 11, "title": "PostgreSQL Advanced", "description": "Advanced PostgreSQL features and optimization techniques.",
     "instructorId": 19, "category": "Database", "difficultyLevel": "advanced", "duration": 30, "price": 99.99,
     "tags": ["postgresql", "database", "advanced"], "isPublished": True, "isActive": True, "reviewRate": 4.6,
     "createdAt": datetime(2023, 3, 1, 10, 0, 0), "updatedAt": datetime(2023, 6, 25, 14, 0, 0)},
    {"courseId": 12, "title": "Introduction to AI", "description": "Basic concepts and applications of Artificial Intelligence.",
     "instructorId": 20, "category": "AI", "difficultyLevel": "beginner", "duration": 22, "price": 69.99,
     "tags": ["AI", "artificial intelligence", "machine learning"], "isPublished": True, "isActive": True, "reviewRate": 4.3,
     "createdAt": datetime(2023, 2, 1, 11, 0, 0), "updatedAt": datetime(2023, 6, 5, 12, 0, 0)},
    {"courseId": 13, "title": "Advanced AI Techniques", "description": "Deep dive into AI algorithms and neural networks.",
     "instructorId": 17, "category": "AI", "difficultyLevel": "advanced", "duration": 40, "price": 149.99,
     "tags": ["AI", "neural networks", "deep learning"], "isPublished": True, "isActive": True, "reviewRate": 4.7,
     "createdAt": datetime(2023, 3, 15, 9, 0, 0), "updatedAt": datetime(2023, 7, 5, 14, 0, 0)},
    {"courseId": 14, "title": "Cloud Computing Basics", "description": "Learn the fundamentals of cloud services and architecture.",
     "instructorId": 18, "category": "Cloud", "difficultyLevel": "beginner", "duration": 18, "price": 59.99,
     "tags": ["cloud", "AWS", "azure", "basics"], "isPublished": True, "isActive": True, "reviewRate": 4.2,
     "createdAt": datetime(2023, 1, 28, 10, 0, 0), "updatedAt": datetime(2023, 6, 1, 12, 0, 0)},
    {"courseId": 15, "title": "AWS Certified Solutions Architect",
     "description": "Prepare for AWS certification with hands-on labs and examples.", "instructorId": 19,
     "category": "Cloud", "difficultyLevel": "intermidiate", "duration": 35, "price": 129.99,
     "tags": ["AWS", "cloud", "certification"], "isPublished": True, "isActive": True, "reviewRate": 4.6,
     "createdAt": datetime(2023, 2, 10, 9, 30, 0), "updatedAt": datetime(2023, 6, 20, 14, 0, 0)},
    {"courseId": 16, "title": "Cybersecurity Fundamentals", "description": "Introduction to cybersecurity concepts and practices.",
     "instructorId": 20, "category": "Security", "difficultyLevel": "beginner", "duration": 25, "price": 79.99,
     "tags": ["cybersecurity", "security", "basics"], "isPublished": True, "isActive": True, "reviewRate": 4.4,
     "createdAt": datetime(2023, 3, 1, 10, 30, 0), "updatedAt": datetime(2023, 7, 1, 15, 30, 0)}
]


# In[181]:


enrollments_data = [
    {
        "enrollmentId": 1,
        "courseId": 1,
        "studentId": 1,
        "isActive": True,
        "completionStatus": "in_progress",
        "trackProgress": 20,
        "attendance": [
            {"lessonId": 1, "hasFinished": True, "attendedAt": datetime(2023, 9, 1, 10, 0, 0)},
            {"lessonId": 2, "hasFinished": False, "attendedAt":  datetime.now() }
        ],
        "completionDate": None,
        "createdAt": datetime(2023, 8, 20, 12, 0, 0),
        "updatedAt": datetime(2023, 9, 1, 10, 5, 0)
    },
    {
        "enrollmentId": 2,
        "courseId": 2,
        "studentId": 2,
        "isActive": True,
        "completionStatus": "in_progress",
        "trackProgress": 50,
        "attendance": [
            {"lessonId": 3, "hasFinished": True, "attendedAt": datetime(2023, 9, 2, 11, 0, 0)},
            {"lessonId": 4, "hasFinished": True, "attendedAt": datetime(2023, 9, 5, 11, 0, 0)}
        ],
        "completionDate": None,
        "createdAt": datetime(2023, 8, 22, 9, 0, 0),
        "updatedAt": datetime(2023, 9, 5, 12, 0, 0)
    },
    {
        "enrollmentId": 3,
        "courseId": 3,
        "studentId": 3,
        "isActive": True,
        "completionStatus": "not_started",
        "trackProgress": 0,
        "attendance": [
            {"lessonId": 5, "hasFinished": False, "attendedAt":  datetime.now() },
            {"lessonId": 6, "hasFinished": False, "attendedAt":  datetime.now() }
        ],
        "completionDate": None,
        "createdAt": datetime(2023, 8, 25, 10, 30, 0),
        "updatedAt": datetime(2023, 8, 25, 10, 30, 0)
    },
    {
        "enrollmentId": 4,
        "courseId": 1,
        "studentId": 4,
        "isActive": True,
        "completionStatus": "completed",
        "trackProgress": 100,
        "attendance": [
            {"lessonId": 1, "hasFinished": True, "attendedAt": datetime(2023, 8, 28, 9, 0, 0)},
            {"lessonId": 2, "hasFinished": True, "attendedAt": datetime(2023, 8, 30, 9, 30, 0)}
        ],
        "completionDate": datetime(2023, 8, 31, 12, 0, 0),
        "createdAt": datetime(2023, 8, 20, 12, 10, 0),
        "updatedAt": datetime(2023, 8, 31, 12, 5, 0)
    },
    {
        "enrollmentId": 5,
        "courseId": 4,
        "studentId": 5,
        "isActive": True,
        "completionStatus": "in_progress",
        "trackProgress": 30,
        "attendance": [
            {"lessonId": 7, "hasFinished": True, "attendedAt": datetime(2023, 9, 3, 10, 0, 0)},
            {"lessonId": 8, "hasFinished": False, "attendedAt":  datetime.now() }
        ],
        "completionDate": None,
        "createdAt": datetime(2023, 8, 23, 11, 0, 0),
        "updatedAt": datetime(2023, 9, 3, 10, 5, 0)
    },
    {
        "enrollmentId": 6,
        "courseId": 2,
        "studentId": 6,
        "isActive": True,
        "completionStatus": "in_progress",
        "trackProgress": 40,
        "attendance": [
            {"lessonId": 3, "hasFinished": True, "attendedAt": datetime(2023, 9, 4, 9, 30, 0)},
            {"lessonId": 4, "hasFinished": False, "attendedAt":  datetime.now() }
        ],
        "completionDate": None,
        "createdAt": datetime(2023, 8, 22, 9, 15, 0),
        "updatedAt": datetime(2023, 9, 4, 9, 35, 0)
    },
    {
        "enrollmentId": 7,
        "courseId": 5,
        "studentId": 7,
        "isActive": True,
        "completionStatus": "not_started",
        "trackProgress": 0,
        "attendance": [
            {"lessonId": 9, "hasFinished": False, "attendedAt":  datetime.now() },
            {"lessonId": 10, "hasFinished": False, "attendedAt":  datetime.now() }
        ],
        "completionDate": None,
        "createdAt": datetime(2023, 8, 24, 10, 45, 0),
        "updatedAt": datetime(2023, 8, 24, 10, 45, 0)
    },
    {
        "enrollmentId": 8,
        "courseId": 3,
        "studentId": 8,
        "isActive": True,
        "completionStatus": "completed",
        "trackProgress": 100,
        "attendance": [
            {"lessonId": 5, "hasFinished": True, "attendedAt": datetime(2023, 8, 29, 10, 0, 0)},
            {"lessonId": 6, "hasFinished": True, "attendedAt": datetime(2023, 9, 1, 10, 30, 0)}
        ],
        "completionDate": datetime(2023, 9, 2, 12, 0, 0),
        "createdAt": datetime(2023, 8, 25, 10, 35, 0),
        "updatedAt": datetime(2023, 9, 2, 12, 5, 0)
    },
    {
        "enrollmentId": 9,
        "courseId": 6,
        "studentId": 9,
        "isActive": True,
        "completionStatus": "in_progress",
        "trackProgress": 15,
        "attendance": [
            {"lessonId": 11, "hasFinished": True, "attendedAt": datetime(2023, 9, 5, 10, 0, 0)},
            {"lessonId": 12, "hasFinished": False, "attendedAt":  datetime.now() }
        ],
        "completionDate": None,
        "createdAt": datetime(2023, 8, 26, 9, 50, 0),
        "updatedAt": datetime(2023, 9, 5, 10, 5, 0)
    },
    {
        "enrollmentId": 10,
        "courseId": 4,
        "studentId": 10,
        "isActive": True,
        "completionStatus": "not_started",
        "trackProgress": 0,
        "attendance": [
            {"lessonId": 7, "hasFinished": False, "attendedAt":  datetime.now() },
            {"lessonId": 8, "hasFinished": False, "attendedAt":  datetime.now() }
        ],
        "completionDate": None,
        "createdAt": datetime(2023, 8, 23, 11, 15, 0),
        "updatedAt": datetime(2023, 8, 23, 11, 15, 0)
    },
    {
        "enrollmentId": 11,
        "courseId": 5,
        "studentId": 11,
        "isActive": True,
        "completionStatus": "in_progress",
        "trackProgress": 60,
        "attendance": [
            {"lessonId": 9, "hasFinished": True, "attendedAt": datetime(2023, 9, 6, 9, 45, 0)},
            {"lessonId": 10, "hasFinished": True, "attendedAt": datetime(2023, 9, 8, 10, 0, 0)}
        ],
        "completionDate": None,
        "createdAt": datetime(2023, 8, 24, 10, 50, 0),
        "updatedAt": datetime(2023, 9, 8, 10, 5, 0)
    },
    {
        "enrollmentId": 12,
        "courseId": 6,
        "studentId": 12,
        "isActive": True,
        "completionStatus": "in_progress",
        "trackProgress": 25,
        "attendance": [
            {"lessonId": 11, "hasFinished": True, "attendedAt": datetime(2023, 9, 7, 10, 0, 0)},
            {"lessonId": 12, "hasFinished": False, "attendedAt":  datetime.now() }
        ],
        "completionDate": None,
        "createdAt": datetime(2023, 8, 26, 9, 55, 0),
        "updatedAt": datetime(2023, 9, 7, 10, 5, 0)
    },
    {
        "enrollmentId": 13,
        "courseId": 7,
        "studentId": 13,
        "isActive": True,
        "completionStatus": "not_started",
        "trackProgress": 0,
        "attendance": [
            {"lessonId": 13, "hasFinished": False, "attendedAt":  datetime.now() },
            {"lessonId": 14, "hasFinished": False, "attendedAt":  datetime.now() }
        ],
        "completionDate": None,
        "createdAt": datetime(2023, 8, 27, 11, 0, 0),
        "updatedAt": datetime(2023, 8, 27, 11, 0, 0)
    },
    {
        "enrollmentId": 14,
        "courseId": 7,
        "studentId": 14,
        "isActive": True,
        "completionStatus": "in_progress",
        "trackProgress": 45,
        "attendance": [
            {"lessonId": 13, "hasFinished": True, "attendedAt": datetime(2023, 9, 8, 10, 0, 0)},
            {"lessonId": 14, "hasFinished": False, "attendedAt":  datetime.now() }
        ],
        "completionDate": None,
        "createdAt": datetime(2023, 8, 27, 11, 10, 0),
        "updatedAt": datetime(2023, 9, 8, 10, 5, 0)
    },
    {
        "enrollmentId": 15,
        "courseId": 8,
        "studentId": 15,
        "isActive": True,
        "completionStatus": "completed",
        "trackProgress": 100,
        "attendance": [
            {"lessonId": 15, "hasFinished": True, "attendedAt": datetime(2023, 8, 30, 10, 0, 0)},
            {"lessonId": 16, "hasFinished": True, "attendedAt": datetime(2023, 9, 1, 10, 30, 0)}
        ],
        "completionDate": datetime(2023, 9, 2, 12, 0, 0),
        "createdAt": datetime(2023, 8, 28, 12, 0, 0),
        "updatedAt": datetime(2023, 9, 2, 12, 5, 0)
    },
    {
        "enrollmentId": 16,
        "courseId": 8,
        "studentId": 16,
        "isActive": True,
        "completionStatus": "in_progress",
        "trackProgress": 30,
        "attendance": [
            {"lessonId": 15, "hasFinished": True, "attendedAt": datetime(2023, 9, 9, 10, 0, 0)},
            {"lessonId": 16, "hasFinished": False, "attendedAt":  datetime.now() }
        ],
        "completionDate": None,
        "createdAt": datetime(2023, 8, 28, 12, 10, 0),
        "updatedAt": datetime(2023, 9, 9, 10, 5, 0)
    },
    {
        "enrollmentId": 17,
        "courseId": 9,
        "studentId": 6,
        "isActive": True,
        "completionStatus": "not_started",
        "trackProgress": 0,
        "attendance": [
            {"lessonId": 17, "hasFinished": False, "attendedAt":  datetime.now() },
            {"lessonId": 18, "hasFinished": False, "attendedAt":  datetime.now() }
        ],
        "completionDate": None,
        "createdAt": datetime(2023, 8, 29, 11, 0, 0),
        "updatedAt": datetime(2023, 8, 29, 11, 0, 0)
    },
    {
        "enrollmentId": 18,
        "courseId": 9,
        "studentId": 8,
        "isActive": True,
        "completionStatus": "in_progress",
        "trackProgress": 50,
        "attendance": [
            {"lessonId": 17, "hasFinished": True, "attendedAt": datetime(2023, 9, 10, 10, 0, 0)},
            {"lessonId": 18, "hasFinished": False, "attendedAt":  datetime.now() }
        ],
        "completionDate": None,
        "createdAt": datetime(2023, 8, 29, 11, 10, 0),
        "updatedAt": datetime(2023, 9, 10, 10, 5, 0)
    },
    {
        "enrollmentId": 19,
        "courseId": 10,
        "studentId": 13,
        "isActive": True,
        "completionStatus": "not_started",
        "trackProgress": 0,
        "attendance": [
            {"lessonId": 19, "hasFinished": False, "attendedAt":  datetime.now() },
            {"lessonId": 20, "hasFinished": False, "attendedAt":  datetime.now() }
        ],
        "completionDate": None,
        "createdAt": datetime(2023, 8, 30, 10, 0, 0),
        "updatedAt": datetime(2023, 8, 30, 10, 0, 0)
    },
    {
        "enrollmentId": 20,
        "courseId": 10,
        "studentId": 21,
        "isActive": True,
        "completionStatus": "in_progress",
        "trackProgress": 35,
        "attendance": [
            {"lessonId": 19, "hasFinished": True, "attendedAt": datetime(2023, 9, 11, 10, 0, 0)},
            {"lessonId": 20, "hasFinished": False, "attendedAt":  datetime.now() }
        ],
        "completionDate": None,
        "createdAt": datetime(2023, 8, 30, 10, 10, 0),
        "updatedAt": datetime(2023, 9, 11, 10, 5, 0)
    },
    {
        "enrollmentId": 21,
        "courseId": 11,
        "studentId": 21,
        "isActive": True,
        "completionStatus": "completed",
        "trackProgress": 100,
        "attendance": [
            {"lessonId": 21, "hasFinished": True, "attendedAt": datetime(2023, 8, 31, 10, 0, 0)},
            {"lessonId": 22, "hasFinished": True, "attendedAt": datetime(2023, 9, 2, 10, 30, 0)}
        ],
        "completionDate": datetime(2023, 9, 3, 12, 0, 0),
        "createdAt": datetime(2023, 8, 31, 12, 0, 0),
        "updatedAt": datetime(2023, 9, 3, 12, 5, 0)
    },
    {
        "enrollmentId": 22,
        "courseId": 11,
        "studentId": 22,
        "isActive": True,
        "completionStatus": "in_progress",
        "trackProgress": 40,
        "attendance": [
            {"lessonId": 21, "hasFinished": True, "attendedAt": datetime(2023, 9, 12, 10, 0, 0)},
            {"lessonId": 22, "hasFinished": False, "attendedAt":  datetime.now() }
        ],
        "completionDate": None,
        "createdAt": datetime(2023, 8, 31, 12, 10, 0),
        "updatedAt": datetime(2023, 9, 12, 10, 5, 0)
    },
    {
        "enrollmentId": 23,
        "courseId": 12,
        "studentId": 23,
        "isActive": True,
        "completionStatus": "not_started",
        "trackProgress": 0,
        "attendance": [
            {"lessonId": 23, "hasFinished": False, "attendedAt":  datetime.now() },
            {"lessonId": 24, "hasFinished": False, "attendedAt":  datetime.now() }
        ],
        "completionDate": None,
        "createdAt": datetime(2023, 9, 1, 11, 0, 0),
        "updatedAt": datetime(2023, 9, 1, 11, 0, 0)
    },
    {
        "enrollmentId": 24,
        "courseId": 12,
        "studentId": 24,
        "isActive": True,
        "completionStatus": "in_progress",
        "trackProgress": 50,
        "attendance": [
            {"lessonId": 23, "hasFinished": True, "attendedAt": datetime(2023, 9, 13, 10, 0, 0)},
            {"lessonId": 24, "hasFinished": False, "attendedAt":  datetime.now() }
        ],
        "completionDate": None,
        "createdAt": datetime(2023, 9, 1, 11, 10, 0),
        "updatedAt": datetime(2023, 9, 13, 10, 5, 0)
    },
    {
        "enrollmentId": 25,
        "courseId": 13,
        "studentId": 25,
        "isActive": True,
        "completionStatus": "completed",
        "trackProgress": 100,
        "attendance": [
            {"lessonId": 25, "hasFinished": True, "attendedAt": datetime(2023, 9, 1, 10, 0, 0)},
            {"lessonId": 26, "hasFinished": True, "attendedAt": datetime(2023, 9, 3, 10, 30, 0)}
        ],
        "completionDate": datetime(2023, 9, 4, 12, 0, 0),
        "createdAt": datetime(2023, 9, 1, 12, 0, 0),
        "updatedAt": datetime(2023, 9, 4, 12, 5, 0)
    },
    {
        "enrollmentId": 26,
        "courseId": 13,
        "studentId": 26,
        "isActive": True,
        "completionStatus": "in_progress",
        "trackProgress": 30,
        "attendance": [
            {"lessonId": 25, "hasFinished": True, "attendedAt": datetime(2023, 9, 14, 10, 0, 0)},
            {"lessonId": 26, "hasFinished": False, "attendedAt":  datetime.now() }
        ],
        "completionDate": None,
        "createdAt": datetime(2023, 9, 1, 12, 10, 0),
        "updatedAt": datetime(2023, 9, 14, 10, 5, 0)
    },
    {
        "enrollmentId": 27,
        "courseId": 14,
        "studentId": 27,
        "isActive": True,
        "completionStatus": "not_started",
        "trackProgress": 0,
        "attendance": [
            {"lessonId": 27, "hasFinished": False, "attendedAt":  datetime.now() },
            {"lessonId": 28, "hasFinished": False, "attendedAt":  datetime.now() }
        ],
        "completionDate": None,
        "createdAt": datetime(2023, 9, 2, 11, 0, 0),
        "updatedAt": datetime(2023, 9, 2, 11, 0, 0)
    },
    {
        "enrollmentId": 28,
        "courseId": 14,
        "studentId": 28,
        "isActive": True,
        "completionStatus": "in_progress",
        "trackProgress": 55,
        "attendance": [
            {"lessonId": 27, "hasFinished": True, "attendedAt": datetime(2023, 9, 15, 10, 0, 0)},
            {"lessonId": 28, "hasFinished": False, "attendedAt":  datetime.now() }
        ],
        "completionDate": None,
        "createdAt": datetime(2023, 9, 2, 11, 10, 0),
        "updatedAt": datetime(2023, 9, 15, 10, 5, 0)
    },
    {
        "enrollmentId": 29,
        "courseId": 15,
        "studentId": 29,
        "isActive": True,
        "completionStatus": "completed",
        "trackProgress": 100,
        "attendance": [
            {"lessonId": 29, "hasFinished": True, "attendedAt": datetime(2023, 9, 2, 10, 0, 0)},
            {"lessonId": 30, "hasFinished": True, "attendedAt": datetime(2023, 9, 4, 10, 30, 0)}
        ],
        "completionDate": datetime(2023, 9, 5, 12, 0, 0),
        "createdAt": datetime(2023, 9, 2, 12, 0, 0),
        "updatedAt": datetime(2023, 9, 5, 12, 5, 0)
    },
    {
        "enrollmentId": 30,
        "courseId": 15,
        "studentId": 30,
        "isActive": True,
        "completionStatus": "in_progress",
        "trackProgress": 40,
        "attendance": [
            {"lessonId": 29, "hasFinished": True, "attendedAt": datetime(2023, 9, 16, 10, 0, 0)},
            {"lessonId": 30, "hasFinished": False, "attendedAt":  datetime.now() }
        ],
        "completionDate": None,
        "createdAt": datetime(2023, 9, 2, 12, 10, 0),
        "updatedAt": datetime(2023, 9, 16, 10, 5, 0)
    },
    {
        "enrollmentId": 31,
        "courseId": 16,
        "studentId": 31,
        "isActive": True,
        "completionStatus": "not_started",
        "trackProgress": 0,
        "attendance": [
            {"lessonId": 31, "hasFinished": False, "attendedAt":  datetime.now() },
            {"lessonId": 32, "hasFinished": False, "attendedAt":  datetime.now() }
        ],
        "completionDate": None,
        "createdAt": datetime(2023, 9, 3, 11, 0, 0),
        "updatedAt": datetime(2023, 9, 3, 11, 0, 0)
    },
    {
        "enrollmentId": 32,
        "courseId": 16,
        "studentId": 32,
        "isActive": True,
        "completionStatus": "in_progress",
        "trackProgress": 45,
        "attendance": [
            {"lessonId": 31, "hasFinished": True, "attendedAt": datetime(2023, 9, 17, 10, 0, 0)},
            {"lessonId": 32, "hasFinished": False, "attendedAt":  datetime.now() }
        ],
        "completionDate": None,
        "createdAt": datetime(2023, 9, 3, 11, 10, 0),
        "updatedAt": datetime(2023, 9, 17, 10, 5, 0)
    }
]


# In[172]:


lessons_data = [
    {"lessonId": 1, "courseId": 1, "title": "Intro to Python", "description": "Python basics, syntax and setup.", "assignmentId": 1, "isActive": True, "createdAt": datetime(2023,1,1,10,0,0), "updatedAt": datetime(2023,1,1,10,0,0)},
    {"lessonId": 2, "courseId": 1, "title": "Data Types and Variables", "description": "Understanding Python data types.", "assignmentId": 2, "isActive": True, "createdAt": datetime(2023,1,2,10,0,0), "updatedAt": datetime(2023,1,2,10,0,0)},
    {"lessonId": 3, "courseId": 2, "title": "HTML Basics", "description": "Intro to HTML and structure of web pages.", "assignmentId": 3, "isActive": True, "createdAt": datetime(2023,1,3,10,0,0), "updatedAt": datetime(2023,1,3,10,0,0)},
    {"lessonId": 4, "courseId": 2, "title": "CSS Basics", "description": "Styling web pages with CSS.", "assignmentId": 4, "isActive": True, "createdAt": datetime(2023,1,4,10,0,0), "updatedAt": datetime(2023,1,4,10,0,0)},
    {"lessonId": 5, "courseId": 3, "title": "JavaScript Fundamentals", "description": "Variables, functions and events in JS.", "assignmentId": 5, "isActive": True, "createdAt": datetime(2023,1,5,10,0,0), "updatedAt": datetime(2023,1,5,10,0,0)},
    {"lessonId": 6, "courseId": 3, "title": "DOM Manipulation", "description": "Selecting and modifying HTML elements.", "assignmentId": 6, "isActive": True, "createdAt": datetime(2023,1,6,10,0,0), "updatedAt": datetime(2023,1,6,10,0,0)},
    {"lessonId": 7, "courseId": 4, "title": "SQL Basics", "description": "Introduction to SQL queries.", "assignmentId": 7, "isActive": True, "createdAt": datetime(2023,1,7,10,0,0), "updatedAt": datetime(2023,1,7,10,0,0)},
    {"lessonId": 8, "courseId": 4, "title": "Joins and Aggregation", "description": "Advanced SQL queries.", "assignmentId": 8, "isActive": True, "createdAt": datetime(2023,1,8,10,0,0), "updatedAt": datetime(2023,1,8,10,0,0)},
    {"lessonId": 9, "courseId": 5, "title": "Git Basics", "description": "Version control introduction.", "assignmentId": 9, "isActive": True, "createdAt": datetime(2023,1,9,10,0,0), "updatedAt": datetime(2023,1,9,10,0,0)},
    {"lessonId": 10, "courseId": 5, "title": "Git Branching", "description": "Working with branches in Git.", "assignmentId": 10, "isActive": True, "createdAt": datetime(2023,1,10,10,0,0), "updatedAt": datetime(2023,1,10,10,0,0)},
    {"lessonId": 11, "courseId": 6, "title": "Docker Introduction", "description": "Containers and Docker basics.", "assignmentId": 11, "isActive": True, "createdAt": datetime(2023,1,11,10,0,0), "updatedAt": datetime(2023,1,11,10,0,0)},
    {"lessonId": 12, "courseId": 6, "title": "Docker Compose", "description": "Managing multi-container apps.", "assignmentId": 12, "isActive": True, "createdAt": datetime(2023,1,12,10,0,0), "updatedAt": datetime(2023,1,12,10,0,0)},
    {"lessonId": 13, "courseId": 7, "title": "Python OOP", "description": "Object-oriented programming concepts.", "assignmentId": 13, "isActive": True, "createdAt": datetime(2023,1,13,10,0,0), "updatedAt": datetime(2023,1,13,10,0,0)},
    {"lessonId": 14, "courseId": 7, "title": "Advanced OOP", "description": "Inheritance, polymorphism, and encapsulation.", "assignmentId": 14, "isActive": True, "createdAt": datetime(2023,1,14,10,0,0), "updatedAt": datetime(2023,1,14,10,0,0)},
    {"lessonId": 15, "courseId": 8, "title": "REST APIs Intro", "description": "Building RESTful services.", "assignmentId": 15, "isActive": True, "createdAt": datetime(2023,1,15,10,0,0), "updatedAt": datetime(2023,1,15,10,0,0)},
    {"lessonId": 16, "courseId": 8, "title": "API Security", "description": "Authentication and authorization.", "assignmentId": 16, "isActive": True, "createdAt": datetime(2023,1,16,10,0,0), "updatedAt": datetime(2023,1,16,10,0,0)},
    {"lessonId": 17, "courseId": 9, "title": "Cloud Basics", "description": "Intro to cloud computing.", "assignmentId": 17, "isActive": True, "createdAt": datetime(2023,1,17,10,0,0), "updatedAt": datetime(2023,1,17,10,0,0)},
    {"lessonId": 18, "courseId": 9, "title": "AWS Services", "description": "Core AWS services overview.", "assignmentId": 18, "isActive": True, "createdAt": datetime(2023,1,18,10,0,0), "updatedAt": datetime(2023,1,18,10,0,0)},
    {"lessonId": 19, "courseId": 10, "title": "Kubernetes Intro", "description": "Containers orchestration basics.", "assignmentId": 19, "isActive": True, "createdAt": datetime(2023,1,19,10,0,0), "updatedAt": datetime(2023,1,19,10,0,0)},
    {"lessonId": 20, "courseId": 10, "title": "K8s Deployments", "description": "Managing deployments in Kubernetes.", "assignmentId": 20, "isActive": True, "createdAt": datetime(2023,1,20,10,0,0), "updatedAt": datetime(2023,1,20,10,0,0)},
    {"lessonId": 21, "courseId": 11, "title": "Data Analysis Intro", "description": "Analyzing data with Pandas.", "assignmentId": 21, "isActive": True, "createdAt": datetime(2023,1,21,10,0,0), "updatedAt": datetime(2023,1,21,10,0,0)},
    {"lessonId": 22, "courseId": 11, "title": "Data Visualization", "description": "Creating charts with matplotlib.", "assignmentId": 22, "isActive": True, "createdAt": datetime(2023,1,22,10,0,0), "updatedAt": datetime(2023,1,22,10,0,0)},
    {"lessonId": 23, "courseId": 12, "title": "Machine Learning Intro", "description": "Supervised and unsupervised learning.", "assignmentId": 23, "isActive": True, "createdAt": datetime(2023,1,23,10,0,0), "updatedAt": datetime(2023,1,23,10,0,0)},
    {"lessonId": 24, "courseId": 12, "title": "ML Algorithms", "description": "Regression, classification, clustering.", "assignmentId": 24, "isActive": True, "createdAt": datetime(2023,1,24,10,0,0), "updatedAt": datetime(2023,1,24,10,0,0)},
    {"lessonId": 25, "courseId": 13, "title": "Big Data Overview", "description": "Introduction to Big Data concepts.", "assignmentId": 25, "isActive": True, "createdAt": datetime(2023,1,25,10,0,0), "updatedAt": datetime(2023,1,25,10,0,0)},
    {"lessonId": 26, "courseId": 13, "title": "Hadoop Basics", "description": "Hadoop ecosystem and architecture.", "assignmentId": 26, "isActive": True, "createdAt": datetime(2023,1,26,10,0,0), "updatedAt": datetime(2023,1,26,10,0,0)},
    {"lessonId": 27, "courseId": 14, "title": "NoSQL Intro", "description": "Key concepts of NoSQL databases.", "assignmentId": 27, "isActive": True, "createdAt": datetime(2023,1,27,10,0,0), "updatedAt": datetime(2023,1,27,10,0,0)},
    {"lessonId": 28, "courseId": 14, "title": "MongoDB Basics", "description": "CRUD operations and aggregation.", "assignmentId": 28, "isActive": True, "createdAt": datetime(2023,1,28,10,0,0), "updatedAt": datetime(2023,1,28,10,0,0)},
    {"lessonId": 29, "courseId": 15, "title": "Python Data Science", "description": "Using Python for data analysis.", "assignmentId": 29, "isActive": True, "createdAt": datetime(2023,1,29,10,0,0), "updatedAt": datetime(2023,1,29,10,0,0)},
    {"lessonId": 30, "courseId": 15, "title": "Data Science Project", "description": "Hands-on project for data science.", "assignmentId": 30, "isActive": True, "createdAt": datetime(2023,1,30,10,0,0), "updatedAt": datetime(2023,1,30,10,0,0)},
    {"lessonId": 31, "courseId": 1, "title": "Python Functions", "description": "Defining and using functions in Python.", "assignmentId": 31, "isActive": True, "createdAt": datetime(2023,2,1,10,0,0), "updatedAt": datetime(2023,2,1,10,0,0)},
    {"lessonId": 32, "courseId": 2, "title": "Forms in HTML", "description": "Creating interactive forms.", "assignmentId": 32, "isActive": True, "createdAt": datetime(2023,2,2,10,0,0), "updatedAt": datetime(2023,2,2,10,0,0)},
    {"lessonId": 33, "courseId": 3, "title": "Events in JavaScript", "description": "Handling user interactions.", "assignmentId": 33, "isActive": True, "createdAt": datetime(2023,2,3,10,0,0), "updatedAt": datetime(2023,2,3,10,0,0)},
    {"lessonId": 34, "courseId": 4, "title": "Indexes in SQL", "description": "Creating and using indexes for performance.", "assignmentId": 34, "isActive": True, "createdAt": datetime(2023,2,4,10,0,0), "updatedAt": datetime(2023,2,4,10,0,0)},
    {"lessonId": 35, "courseId": 5, "title": "Git Merge Conflicts", "description": "Resolving conflicts in Git.", "assignmentId": 35, "isActive": True, "createdAt": datetime(2023,2,5,10,0,0), "updatedAt": datetime(2023,2,5,10,0,0)},
    {"lessonId": 36, "courseId": 6, "title": "Docker Networking", "description": "Managing networks in Docker.", "assignmentId": 36, "isActive": True, "createdAt": datetime(2023,2,6,10,0,0), "updatedAt": datetime(2023,2,6,10,0,0)},
    {"lessonId": 37, "courseId": 7, "title": "OOP Design Patterns", "description": "Common patterns in software design.", "assignmentId": 37, "isActive": True, "createdAt": datetime(2023,2,7,10,0,0), "updatedAt": datetime(2023,2,7,10,0,0)},
    {"lessonId": 38, "courseId": 8, "title": "API Testing", "description": "Testing REST APIs with Postman.", "assignmentId": 38, "isActive": True, "createdAt": datetime(2023,2,8,10,0,0), "updatedAt": datetime(2023,2,8,10,0,0)},
    {"lessonId": 39, "courseId": 9, "title": "Cloud Security", "description": "Best practices for securing cloud services.", "assignmentId": 39, "isActive": True, "createdAt": datetime(2023,2,9,10,0,0), "updatedAt": datetime(2023,2,9,10,0,0)},
    {"lessonId": 40, "courseId": 10, "title": "Kubernetes Services", "description": "Exposing applications with services.", "assignmentId": 40, "isActive": True, "createdAt": datetime(2023,2,10,10,0,0), "updatedAt": datetime(2023,2,10,10,0,0)}
]


# In[173]:


assignments_data = [
    {"assignmentId": 1, "lessonId": 1, "courseId": 1, "title": "Python Setup Assignment", "description": "Install Python and run your first script.", "dueDate": datetime(2023,1,5,23,59,0), "isActive": True, "createdAt": datetime(2023,1,1,10,0,0), "updatedAt": datetime(2023,1,1,10,0,0)},
    {"assignmentId": 2, "lessonId": 2, "courseId": 1, "title": "Variables and Data Types", "description": "Create variables of different types.", "dueDate": datetime(2023,1,6,23,59,0), "isActive": True, "createdAt": datetime(2023,1,2,10,0,0), "updatedAt": datetime(2023,1,2,10,0,0)},
    {"assignmentId": 3, "lessonId": 3, "courseId": 2, "title": "HTML Structure Assignment", "description": "Build a basic HTML page with headings and paragraphs.", "dueDate": datetime(2023,1,7,23,59,0), "isActive": True, "createdAt": datetime(2023,1,3,10,0,0), "updatedAt": datetime(2023,1,3,10,0,0)},
    {"assignmentId": 4, "lessonId": 4, "courseId": 2, "title": "CSS Styling Assignment", "description": "Style your HTML page using CSS.", "dueDate": datetime(2023,1,8,23,59,0), "isActive": True, "createdAt": datetime(2023,1,4,10,0,0), "updatedAt": datetime(2023,1,4,10,0,0)},
    {"assignmentId": 5, "lessonId": 5, "courseId": 3, "title": "JavaScript Functions", "description": "Write JS functions to handle basic operations.", "dueDate": datetime(2023,1,9,23,59,0), "isActive": True, "createdAt": datetime(2023,1,5,10,0,0), "updatedAt": datetime(2023,1,5,10,0,0)},
    {"assignmentId": 6, "lessonId": 6, "courseId": 3, "title": "DOM Manipulation Task", "description": "Change HTML content using JS DOM methods.", "dueDate": datetime(2023,1,10,23,59,0), "isActive": True, "createdAt": datetime(2023,1,6,10,0,0), "updatedAt": datetime(2023,1,6,10,0,0)},
    {"assignmentId": 7, "lessonId": 7, "courseId": 4, "title": "Basic SQL Queries", "description": "Write SELECT queries to fetch data from tables.", "dueDate": datetime(2023,1,11,23,59,0), "isActive": True, "createdAt": datetime(2023,1,7,10,0,0), "updatedAt": datetime(2023,1,7,10,0,0)},
    {"assignmentId": 8, "lessonId": 8, "courseId": 4, "title": "SQL Joins Assignment", "description": "Use INNER, LEFT, and RIGHT joins on sample tables.", "dueDate": datetime(2023,1,12,23,59,0), "isActive": True, "createdAt": datetime(2023,1,8,10,0,0), "updatedAt": datetime(2023,1,8,10,0,0)},
    {"assignmentId": 9, "lessonId": 9, "courseId": 5, "title": "Git Init and Commit", "description": "Initialize a repo and make your first commits.", "dueDate": datetime(2023,1,13,23,59,0), "isActive": True, "createdAt": datetime(2023,1,9,10,0,0), "updatedAt": datetime(2023,1,9,10,0,0)},
    {"assignmentId": 10, "lessonId": 10, "courseId": 5, "title": "Git Branching Exercise", "description": "Create and merge branches in Git.", "dueDate": datetime(2023,1,14,23,59,0), "isActive": True, "createdAt": datetime(2023,1,10,10,0,0), "updatedAt": datetime(2023,1,10,10,0,0)},
    {"assignmentId": 11, "lessonId": 11, "courseId": 6, "title": "Dockerfile Creation", "description": "Create a Dockerfile and build an image.", "dueDate": datetime(2023,1,15,23,59,0), "isActive": True, "createdAt": datetime(2023,1,11,10,0,0), "updatedAt": datetime(2023,1,11,10,0,0)},
    {"assignmentId": 12, "lessonId": 12, "courseId": 6, "title": "Docker Compose File", "description": "Set up a multi-container application using Docker Compose.", "dueDate": datetime(2023,1,16,23,59,0), "isActive": True, "createdAt": datetime(2023,1,12,10,0,0), "updatedAt": datetime(2023,1,12,10,0,0)},
    {"assignmentId": 13, "lessonId": 13, "courseId": 7, "title": "OOP Class Design", "description": "Design classes using inheritance and encapsulation.", "dueDate": datetime(2023,1,17,23,59,0), "isActive": True, "createdAt": datetime(2023,1,13,10,0,0), "updatedAt": datetime(2023,1,13,10,0,0)},
    {"assignmentId": 14, "lessonId": 14, "courseId": 7, "title": "Polymorphism Exercise", "description": "Implement polymorphism in a small program.", "dueDate": datetime(2023,1,18,23,59,0), "isActive": True, "createdAt": datetime(2023,1,14,10,0,0), "updatedAt": datetime(2023,1,14,10,0,0)},
    {"assignmentId": 15, "lessonId": 15, "courseId": 8, "title": "API REST Assignment", "description": "Build a simple REST API with GET and POST endpoints.", "dueDate": datetime(2023,1,19,23,59,0), "isActive": True, "createdAt": datetime(2023,1,15,10,0,0), "updatedAt": datetime(2023,1,15,10,0,0)}
]


# In[174]:


submissions_data = [
    {"submissionId":1,"enrollmentId":1,"assignmentId":1,"submittedAt":datetime(2023,1,5,22,0,0),"feedback":"Good work","grade":95,"createdAt":datetime(2023,1,5,22,10,0),"updatedAt":datetime(2023,1,5,22,10,0)},
    {"submissionId":2,"enrollmentId":2,"assignmentId":1,"submittedAt":datetime(2023,1,5,21,30,0),"feedback":"Well done","grade":90,"createdAt":datetime(2023,1,5,21,35,0),"updatedAt":datetime(2023,1,5,21,35,0)},
    {"submissionId":3,"enrollmentId":1,"assignmentId":2,"submittedAt":datetime(2023,1,6,23,0,0),"feedback":"Excellent","grade":98,"createdAt":datetime(2023,1,6,23,5,0),"updatedAt":datetime(2023,1,6,23,5,0)},
    {"submissionId":4,"enrollmentId":3,"assignmentId":3,"submittedAt":datetime(2023,1,7,20,30,0),"feedback":"Good start","grade":88,"createdAt":datetime(2023,1,7,20,35,0),"updatedAt":datetime(2023,1,7,20,35,0)},
    {"submissionId":5,"enrollmentId":2,"assignmentId":2,"submittedAt":datetime(2023,1,6,22,45,0),"feedback":"Nice work","grade":92,"createdAt":datetime(2023,1,6,22,50,0),"updatedAt":datetime(2023,1,6,22,50,0)},
    {"submissionId":6,"enrollmentId":4,"assignmentId":4,"submittedAt":datetime(2023,1,8,19,0,0),"feedback":"Check styling","grade":85,"createdAt":datetime(2023,1,8,19,5,0),"updatedAt":datetime(2023,1,8,19,5,0)},
    {"submissionId":7,"enrollmentId":1,"assignmentId":3,"submittedAt":datetime(2023,1,7,21,0,0),"feedback":"Good","grade":90,"createdAt":datetime(2023,1,7,21,5,0),"updatedAt":datetime(2023,1,7,21,5,0)},
    {"submissionId":8,"enrollmentId":5,"assignmentId":5,"submittedAt":datetime(2023,1,9,22,15,0),"feedback":"Well implemented","grade":93,"createdAt":datetime(2023,1,9,22,20,0),"updatedAt":datetime(2023,1,9,22,20,0)},
    {"submissionId":9,"enrollmentId":3,"assignmentId":4,"submittedAt":datetime(2023,1,8,21,30,0),"feedback":"Good CSS","grade":89,"createdAt":datetime(2023,1,8,21,35,0),"updatedAt":datetime(2023,1,8,21,35,0)},
    {"submissionId":10,"enrollmentId":6,"assignmentId":6,"submittedAt":datetime(2023,1,10,20,45,0),"feedback":"Excellent","grade":97,"createdAt":datetime(2023,1,10,20,50,0),"updatedAt":datetime(2023,1,10,20,50,0)},
    {"submissionId":11,"enrollmentId":4,"assignmentId":5,"submittedAt":datetime(2023,1,9,21,0,0),"feedback":"Nice logic","grade":91,"createdAt":datetime(2023,1,9,21,5,0),"updatedAt":datetime(2023,1,9,21,5,0)},
    {"submissionId":12,"enrollmentId":2,"assignmentId":6,"submittedAt":datetime(2023,1,10,22,0,0),"feedback":"Well done","grade":94,"createdAt":datetime(2023,1,10,22,5,0),"updatedAt":datetime(2023,1,10,22,5,0)},
    {"submissionId":13,"enrollmentId":1,"assignmentId":7,"submittedAt":datetime(2023,1,11,20,0,0),"feedback":"SQL queries fine","grade":96,"createdAt":datetime(2023,1,11,20,5,0),"updatedAt":datetime(2023,1,11,20,5,0)},
    {"submissionId":14,"enrollmentId":5,"assignmentId":7,"submittedAt":datetime(2023,1,11,21,15,0),"feedback":"Great work","grade":95,"createdAt":datetime(2023,1,11,21,20,0),"updatedAt":datetime(2023,1,11,21,20,0)},
    {"submissionId":15,"enrollmentId":6,"assignmentId":8,"submittedAt":datetime(2023,1,12,20,30,0),"feedback":"Correct joins","grade":92,"createdAt":datetime(2023,1,12,20,35,0),"updatedAt":datetime(2023,1,12,20,35,0)},
    {"submissionId":16,"enrollmentId":3,"assignmentId":8,"submittedAt":datetime(2023,1,12,19,50,0),"feedback":"Well done","grade":90,"createdAt":datetime(2023,1,12,19,55,0),"updatedAt":datetime(2023,1,12,19,55,0)},
    {"submissionId":17,"enrollmentId":4,"assignmentId":9,"submittedAt":datetime(2023,1,13,22,0,0),"feedback":"Good repo setup","grade":94,"createdAt":datetime(2023,1,13,22,5,0),"updatedAt":datetime(2023,1,13,22,5,0)},
    {"submissionId":18,"enrollmentId":5,"assignmentId":10,"submittedAt":datetime(2023,1,14,20,30,0),"feedback":"Branches correct","grade":93,"createdAt":datetime(2023,1,14,20,35,0),"updatedAt":datetime(2023,1,14,20,35,0)},
    {"submissionId":19,"enrollmentId":1,"assignmentId":11,"submittedAt":datetime(2023,1,15,21,0,0),"feedback":"Dockerfile ok","grade":95,"createdAt":datetime(2023,1,15,21,5,0),"updatedAt":datetime(2023,1,15,21,5,0)},
    {"submissionId":20,"enrollmentId":6,"assignmentId":12,"submittedAt":datetime(2023,1,16,22,0,0),"feedback":"Compose setup fine","grade":96,"createdAt":datetime(2023,1,16,22,5,0),"updatedAt":datetime(2023,1,16,22,5,0)}
]


# In[ ]:


##Part 2: Data Population
#Task 2.1: Insert Sample Data

create_documents(db, user_collection, users_data, "userId")
create_documents(db, course_collection, courses_data, "courseId")
create_documents(db, enrollment_collection, enrollments_data, "enrollmentId")
create_documents(db, lesson_collection, lessons_data, "lessonId")
create_documents(db, assignment_collection, assignments_data, "assignmentId")
create_documents(db, submission_collection, submissions_data, "submissionId")


# In[186]:


# Part 3: Basic CRUD Operations
# Task 3.1: Create Operations
# 1. Add a new student user

# Define a new student document with required information
new_user = {
    "userId": get_next_id(db, user_collection, "userId"), #generate a new unique userId
    "firstName": "antonio",                 
    "lastName": "furtado",                  
    "email": "antonio.student@example.com",   
    "role": "student",                    
    "joinedAt": datetime.now()      #Timestamp of when the student joined
}

#insert the new student document into the 'user_collection'
result = create_document(db, user_collection, new_user)

# print the result of the insertion
print(result)


# In[187]:


# 2. Create a new course

# Define a new course document with all necessary details
new_course = {

    "courseId": get_next_id(db, course_collection, "courseId"), #Generate a unique courseId
    "title": "Advanced Python Programming",  # Course title
    "description": "Deep dive into Python for real-world applications.",  
    "instructorId": 17,                      
    "category": "Programming",                
    "difficultyLevel": "advanced",            
    "duration": 40,                           
    "price": 199.99,                          
    "tags": ["python", "advanced", "programming", "oop", "data"], 
    "isPublished": True,                      
    "isActive": True,                         
    "reviewRate": 0.0,                        
    "createdAt": datetime.now()        #Timestamp of creation
}

# Insert the new course document into the course_collection
result = create_document(db, course_collection, new_course)

# print the result of the insertion
print(result)


# In[188]:


# 3. Enroll a student in a course

# Define a new enrollment document linking a student to a course
new_enrollment = {
    "enrollmentId": get_next_id(db, enrollment_collection, "enrollmentId"),      

    "courseId": 21,             # ID of the course the student is enrolling in
    "studentId": 31,            # ID of the student being enrolled

    "isActive": True,           
    "completionStatus": "not_started",  
    "trackProgress": 0,         

    # Attendance list stores each lesson's status for the student
    "attendance": [             
        {
            "lessonId": 1,          
            "hasFinished": False,   
            "attendedAt": datetime.now()  
        }
    ],

    "completionDate": None,     
    "createdAt": datetime.now() 
}

# Insert the new enrollment document into the 'enrollment_collection'
result = create_document(db, enrollment_collection, new_enrollment)

# Print the result to verify insertion success
print(result)


# In[189]:


# 4. Add a new lesson to an existing course

# Define a new lesson document that belongs to a specific course
new_lesson = {
    "lessonId": get_next_id(db, lesson_collection, "lessonId"),
    "courseId": 21,               #ID of the course this lesson belongs to

    "title": "Advanced Python Functions",  # Title of the lesson
    "description": "Deep dive into functions, closures, and decorators in Python.",                                 
    "assignmentId": 12,           
    "isActive": True,              
    "createdAt": datetime.now() 
}

# Insert the new lesson document into the 'lesson_collection'
result = create_document(db, lesson_collection, new_lesson)

print(result)


# In[117]:


# Task 3.2: Read Operations


# In[190]:


# 1. Find all active students

# Define the query to filter documents in the user collection
query = {
    "role": "student",   #consider users with the role "student"
    "isActive": True     #include users who are active
}

# Execute the read operation on the user_collection with the specified query
result = read_document(db, user_collection, query)

#print each active student
for active_student in result:
    print(active_student)


# In[192]:


# 2. Retrieve course details along with instructor information

#instructors in the dataset have IDs 17, 18, 19, 20,
instructor_id = 17
result = ""

#query to find all courses taught by this instructor
query = {"instructorId": instructor_id}

result = read_document(db, course_collection, query)

#print each course's details
for course in result:
    print(course)


# In[193]:


# 3. Get all courses in a specific category

#the category you want to filter by
category = "Programming"
result = ""

#query to find all courses where "category" matches 
query = {"category": category}

result = read_document(db, course_collection, query)

for course in result:
    print(course)


# In[194]:


#4. Find students enrolled in a particular course

#specify course ID to filter
course_id = 5
result = ""

query = {"courseId": course_id}
result = read_document(db, enrollment_collection, query)

for enrollment in result:
    print(enrollment)


# In[195]:


#5. Search courses by title (case-insensitive, partial match)

# search term to match in course titles
search_title = "aWs"
result = ""

#query using a regular expression ($regex)
#$options ->i makes the search case-insensitive
query = {"title": {"$regex": search_title, "$options": "i"}}

result = read_document(db, course_collection, query)

for course in result:
    print(course)


# In[131]:


#Task 3.3: Update Operations


# In[196]:


# 1. Update a user's profile information

#specify the userId of the user profile to update
user_id = 1
result = ""

#new profile data to be updated
new_profile = {
    "bio": "Learning Data Engineering",
    "avatar": "https://avatarlink.com/avatar.png",
    "skills": ["SQL", "Python"]
}

#query to find the user document with the given userId to be update
query = {"userId": user_id}

#the update object using MongoDB's $set operator
#sets the new profile data in the user's document and updates the updatedAt timestamp
update_data = {
    "$set": {
        "profile.bio": new_profile.get("bio"),
        "profile.avatar": new_profile.get("avatar"),
        "profile.skills": new_profile.get("skills"),
        "updatedAt": datetime.now()
    }
}

result = update_document(db, user_collection, query, update_data)

#show how many documents were updated
print(f"Number of documents updated {result}")


# In[197]:


# 2. Mark a course as published
result = ""

query = {"courseId":1}

#will set the 'isPublished' field to True and update the 'updatedAt' timestamp
update_data = {
    "$set": {
        "isPublished": True,          #mark the course as published
        "updatedAt": datetime.now()  
    }
}

result = update_document(db, course_collection, query, update_data)

print(f"Number of documents updated {result}")


# In[198]:


# 3. Update assignment grades

result = ""

#the grade value to assign
grade_value = 98

#the submissionId of the submission to update
submissionId = 1

query = {"submissionId": submissionId}

#sets the new grade value and updates the 'updatedAt' timestamp
update_data = {
    "$set": {
        "grade": grade_value,        
        "updatedAt": datetime.now()
    }
}

result = update_document(db, submission_collection, query, update_data)

print(f"Number of documents updated {result}")


# In[199]:


# 4. Add tags to an existing course
result = ""

#the courseId of the course to update
courseId = 1

#the new tags to add
new_tags = ["backend"]

query = {"courseId": courseId}

#the update object
update_data = {
    "$addToSet": {"tags": {"$each": new_tags}},  #add new tags uniquely
    "$set": {"updatedAt": datetime.now()}       
}

result = update_document(db, course_collection, query, update_data)

print(f"Number of documents updated {result}")


# In[140]:


#Task 3.4: Delete Operations


# In[200]:


# 1. Remove a user (soft delete by setting isActive to false)
result = ""

#userId of the user to remove
userId = 1

document_query = {"userId": userId}

#delete_document perform a soft delete by updating isActive to False
result = delete_document(db, user_collection, document_query)

# show how many documents were deleted
print(f"Number of documents deleted {result}")


# In[201]:


# 2. Delete an enrollment

result = ""
enrollmentId = 1

document_query = {"enrollmentId": enrollmentId}

result = delete_document(db, enrollment_collection, document_query)

print(f"Number of documents deleted {result}")



# In[202]:


# 3. Remove a lesson from a course
result = ""

lessonId = 1

document_query = {"lessonId": lessonId}

result = delete_document(db, lesson_collection, document_query)

print(f"Number of documents removed {result}")


# In[143]:


# Part 4: Advanced Queries and Aggregation


# In[203]:


# Task 4.1: Complex Queries
# 1. Find courses with price between $50 and $200
result = ""

#"$gte" means "greater than or equal to"
#"$lte" means "less than or equal to"
# will match courses where the price is between 50 and 200 inclusive
document_query = {"price": {"$gte": 50, "$lte": 200}}

result = read_document(db, course_collection, document_query)

for course in result:
    print(course)


# In[204]:


# 2.Get users who joined in the last 6 months

result = ""

#calculate date for 6 month ago approximating 6 months as 180 days
six_months_ago = datetime.now() - timedelta(days=6*30)

document_query = {"joinedAt": {"$gte": six_months_ago}}

result = read_document(db, user_collection, document_query)

for user in result:
    print(user)


# In[205]:


# Task 4.1: Complex Queries
# 3. Find courses that have specific tags using the $in operator
result = ""

#the list of tags to search for
search_tags = ["python", "programming"]

#"$in" operator checks if the 'tags' field contains any of the values in search_tags
#will match courses that have at least one of the tags in the list
document_query = {"tags": {"$in": search_tags}}

result = read_document(db, course_collection, document_query)

for course in result:
    print(course)


# In[154]:


# Task 4.1: Complex Queries
# 4. Retrieve assignments with due dates in the next week
result = ""

# Calculate the date 7 days from now 
next_week = datetime.now() + timedelta(days=7)

#will match assignments due within the next 7 days
document_query = {"dueDate": {"$gte": datetime.now(), "$lte": next_week}}

result = read_document(db, assignment_collection, document_query)

for assignment in result:
    print(assignment)


# In[35]:


# Task 4.2: Aggregation Pipeline


# In[206]:


# 1. Course Enrollment Statistics
# 1.1 Count total enrollments per course
result = ""

#Get the enrollment collection from the database
enrollments = db[enrollment_collection]

#the aggregation pipeline
pipeline = [
    {
        # $group stage aggregates documents by a specified key
        "$group": {
            "_id": "$courseId",                #group by 'courseId'
            "totalEnrollments": {"$sum": 1}   #count each document in the group
        }
    }
]

results = enrollments.aggregate(pipeline)

for result in results:
    print(f"courseId: {result['_id']}, total enrollments: {result['totalEnrollments']}")


# In[207]:


# 1.2-Calculate average course rating
# Group by course category
result = ""
course = db[course_collection]

pipeline = [
    {
        "$group": {
            "_id": "$category",               # group by course category
            "averageRating": {"$avg": "$reviewRate"}  #average rating per category
        }
    },
    {
        "$sort": {"averageRating": -1}      #sort by highest rating
    }
]

result = list(course.aggregate(pipeline))

for statistic in result:
    print(f"category: {statistic['_id']}, avg rating: {statistic['averageRating']:.2f}")



# In[150]:


# 2.Student Performance Analysis


# In[208]:


# 1.Average grade per student
result = ""
enrollment = db[enrollment_collection]

pipeline = [
    {
        "$lookup": {
            "from": "submission",       #submission collection with grades
            "localField": "enrollmentId", 
            "foreignField": "enrollmentId",
            "as": "submissions"
        }
    },
    {
        "$unwind": "$submissions"     #flat the submissions array
    },
    {
        "$group": {
            "_id": "$studentId",      #group by student
            "averageGrade": { "$avg": "$submissions.grade" }
        }
    },
    {
        "$sort": { "averageGrade": -1 } #highest first
    }
]


result = list(enrollment.aggregate(pipeline))
for res in result:
    print(f"studentId: {res['_id']}, average grade: {res['averageGrade']:.2f}")


# In[209]:


# Part 5: Indexing and Performance


# In[210]:


# 1-User email lookup
user = db[user_collection]

#unique=True ensures no duplicate emails
user.create_index("email", unique=True)


# In[211]:


# 2-Course search by title and category 

course = db[course_collection]

#compound index
course.create_index([("title", "text"), ("category", 1)])


# In[212]:


# 3-Assignment queries by due date

assignment = db[assignment_collection]

assignment.create_index("dueDate")


# In[213]:


# 4-Assignment queries by due date

enrollment = db[enrollment_collection]

enrollment.create_index([("studentId", 1), ("courseId", 1)])

