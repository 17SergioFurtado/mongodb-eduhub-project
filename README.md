# EduHub MongoDB Project

## 1. Project Overview
This project demonstrates the design and management of an **e-learning platform database** using MongoDB. It covers the following:

- User, course, lesson, assigment, submission and enrollment management
- Querying, filtering, and aggregating data
- Indexing for optimized search
- Reporting and analytics

---

## 2. Project Setup

### 2.1 Prerequisites
- Python 3.10+  
- MongoDB v8.0+  
- PyMongo library  

Install PyMongo:
```bash
pip install pymongo
```

### 2.2 Database Setup
1. Start your MongoDB server.
2. Create the `eduhub` database
3. Insert collections: `user`, `course`, `lesson`,`assignment`, `enrollment`, `assignment`.
4. Apply **schema validation** as provided in project files.

---

## 3. Running the Project

### 3.1 Interactive Notebook
- Open `eduhub_mongodb_project.ipynb` in Jupyter.
- Sections include:
  - Data insertion
  - Queries and aggregations
  - Index creation


### 3.2 Python Script
- `eduhub_queries.py` contains all PyMongo operations.
- Run with:
```bash
python eduhub_queries.py
```

---


## 4. Queries and Operations
- Search courses by title, category, price, and tags
- Retrieve students in a course or their progress
- Aggregate enrollments, completion rates, average ratings
- Retrieve assignments due in the next week
- Update user profiles and add tags to courses

---

## 5. Indexing & Optimization
- Indexed fields:
  - `users.email` (unique)
  - `courses.title` and `category` (text + field indexes)
  - `assignments.dueDate`
  - `enrollments.studentId` and `courseId`
- Performance analyzed using `explain()` and Python timing
- Index usage reduces document scans and execution time

---

## 6. Test Results
- Documented in notebook outputs
- Includes execution times before and after indexing

---

## 7. References
- [MongoDB Documentation](https://www.mongodb.com/docs/)
- [PyMongo Documentation](https://pymongo.readthedocs.io/en/stable/)


