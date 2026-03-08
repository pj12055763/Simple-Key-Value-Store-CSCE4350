# KV Store Project

Student: Joshua Parel  
EUID: jgp0127

## Description
This project implements a simple command-line key-value store with persistent storage.

## Supported Commands
SET <key> <value>  
GET <key>  
EXIT

## Features
- Key overwrite support (last write wins)
- Persistence via append-only log (`data.db`)
- Correct handling of nonexistent keys
- State reconstruction on restart by replaying the log
- Custom in-memory index implemented using a linked list

## Testing
The program was tested using the provided **Gradebot** black-box testing tool.

Final Gradebot Score: **94%**

A screenshot of the Gradebot output is included in this repository.

## AI Disclosure
ChatGPT was used as a supplementary tool to help explain concepts and assist with debugging during development, and also to write this README.  
All code was reviewed, tested, and modified by me to ensure it met the assignment requirements and worked correctly with the provided Gradebot tester. My prompts were usually about how to code the database, figure out what is going on with gradebot, and help me navigate command snytaxes.