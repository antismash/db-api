#!/bin/bash
which flask-sqlacodegen > /dev/null
if [ $? -ne 0 ]; then
    pip install flask-sqlacodegen
fi
(echo "# Autogenerated file, do not edit manually! Run generate_model.sh to update instead"; flask-sqlacodegen --flask --schema antismash postgres://postgres:secret@localhost:15432/antismash) > api/models.py