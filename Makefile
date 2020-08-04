unit:
	AS_DB_URI=postgres://postgres:secret@localhost:5432/antismash_test python -m pytest -v

coverage:
	AS_DB_URI=postgres://postgres:secret@localhost:5432/antismash_test python -m pytest --cov=api --cov-report=html --cov-report=term-missing
