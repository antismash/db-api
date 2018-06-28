unit:
	AS_DB_URI=postgres://postgres:secret@localhost:5432/antismash_test py.test -v

coverage:
	AS_DB_URI=postgres://postgres:secret@localhost:5432/antismash_test py.test --cov=api --cov-report=html --cov-report=term-missing
