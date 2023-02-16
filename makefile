cleanup:
	rm -rf avro_files/*

test: cleanup
	pytest -q ah_tests.py 