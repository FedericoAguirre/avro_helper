import os.path

from avro_helper import back_pressure_save_records, back_pressure_read_records, save_records

SCHEMA_PATH = "schemas/split.avsc"
FILE_PATH = "avro_files/splits_20230124.avro"
BP_FILE_PATH = "avro_files/bp_splits_20230124.avro"

def test_create_bk_avro_file():
    back_pressure_save_records((
            {
                "cadence": 167,
                "distance": 0.5,
                "duration": 221,
                "impact": 891,
                "lap": counter,
                "pace": 445,
                "steps": 617
            } for counter in range(10_0000)
        ), 
        SCHEMA_PATH, BP_FILE_PATH)

def test_save_records():
    save_records(
        [{
                "cadence": 167,
                "distance": 0.5,
                "duration": 221,
                "impact": 891,
                "lap": counter,
                "pace": 445,
                "steps": 617
            } for counter in range(10_000)],
        SCHEMA_PATH,
        BP_FILE_PATH
    )

if __name__ == "__main__":
    test_create_bk_avro_file()

    for record in back_pressure_read_records(BP_FILE_PATH):
        print(f"{record.get('lap')}")