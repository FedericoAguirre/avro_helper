import os.path

from .avro_helper import save_records, read_records, \
    back_pressure_save_records, back_pressure_read_records

SCHEMA_PATH = "schemas/split.avsc"
FILE_PATH = "avro_files/splits_20230124.avro"
BP_FILE_PATH = "avro_files/bp_splits_20230124.avro"

def test_create_avro_file():
    laps = []
    lap1 = {
        "cadence": 167,
        "distance": 0.5,
        "duration": 221,
        "impact": 891,
        "lap": 1,
        "pace": 445,
        "steps": 617
    }
    lap2 = {
        "cadence": 170,
        "distance": 0.5,
        "duration": 210,
        "impact": 944,
        "lap": 2,
        "pace": 421,
        "steps": 597
    }
    laps.append(lap1)
    laps.append(lap2)
    save_records(laps, SCHEMA_PATH, FILE_PATH)
    assert os.path.exists(FILE_PATH)

def test_read_avro_file():
    laps = read_records(FILE_PATH)
    assert laps[0].get("lap") == 1
    assert laps[0].get("steps") == 617
    assert laps[1].get("lap") == 2
    assert laps[1].get("steps") == 597

def test_create_bp_avro_file():
    back_pressure_save_records((
            {
                "cadence": 167,
                "distance": 0.5,
                "duration": 221,
                "impact": 891,
                "lap": counter,
                "pace": 445,
                "steps": 617
            } for counter in range(10_000)
        ), 
        SCHEMA_PATH, BP_FILE_PATH)
    assert os.path.exists(BP_FILE_PATH)