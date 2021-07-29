from fastavro import writer, parse_schema
from datetime import datetime, timedelta
from decimal import Decimal


schema = {
    "doc": "A weather reading.",
    "name": "Weather",
    "namespace": "test",
    "type": "record",
    "fields": [
        {"name": "f_str", "type": "string"},
        {"name": "f_bool", "type": "boolean"},
        {"name": "f_long", "type": "long"},
        {"name": "f_int", "type": "int"},
        {"name": "f_float", "type": "float"},
        {"name": "f_double", "type": "double"},
        {"name": "f_tsms", "type": {"type": "long", "logicalType": "timestamp-millis"}},
        {"name": "f_tsus", "type": {"type": "long", "logicalType": "timestamp-micros"}},
        {"name": "f_tms", "type": {"type": "int", "logicalType": "time-millis"}},
        {"name": "f_tus", "type": {"type": "long", "logicalType": "time-micros"}},
        {"name": "f_date", "type": {"type": "int", "logicalType": "date"}},
        {
            "name": "f_decimal",
            "type": {
                "type": "bytes",
                "logicalType": "decimal",
                "precision": 20,
                "scale": 3,
            },
        },
    ],
}
parsed_schema = parse_schema(schema)

with open("./test_type.avro", "wb") as out:
    iter = list(
        {
            "f_str": "C" + "e" * (i % 20 + 1) + "b",
            "f_bool": i % 2 == 0,
            "f_long": i * 8,
            "f_int": i * 4,
            "f_float": float(i * 4 / 3),
            "f_double": float(i * 5 / 3),
            "f_tsms": datetime.now() + timedelta(milliseconds=i),
            "f_tsus": datetime.now() + timedelta(microseconds=i),
            "f_tms": (datetime.now() + timedelta(milliseconds=i)).time(),
            "f_tus": (datetime.now() + timedelta(microseconds=i)).time(),
            "f_date": (datetime.now() + timedelta(days=i)).date(),
            "f_decimal": round(Decimal(i * 8 / 5), 2),
        }
        for i in range(20)
    )
    # for r in iter:
    #     print(r)
    writer(out, parsed_schema, iter, 'snappy')
