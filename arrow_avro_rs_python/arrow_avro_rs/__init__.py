from arrow_avro_rs.arrow_avro_rs import read_avro_struct
import pyarrow as pa


def read_avro(file: str):
    """
    Read a file and return pyarrow table

    Parameters
    ----------
    file
        file path
    """
    array = read_avro_struct(file)
    return pa.Table.from_batches([pa.RecordBatch.from_struct_array(array)])
