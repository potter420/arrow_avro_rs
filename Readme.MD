# Read Arvo into Arrow

This repo intended to have a package that read straight from avro file into arrow Table format

Currently, it only support flattened schema type.

## How it works:

We use avro-rs to read the file, arrow2 to parse them into StructArray.
The array then transfered to python via ffi and then convert into arrow Table


# To do
- Add complex types
- Options for reading big avro files: into chunks rather than single big arrow array
- Parallelization: Either columns parsing should be in parralel or into chunks of struct array?
- Interacting with python `fs` rather than just flat file

## Example

```python
from arrow_avro_rs import read_avro

file = 'some_file.avro'
table = read_avro(file)

```