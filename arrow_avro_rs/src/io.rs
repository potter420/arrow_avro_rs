use crate::deserializer::{parse_schema, read_avro_value};
use crate::errors::Error;
use arrow2::{
    array::StructArray,
    datatypes::{DataType, Field},
    record_batch::RecordBatch,
};
use avro_rs::{types::Value, Reader, Schema};
use std::io::Read;

pub fn read_into_struct(input: impl Read + Send) -> Result<StructArray, Error> {
    let reader = Reader::new(input)?;
    let schema = reader.writer_schema();

    let data_type = DataType::Struct(match *schema {
        Schema::Record { ref fields, .. } => fields
            .iter()
            .map(|rf| {
                let (dtype, nullable) = parse_schema(&rf.schema, Some(false));
                Field::new(&rf.name, dtype, nullable)
            })
            .collect(),
        _ => todo!(),
    });
    let fields = StructArray::get_fields(&data_type);
    let mut values = fields
        .iter()
        .enumerate()
        .map(|(i, f)| (i, f.data_type(), Vec::<Value>::new()))
        .collect::<Vec<_>>();
    reader.for_each(|row| {
        if let Ok(Value::Record(ref rf)) = row {
            values.iter_mut().for_each(|(i, _, inner)| {
                let (_, value) = &rf[*i];
                inner.reserve_exact(1);
                inner.push(value.clone());
            });
        }
    });
    let values = values
        .into_iter()
        .map(|(_, data_type, value)| read_avro_value(&value, data_type))
        .collect::<Vec<_>>();
    Ok(StructArray::from_data(fields.to_vec(), values, None))
}

pub fn read_into_batch(input: impl Read + Send) -> Result<RecordBatch, Error> {
    let s = read_into_struct(input)?;
    Ok(RecordBatch::from(&s))
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow2::io::print;

    #[test]
    fn test_read_avro() {
        let f = std::fs::File::open("../examples/test_type.avro").unwrap();
        match read_into_batch(f) {
            Ok(rb) => {
                let e = print::print(&[rb]).map(|_| "Ok");
                println!("{:?}", e);
                assert!(true)
            }
            Err(e) => {
                println!("{:?}", e);
                assert!(false)
            }
        };
    }
}
