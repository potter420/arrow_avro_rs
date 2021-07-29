use std::{convert::TryInto, sync::Arc};

use arrow2::{
    array::{Array, BooleanArray, NullArray, Offset, PrimitiveArray, Utf8Array},
    datatypes::{DataType, IntervalUnit, TimeUnit},
    types::{NativeType, NaturalDataType},
};
use avro_rs::{types::Value, Schema};
use num::{BigInt, NumCast, ToPrimitive};

fn read_utf8<O: Offset>(rows: &[Value]) -> Utf8Array<O> {
    let iter = rows.iter().map(|row| match row {
        Value::String(v) => Some(v.clone()),
        Value::Union(v) => match &**v {
            Value::String(h) => Some(h.clone()),
            _ => None,
        },
        _ => None,
    });
    Utf8Array::<O>::from_trusted_len_iter(iter)
}
fn read_boolean(rows: &[Value]) -> BooleanArray {
    let iter = rows.iter().map(|row| match row {
        Value::Boolean(v) => Some(v),
        _ => None,
    });
    BooleanArray::from_trusted_len_iter(iter)
}

fn read_primitive<T: NativeType + NaturalDataType + NumCast>(
    rows: &[Value],
    data_type: DataType,
) -> PrimitiveArray<T> {
    let f = |v: &Value| -> Option<T> {
        match v {
            Value::Double(number) => num::cast::cast::<f64, T>(*number),
            Value::Long(number) => num::cast::cast::<i64, T>(*number),
            Value::Float(number) => num::cast::cast::<f32, T>(*number),
            Value::Boolean(number) => num::cast::cast::<i32, T>(*number as i32),
            Value::Int(number) => num::cast::cast::<i32, T>(*number),
            Value::Decimal(number) => {
                let buf: Vec<u8> = match number.try_into() {
                    Ok(v) => v,
                    Err(_) => vec![],
                };
                let e = BigInt::from_signed_bytes_be(&buf);
                match e.to_i128() {
                    Some(v) => num::cast::cast::<i128, T>(v),
                    None => None,
                }
            }
            Value::TimestampMillis(number) => num::cast::<i64, T>(*number),
            Value::TimestampMicros(number) => num::cast::<i64, T>(*number),
            Value::TimeMicros(number) => num::cast::<i64, T>(*number),
            Value::TimeMillis(number) => num::cast::<i32, T>(*number),
            Value::Date(number) => num::cast::<i32, T>(*number),
            _ => None,
        }
    };
    let iter = rows.iter().map(|row| -> Option<T> {
        match row {
            Value::Double(_)
            | Value::Long(_)
            | Value::Float(_)
            | Value::Boolean(_)
            | Value::Int(_)
            | Value::Decimal(_)
            | Value::TimestampMillis(_)
            | Value::TimestampMicros(_)
            | Value::TimeMicros(_)
            | Value::TimeMillis(_)
            | Value::Date(_) => f(row),
            Value::Union(v) => match **v {
                Value::Double(_)
                | Value::Long(_)
                | Value::Float(_)
                | Value::Boolean(_)
                | Value::Int(_)
                | Value::Decimal(_)
                | Value::TimestampMillis(_)
                | Value::TimestampMicros(_)
                | Value::TimeMicros(_)
                | Value::TimeMillis(_)
                | Value::Date(_) => f(&*v),
                Value::Null => None,
                Value::Union(_) => {
                    println!("Still boxed for some reason, Send HELP");
                    None
                }
                _ => None,
            },
            _ => None,
        }
    });
    PrimitiveArray::from_trusted_len_iter(iter).to(data_type)
}

pub(crate) fn read_avro_value(rows: &[Value], data_type: &DataType) -> Arc<dyn Array> {
    match &data_type {
        DataType::Null => Arc::new(NullArray::from_data(rows.len())),
        DataType::Utf8 => Arc::new(read_utf8::<i32>(rows)),
        DataType::Boolean => Arc::new(read_boolean(rows)),
        // DataType::Int8 => todo!(), Not implemented
        // DataType::Int16 => todo!(),
        DataType::Int32
        | DataType::Date32
        | DataType::Time32(_)
        | DataType::Interval(IntervalUnit::YearMonth) => {
            Arc::new(read_primitive::<i32>(rows, data_type.clone()))
        }
        DataType::Int64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_) => Arc::new(read_primitive::<i64>(rows, data_type.clone())),
        // DataType::UInt8 => todo!(),
        // DataType::UInt16 => todo!(),
        // DataType::UInt32 => todo!(),
        // DataType::UInt64 => todo!(),
        // DataType::Float16 => todo!(),
        DataType::Float32 => Arc::new(read_primitive::<f32>(rows, data_type.clone())),
        DataType::Float64 => Arc::new(read_primitive::<f64>(rows, data_type.clone())),
        // DataType::Interval(_) => todo!(),
        // DataType::Binary => todo!(),
        // DataType::FixedSizeBinary(_) => todo!(),
        // DataType::LargeBinary => todo!(),
        DataType::LargeUtf8 => Arc::new(read_utf8::<i64>(rows)),
        // DataType::List(_) => todo!(),
        // DataType::FixedSizeList(_, _) => todo!(),
        // DataType::LargeList(_) => todo!(),
        // DataType::Struct(_) => todo!(),
        // DataType::Union(_) => todo!(),
        // DataType::Dictionary(_, _) => todo!(),
        DataType::Decimal(_, _) => Arc::new(read_primitive::<i128>(rows, data_type.clone())),
        _ => unimplemented!(),
    }
}

pub(crate) fn parse_schema(schema: &Schema, nullable: Option<bool>) -> (DataType, bool) {
    match schema {
        Schema::Union(ref uni) => {
            let nullable = Some(uni.is_nullable());
            let inner_schema = uni.variants().iter().find(|s| !matches!(s, Schema::Null));
            parse_schema(inner_schema.unwrap_or(&Schema::Null), nullable)
        }
        Schema::Null => (DataType::Null, nullable.unwrap_or(true)),
        Schema::Boolean => (DataType::Boolean, nullable.unwrap_or(true)),
        Schema::Int => (DataType::Int32, nullable.unwrap_or(true)),
        Schema::Long => (DataType::Int64, nullable.unwrap_or(true)),
        Schema::Float => (DataType::Float32, nullable.unwrap_or(true)),
        Schema::Double => (DataType::Float64, nullable.unwrap_or(true)),
        Schema::Bytes => (DataType::Binary, nullable.unwrap_or(true)),
        Schema::String => (DataType::LargeUtf8, nullable.unwrap_or(true)),
        Schema::Array(_) => todo!(),
        Schema::Map(_) => todo!(),
        Schema::Record {
            // name,
            // doc,
            // fields,
            // lookup,
            ..
        } => todo!(),
        Schema::Enum {
            // name, doc, symbols 
            ..
        } => todo!(),
        Schema::Fixed {
            // name, size 
            ..
        } => todo!(),
        Schema::Decimal {
            precision,
            scale,
            ..
        } => (
            DataType::Decimal(*precision, *scale),
            // DataType::Int64,
            nullable.unwrap_or(true),
        ),
        Schema::Uuid => (DataType::Utf8, nullable.unwrap_or(true)),
        Schema::Date => (DataType::Date32, nullable.unwrap_or(true)),
        Schema::TimeMillis => (
            DataType::Time32(TimeUnit::Millisecond),
            nullable.unwrap_or(true),
        ),
        Schema::TimeMicros => (
            DataType::Time64(TimeUnit::Microsecond),
            nullable.unwrap_or(true),
        ),
        Schema::TimestampMillis => (
            DataType::Timestamp(TimeUnit::Millisecond, None),
            // DataType::Date64,
            nullable.unwrap_or(true),
        ),
        Schema::TimestampMicros => (
            DataType::Timestamp(TimeUnit::Microsecond, None),
            nullable.unwrap_or(true),
        ),
        Schema::Duration => (
            DataType::Duration(TimeUnit::Microsecond),
            nullable.unwrap_or(true),
        ),
    }
}
