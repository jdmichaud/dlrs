/**
 * This is a quick and dirty reimplementation of the example of a
 * [Serializer](https://serde.rs/impl-serializer.html) and
 * [error handling](https://serde.rs/error-handling.html) from the Serde documentation.
 * There is 2 serializers here:
 * - one that generates a CREATE TABLE request and an INSERT request statement and
 * - one that binds the insert statement with values from the structure.
 * for any "serde" serializable structure.
 * This should be refactored and simplified.
 */

use serde::{de, ser, Serialize};

/******************************************************************************/
/********************************** error *************************************/
/******************************************************************************/
use std;
use std::fmt::{self, Display};

// use serde::{de, ser};

pub type Result<T> = std::result::Result<T, Error>;

// This is a bare-bones implementation. A real library would provide additional
// information in its error type, for example the line and column at which the
// error occurred, the byte offset into the input, or the current key being
// processed.
#[derive(Debug)]
pub enum Error {
    // One or more variants that can be created by data structures through the
    // `ser::Error` and `de::Error` traits. For example the Serialize impl for
    // Mutex<T> might return an error because the mutex is poisoned, or the
    // Deserialize impl for a struct may return an error because a required
    // field is missing.
    Message(String),

    // Zero or more variants that can be created directly by the Serializer and
    // Deserializer without going through `ser::Error` and `de::Error`. These
    // are specific to the format, in this case JSON.
    Eof,
    Syntax,
    ExpectedBoolean,
    ExpectedInteger,
    ExpectedString,
    ExpectedNull,
    ExpectedArray,
    ExpectedArrayComma,
    ExpectedArrayEnd,
    ExpectedMap,
    ExpectedMapColon,
    ExpectedMapComma,
    ExpectedMapEnd,
    ExpectedEnum,
    TrailingCharacters,
}

impl ser::Error for Error {
  fn custom<T: Display>(msg: T) -> Self {
    Error::Message(msg.to_string())
  }
}

impl de::Error for Error {
  fn custom<T: Display>(msg: T) -> Self {
    Error::Message(msg.to_string())
  }
}

impl Display for Error {
  fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
    match self {
      Error::Message(msg) => formatter.write_str(msg),
      Error::Eof => formatter.write_str("unexpected end of input"),
      _ => formatter.write_str("some sort of error"),
    }
  }
}

impl std::error::Error for Error {}
/******************************************************************************/
/********************************** error *************************************/
/******************************************************************************/

// INSERT INTO table_name (column1, column2, column3, ...) VALUES (value1, value2, value3, ...);
// CREATE TABLE IF NOT EXISTS table_name (column1 datatype, column2 datatype, column3 datatype);

#[derive(Clone, Debug, PartialEq)]
pub enum SqlValue {
  INTEGER(i64),
  REAL(f64),
  TEXT(String),
}

pub struct Serializer {
  sql_value: Option<SqlValue>,
  insert_stmt: String,
  create_stmt: String,
  table_name: String,
  keys: Vec<(String, SqlValue)>,
  values: Vec<SqlValue>,
}

// Creates a "create" statement. To be executable once to create the table and
// creates an insert query used to prepare a statement.
// INSERT INTO table VALUE (?, ?, ...)
pub fn to_init_table<T>(value: &T, table_name: &str) -> Result<(String, String)> where T: Serialize {
  let mut serializer = Serializer {
    sql_value: None,
    insert_stmt: String::new(),
    create_stmt: String::new(),
    table_name: table_name.to_string(),
    keys: Vec::new(),
    values: Vec::new(),
  };
  value.serialize(&mut serializer)?;
  Ok((serializer.create_stmt, serializer.insert_stmt))
}

impl<'a> ser::Serializer for &'a mut Serializer {
  type Ok = ();
  type Error = Error;
  type SerializeSeq = ser::Impossible<Self::Ok, Self::Error>;
  type SerializeTuple = ser::Impossible<Self::Ok, Self::Error>;
  type SerializeTupleStruct = ser::Impossible<Self::Ok, Self::Error>;
  type SerializeTupleVariant = ser::Impossible<Self::Ok, Self::Error>;
  type SerializeMap = Self;
  type SerializeStruct = Self;
  type SerializeStructVariant = ser::Impossible<Self::Ok, Self::Error>;

  fn serialize_bool(self, v: bool) -> Result<()> {
    self.sql_value = Some(SqlValue::TEXT(String::from(if v { "true" } else { "false" })));
    Ok(())
  }
  fn serialize_i8(self, v: i8) -> Result<()> { self.serialize_i64(i64::from(v)) }
  fn serialize_i16(self, v: i16) -> Result<()> { self.serialize_i64(i64::from(v)) }
  fn serialize_i32(self, v: i32) -> Result<()> { self.serialize_i64(i64::from(v)) }
  fn serialize_i64(self, v: i64) -> Result<()> {
    self.sql_value = Some(SqlValue::INTEGER(v));
    Ok(())
  }
  fn serialize_u8(self, v: u8) -> Result<()> { self.serialize_u64(u64::from(v)) }
  fn serialize_u16(self, v: u16) -> Result<()> { self.serialize_u64(u64::from(v)) }
  fn serialize_u32(self, v: u32) -> Result<()> { self.serialize_u64(u64::from(v)) }
  fn serialize_u64(self, v: u64) -> Result<()> {
    self.sql_value = Some(SqlValue::INTEGER(v as i64));
    Ok(())
  }
  fn serialize_f32(self, v: f32) -> Result<()> { self.serialize_f64(f64::from(v)) }
  fn serialize_f64(self, v: f64) -> Result<()> {
    self.sql_value = Some(SqlValue::REAL(v));
    Ok(())
  }
  fn serialize_char(self, v: char) -> Result<()> { self.serialize_str(&v.to_string()) }
  fn serialize_str(self, v: &str) -> Result<()> {
    self.sql_value = Some(SqlValue::TEXT(String::from(v)));
    Ok(())
  }
  fn serialize_bytes(self, _v: &[u8]) -> Result<()> { panic!("serialize_bytes not supported") }
  fn serialize_none(self) -> Result<()> {
    self.sql_value = Some(SqlValue::TEXT(String::from("NULL")));
    Ok(())
  }
  fn serialize_some<T>(self, value: &T) -> Result<()>
  where
    T: ?Sized + Serialize,
  {
    value.serialize(self)
  }
  fn serialize_unit(self) -> Result<()> { panic!("serialize_unit not supported") }
  fn serialize_unit_struct(self, _name: &'static str) -> Result<()> { panic!("serialize_unit_struct not supported") }
  fn serialize_unit_variant(
    self,
    _name: &'static str,
    _variant_index: u32,
    variant: &'static str,
  ) -> Result<()> {
    self.serialize_str(variant)
  }
  fn serialize_newtype_struct<T>(
    self,
    _name: &'static str,
    _value: &T,
  ) -> Result<()>
  where
    T: ?Sized + Serialize,
  {
    panic!("serialize_newtype_struct not supported")
  }
  fn serialize_newtype_variant<T>(
    self,
    _name: &'static str,
    _variant_index: u32,
    _variant: &'static str,
    _value: &T,
  ) -> Result<()>
  where
    T: ?Sized + Serialize,
  {
    panic!("serialize_newtype_variant not supported");
  }
  fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq> {
    panic!("serialize_seq not supported");
  }
  fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple> {
    panic!("serialize_tuple not supported");
  }
  fn serialize_tuple_struct(
    self,
    _name: &'static str,
    _len: usize,
  ) -> Result<Self::SerializeTupleStruct> {
    panic!("serialize_tuple_struct not supported");
  }
  fn serialize_tuple_variant(
    self,
    _name: &'static str,
    _variant_index: u32,
    _variant: &'static str,
    _len: usize,
  ) -> Result<Self::SerializeTupleVariant> {
    panic!("serialize_tuple_variant not supported")
  }
  fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
    Ok(self)
  }
  fn serialize_struct(
    self,
    _name: &'static str,
    len: usize,
  ) -> Result<Self::SerializeStruct> {
    self.insert_stmt += "INSERT INTO [";
    self.insert_stmt += &self.table_name;
    self.insert_stmt += "] (";

    self.create_stmt += "CREATE TABLE IF NOT EXISTS [";
    self.create_stmt += &self.table_name;
    self.create_stmt += "] (";
    self.serialize_map(Some(len))
  }

  fn serialize_struct_variant(
    self,
    _name: &'static str,
    _variant_index: u32,
    _variant: &'static str,
    _len: usize,
  ) -> Result<Self::SerializeStructVariant> {
    panic!("serialize_struct_variant not supported")
  }
}

impl<'a> ser::SerializeMap for &'a mut Serializer {
  type Ok = ();
  type Error = Error;

  fn serialize_key<T>(&mut self, _key: &T) -> Result<()>
  where
    T: ?Sized + Serialize,
  {
    Ok(())
  }

  fn serialize_value<T>(&mut self, _value: &T) -> Result<()>
  where
    T: ?Sized + Serialize,
  {
    Ok(())
  }

  fn end(self) -> Result<()> {
    Ok(())
  }
}

// \"@SomeKey\" -> \"some_key\"
fn sanitize_key(key: &str) -> String {
  let mut first = true;
  key.chars()
    // Remove the initial @ of XML structs.
    .filter(|c| *c != '@')
  // Convert PascalCase to snake_case
    .map(|c| if c.is_ascii_uppercase() {
      if first {
        first = false;
        format!("{}", c.to_lowercase())
      } else {
        format!("_{}", c.to_lowercase())
      }
    } else {
      format!("{}", c)
    })
    .collect::<Vec<String>>()
    .join("")
}

// Structs are like maps in which the keys are constrained to be compile-time
// constant strings.
impl<'a> ser::SerializeStruct for &'a mut Serializer {
  type Ok = ();
  type Error = Error;

  fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<()>
  where
    T: ?Sized + Serialize,
  {
    {
      let mut serializer = Serializer {
        sql_value: None,
        table_name: self.table_name.clone(),
        insert_stmt: String::from(""),
        create_stmt: String::from(""),
        keys: Vec::new(),
        values: Vec::new(),
      };
      key.serialize(&mut serializer)?;
      let sql_value = serializer.sql_value.unwrap();
      let column_name = match sql_value.clone() {
        SqlValue::TEXT(v) => v,
        _ => panic!("error"),
      };
      self.keys.push((sanitize_key(&column_name), sql_value));
    }

    {
      let mut serializer = Serializer {
        sql_value: None,
        table_name: self.table_name.clone(),
        insert_stmt: String::from(""),
        create_stmt: String::from(""),
        keys: Vec::new(),
        values: Vec::new(),
      };
      value.serialize(&mut serializer)?;
      self.values.push(serializer.sql_value.unwrap());
    }

    Ok(())
  }

  fn end(self) -> Result<()> {
    self.insert_stmt += &self.keys.iter().map(|(column_name, _)| column_name.clone()).collect::<Vec<String>>().join(",");
    self.create_stmt += &self.keys.iter().zip(self.values.iter()).map(|((column_name, _), sql_type)| {
      format!("{} {}", column_name, if column_name == "id" {
        "INTEGER PRIMARY KEY UNIQUE"
      } else {
        match sql_type {
          SqlValue::TEXT(_) => "TEXT",
          SqlValue::INTEGER(_) => "INTEGER",
          SqlValue::REAL(_) => "REAL",
        }
      })
    }).collect::<Vec<String>>().join(",");
    self.insert_stmt += ") VALUES (";
    // self.insert_stmt += &self.values.join(",");
    self.insert_stmt += &vec!["?"; self.values.len()].join(",");
    self.insert_stmt += ");";
    self.create_stmt += ");";
    Ok(())
  }
}

pub struct Binder {
  output: Vec<String>,
}

// Binds an INSERT statement to values
pub fn bind_stmt<T>(value: &T) -> Result<Vec<String>> where T: Serialize {
  let mut binder = Binder {
    output: Vec::new(),
  };
  value.serialize(&mut binder)?;
  Ok(binder.output)
}

impl<'a> ser::Serializer for &'a mut Binder {
  type Ok = ();
  type Error = Error;
  type SerializeSeq = ser::Impossible<Self::Ok, Self::Error>;
  type SerializeTuple = ser::Impossible<Self::Ok, Self::Error>;
  type SerializeTupleStruct = ser::Impossible<Self::Ok, Self::Error>;
  type SerializeTupleVariant = ser::Impossible<Self::Ok, Self::Error>;
  type SerializeMap = Self;
  type SerializeStruct = Self;
  type SerializeStructVariant = ser::Impossible<Self::Ok, Self::Error>;

  fn serialize_bool(self, v: bool) -> Result<()> {
    self.output.push((if v { "true" } else { "false" }).into());
    Ok(())
  }
  fn serialize_i8(self, v: i8) -> Result<()> { self.serialize_i64(i64::from(v)) }
  fn serialize_i16(self, v: i16) -> Result<()> { self.serialize_i64(i64::from(v)) }
  fn serialize_i32(self, v: i32) -> Result<()> { self.serialize_i64(i64::from(v)) }
  fn serialize_i64(self, v: i64) -> Result<()> { self.output.push(v.to_string()); Ok(()) }
  fn serialize_u8(self, v: u8) -> Result<()> { self.serialize_u64(u64::from(v)) }
  fn serialize_u16(self, v: u16) -> Result<()> { self.serialize_u64(u64::from(v)) }
  fn serialize_u32(self, v: u32) -> Result<()> { self.serialize_u64(u64::from(v)) }
  fn serialize_u64(self, v: u64) -> Result<()> { self.output.push(v.to_string()); Ok(()) }
  fn serialize_f32(self, v: f32) -> Result<()> { self.serialize_f64(f64::from(v)) }
  fn serialize_f64(self, v: f64) -> Result<()> { self.output.push(v.to_string()); Ok(()) }
  fn serialize_char(self, v: char) -> Result<()> { self.serialize_str(&v.to_string()) }
  fn serialize_str(self, v: &str) -> Result<()> { self.output.push(v.into()); Ok(()) }
  fn serialize_bytes(self, _v: &[u8]) -> Result<()> { panic!("serialize_bytes not supported") }
  fn serialize_none(self) -> Result<()> { self.output.push("NULL".into()); Ok(()) }
  fn serialize_some<T>(self, value: &T) -> Result<()>
  where
    T: ?Sized + Serialize,
  {
    value.serialize(self)
  }
  fn serialize_unit(self) -> Result<()> { panic!("serialize_unit not supported") }
  fn serialize_unit_struct(self, _name: &'static str) -> Result<()> { panic!("serialize_unit_struct not supported") }
  fn serialize_unit_variant(
    self,
    _name: &'static str,
    _variant_index: u32,
    variant: &'static str,
  ) -> Result<()> {
    self.serialize_str(variant)
  }
  fn serialize_newtype_struct<T>(
    self,
    _name: &'static str,
    _value: &T,
  ) -> Result<()>
  where
    T: ?Sized + Serialize,
  {
    panic!("serialize_newtype_struct not supported")
  }
  fn serialize_newtype_variant<T>(
    self,
    _name: &'static str,
    _variant_index: u32,
    _variant: &'static str,
    _value: &T,
  ) -> Result<()>
  where
    T: ?Sized + Serialize,
  {
    panic!("serialize_newtype_variant not supported");
  }
  fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq> {
    panic!("serialize_seq not supported");
  }
  fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple> {
    panic!("serialize_tuple not supported");
  }
  fn serialize_tuple_struct(
    self,
    _name: &'static str,
    _len: usize,
  ) -> Result<Self::SerializeTupleStruct> {
    panic!("serialize_tuple_struct not supported");
  }
  fn serialize_tuple_variant(
    self,
    _name: &'static str,
    _variant_index: u32,
    _variant: &'static str,
    _len: usize,
  ) -> Result<Self::SerializeTupleVariant> {
    panic!("serialize_tuple_variant not supported")
  }
  fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
    Ok(self)
  }
  fn serialize_struct(
    self,
    _name: &'static str,
    len: usize,
  ) -> Result<Self::SerializeStruct> {
    self.serialize_map(Some(len))
  }

  fn serialize_struct_variant(
    self,
    _name: &'static str,
    _variant_index: u32,
    _variant: &'static str,
    _len: usize,
  ) -> Result<Self::SerializeStructVariant> {
    panic!("serialize_struct_variant not supported")
  }
}

impl<'a> ser::SerializeMap for &'a mut Binder {
  type Ok = ();
  type Error = Error;

  fn serialize_key<T>(&mut self, _key: &T) -> Result<()>
  where
    T: ?Sized + Serialize,
  {
    Ok(())
  }

  fn serialize_value<T>(&mut self, _value: &T) -> Result<()>
  where
    T: ?Sized + Serialize,
  {
    Ok(())
  }

  fn end(self) -> Result<()> {
    Ok(())
  }
}

// Structs are like maps in which the keys are constrained to be compile-time
// constant strings.
impl<'a> ser::SerializeStruct for &'a mut Binder {
  type Ok = ();
  type Error = Error;

  fn serialize_field<T>(&mut self, _key: &'static str, value: &T) -> Result<()>
  where
    T: ?Sized + Serialize,
  {
    let mut binder = Binder {
      output: Vec::new(),
    };
    value.serialize(&mut binder)?;
    self.output.append(&mut binder.output);
    Ok(())
  }

  fn end(self) -> Result<()> {
    Ok(())
  }
}
