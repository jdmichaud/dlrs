// https://serde.rs/impl-deserializer.html
// TODO: try https://github.com/launchbadge/sqlx/issues/182#issuecomment-1574524634
use std::fmt::{self, Display};

use serde::{de, ser};

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

use serde::{Deserialize, forward_to_deserialize_any};
use serde::de::{
  Deserializer, DeserializeSeed, IntoDeserializer, MapAccess, Visitor,
};


pub struct SqlDeserializer<'de> {
  row: &'de sqlite::Row,
  field_name: String,
  column_names: &'de [String],
}

impl<'de> SqlDeserializer<'de> {
  pub fn from_row(row: &'de sqlite::Row, column_names: &'de [String]) -> Self {
    SqlDeserializer { row, field_name: "".to_string(), column_names }
  }
}

pub fn from_row<'a, T>(row: &'a sqlite::Row, column_names: &'a [String]) -> Result<T>
where
  T: Deserialize<'a>,
{
  let mut deserializer = SqlDeserializer::from_row(row, column_names);
  Ok(T::deserialize(&mut deserializer)?)
}

// impl<'de> SqlDeserializer<'de> {
// }

impl<'de, 'a> de::Deserializer<'de> for &'a mut SqlDeserializer<'de> {
  type Error = Error;


  // To string by default
  fn deserialize_any<V>(self, visitor: V) -> Result<V::Value>
  where
    V: Visitor<'de>,
  {
    self.deserialize_str(visitor)
  }

  fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value>
  where
    V: Visitor<'de>,
  {
    visitor.visit_bool(self.row.read::<&str, _>(self.field_name.as_str()) == "true")
  }

  // The `parse_signed` function is generic over the integer type `T` so here
  // it is invoked with `T=i8`. The next 8 methods are similar.
  fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value>
  where
    V: Visitor<'de>,
  {
    let value = self.row.read::<i64, _>(self.field_name.as_str()).try_into()
      .map_err(|_| Error::ExpectedInteger);
    visitor.visit_i8(value?)
  }

  fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value>
  where
    V: Visitor<'de>,
  {
    let value = self.row.read::<i64, _>(self.field_name.as_str()).try_into()
      .map_err(|_| Error::ExpectedInteger);
    visitor.visit_i16(value?)
  }

  fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value>
  where
    V: Visitor<'de>,
  {
    let value = self.row.read::<i64, _>(self.field_name.as_str()).try_into()
      .map_err(|_| Error::ExpectedInteger);
    visitor.visit_i32(value?)
  }

  fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value>
  where
    V: Visitor<'de>,
  {
    visitor.visit_i64(self.row.read::<i64, _>(self.field_name.as_str()))
  }

  fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value>
  where
    V: Visitor<'de>,
  {
    let value = self.row.try_read::<i64, _>(self.field_name.as_str())
      .map_err(|_| Error::ExpectedInteger);
    println!("deserialize_u8 {:?}", value);
      // .try_into()
      // .map_err(|_| Error::ExpectedInteger);
    println!("deserialize_u8 {:?}", value);
    visitor.visit_u8(value?.try_into().map_err(|_| Error::ExpectedInteger)?)
  }

  fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value>
  where
    V: Visitor<'de>,
  {
    let value = self.row.read::<i64, _>(self.field_name.as_str()).try_into()
      .map_err(|_| Error::ExpectedInteger);
    visitor.visit_u16(value?)
  }

  fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value>
  where
    V: Visitor<'de>,
  {
    let value = self.row.read::<i64, _>(self.field_name.as_str()).try_into()
      .map_err(|_| Error::ExpectedInteger);
    visitor.visit_u32(value?)
  }

  fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value>
  where
    V: Visitor<'de>,
  {
    let value = self.row.read::<i64, _>(self.field_name.as_str()).try_into()
      .map_err(|_| Error::ExpectedInteger);
    visitor.visit_u64(value?)
  }

  fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value>
  where
    V: Visitor<'de>,
  {
    let value = self.row.read::<f64, _>(self.field_name.as_str());
    visitor.visit_f32(value as f32)
  }

  fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value>
  where
    V: Visitor<'de>,
  {
    visitor.visit_f64(self.row.read::<f64, _>(self.field_name.as_str()))
  }

  // The `Serializer` implementation on the previous page serialized chars as
  // single-character strings so handle that representation here.
  fn deserialize_char<V>(self, _visitor: V) -> Result<V::Value>
  where
    V: Visitor<'de>,
  {
    // Parse a string, check that it is one character, call `visit_char`.
    unimplemented!()
  }

  // Refer to the "Understanding deserializer lifetimes" page for information
  // about the three deserialization flavors of strings in Serde.
  fn deserialize_str<V>(self, visitor: V) -> Result<V::Value>
  where
    V: Visitor<'de>,
  {
    println!("self.field_name {} self.row {:?}", self.field_name, self.row);
    let value = self.row.read::<&str, _>(self.field_name.as_str());
    visitor.visit_borrowed_str(&value)
  }

  fn deserialize_string<V>(self, visitor: V) -> Result<V::Value>
  where
    V: Visitor<'de>,
  {
    self.deserialize_str(visitor)
  }

  // The `Serializer` implementation on the previous page serialized byte
  // arrays as JSON arrays of bytes. Handle that representation here.
  fn deserialize_bytes<V>(self, _visitor: V) -> Result<V::Value>
  where
    V: Visitor<'de>,
  {
    unimplemented!()
  }

  fn deserialize_byte_buf<V>(self, _visitor: V) -> Result<V::Value>
  where
    V: Visitor<'de>,
  {
    unimplemented!()
  }

  // An absent optional is represented as the JSON `null` and a present
  // optional is represented as just the contained value.
  //
  // As commented in `Serializer` implementation, this is a lossy
  // representation. For example the values `Some(())` and `None` both
  // serialize as just `null`. Unfortunately this is typically what people
  // expect when working with JSON. Other formats are encouraged to behave
  // more intelligently if possible.
  fn deserialize_option<V>(self, visitor: V) -> Result<V::Value>
  where
    V: Visitor<'de>,
  {
    if self.row.read::<&str, _>(self.field_name.as_str()) != "NULL" {
      return visitor.visit_some(self);
    }
    visitor.visit_none()
  }

  // In Serde, unit means an anonymous value containing no data.
  fn deserialize_unit<V>(self, _visitor: V) -> Result<V::Value>
  where
    V: Visitor<'de>,
  {
    unimplemented!()
    // if let Ok(value) = self.row.read::<String, _>(self.field_name.as_str()) {
    //   if value == "NULL" {
    //     return visitor.visit_some(self);
    //   }
    // }
    // Err(Error::ExpectedNull)
  }

  // Unit struct means a named value containing no data.
  fn deserialize_unit_struct<V>(
    self,
    _name: &'static str,
    visitor: V,
  ) -> Result<V::Value>
  where
    V: Visitor<'de>,
  {
    self.deserialize_unit(visitor)
  }

  // As is done here, serializers are encouraged to treat newtype structs as
  // insignificant wrappers around the data they contain. That means not
  // parsing anything other than the contained value.
  fn deserialize_newtype_struct<V>(
    self,
    _name: &'static str,
    visitor: V,
  ) -> Result<V::Value>
  where
    V: Visitor<'de>,
  {
    visitor.visit_newtype_struct(self)
  }

  // Deserialization of compound types like sequences and maps happens by
  // passing the visitor an "Access" object that gives it the ability to
  // iterate through the data contained in the sequence.
  fn deserialize_seq<V>(self, _visitor: V) -> Result<V::Value>
  where
    V: Visitor<'de>,
  {
    unimplemented!()
    // Err(Error::ExpectedArray)
  }

  // Tuples look just like sequences in JSON. Some formats may be able to
  // represent tuples more efficiently.
  //
  // As indicated by the length parameter, the `Deserialize` implementation
  // for a tuple in the Serde data model is required to know the length of the
  // tuple before even looking at the input data.
  fn deserialize_tuple<V>(self, _len: usize, visitor: V) -> Result<V::Value>
  where
    V: Visitor<'de>,
  {
    self.deserialize_seq(visitor)
  }

  // Tuple structs look just like sequences in JSON.
  fn deserialize_tuple_struct<V>(
    self,
    _name: &'static str,
    _len: usize,
    visitor: V,
  ) -> Result<V::Value>
  where
    V: Visitor<'de>,
  {
    self.deserialize_seq(visitor)
  }

  // Much like `deserialize_seq` but calls the visitors `visit_map` method
  // with a `MapAccess` implementation, rather than the visitor's `visit_seq`
  // method with a `SeqAccess` implementation.
  fn deserialize_map<V>(self, visitor: V) -> Result<V::Value>
  where
    V: Visitor<'de>,
  {
    Ok(visitor.visit_map(RowExtractor::new(self))?)
  }

  // Structs look just like maps in JSON.
  //
  // Notice the `fields` parameter - a "struct" in the Serde data model means
  // that the `Deserialize` implementation is required to know what the fields
  // are before even looking at the input data. Any key-value pairing in which
  // the fields cannot be known ahead of time is probably a map.
  fn deserialize_struct<V>(
    self,
    _name: &'static str,
    _fields: &'static [&'static str],
    visitor: V,
  ) -> Result<V::Value>
  where
    V: Visitor<'de>,
  {
    self.deserialize_map(visitor)
  }

  fn deserialize_enum<V>(
    self,
    _name: &'static str,
    _variants: &'static [&'static str],
    visitor: V,
  ) -> Result<V::Value>
  where
    V: Visitor<'de>,
  {
    let value = u32::from_str_radix(self.row.read::<&str, _>(self.field_name.as_str()), 10)
      .map_err(|_| Error::ExpectedEnum)?;
    println!("deserialize_enum {}", value);
    visitor.visit_enum(value.into_deserializer())
    // visitor.visit_enum(self.row.read::<&str, _>(self.field_name.as_str())
    //   .into_deserializer())
  }

  // An identifier in Serde is the type that identifies a field of a struct or
  // the variant of an enum. In JSON, struct fields and enum variants are
  // represented as strings. In other formats they may be represented as
  // numeric indices.
  fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value>
  where
    V: Visitor<'de>,
  {
    self.deserialize_str(visitor)
  }

  // Like `deserialize_any` but indicates to the `Deserializer` that it makes
  // no difference which `Visitor` method is called because the data is
  // ignored.
  //
  // Some deserializers are able to implement this more efficiently than
  // `deserialize_any`, for example by rapidly skipping over matched
  // delimiters without paying close attention to the data in between.
  //
  // Some formats are not able to implement this at all. Formats that can
  // implement `deserialize_any` and `deserialize_ignored_any` are known as
  // self-describing.
  fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value>
  where
    V: Visitor<'de>,
  {
    println!("{}", std::any::type_name::<V>());
    self.deserialize_any(visitor)
  }
}

struct RowExtractor<'a, 'de: 'a> {
  de: &'a mut SqlDeserializer<'de>,
  column_index: usize,
}

impl<'a, 'de> RowExtractor<'a, 'de> {
  fn new(de: &'a mut SqlDeserializer<'de>) -> Self {
    RowExtractor {
      de,
      column_index: 0,
    }
  }
}

struct Key {
  key: String,
}

impl<'de> Deserializer<'de> for Key {
    type Error = Error;

    fn deserialize_identifier<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_str(&self.key)
    }

    fn deserialize_any<V>(self, _visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::Syntax)
    }

    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str string bytes
            byte_buf option unit unit_struct newtype_struct seq tuple
            tuple_struct map struct enum ignored_any
    }
}

// \"some_key\" -> \"@SomeKey\"
fn to_pascal_case(s: &str) -> String {
  let slice = s.as_bytes();
  let mut result = String::from("@");
  let mut i = 0;
  while i < slice.len() {
    match slice[i] as char {
      '_' if i < slice.len() - 1 => {
        i += 1;
        result.push((slice[i] as char).to_ascii_uppercase());
      },
      _ if i == 0 => result.push((slice[i] as char).to_ascii_uppercase()),
      _ => result.push(slice[i] as char),
    }
    i += 1;
  }
  result
}

impl<'de, 'a> MapAccess<'de> for RowExtractor<'a, 'de> {
  type Error = Error;

  fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>>
  where
    K: DeserializeSeed<'de>,
  {
    if self.column_index >= self.de.column_names.len() {
      return Ok(None);
    }
    self.de.field_name = to_pascal_case(&self.de.column_names[self.column_index]);
    self.column_index += 1;
    println!("key {:?}", self.de.field_name);
    Ok(Some(seed.deserialize(Key { key: self.de.field_name.clone() })?))
  }

  fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value>
  where
    V: DeserializeSeed<'de>,
  {
    println!("next_value_seed {}", std::any::type_name::<V>());
    // Deserialize a map value.
    seed.deserialize(&mut *self.de)
  }
}

////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_struct() {
  use serde_repr::Deserialize_repr;
  use serde::Serialize;

  #[derive(Debug, Deserialize_repr, Serialize)]
  #[repr(u8)]
  #[derive(PartialEq)]
  enum TestEnum {
    Foo = 1,
    Bar = 2,
  }

  #[derive(Deserialize, PartialEq, Debug)]
  struct Test {
    int: u32,
    name: String,
    test_enum: TestEnum,
  }

  let expected = Test {
    int: 1,
    name: String::from("Jules"),
    test_enum: TestEnum::Bar,
  };

  let connection = sqlite::Connection::open(":memory:").unwrap();
  connection.execute("CREATE TABLE test (int INTEGER NOT NULL, name TEXT NOT NULL, test_enum TEXT);").unwrap();
  connection.execute("INSERT INTO test VALUES (1, \"Jules\", \"2\");").unwrap();
  let mut stmt = connection.prepare("SELECT * FROM test").unwrap();
  if let Some(Ok(row)) = stmt.iter().next() {
    let test: Test = from_row(&row, stmt.column_names()).unwrap();
    assert_eq!(expected, test);
  } else {
    panic!("test fail");
  }
}

// #[test]
// fn test_enum() {
//   #[derive(Deserialize, PartialEq, Debug)]
//   enum E {
//     Unit,
//     Newtype(u32),
//     Tuple(u32, u32),
//     Struct { a: u32 },
//   }

//   let j = r#""Unit""#;
//   let expected = E::Unit;
//   assert_eq!(expected, from_str(j).unwrap());

//   let j = r#"{"Newtype":1}"#;
//   let expected = E::Newtype(1);
//   assert_eq!(expected, from_str(j).unwrap());

//   let j = r#"{"Tuple":[1,2]}"#;
//   let expected = E::Tuple(1, 2);
//   assert_eq!(expected, from_str(j).unwrap());

//   let j = r#"{"Struct":{"a":1}}"#;
//   let expected = E::Struct { a: 1 };
//   assert_eq!(expected, from_str(j).unwrap());
// }

mod se_struct;

fn main() {
  #[derive(Deserialize, PartialEq, Debug)]
  struct Test {
    int: u32,
    name: String,
  }

  let connection = sqlite::Connection::open("dlrs.db").unwrap();
  let mut stmt = connection.prepare("SELECT * FROM [tor.stackexchange_Post]").unwrap();
  if let Some(Ok(row)) = stmt.iter().next() {
    let post: se_struct::Post = from_row(&row, stmt.column_names()).unwrap();
    println!("{post:?}");
  } else {
    panic!("test fail");
  }
}