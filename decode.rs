use anyhow::Result;
use clap::Parser;
use quick_xml::events::Event;
use std::path::PathBuf;
use std::fs::File;
use sqlite::Connection;

mod se_struct;
mod sql_utils;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Config {
  /// XML file
  #[arg(value_name = "FILE")]
  xml_file: PathBuf,
  /// sqlite3 database file
  #[arg(value_name = "FILE")]
  sql_file: PathBuf,
}

fn get_site_from_filepath(filepath: &PathBuf) -> Result<String> {
  let mut filepath = filepath.clone();
  filepath.pop();
  return Ok(filepath.file_stem().ok_or(anyhow::anyhow!("error"))?
    .to_string_lossy().to_string());
}

fn main() -> Result<()> {
  let config = Config::parse();

  let connection = Connection::open(&config.sql_file)?;

  let f = File::open(&config.xml_file)?;
  let table_name = get_site_from_filepath(&config.xml_file)?;
  println!("table_name {}",table_name);
  let bufreader = std::io::BufReader::new(f);
  let mut reader = quick_xml::Reader::from_reader(bufreader);

  let mut count = 0;
  let mut insert_statement = connection.prepare("")?;
  connection.execute("BEGIN TRANSACTION;")?;
  loop {
    let mut buf = Vec::new();
    match reader.read_event_into(&mut buf) {
      Err(e) => panic!(
        "Error at position {}: {:?}",
        reader.buffer_position(),
        e
      ),
      Ok(Event::Eof) => break,
      Ok(Event::Empty(e)) => {
        let s = format!("<{}/>", std::str::from_utf8(&e)?);
        println!("s {}", s);
        let tag: se_struct::Badge = quick_xml::de::from_str(&s)?;
        if count == 0 {
          let (create_stmt, insert_stmt) = sql_utils::to_init_table(&tag, &table_name)?;
          println!("{}", create_stmt);
          println!("{}", insert_stmt);
          connection.execute(create_stmt)?;
          insert_statement = connection.prepare(insert_stmt)?;
        }
        insert_statement.reset()?;
        let bindings = sql_utils::bind_stmt(&tag)?;
        for (index, value) in bindings.iter().enumerate() {
          insert_statement.bind((index + 1, value.as_str()))?;
        }
        insert_statement.next()?;
        count += 1;
        if count % 1_000_000 == 0 {
          println!("{}", count);
        }
      },
      _ => (),
    }
  }

  connection.execute("END TRANSACTION;")?;
  println!("{} entries.", count);
  Ok(())
}
