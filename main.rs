#![allow(dead_code)]
#![feature(core_intrinsics)] // for breakpoint
#![feature(let_chains)] // for macro

use clap::Parser;
use core::convert::Infallible;
use error_chain::error_chain;
use futures::StreamExt;
use quick_xml::events::Event;
use sevenz_rust;
use std::fs::File;
use std::io::{BufRead, stdout, Write};
use std::sync::{Arc, Mutex};
use std::path::{Path, PathBuf};
use sqlite::Connection;
use tokio;

mod se_struct;
mod sql_utils;

#[derive(Parser, Clone)]
#[command(author, version, about, long_about = None)]
struct Config {
  /// Where to store Stack Exchange files (zipped and unzipped)
  #[arg(short='f', long, default_value=PathBuf::from("./data").into_os_string(), value_name = "PATH")]
  data_path: PathBuf,
  /// List of files/urls to download
  #[arg(short, long, /*default_value=PathBuf::from("site.list").into_os_string(), */value_name = "FILE")]
  site_list: Option<PathBuf>,
  /// database file
  #[arg(short, long, default_value=PathBuf::from("dlrs.db").into_os_string(), value_name = "FILE")]
  database_filename: PathBuf,
  /// Maximum number of parallel threads to use (including max parallel download)
  #[arg(short, long, default_value_t=3)]
  max_threads: u8,
}

error_chain! {
  foreign_links {
    Io(std::io::Error);
    Reqwest(reqwest::Error);
    Header(reqwest::header::ToStrError);
    Parser(quick_xml::Error);
    Deserializer(quick_xml::DeError);
    Decompress(sevenz_rust::Error);
    TryFromIntError(core::num::TryFromIntError);
    Infallible(Infallible);
    SystemTimeError(std::time::SystemTimeError);
    SqliteError(sqlite::Error);
    SqlUtilsError(sql_utils::Error);
    Utf8Error(std::str::Utf8Error);
  }
}

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
enum State {
  Error(String),
  Wait,
  Downloading((u64, u64)),
  Unzipping(u8),
  Parsing((u8, String)),
  Done,
}

#[derive(Debug, Clone)]
struct Job {
  url: String,
  filepath: String,
  state: State,
}

fn update_display(jobs: &Vec<Job>) -> Result<()> {
  if jobs.len() == 0 {
    return Ok(())
  }

  // This function has no state so we have to recompute a bunch of things every time.
  let max_filename_length = jobs.iter().map(|job| job.filepath.len()).fold(std::i32::MIN, |a,b| a.max(b as i32));
  let terminal_size = crossterm::terminal::size()?;
  let expected_progress_bar_width =
    (terminal_size.0 as usize).saturating_sub(max_filename_length as usize).saturating_sub(30 as usize);
  let progress_bar_width: usize = std::cmp::min(expected_progress_bar_width, 50);
  let mut current_jobs = jobs.iter()
    .filter(|j| j.state != State::Wait && j.state != State::Done) // Do not display waiting and done jobs
    .take(terminal_size.1 as usize - 1) // Do not display more than the size of the terminal
    .collect::<Vec<_>>();
  current_jobs.sort_by(|a, b| {
    match (a.state.clone(), b.state.clone()) {
      (State::Downloading((adownloaded, atotal)), State::Downloading((bdownloaded, btotal))) => {
        let avalue = (adownloaded as f32 / atotal as f32 * 100.0) as u8;
        let bvalue = (bdownloaded as f32 / btotal as f32 * 100.0) as u8;
        bvalue.cmp(&avalue)
      },
      (State::Unzipping(avalue), State::Unzipping(bvalue)) => bvalue.cmp(&avalue),
      (State::Parsing((avalue, _)), State::Parsing((bvalue, _))) => bvalue.cmp(&avalue),
      _ => b.state.cmp(&a.state),
    }
  });
  let done_jobs = jobs.iter().filter(|j| j.state != State::Done).collect::<Vec<_>>();

  println!("Files to be processed: {} done / {} total", jobs.len() - done_jobs.len(), jobs.len());
  if progress_bar_width > 3 {
    for (index, job) in current_jobs.iter().enumerate() {
      crossterm::execute!(stdout(), crossterm::terminal::Clear(crossterm::terminal::ClearType::CurrentLine))?;
      let filename = PathBuf::from(job.filepath.clone()).file_name().unwrap().to_str().unwrap().to_string();
      print!("{:width$} ", filename, width = max_filename_length as usize);
      match job.state.clone() {
        State::Wait => print!("{:width$}waiting", "", width = progress_bar_width),
        State::Downloading((downloaded, total)) => {
          let progress = (downloaded as f32 / total as f32 * 100.0) as u8;
          let nbhash = ((progress_bar_width) as f32 * progress as f32 / 100.0) as u8;
          // ⎯
          let progress_bar = (0..nbhash).map(|_| "━").collect::<String>();
          print!("[{:width$}] downloading {}% ({}/{})", progress_bar, progress, downloaded, total,
            width = progress_bar_width);
        },
        State::Unzipping(progress) => {
          let nbhash = ((progress_bar_width) as f32 * progress as f32 / 100.0) as u8;
          let progress_bar = (0..nbhash).map(|_| "■").collect::<String>();
          print!("[{:━<width$}] unzipping {}%", progress_bar, progress, width = progress_bar_width);
        },
        State::Parsing((progress, filename)) => {
          let nbhash = ((progress_bar_width) as f32 * progress as f32 / 100.0) as u8;
          let progress_bar = (0..nbhash).map(|_| "█").collect::<String>();
          print!("[{:■<width$}] parsing {}% ({})", progress_bar, progress, filename, width = progress_bar_width);
        },
        State::Done => {
          let full_progress_bar = (0..progress_bar_width).map(|_| "█").collect::<String>();
          print!("[{:width$}] done.", full_progress_bar, width = progress_bar_width);
        },
        State::Error(label) => {
          print!(" {:width$}  ", " ", width = progress_bar_width);
          let position = crossterm::cursor::position()?;
          let max: usize = (terminal_size.0).saturating_sub(position.0).saturating_sub(1) as usize;
          print!("{}", &label[..std::cmp::min(max, label.len())]);
        },
      }
      if index <= current_jobs.len() - 1 {
        println!("");
      }
    }
  }
  let position = crossterm::cursor::position()?;
  let nb_empty_line = (terminal_size.1).saturating_sub(position.1);
  for index in 0..nb_empty_line {
    crossterm::execute!(stdout(), crossterm::terminal::Clear(crossterm::terminal::ClearType::CurrentLine))?;
    if index < nb_empty_line - 1 {
      println!("");
    }
  }
  let position = crossterm::cursor::position()?;
  let initial_y_position = (position.1 + 1).saturating_sub(nb_empty_line).saturating_sub(current_jobs.len() as u16);
  crossterm::execute!(stdout(), crossterm::cursor::MoveTo(0, initial_y_position.saturating_sub(1)))?;
  Ok(())
}

fn get_data_path(filepath: &Path) -> PathBuf {
  let filestem = filepath.file_stem().unwrap().to_string_lossy().to_string(); // why does std::path uses OsStr!?
  let mut output_path: PathBuf = PathBuf::from(filepath.parent().unwrap());
  output_path.push(filestem);
  output_path
}

async fn download(_config: &Config, jobs: &Arc<Mutex<Vec<Job>>>, job_index: usize, _mutex: Arc<Mutex<bool>>) -> Result<()> {
  // inspired by https://gist.github.com/giuliano-oliveira/4d11d6b3bb003dba3a1b53f43d81b30d
  let url = &jobs.lock().unwrap()[job_index].url.clone();
  if url == "" { return Ok(()) } // If the job is not created from the site list, not download is needed
  let filename = &jobs.lock().unwrap()[job_index].filepath.clone();

let client = reqwest::Client::new();

  let res = client
      .get(url)
      .send()
      .await
      .or(Err(format!("Failed to GET from '{}'", &url)))?;
  let content_length = res
      .content_length()
      .ok_or(format!("Failed to get content length from '{}'", &url))?;

  jobs.lock().unwrap()[job_index].state = State::Downloading((0, content_length));
  update_display(&jobs.lock().unwrap())?;

  // Check if the file exists...
  if let Ok(metadata) = std::fs::metadata(filename) {
    // ...and if it does, get its size and compare with the size of the file on the server
    if metadata.len() == content_length {
      // We assume the file we have is already downloaded and correct.
      return Ok(());
    }
  }

  // download chunks
  let mut file = File::create(filename)?;
  let mut downloaded: u64 = 0;
  let mut stream = res.bytes_stream();

  while let Some(item) = stream.next().await {
      let chunk = item.or(Err(format!("Error while downloading file")))?;
      file.write_all(&chunk)
          .or(Err(format!("Error while writing to file")))?;
      let new = std::cmp::min(downloaded + (chunk.len() as u64), content_length);
      downloaded = new;
      jobs.lock().unwrap()[job_index].state = State::Downloading((downloaded, content_length));
      update_display(&jobs.lock().unwrap())?;
  }

  Ok(())
}

async fn unzip(_config: &Config, jobs: &Arc<Mutex<Vec<Job>>>, job_index: usize, _mutex: Arc<Mutex<bool>>) -> Result<()> {
  let filepath = &jobs.lock().unwrap()[job_index].filepath.clone();
  // https://github.com/dyz1990/sevenz-rust/blob/main/examples/decompress_progress.rs
  let mut sz = sevenz_rust::SevenZReader::open(filepath, "".into())?;
  let total_size: u64 = sz
    .archive()
    .files
    .iter()
    .filter(|e| e.has_stream())
    .map(|e| e.size())
    .sum();
  let mut uncompressed_size = 0;
  let dest = PathBuf::from(get_data_path(&PathBuf::from(filepath)));
  sz.for_each_entries(|entry, reader| {
    let mut buf = vec![0; (total_size as f32 / 100.0) as usize];
    let unzipped_filename = dest.join(entry.name());
    std::fs::create_dir_all(unzipped_filename.parent().unwrap()).unwrap();
    // Check if the file exists
    if let Ok(metadata) = std::fs::metadata(&unzipped_filename) {
      // ...and if it does, get its size and compare with the size of the file in the zipped file
      if metadata.len() == entry.size {
        // We assume the file we have is already unzipped.
        return Ok(true);
      }
    }
    let mut file = File::create(unzipped_filename).unwrap();
    loop {
      let read_size = reader.read(&mut buf)?;
      if read_size == 0 {
        break Ok(true);
      }
      file.write_all(&buf[..read_size])?;
      uncompressed_size += read_size;
      jobs.lock().unwrap()[job_index].state = State::Unzipping(((uncompressed_size as f32 / total_size as f32) * 100.0) as u8);
      update_display(&jobs.lock().unwrap()).unwrap(); // TODO: get rid of unwrap
    }
  })?;

  Ok(())
}

fn inject<R: BufRead, T>(config: &Config, reader: &mut quick_xml::reader::Reader<R>,
  table_name: &str, mutex: Arc<Mutex<bool>>) -> Result<()>
  where T: serde::Serialize + for<'de> serde::Deserialize<'de> {
  let _lock = mutex.lock().unwrap(); // Take a mutex so that database access are not concurrent.
  let connection = Connection::open(&config.database_filename)?;
  connection.execute("BEGIN TRANSACTION;")?;

  let mut insert_statement = connection.prepare("")?;
  let mut count = 0;
  loop {
    let mut buf = Vec::new();
    match reader.read_event_into(&mut buf) {
      Err(e) => error_chain::bail!(
        "Error at position {}: {:?}",
        reader.buffer_position(),
        e
      ),
      Ok(Event::Eof) => break,
      Ok(Event::Empty(e)) => {
        let s = format!("<{}/>", std::str::from_utf8(&e)?);
        let tag: T = quick_xml::de::from_str(&s)?;
        if count == 0 {
          let (create_stmt, insert_stmt) = sql_utils::to_init_table(&tag, table_name)?;
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
      },
      _ => (),
    }
  }

  connection.execute("END TRANSACTION;")?;
  Ok(())
}

fn get_site_from_filepath(filepath: &PathBuf) -> Result<String> {
  let mut filepath = filepath.clone();
  filepath.pop();
  return Ok(filepath.file_stem().ok_or("Could not retrieve site")?.to_string_lossy().to_string());
}

fn table_exists(database_filename: &str, table_name: &str, mutex: Arc<Mutex<bool>>) -> Result<bool> {
  let _mutex = mutex.lock().unwrap();
  let connection = Connection::open(database_filename)?;
  let mut result = false;
  connection.iterate(format!("SELECT name FROM sqlite_master WHERE name = \"{}\";", table_name), |pairs| {
    result = pairs.len() > 0;
    return true;
  })?;

  Ok(result)
}

macro_rules! do_load_se_file {
  ($config:ident, $filename:expr, $table_suffix:expr, $t:path, $completion:expr, $jobs:expr, $job_index:expr, $mutex:expr) => {
    let mut filepath = get_data_path(&PathBuf::from(&$jobs.lock().unwrap()[$job_index].filepath));
    filepath.push($filename);
    let sfilepath = filepath.to_string_lossy().to_string();
    let table_name = format!("{}_{}", get_site_from_filepath(&filepath)?, $table_suffix);
    if let Ok(result) = table_exists(&$config.database_filename.to_string_lossy(), &table_name, $mutex) && !result {
      $jobs.lock().unwrap()[$job_index].state = State::Parsing(($completion, sfilepath.clone()));
      update_display(&$jobs.lock().unwrap())?;
      if filepath.exists() {
        let f = File::open(&sfilepath)?;
        let reader = std::io::BufReader::new(f);
        let mut xmlreader = quick_xml::Reader::from_reader(reader);
        inject::<std::io::BufReader<File>, $t>($config, &mut xmlreader, &table_name, $mutex)?
      } else {
        error_chain::bail!("file {:?} do not exists", filepath)
      }
    }
  };
}

async fn parse(config: &Config, jobs: &Arc<Mutex<Vec<Job>>>, job_index: usize, mutex: Arc<Mutex<bool>>) -> Result<()> {
  do_load_se_file!(config, "Badges.xml", "Badge", se_struct::Badge, 0, jobs, job_index, mutex.clone());
  do_load_se_file!(config, "Comments.xml", "Comment", se_struct::Comment, 10, jobs, job_index, mutex.clone());
  do_load_se_file!(config, "PostHistory.xml", "PostHistory", se_struct::PostHistory, 40, jobs, job_index, mutex.clone());
  do_load_se_file!(config, "PostLinks.xml", "PostLink", se_struct::PostLink, 50, jobs, job_index, mutex.clone());
  do_load_se_file!(config, "Posts.xml", "Post", se_struct::Post, 60, jobs, job_index, mutex.clone());
  do_load_se_file!(config, "Tags.xml", "Tag", se_struct::Tag, 70, jobs, job_index, mutex.clone());
  do_load_se_file!(config, "Users.xml", "User", se_struct::User, 80, jobs, job_index, mutex.clone());
  do_load_se_file!(config, "Votes.xml", "Vote", se_struct::Vote, 90, jobs, job_index, mutex.clone());

  Ok(())
}

// Will asynchronously call the various functions of the provided job.
// It is the responsibility of these function to call update_display regularly.
// `mutex` here is used if threads need a mutual exclusion of some kind. Now used for writing to the DB.
async fn process(config: Config, jobs: Arc<Mutex<Vec<Job>>>, job_index: usize, mutex: Arc<Mutex<bool>>) -> Result<()> {
  match download(&config, &jobs, job_index, mutex.clone()).await {
    Err(e) => {
      {
        let mut jobs = jobs.lock().unwrap();
        jobs[job_index].state = State::Error(format!("download error: {} ({})", e, jobs[job_index].url));
        update_display(&jobs)?;
      }
      return Err(e);
    },
    _ => (),
  };
  match unzip(&config, &jobs, job_index, mutex.clone()).await {
    Err(e) => {
      jobs.lock().unwrap()[job_index].state = State::Error(format!("decompression error: {}", e));
      update_display(&jobs.lock().unwrap())?;
      return Err(e);
    },
    _ => (),
  }
  match parse(&config, &jobs, job_index, mutex.clone()).await {
    Err(e) => {
      jobs.lock().unwrap()[job_index].state = State::Error(format!("parsing error: {}", e));
      update_display(&jobs.lock().unwrap())?;
      return Err(e);
    },
    _ => (),
  }

  jobs.lock().unwrap()[job_index].state = State::Done;
  update_display(&jobs.lock().unwrap())?;
  Ok(())
}

fn create_job_list(config: &Config, site_list: String) -> Vec<Job> {
  site_list.lines()
    .map(|line| line.trim())
    .filter(|line| !line.starts_with('#'))
    .filter(|line| line.len() != 0)
    .map(|line| {
      let split = line.split_whitespace().map(|s| s).collect::<Vec<&str>>();
      if split.len() != 2 {
        panic!("Incorrect format in site list file: {line}");
      }
      let mut filepath = config.data_path.clone();
      filepath.push(split[0].to_string());
      Job { url: split[1].to_string(), filepath: filepath.to_string_lossy().to_string(), state: State::Wait }
    })
    .collect()
}

#[tokio::main]
async fn main() -> Result<()> {
  let config = Config::parse();
  if !config.data_path.exists() {
    std::fs::create_dir_all(config.data_path.clone())?;
  }

  let jobs = if let Some(site_list)  = config.site_list.clone() {
    if !site_list.exists() {
      Err(format!("site list file {:?} does not exists", site_list))
    } else {
      let site_list = std::fs::read_to_string(site_list)?.parse()?;
      Ok(Arc::new(Mutex::new(create_job_list(&config, site_list))))
    }
  } else {
    Ok(Arc::new(Mutex::new(Vec::new())))
  }?;
  // if the job list is empty here, it is because no files where provided to
  // download. So we will just be looking into the data folder and see what
  // 7z files there is to unzip. This scenario might happen if the user downloaded
  // the files by other means (like torrent).
  {
    let mut jobs = jobs.lock().unwrap();
    if jobs.len() == 0 {
      jobs.append(&mut std::fs::read_dir(config.data_path.clone())?
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().unwrap().to_string_lossy() == "7z")
        .map(|path| Job {
          url: String::from(""), filepath: path.to_string_lossy().to_string(), state: State::Wait,
        })
      .collect::<Vec<Job>>());
    }
  }

  // Set in Write Ahead Logging to allow simultaneous transactions
  {
    let connection = Connection::open(&config.database_filename)?;
    connection.execute("PRAGMA journal_mode = wal;")?;
  }

  crossterm::execute!(stdout(), crossterm::cursor::Hide)?;
  // Restore the cursor on ctrl-c
  // TODO: Should probably do it in other circumstances
  ctrlc::set_handler(|| {
    let _ = crossterm::execute!(stdout(), crossterm::cursor::Show);
    // We need to force exit here which is what the default handler does.
    println!("interrupted");
    std::process::exit(0);
  }).expect("Error setting Ctrl-C handler");

  // let jobs = Rc::new(RefCell::new(vec![
  //   Job { url: "http://speedtest.ftp.otenet.gr/files/test100k.db".to_string(), filepath: "test100k.db".to_string(), state: State::Wait },
  //   Job { url: "http://speedtest.ftp.otenet.gr/files/test1Mb.db".to_string(), filepath: "test1Mb.db".to_string(), state: State::Wait },
  //   Job { url: "http://speedtest.ftp.otenet.gr/files/test10Mb.db".to_string(), filepath: "test10Mb.db".to_string(), state: State::Wait },
  // ]));
  update_display(&jobs.lock().unwrap())?;

  let nbjobs = jobs.lock().unwrap().len();

  // {
  //   // We convert the jobs to futures that we will wait simultaneously
  //   // Concurrent requests (https://gist.github.com/joseluisq/e7f926d73e02fb9dd6114f4d8be6607d)
  //   let tasks = futures::stream::iter(
  //     (0..nbjobs).map(|index| process(config.clone(), jobs.clone(), index))
  //   ).buffer_unordered(3).collect::<Vec<_>>();
  //   // Waiting on all the future
  //   tasks.await;
  // }

  {
    // Here we spawn the jobs for parallel processing
    let mut tokio_jobs = futures::stream::FuturesUnordered::new();
    let thread_mutex = Arc::new(Mutex::new(true));
    for index in 0..nbjobs {
      tokio_jobs.push(tokio::spawn(process(config.clone(), jobs.clone(), index, thread_mutex.clone())));
      if tokio_jobs.len() == config.max_threads as usize {
        tokio_jobs.next().await;
      }
    }
    while let Some(_) = tokio_jobs.next().await {}
  }

  update_display(&jobs.lock().unwrap())?;
  let number_of_unfinished_jobs: u16 = jobs.lock().unwrap().iter().filter(|job| job.state != State::Done).count() as u16;
  crossterm::execute!(stdout(), crossterm::cursor::MoveDown(number_of_unfinished_jobs + 1))?;
  crossterm::execute!(stdout(), crossterm::cursor::Show)?;
  Ok(())
}

