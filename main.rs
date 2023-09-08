#![allow(dead_code)]
#![feature(core_intrinsics)] // for breakpoint
#![feature(let_chains)] // for macro

use bytes::Buf;
use clap::Parser;
use core::convert::Infallible;
use error_chain::error_chain;
use futures::StreamExt;
use reqwest::header::{HeaderValue, CONTENT_LENGTH, RANGE};
use reqwest::StatusCode;
use sevenz_rust;
use std::fs::File;
use std::io::{stdout, Write};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::path::{Path, PathBuf};
use tokio;

mod se_struct;

#[derive(Parser, Clone)]
#[command(author, version, about, long_about = None)]
struct Config {
  /// Where to store Stack Exchange files (zipped and unzipped)
  #[arg(short, long, default_value=PathBuf::from("./data").into_os_string(), value_name = "PATH")]
  data_path: PathBuf,
  /// List of files/urls to download
  #[arg(short, long, default_value=PathBuf::from("site.list").into_os_string(), value_name = "FILE")]
  site_list: PathBuf,
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
    Infallible(Infallible);
  }
}

struct PartialRangeIter {
  start: u64,
  end: u64,
  buffer_size: u32,
}

impl PartialRangeIter {
  pub fn new(start: u64, end: u64, buffer_size: u32) -> Result<Self> {
    if buffer_size == 0 {
      Err("invalid buffer_size, give a value greater than zero.")?;
    }
    Ok(PartialRangeIter {
      start,
      end,
      buffer_size,
    })
  }
}

impl Iterator for PartialRangeIter {
  // type Item = reqwest::header::HeaderValue;
  type Item = std::ops::Range<u64>;
  fn next(&mut self) -> Option<Self::Item> {
    if self.start > self.end {
      None
    } else {
      let prev_start = self.start;
      self.start += std::cmp::min(self.buffer_size as u64, self.end - self.start + 1);
      Some(std::ops::Range { start: prev_start, end: self.start - 1 })
    }
  }
}

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
enum State {
  Error(String),
  Wait,
  Downloading(u8),
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
      (State::Downloading(avalue), State::Downloading(bvalue)) => bvalue.cmp(&avalue),
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
        State::Downloading(progress) => {
          let nbhash = ((progress_bar_width) as f32 * progress as f32 / 100.0) as u8;
          // ⎯
          let progress_bar = (0..nbhash).map(|_| "━").collect::<String>();
          print!("[{:width$}] downloading {}%", progress_bar, progress, width = progress_bar_width);
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
          print!("{}", &label[..max]);
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

async fn download(_config: &Config, jobs: &Arc<Mutex<Vec<Job>>>, job_index: usize) -> Result<()> {
  const CHUNK_SIZE: u32 = 1024 * 1024;

  let url = &jobs.lock().unwrap()[job_index].url.clone();
  let filename = &jobs.lock().unwrap()[job_index].filepath.clone();

  let client = reqwest::Client::new();
  let response = client.head(url).send().await?;
  let length = response
    .headers()
    .get(CONTENT_LENGTH)
    .ok_or("response doesn't include the content length")?;
  let length = u64::from_str(length.to_str()?).map_err(|_| "invalid Content-Length header")?;
  let mut output_file = std::io::BufWriter::new(File::create(filename)?);

  jobs.lock().unwrap()[job_index].state = State::Downloading(0);
  update_display(&jobs.lock().unwrap())?;
  for range in PartialRangeIter::new(0, length - 1, CHUNK_SIZE)? {
  // for range in PartialRangeIter::new(0, length - 1, (length / 100) as u32)? {
    let range_header = HeaderValue::from_str(&format!("bytes={}-{}", range.start, range.end))
      .expect("string provided by format!");
    let response = client.get(url).header(RANGE, range_header).send().await?;

    let status = response.status();
    if !(status == StatusCode::OK || status == StatusCode::PARTIAL_CONTENT) {
      println!("status {}", status);
      error_chain::bail!("Unexpected server response: {}", status)
    }

    let content = bytes::Bytes::from(response.bytes().await?);
    std::io::copy(&mut content.reader(), &mut output_file)?;
    jobs.lock().unwrap()[job_index].state = State::Downloading((range.start as f32 / length as f32 * 100.0) as u8);
    update_display(&jobs.lock().unwrap())?;
  }

  Ok(())
}

async fn unzip(_config: &Config, jobs: &Arc<Mutex<Vec<Job>>>, job_index: usize) -> Result<()> {
  let filepath = &jobs.lock().unwrap()[job_index].filepath.clone();
  // sevenz_rust::decompress_file(filepath, get_data_path(&PathBuf::from(filepath))).map_err(|e| e.to_string())?;

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
    let path = dest.join(entry.name());
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut file = File::create(path).unwrap();
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

macro_rules! do_load_se_file {
  ($content:ident, $filename:expr, $t:path, $completion:expr, $jobs:expr, $job_index:expr) => {
    let mut filepath = get_data_path(&PathBuf::from(&$jobs.lock().unwrap()[$job_index].filepath));
    filepath.push($filename);
    let sfilepath = filepath.to_string_lossy().to_string();
    let $content = if filepath.exists() {
      let f = File::open(&sfilepath)?;
      let reader = std::io::BufReader::new(f);
      let foo: $t = quick_xml::de::from_reader(reader)?;
      Some(foo.row)
    } else { None };
    $jobs.lock().unwrap()[$job_index].state = State::Parsing(($completion, sfilepath));
  };
}

async fn parse(_config: &Config, jobs: &Arc<Mutex<Vec<Job>>>, job_index: usize) -> Result<()> {
  jobs.lock().unwrap()[job_index].state = State::Parsing((0, String::from("")));
  update_display(&jobs.lock().unwrap())?;

  do_load_se_file!(_badges, "Badges.xml", se_struct::Badges, 10, jobs, job_index);
  update_display(&jobs.lock().unwrap())?;
  do_load_se_file!(_comments, "Comments.xml", se_struct::Comments, 20, jobs, job_index);
  update_display(&jobs.lock().unwrap())?;
  do_load_se_file!(_post_histories, "PostHistory.xml", se_struct::PostHistories, 50, jobs, job_index);
  update_display(&jobs.lock().unwrap())?;
  do_load_se_file!(_post_links, "PostLinks.xml", se_struct::PostLinks, 60, jobs, job_index);
  update_display(&jobs.lock().unwrap())?;
  do_load_se_file!(_posts, "Posts.xml", se_struct::Posts, 70, jobs, job_index);
  update_display(&jobs.lock().unwrap())?;
  do_load_se_file!(_tags, "Tags.xml", se_struct::Tags, 80, jobs, job_index);
  update_display(&jobs.lock().unwrap())?;
  do_load_se_file!(_users, "Users.xml", se_struct::Users, 90, jobs, job_index);
  update_display(&jobs.lock().unwrap())?;
  do_load_se_file!(_votes, "Votes.xml", se_struct::Votes, 100, jobs, job_index);
  update_display(&jobs.lock().unwrap())?;

  Ok(())
}

// Will asynchronously call the various functions of the provided job.
// It is the responsibility of these function to call update_display regularly.
async fn process(config: Config, jobs: Arc<Mutex<Vec<Job>>>, job_index: usize) -> Result<()> {
  match download(&config, &jobs, job_index).await {
    Err(e) => {
      jobs.lock().unwrap()[job_index].state = State::Error(format!("download error: {}", e));
      update_display(&jobs.lock().unwrap())?;
      return Err(e);
    },
    _ => (),
  };
  match unzip(&config, &jobs, job_index).await {
    Err(e) => {
      jobs.lock().unwrap()[job_index].state = State::Error(format!("decompression error: {}", e));
      update_display(&jobs.lock().unwrap())?;
      return Err(e);
    },
    _ => (),
  }
  match parse(&config, &jobs, job_index).await {
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
  if !config.site_list.exists() {
    Err(format!("site list file {:?} does not exists", config.site_list))?;
  }
  let site_list = std::fs::read_to_string(config.site_list.clone())?.parse()?;

  crossterm::execute!(stdout(), crossterm::cursor::Hide)?;
  // Restore the cursor on ctrl-c
  // TODO: Should probably do it in other circumstances
  ctrlc::set_handler(|| {
    let _ = crossterm::execute!(stdout(), crossterm::cursor::Show);
    // We need to force exit here which is what the default handler does.
    println!("interrupted");
    std::process::exit(0);
  }).expect("Error setting Ctrl-C handler");

  let jobs = Arc::new(Mutex::new(create_job_list(&config, site_list)));
  // let jobs = Rc::new(RefCell::new(vec![
  //   Job { url: "http://speedtest.ftp.otenet.gr/files/test100k.db".to_string(), filename: "test100k.db".to_string(), state: State::Wait },
  //   Job { url: "http://speedtest.ftp.otenet.gr/files/test1Mb.db".to_string(), filename: "test1Mb.db".to_string(), state: State::Wait },
  //   Job { url: "http://speedtest.ftp.otenet.gr/files/test10Mb.db".to_string(), filename: "test10Mb.db".to_string(), state: State::Wait },
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
    for index in 0..nbjobs {
      tokio_jobs.push(tokio::spawn(process(config.clone(), jobs.clone(), index)));
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

