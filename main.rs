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
use std::time::Instant;
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
    TryFromIntError(core::num::TryFromIntError);
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
  let mut chunk_size: u32 = 1024 * 1024;

  let url = &jobs.lock().unwrap()[job_index].url.clone();
  let filename = &jobs.lock().unwrap()[job_index].filepath.clone();

  let client = reqwest::Client::new();
  // Remotely get the size of the file to download
  let response = client.head(url).send().await?;
  let content_length = response
    .headers()
    .get(CONTENT_LENGTH)
    .ok_or("response doesn't include the content length")?;
  let content_length = u64::from_str(content_length.to_str()?).map_err(|_| "invalid Content-Length header")?;
  // Check if the file exists...
  if let Ok(metadata) = std::fs::metadata(filename) {
    // ...and if it does, get its size and compare with the size of the file on the server
    if metadata.len() == content_length {
      // We assume the file we have is already downloaded and correct.
      return Ok(());
    }
  }
  let mut output_file = std::io::BufWriter::new(File::create(filename)?);

  jobs.lock().unwrap()[job_index].state = State::Downloading(0);
  update_display(&jobs.lock().unwrap())?;
  let mut downloaded: usize = 0;
  for range in PartialRangeIter::new(0, content_length - 1, chunk_size)? {
    let then = Instant::now();
    let range_header = HeaderValue::from_str(&format!("bytes={}-{}", range.start, range.end))
      .expect("string provided by format!");
    let response = client.get(url).header(RANGE, range_header).send().await?;
    let time_to_download_chunk = (Instant::now() - then).as_secs();
    if time_to_download_chunk > 1 { // we are aiming at updating the display once per second
      chunk_size = (chunk_size as f32 * 0.9) as u32;
    } else {
      chunk_size = (chunk_size as f32 * 1.1) as u32;
    }

    let status = response.status();
    if !(status == StatusCode::OK || status == StatusCode::PARTIAL_CONTENT) {
      error_chain::bail!("Unexpected server response: {}", status)
    }

    let content = bytes::Bytes::from(response.bytes().await?);
    // Some server do not honor the range request (like python's SimpleHTTPServer) so we need to
    // keep track of what is downloaded and stop when we are done.
    downloaded += content.len();
    std::io::copy(&mut content.reader(), &mut output_file)?;
    jobs.lock().unwrap()[job_index].state = State::Downloading((downloaded as f32 / content_length as f32 * 100.0) as u8);
    update_display(&jobs.lock().unwrap())?;
    if downloaded >= content_length.try_into()? {
      break;
    }
  }

  Ok(())
}

async fn unzip(_config: &Config, jobs: &Arc<Mutex<Vec<Job>>>, job_index: usize) -> Result<()> {
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

macro_rules! do_load_se_file {
  ($content:ident, $filename:expr, $t:path, $completion:expr, $jobs:expr, $job_index:expr) => {
    let mut filepath = get_data_path(&PathBuf::from(&$jobs.lock().unwrap()[$job_index].filepath));
    filepath.push($filename);
    let sfilepath = filepath.to_string_lossy().to_string();
    $jobs.lock().unwrap()[$job_index].state = State::Parsing(($completion, sfilepath.clone()));
    update_display(&$jobs.lock().unwrap())?;
    let $content = if filepath.exists() {
      let f = File::open(&sfilepath)?;
      let reader = std::io::BufReader::new(f);
      let foo: $t = quick_xml::de::from_reader(reader)?;
      Some(foo.row)
    } else { None };
  };
}

async fn parse(_config: &Config, jobs: &Arc<Mutex<Vec<Job>>>, job_index: usize) -> Result<()> {
  do_load_se_file!(_badges, "Badges.xml", se_struct::Badges, 0, jobs, job_index);
  do_load_se_file!(_comments, "Comments.xml", se_struct::Comments, 10, jobs, job_index);
  do_load_se_file!(_post_histories, "PostHistory.xml", se_struct::PostHistories, 40, jobs, job_index);
  do_load_se_file!(_post_links, "PostLinks.xml", se_struct::PostLinks, 50, jobs, job_index);
  do_load_se_file!(_posts, "Posts.xml", se_struct::Posts, 60, jobs, job_index);
  do_load_se_file!(_tags, "Tags.xml", se_struct::Tags, 70, jobs, job_index);
  do_load_se_file!(_users, "Users.xml", se_struct::Users, 80, jobs, job_index);
  do_load_se_file!(_votes, "Votes.xml", se_struct::Votes, 90, jobs, job_index);

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

