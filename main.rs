use error_chain::error_chain;
use futures::StreamExt;
use reqwest::header::{HeaderValue, CONTENT_LENGTH, RANGE};
use reqwest::StatusCode;
use std::fs::File;
use std::io::stdout;
use std::str::FromStr;
use std::rc::Rc;
use std::cell::RefCell;
use tokio;

error_chain! {
  foreign_links {
    Io(std::io::Error);
    Reqwest(reqwest::Error);
    Header(reqwest::header::ToStrError);
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

#[derive(Debug, Clone, Copy)]
enum State {
  Wait,
  Downloading(u8),
  Done,
}

#[derive(Debug, Clone)]
struct Job {
  url: String,
  filename: String,
  state: State,
}

fn update_display(jobs: &Vec<Job>) -> Result<()> {
  if jobs.len() == 0 {
    return Ok(())
  }
  // This function has no state so we have to recompute a bunch of things every time.
  let max_filename_length = jobs.iter().map(|job| job.filename.len()).fold(std::i32::MIN, |a,b| a.max(b as i32));
  let terminal_size = crossterm::terminal::size()?;
  let progress_bar_width: usize = std::cmp::min(terminal_size.0 as usize - max_filename_length as usize - 40 as usize, 50);
  for job in jobs.iter() {
    crossterm::execute!(stdout(), crossterm::terminal::Clear(crossterm::terminal::ClearType::CurrentLine))?;
    print!("{:width$} ", job.filename, width = max_filename_length as usize);
    match job.state {
      State::Wait => println!("{:width$}waiting", "", width = progress_bar_width),
      State::Downloading(progress) => {
        let nbhash = ((progress_bar_width) as f32 * progress as f32 / 100.0) as u8;
        let progress_bar = (0..nbhash).map(|_| "#").collect::<String>();
        println!("[{:width$}] {}%", progress_bar, progress, width = progress_bar_width);
      },
      State::Done => {
        let full_progress_bar = (0..progress_bar_width).map(|_| "#").collect::<String>();
        println!("[{:width$}] done.", full_progress_bar, width = progress_bar_width);
      }
    }
  }
  let position = crossterm::cursor::position()?;
  crossterm::execute!(stdout(), crossterm::cursor::MoveTo(position.0, position.1 - jobs.len() as u16))?;
  Ok(())
}

async fn download(jobs: &Rc<RefCell<Vec<Job>>>, job_index: usize) -> Result<()> {
  const CHUNK_SIZE: u32 = 1024 * 30;
  
  let url = &jobs.borrow()[job_index].url.clone();
  let filename = &jobs.borrow()[job_index].filename.clone();

  let client = reqwest::Client::new();
  let response = client.head(url).send().await?;
  let length = response
    .headers()
    .get(CONTENT_LENGTH)
    .ok_or("response doesn't include the content length")?;
  let length = u64::from_str(length.to_str()?).map_err(|_| "invalid Content-Length header")?;
  let mut output_file = File::create(filename)?;
    
  jobs.borrow_mut()[job_index].state = State::Downloading(0);
  update_display(&jobs.borrow())?;
  for range in PartialRangeIter::new(0, length - 1, CHUNK_SIZE)? {
    let range_header = HeaderValue::from_str(&format!("bytes={}-{}", range.start, range.end))
      .expect("string provided by format!");
    let response = client.get(url).header(RANGE, range_header).send().await?;
    
    let status = response.status();
    if !(status == StatusCode::OK || status == StatusCode::PARTIAL_CONTENT) {
      println!("status {}", status);
      error_chain::bail!("Unexpected server response: {}", status)
    }

    let content = response.text().await?;
    std::io::copy(&mut content.as_bytes(), &mut output_file)?;
    jobs.borrow_mut()[job_index].state = State::Downloading((range.start as f32 / length as f32 * 100.0) as u8);
    update_display(&jobs.borrow())?;
  }
    
  Ok(())
}

async fn unzip(_jobs: &Rc<RefCell<Vec<Job>>>, _job_index: usize) -> Result<()> {
  Ok(())
}

// Will asynchronously call the various functions of the provided job.
// It is the responsibility of these function to call update_display regularly.
async fn process(jobs: &Rc<RefCell<Vec<Job>>>, job_index: usize) -> Result<()> {
  download(jobs, job_index).await?;
  unzip(jobs, job_index).await?;
  jobs.borrow_mut()[job_index].state = State::Done;
  update_display(&jobs.borrow())?;
  Ok(())
}

pub const SITE_LIST: &str = include_str!("site.list");

fn create_job_list() -> Vec<Job> {
  SITE_LIST.lines()
    .map(|line| line.trim())
    .filter(|line| !line.starts_with('#'))
    .filter(|line| line.len() != 0)
    .map(|line| {
      let split = line.split_whitespace().map(|s| s).collect::<Vec<&str>>();
      Job { url: split[1].to_string(), filename: split[0].to_string(), state: State::Wait }
    })
    .collect()
}

#[tokio::main]
async fn main() -> Result<()> {
  crossterm::execute!(stdout(), crossterm::cursor::Hide)?;
  // Restore the cursor on ctrl-c
  // TODO: Should probably do it in other circumstances
  ctrlc::set_handler(|| {
    let _ = crossterm::execute!(stdout(), crossterm::cursor::Show);
    // We need to force exit here which is what the default handler does.
    println!("interrupted");
    std::process::exit(0);
  }).expect("Error setting Ctrl-C handler");

  let jobs = Rc::new(RefCell::new(create_job_list()));
  // let jobs = Rc::new(RefCell::new(vec![
  //   Job { url: "http://speedtest.ftp.otenet.gr/files/test100k.db".to_string(), filename: "test100k.db".to_string(), state: State::Wait },
  //   Job { url: "http://speedtest.ftp.otenet.gr/files/test1Mb.db".to_string(), filename: "test1Mb.db".to_string(), state: State::Wait },
  //   Job { url: "http://speedtest.ftp.otenet.gr/files/test10Mb.db".to_string(), filename: "test10Mb.db".to_string(), state: State::Wait },
  // ]));
  update_display(&jobs.borrow())?;

  let nbjobs = jobs.borrow().len();
  // We convert the jobs to futures that we will wait simultaneously
  // Concurrent requests (https://gist.github.com/joseluisq/e7f926d73e02fb9dd6114f4d8be6607d)
  let tasks = futures::stream::iter(
    (0..nbjobs).map(|index| process(&jobs, index))
  ).buffer_unordered(3).collect::<Vec<_>>();
  // Waiting on all the future
  tasks.await;
  
  crossterm::execute!(stdout(), crossterm::cursor::Show)?;
  Ok(())
}

