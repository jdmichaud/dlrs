use error_chain::error_chain;
use futures::future;
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
  type Item = reqwest::header::HeaderValue;
  fn next(&mut self) -> Option<Self::Item> {
    if self.start > self.end {
      None
    } else {
      let prev_start = self.start;
      self.start += std::cmp::min(self.buffer_size as u64, self.end - self.start + 1);
      Some(HeaderValue::from_str(&format!("bytes={}-{}", prev_start, self.start - 1))
        .expect("string provided by format!"))
    }
  }
}

async fn download<'a>(jobs: &Rc<RefCell<Vec<Job<'a>>>>, job_index: usize) -> Result<()> {
  const CHUNK_SIZE: u32 = 1024;
  
  let url = jobs.borrow_mut()[job_index].url;
  let filename = jobs.borrow_mut()[job_index].filename;

  let client = reqwest::Client::new();
  let response = client.head(url).send().await?;
  let length = response
    .headers()
    .get(CONTENT_LENGTH)
    .ok_or("response doesn't include the content length")?;
  let length = u64::from_str(length.to_str()?).map_err(|_| "invalid Content-Length header")?;
    
  let mut output_file = File::create(filename)?;
    
  jobs.borrow_mut()[job_index].state = State::Downloading(0);
  update_display(&jobs.borrow());
  for range in PartialRangeIter::new(0, length - 1, CHUNK_SIZE)? {
    // println!("{}: range {:?}", filename, range);
    let response = client.get(url).header(RANGE, range).send().await?;
    
    let status = response.status();
    if !(status == StatusCode::OK || status == StatusCode::PARTIAL_CONTENT) {
      error_chain::bail!("Unexpected server response: {}", status)
    }

    let content = response.text().await?;
    std::io::copy(&mut content.as_bytes(), &mut output_file)?;
    jobs.borrow_mut()[job_index].state = State::Downloading(length / (range.start + CHUNK_SIZE));
  }
    
  Ok(())
}

#[derive(Debug, Clone, Copy)]
enum State {
  Wait,
  Downloading(u8),
  Unzipping,
  Injecting,
}

#[derive(Debug, Clone, Copy)]
struct Job<'a> {
  url: &'a str,
  filename: &'a str,
  state: State,
}

fn update_display(jobs: &Vec<Job>) -> Result<()> {
  crossterm::execute!(stdout(), crossterm::cursor::SavePosition)?;
  for job in jobs.iter() {
    match job.state {
      State::Wait => println!("{}: waiting", job.filename),
      _ => println!("{}: unknown state", job.filename),
    }
  }
  crossterm::execute!(stdout(), crossterm::cursor::RestorePosition)?;
  Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
  crossterm::execute!(stdout(), crossterm::cursor::Hide)?;
  let url = "http://speedtest.ftp.otenet.gr/files/test100k.db";
  let jobs = Rc::new(RefCell::new(vec![
    Job { url: url, filename: "download.bin", state: State::Wait },
    Job { url: url, filename: "download2.bin", state: State::Wait },
  ]));
  update_display(&jobs.borrow());

  let first = download(&jobs, 0);
  let second = download(&jobs, 1);
  future::join_all([first, second]).await;

  println!("Finished with success!");
  crossterm::execute!(stdout(), crossterm::cursor::Show)?;
  Ok(())
}

