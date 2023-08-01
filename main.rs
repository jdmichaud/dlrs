use chrono::{DateTime, Utc};
use chrono::serde::{ts_seconds, ts_seconds_option};
use error_chain::error_chain;
use futures::StreamExt;
use reqwest::header::{HeaderValue, CONTENT_LENGTH, RANGE};
use reqwest::StatusCode;
use serde::Deserialize;
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

#[derive(Debug, Deserialize)]
enum BadgeClass {
  Gold = 1,
  Silver = 2,
  Bronze = 3,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Badge {
  id: String,
  user_id: String,
  name: String, 
  date: DateTime<Utc>, 
  class: BadgeClass,
  TagBased: bool, // true if is for a tag
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Comment {
  id: String,
  post_id: String,
  score: i64,
  text: String, 
  #[serde(with = "ts_seconds")]
  creation_date: DateTime<Utc>, 
  // populated if a user has been removed and no longer referenced by user Id
  user_display_name: String,
  user_id: String,
}

#[derive(Debug, Deserialize)]
enum PostHistoryType {
  InitialTitle = 1, // The first title a question is asked with.
  InitialBody = 2, // The first raw body text a post is submitted with.
  InitialTags = 3, // The first tags a question is asked with.
  EditTitle = 4, // A question's title has been changed.
  EditBody = 5, // A post's body has been changed, the raw text is stored here as markdown.
  EditTags = 6, // A question's tags have been changed.
  RollbackTitle = 7, // A question's title has reverted to a previous version.
  RollbackBody = 8, // A post's body has reverted to a previous version - the raw text is stored here.
  RollbackTags = 9, // A question's tags have reverted to a previous version.
  PostClosed = 10, // A post was voted to be closed.
  PostReopened = 11, // A post was voted to be reopened.
  PostDeleted = 12, // A post was voted to be removed.
  PostUndeleted = 13, // A post was voted to be restored.
  PostLocked = 14, // A post was locked by a moderator.
  PostUnlocked = 15, // A post was unlocked by a moderator.
  CommunityOwned = 16, // A post has become community owned.
  PostMigrated = 17, // A post was migrated.
  QuestionMerged = 18, // A question has had another, deleted question merged into itself.
  QuestionProtected = 19, // A question was protected by a moderator
  QuestionUnprotected = 20, // A question was unprotected by a moderator
  PostDisassociated = 21, // An admin removes the OwnerUserId from a post.
  QuestionUnmerged = 22, // A previously merged question has had its answers and votes restored.
  SuggestedEditApplied = 24,
  PostTweeted = 25,
  MovedToChat = 31,
  PostNoticeAdded = 33, // Post notice added comment contains foreign key to PostNotices
  PostNoticeRemoved = 34, // Post notice removed comment contains foreign key to PostNotices
  PostMigratedAway = 35, // (replaces id 17)
  PostMigratedHere = 36, // (replaces id 17)
  PostMergeSource = 37,
  PostMergeDestination = 38,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct PostHistory {
  id: String,
  post_history_type: PostHistoryType,
  post_id: String,
  // At times more than one type of history record can be recorded by a single action.  All of these will be grouped using the same RevisionGUID
  revision_guid: String,
  creation_date: DateTime<Utc>,
  user_id: String,
  // populated if a user has been removed and no longer referenced by user Id
  user_display_name: Option<String>,
  // This field will contain the comment made by the user who edited a post
  comment: String,
  // A raw version of the new value for a given revision
  // - If PostHistoryTypeId = 10, 11, 12, 13, 14, or 15  this column will contain a JSON encoded string with all users who have voted for the PostHistoryTypeId
  // - If PostHistoryTypeId = 17 this column will contain migration details of either "from <url>" or "to <url>"
  text: String,
}

#[derive(Debug, Deserialize)]
enum LinkType {
  Linked = 1,
  Duplicate = 3,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct PostLink {
 id: String,
 creation_date: DateTime<Utc>,
 post_id: String,
 related_post_id: String,
 link_type_id: LinkType,
}

#[derive(Debug, Deserialize)]
enum PostType {
  Question = 1,
  Answer = 2,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Post {
  id: String,
  post_type_id: PostType,
  // only present if PostTypeId is 2
  parent_id: Option<String>,
  // only present if PostTypeId is 1
  accepted_answer_id: Option<String>,
  #[serde(with = "ts_seconds")]
  creation_date: DateTime<Utc>,
  #[serde(with = "ts_seconds_option")]
  deletion_date: Option<DateTime<Utc>>,
  score: i64,
  view_count: i64,
  body: String,
  owner_user_id: String,
  // populated if a user has been removed and no longer referenced by user Id or if the user was anonymous
  owner_display_name: Option<String>,
  last_editor_user_id: String,
  last_editor_display_name: String, 
  #[serde(with = "ts_seconds")]
  last_edit_date: DateTime<Utc>, // "2009-03-05T22:28:34.823" 
  #[serde(with = "ts_seconds")]
  last_activity_date: DateTime<Utc>, // "2009-03-11T12:51:01.480" 
  title: String,
  tags: String,
  answer_count: i64,
  comment_count: i64,
  favorite_count: i64,
  // populated if the post is closed
  #[serde(with = "ts_seconds_option")]
  closed_date: Option<DateTime<Utc>>,
  // populated if post is community wikied
  #[serde(with = "ts_seconds_option")]
  community_owned_date: Option<DateTime<Utc>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Tag {
  id: String,
  tag_name: String,
  count: i64,
  // if an Excerpt is created
  excerpt_post_id: Option<String>,
  // if an Wiki is created
  wiki_post_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct User {
  id: String,
  reputation: i64,
  #[serde(with = "ts_seconds")]
  creation_date: DateTime<Utc>,
  display_name: String,
  email_hash: String,
  profile_image_url: String,
  #[serde(with = "ts_seconds")]
  last_access_date: DateTime<Utc>,
  website_url: String,
  location: String,
  age: u8,
  about_me: String,
  views: u32,
  up_votes: u32,
  down_votes: u32,
  account_id: String,
}

#[derive(Debug, Deserialize)]
enum VoteType {
  AcceptedByOriginator = 1,
  UpMod = 2, //  upvote
  DownMod = 3, // downvote
  Offensive = 4,
  Favorite = 5,
  Close = 6,
  Reopen = 7,
  BountyStart = 8,
  BountyClose = 9,
  Deletion = 10,
  Undeletion = 11,
  Spam = 12,
  InformModerator = 13,
  ModeratorReview = 15,
  ApproveEditSuggestion = 16,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Vote { 
 id: String,
 post_id: String,
 vote_type_id: VoteType,
 creation_date: DateTime<Utc>,
 // only for VoteTypeId 5
  #[serde(with = "ts_seconds_option")]
 user_id: Option<DateTime<Utc>>,
 // only for VoteTypeId 9
  #[serde(with = "ts_seconds_option")]
 bounty_amount: Option<DateTime<Utc>>,
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

