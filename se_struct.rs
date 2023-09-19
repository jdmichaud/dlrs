#![allow(unused)]

/**
 * Serded rust structure matching Stack Exchange data table.
 * source: https://meta.stackexchange.com/a/2678
 *
 * The strange technical decisions made in this file are a direct result of:
 * 1. The input format file provided by Stack Exchange
 * 2. The limitations of quick_xml and serde
 *
 * The flow of information is as follows:
 * xml (stackechange) ----> sql ----> json
 *
 * We could the XML format to look like this:
 * ```
 * <posts>
 *   <post>
 *     <id>23</id>
 *     <body>This is a post</body>
 *   </post>
 *   [...]
 * </posts>
 * ```
 * Where in reality, the file is presented this way:
 * ```
 * <posts>
 *   <row Id="" Body="This is a post"/>
 *   [...]
 * </posts>
 * ```
 * quick_xml was not design to handle data fields as attribute and due to serde
 * limitations a workaround is needed to read the struct field from the attributes.
 * We need to prefix the field name with '@' (`#[serde(rename = "@Id"`). As a
 * consequence, any downstream Serializer/Deserializer will have to deal with
 * removing that prefix in order to deal with regularly named field (see the
 * sql Serializer for example).
 * Moreover, because data comes from attributes, everything is treated as a
 * string and necessitate some massaging in order to fit in a reasonable model.
 * For example, we want our IDs to be integer so that sql is able to index them
 * quickly. That's why "deserialize_with" are being used here and there.
 */
use serde_with::chrono::naive::NaiveDateTime;
use serde::{Deserialize, Serialize};
use serde::de::Error;
use serde_repr::Deserialize_repr;

mod naive_date_parser {
  use serde_with::chrono::naive::NaiveDateTime;

  struct NaiveDateTimeVisitor;

  // All this seems overly complicated just to handle an Optional DateTime...
  impl<'de> serde::de::Visitor<'de> for NaiveDateTimeVisitor {
    type Value = Option<NaiveDateTime>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
      write!(formatter, "a string represents chrono::NaiveDateTime")
    }

    fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
      match NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S.%f") {
        Ok(t) => Ok(Some(t)),
        Err(_) => Err(serde::de::Error::invalid_value(serde::de::Unexpected::Str(s), &self)),
      }
    }
  }

  pub fn from_rfc3339_without_timezone<'de, D>(d: D) -> Result<Option<NaiveDateTime>, D::Error>
  where
      D: serde::de::Deserializer<'de>,
  {
    d.deserialize_str(NaiveDateTimeVisitor)
  }
}

use naive_date_parser::from_rfc3339_without_timezone;

// Because of https://github.com/serde-rs/serde/issues/1183, quick_xml is not
// able to convert attribute to something else than a String
// (https://github.com/tafia/quick-xml/issues/433). However, using a custom
// deserializer (https://stackoverflow.com/a/46755370/2603925) we can get
// the ids as integers and thus allow faster indexing in the DB and properly
// formated JSON.
fn from_string<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
  D: serde::Deserializer<'de>,
{
  let s: &str = Deserialize::deserialize(deserializer)?;
  i64::from_str_radix(s, 10).map_err(D::Error::custom)
}

fn from_string_optional<'de, D>(deserializer: D) -> Result<Option<i64>, D::Error>
where
  D: serde::Deserializer<'de>,
{
  if let Some(s) = Deserialize::deserialize(deserializer)? {
    Ok(Some(i64::from_str_radix(s, 10).map_err(D::Error::custom)?))
  } else {
    Ok(None)
  }
}

#[derive(Debug, Deserialize_repr, Serialize)]
#[repr(u8)]
pub enum BadgeClass {
  Gold = 1,
  Silver = 2,
  Bronze = 3,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Badge {
  #[serde(rename = "@Id", deserialize_with = "from_string")]
  id: i64,
  #[serde(rename = "@UserId", deserialize_with = "from_string")]
  user_id: i64,
  #[serde(rename = "@Name")]
  name: String,
  #[serde(with = "NaiveDateTime")]
  #[serde(rename = "@Date")]
  date: NaiveDateTime,
  #[serde(rename = "@Class")]
  class: BadgeClass,
  #[serde(rename = "@TagBased")]
  tag_based: bool, // true if is for a tag
}

// We need this because the all stack exchange XML file uses the tag "row" for
// some reason.
#[derive(Debug, Deserialize, Serialize)]
pub struct Badges {
  pub row: Vec<Badge>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Comment {
  #[serde(rename = "@Id", deserialize_with = "from_string")]
  id: i64,
  #[serde(rename = "@PostId", deserialize_with = "from_string")]
  post_id: i64,
  #[serde(rename = "@Score")]
  score: i64,
  #[serde(rename = "@Text")]
  text: String,
  #[serde(with = "NaiveDateTime")]
  #[serde(rename = "@CreationDate")]
  creation_date: NaiveDateTime,
  // populated if a user has been removed and no longer referenced by user Id
  #[serde(rename = "@UserDisplayName")]
  user_display_name: Option<String>,
  #[serde(rename = "@UserId", deserialize_with = "from_string_optional", default)]
  user_id: Option<i64>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Comments {
  pub row: Vec<Comment>,
}

#[derive(Debug, Deserialize_repr, Serialize)]
#[repr(u8)]
pub enum PostHistoryType {
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

#[derive(Debug, Deserialize, Serialize)]
pub struct PostHistory {
  #[serde(rename = "@Id", deserialize_with = "from_string")]
  id: i64,
  #[serde(rename = "@PostHistoryTypeId")]
  // This field changes probably very often, might not be wise to use a fixed enum here
  post_history_type_id: i16, // PostHistoryType
  #[serde(rename = "@PostId", deserialize_with = "from_string")]
  post_id: i64,
  // At times more than one type of history record can be recorded by a single action.  All of these will be grouped using the same RevisionGUID
  #[serde(rename = "@RevisionGUID")]
  revision_guid: String,
  #[serde(with = "NaiveDateTime")]
  #[serde(rename = "@CreationDate")]
  creation_date: NaiveDateTime,
  #[serde(rename = "@UserId", deserialize_with = "from_string_optional", default)]
  user_id: Option<i64>,
  // populated if a user has been removed and no longer referenced by user Id
  #[serde(rename = "@UserDisplayName")]
  user_display_name: Option<String>,
  // This field will contain the comment made by the user who edited a post
  #[serde(rename = "@Comment")]
  comment: Option<String>,
  // A raw version of the new value for a given revision
  // - If PostHistoryTypeId = 10, 11, 12, 13, 14, or 15  this column will contain a JSON encoded string with all users who have voted for the PostHistoryTypeId
  // - If PostHistoryTypeId = 17 this column will contain migration details of either "from <url>" or "to <url>"
  #[serde(rename = "@Text")]
  text: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct PostHistories {
  pub row: Vec<PostHistory>,
}

#[derive(Debug, Deserialize_repr, Serialize)]
#[repr(u8)]
pub enum LinkType {
  Linked = 1,
  Duplicate = 3,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct PostLink {
 #[serde(rename = "@Id", deserialize_with = "from_string")]
 id: i64,
 #[serde(with = "NaiveDateTime")]
 #[serde(rename = "@CreationDate")]
 creation_date: NaiveDateTime,
 #[serde(rename = "@PostId", deserialize_with = "from_string")]
 post_id: i64,
 #[serde(rename = "@RelatedPostId", deserialize_with = "from_string")]
 related_post_id: i64,
 #[serde(rename = "@LinkTypeId")]
 link_type_id: LinkType,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct PostLinks {
  pub row: Vec<PostLink>,
}

#[derive(Debug, Deserialize_repr, Serialize)]
#[repr(u8)]
pub enum PostType {
  Question = 1,
  Answer = 2,
  Wiki = 3,
  TagWikiExcerpt = 4,
  TagWiki = 5,
  ModeratorNomination = 6,
  WikiPlaceholder = 7,
  PrivilegeWiki = 8,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Post {
  #[serde(rename = "@Id", deserialize_with = "from_string")]
  id: i64,
  #[serde(rename = "@PostTypeId")]
  post_type_id: PostType,
  // only present if PostTypeId is 2
  #[serde(rename = "@ParentId", deserialize_with = "from_string_optional", default)]
  parent_id: Option<i64>,
  // only present if PostTypeId is 1
  #[serde(rename = "@AcceptedAnswerId", deserialize_with = "from_string_optional", default)]
  accepted_answer_id: Option<i64>,
  #[serde(with = "NaiveDateTime")]
  #[serde(rename = "@CreationDate")]
  creation_date: NaiveDateTime,
  // We need `default` to assign None to the option when the field is absent
  // because deserialize_with does not handle this case properly...
  #[serde(deserialize_with = "from_rfc3339_without_timezone", default)]
  #[serde(rename = "@DeletionDate")]
  deletion_date: Option<NaiveDateTime>,
  #[serde(rename = "@Score")]
  score: i64,
  #[serde(rename = "@ViewCount")]
  view_count: Option<i64>,
  #[serde(rename = "@Body")]
  body: String,
  #[serde(rename = "@OwnerUserId", deserialize_with = "from_string_optional", default)]
  owner_user_id: Option<i64>,
  // populated if a user has been removed and no longer referenced by user Id or if the user was anonymous
  #[serde(rename = "@OwnerDisplayName")]
  owner_display_name: Option<String>,
  #[serde(rename = "@LastEditorUserId", deserialize_with = "from_string_optional", default)]
  last_editor_user_id: Option<i64>,
  #[serde(rename = "@LastEditorDisplayName")]
  last_editor_display_name: Option<String>,
  #[serde(deserialize_with = "from_rfc3339_without_timezone", default)]
  #[serde(rename = "@LastEditDate")]
  last_edit_date: Option<NaiveDateTime>, // "2009-03-05T22:28:34.823"
  #[serde(with = "NaiveDateTime")]
  #[serde(rename = "@LastActivityDate")]
  last_activity_date: NaiveDateTime, // "2009-03-11T12:51:01.480"
  #[serde(rename = "@Title")]
  title: Option<String>,
  #[serde(rename = "@Tags")]
  tags: Option<String>,
  #[serde(rename = "@AnswerCount")]
  answer_count: Option<i64>,
  #[serde(rename = "@CommentCount")]
  comment_count: i64,
  #[serde(rename = "@FavoriteCount")]
  favorite_count: Option<i64>,
  // populated if the post is closed
  #[serde(deserialize_with = "from_rfc3339_without_timezone", default)]
  #[serde(rename = "@ClosedDate")]
  closed_date: Option<NaiveDateTime>,
  // populated if post is community wikied
  #[serde(deserialize_with = "from_rfc3339_without_timezone", default)]
  #[serde(rename = "@CommunityOwnedDate")]
  community_owned_date: Option<NaiveDateTime>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Posts {
  pub row: Vec<Post>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Tag {
  #[serde(rename = "@Id", deserialize_with = "from_string")]
  id: i64,
  #[serde(rename = "@TagName")]
  tag_name: String,
  #[serde(rename = "@Count")]
  count: i64,
  // if an Excerpt is created
  #[serde(rename = "@ExcerptPostId", deserialize_with = "from_string_optional", default)]
  excerpt_post_id: Option<i64>,
  // if an Wiki is created
  #[serde(rename = "@WikiPostId", deserialize_with = "from_string_optional", default)]
  wiki_post_id: Option<i64>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Tags {
  pub row: Vec<Tag>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct User {
  #[serde(rename = "@Id", deserialize_with = "from_string")]
  id: i64,
  #[serde(rename = "@Reputation")]
  reputation: i64,
  #[serde(with = "NaiveDateTime")]
  #[serde(rename = "@CreationDate")]
  creation_date: NaiveDateTime,
  #[serde(rename = "@DisplayName")]
  display_name: String,
  #[serde(rename = "@EmailHash")]
  email_hash: Option<String>,
  #[serde(rename = "@ProfileImageUrl")]
  profile_image_url: Option<String>,
  #[serde(with = "NaiveDateTime")]
  #[serde(rename = "@LastAccessDate")]
  last_access_date: NaiveDateTime,
  #[serde(rename = "@WebsiteUrl")]
  website_url: Option<String>,
  #[serde(rename = "@Location")]
  location: Option<String>,
  #[serde(rename = "@Age")]
  age: Option<u8>,
  #[serde(rename = "@AboutMe")]
  about_me: Option<String>,
  #[serde(rename = "@Views")]
  views: u32,
  #[serde(rename = "@UpVotes")]
  up_votes: u32,
  #[serde(rename = "@DownVotes")]
  down_votes: u32,
  #[serde(rename = "@AccountId", deserialize_with = "from_string_optional", default)]
  account_id: Option<i64>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Users {
  pub row: Vec<User>,
}

#[derive(Debug, Deserialize_repr, Serialize)]
#[repr(u8)]
pub enum VoteType {
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

#[derive(Debug, Deserialize, Serialize)]
pub struct Vote {
 #[serde(rename = "@Id")]
 id: String,
 #[serde(rename = "@PostId", deserialize_with = "from_string")]
 post_id: i64,
 #[serde(rename = "@VoteTypeId")]
 vote_type_id: VoteType,
 #[serde(rename = "@CreationDate")]
 creation_date: NaiveDateTime,
 // only for VoteTypeId 5
 #[serde(rename = "@UserId", deserialize_with = "from_string_optional", default)]
 user_id: Option<i64>,
 // only for VoteTypeId 9
 #[serde(rename = "@BountyAmount")]
 bounty_amount: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Votes {
  pub row: Vec<Vote>,
}
