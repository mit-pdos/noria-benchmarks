use futures;
use futures::Future;
use futures::future::Either;
use my;
use my::prelude::*;
use std::collections::HashSet;
use trawler::UserId;

pub(crate) fn handle<F>(
    c: F,
    acting_as: Option<UserId>,
) -> Box<Future<Item = (my::Conn, bool), Error = my::errors::Error>>
where
    F: 'static + Future<Item = my::Conn, Error = my::errors::Error>,
{
    Box::new(
        c.and_then(move |c| {
            c.query(&format!(
                "SELECT  `comments`.* \
                 FROM `comments` \
                 WHERE `comments`.`is_deleted` = 0 \
                 AND `comments`.`is_moderated` = 0 \
                 {} \
                 ORDER BY id DESC \
                 LIMIT 20 OFFSET 0",
                match acting_as {
                    None => String::from(""),
                    Some(uid) => format!(
                        " AND (NOT EXISTS (\
                         SELECT 1 FROM hidden_stories \
                         WHERE user_id = {} \
                         AND hidden_stories.story_id = comments.story_id)) ",
                        uid
                    ),
                }
            ))
        }).and_then(|comments| {
                comments.reduce_and_drop(
                    (Vec::new(), HashSet::new(), HashSet::new()),
                    |(mut comments, mut users, mut stories), comment| {
                        comments.push(comment.get::<u32, _>("id").unwrap());
                        users.insert(comment.get::<u32, _>("user_id").unwrap());
                        stories.insert(comment.get::<u32, _>("story_id").unwrap());
                        (comments, users, stories)
                    },
                )
            })
            .and_then(|(c, (comments, users, stories))| {
                let users = users
                    .into_iter()
                    .map(|id| format!("{}", id))
                    .collect::<Vec<_>>()
                    .join(",");
                c.drop_query(&format!(
                    "SELECT `users`.* FROM `users` \
                     WHERE `users`.`id` IN ({})",
                    users
                )).map(move |c| (c, comments, stories))
            })
            .and_then(|(c, comments, stories)| {
                let stories = stories
                    .into_iter()
                    .map(|id| format!("{}", id))
                    .collect::<Vec<_>>()
                    .join(",");
                c.query(&format!(
                    "SELECT  `stories`.* FROM `stories` \
                     WHERE `stories`.`id` IN ({})",
                    stories
                )).map(move |stories| (stories, comments))
            })
            .and_then(|(stories, comments)| {
                stories
                    .reduce_and_drop(HashSet::new(), |mut authors, story| {
                        authors.insert(story.get::<u32, _>("user_id").unwrap());
                        authors
                    })
                    .map(move |(c, authors)| (c, authors, comments))
            })
            .and_then(move |(c, authors, comments)| match acting_as {
                None => Either::A(futures::future::ok((c, authors))),
                Some(uid) => {
                    let comments = comments
                        .into_iter()
                        .map(|id| format!("{}", id))
                        .collect::<Vec<_>>()
                        .join(",");
                    Either::B(c.drop_query(&format!(
                        "SELECT `votes`.* FROM `votes` \
                         WHERE `votes`.`user_id` = {} \
                         AND `votes`.`comment_id` IN ({})",
                        uid, comments
                    )).map(move |c| (c, authors)))
                }
            })
            .and_then(|(c, authors)| {
                // NOTE: the real website issues all of these one by one...
                let authors = authors
                    .into_iter()
                    .map(|id| format!("{}", id))
                    .collect::<Vec<_>>()
                    .join(",");
                c.drop_query(&format!(
                    "SELECT  `users`.* FROM `users` \
                     WHERE `users`.`id` IN ({})",
                    authors
                ))
            })
            .map(|c| (c, true)),
    )
}