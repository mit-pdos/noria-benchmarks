use chrono;
use futures::Future;
use my;
use my::prelude::*;
use trawler::{StoryId, UserId};

pub(crate) fn handle<F>(
    c: F,
    acting_as: Option<UserId>,
    id: StoryId,
    title: String,
) -> Box<Future<Item = (my::Conn, bool), Error = my::errors::Error>>
where
    F: 'static + Future<Item = my::Conn, Error = my::errors::Error>,
{
    let user = acting_as.unwrap();
    Box::new(
        c.and_then(|c| {
            // check that tags are active
            c.first::<_, my::Row>(
                "SELECT  `tags`.* FROM `tags` \
                 WHERE `tags`.`inactive` = 0 AND `tags`.`tag` IN ('test') \
                 ORDER BY `tags`.`id` ASC LIMIT 1",
            )
        }).map(|(c, tag)| (c, tag.unwrap().get::<u32, _>("id")))
            .and_then(move |(c, tag)| {
                // check that story id isn't already assigned
                c.drop_exec(
                    "SELECT  1 AS one FROM `stories` \
                     WHERE `stories`.`short_id` = ? LIMIT 1",
                    (::std::str::from_utf8(&id[..]).unwrap(),),
                ).map(move |c| (c, tag))
            })
            .map(|c| {
                // TODO: check for similar stories if there's a url
                // SELECT  `stories`.*
                // FROM `stories`
                // WHERE `stories`.`url` IN (
                //  'https://google.com/test',
                //  'http://google.com/test',
                //  'https://google.com/test/',
                //  'http://google.com/test/',
                //  ... etc
                // )
                // AND (is_expired = 0 OR is_moderated = 1)
                // ORDER BY id DESC LIMIT 1
                c
            })
            .map(|c| {
                // TODO
                // real impl queries `tags` and `users` again here..?
                c
            })
            .and_then(move |(c, tag)| {
                // TODO: real impl checks *new* short_id and duplicate urls *again*
                // TODO: sometimes submit url
                c.start_transaction(my::TransactionOptions::new())
                    .and_then(move |t| {
                        t.prep_exec(
                            "INSERT INTO `stories` \
                             (`created_at`, `user_id`, `title`, \
                             `description`, `short_id`, `upvotes`, `hotness`, \
                             `markeddown_description`) \
                             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                            (
                                chrono::Local::now().naive_local(),
                                user,
                                title,
                                "to infinity", // lorem ipsum?
                                ::std::str::from_utf8(&id[..]).unwrap(),
                                1,
                                -19216.2884921,
                                "<p>to infinity</p>\n",
                            ),
                        )
                    })
                    .and_then(|q| {
                        let story = q.last_insert_id().unwrap();
                        q.drop_result().map(move |t| (t, story))
                    })
                    .and_then(move |(t, story)| {
                        t.drop_exec(
                            "INSERT INTO `taggings` (`story_id`, `tag_id`) \
                             VALUES (?, ?)",
                            (story, tag),
                        ).map(move |t| (t, story))
                    })
                    .and_then(move |(t, story)| {
                        t.drop_query(&format!(
                            "INSERT INTO keystores (`key`, `value`) \
                             VALUES \
                             ('user:{}:stories_submitted', 1) \
                             ON DUPLICATE KEY UPDATE `value` = `value` + 1",
                            user
                        )).map(move |t| (t, story))
                    })
                    .and_then(move |(t, story)| {
                        t.drop_query(&format!(
                            "SELECT  `keystores`.* \
                             FROM `keystores` \
                             WHERE `keystores`.`key` = 'user:{}:stories_submitted' \
                             ORDER BY `keystores`.`key` ASC LIMIT 1",
                            user
                        )).map(move |t| (t, story))
                    })
                    .and_then(move |(t, story)| {
                        t.drop_exec(
                            "SELECT  `votes`.* FROM `votes` \
                             WHERE `votes`.`user_id` = ? \
                             AND `votes`.`story_id` = ? \
                             AND `votes`.`comment_id` IS NULL \
                             ORDER BY `votes`.`id` ASC LIMIT 1",
                            (user, story),
                        ).map(move |t| (t, story))
                    })
                    .and_then(move |(t, story)| {
                        t.drop_exec(
                            "INSERT INTO `votes` (`user_id`, `story_id`, `vote`) \
                             VALUES (?, ?, 1)",
                            (user, story),
                        ).map(move |t| (t, story))
                    })
                    .and_then(move |(t, story)| {
                        t.drop_exec(
                            "SELECT `comments`.`upvotes`, `comments`.`downvotes` \
                             FROM `comments` \
                             WHERE `comments`.`story_id` = ? \
                             AND (user_id <> ?)",
                            (story, user),
                        ).map(move |t| (t, story))
                    })
                    .and_then(move |(t, story)| {
                        // why oh why is story hotness *updated* here?!
                        t.drop_exec(
                            &format!(
                                "UPDATE `stories` \
                                             SET `hotness` = {}
                                             WHERE `stories`.`id` = ?",
                                -19216.5479744,
                            ),
                            (story,),
                        )
                    })
                    .and_then(|t| t.commit())
            })
            .map(|c| (c, false)),
    )
}