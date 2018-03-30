use futures::Future;
use my;
use my::prelude::*;
use trawler::{StoryId, UserId, Vote};

pub(crate) fn handle<F>(
    c: F,
    acting_as: Option<UserId>,
    story: StoryId,
    v: Vote,
) -> Box<Future<Item = (my::Conn, bool), Error = my::errors::Error>>
where
    F: 'static + Future<Item = my::Conn, Error = my::errors::Error>,
{
    let user = acting_as.unwrap();
    Box::new(
        c.and_then(move |c| {
            c.prep_exec(
                "SELECT `stories`.* \
                 FROM `stories` \
                 WHERE `stories`.`short_id` = ? \
                 ORDER BY `stories`.`id` ASC LIMIT 1",
                (::std::str::from_utf8(&story[..]).unwrap(),),
            ).and_then(|result| result.collect_and_drop::<my::Row>())
                .map(|(c, mut story)| (c, story.swap_remove(0)))
        }).and_then(move |(c, story)| {
                let author = story.get::<u32, _>("user_id").unwrap();
                let id = story.get::<u32, _>("id").unwrap();
                let score = story.get::<f64, _>("hotness").unwrap();
                c.drop_exec(
                    "SELECT  `votes`.* \
                     FROM `votes` \
                     WHERE `votes`.`user_id` = ? \
                     AND `votes`.`story_id` = ? \
                     AND `votes`.`comment_id` IS NULL \
                     ORDER BY `votes`.`id` ASC LIMIT 1",
                    (user, id),
                ).map(move |c| (c, author, id, score))
            })
            .and_then(move |(c, author, story, score)| {
                // TODO: do something else if user has already voted
                // TODO: technically need to re-load story under transaction
                c.start_transaction(my::TransactionOptions::new())
                    .and_then(move |t| {
                        t.drop_exec(
                            "INSERT INTO `votes` \
                             (`user_id`, `story_id`, `vote`) \
                             VALUES \
                             (?, ?, ?)",
                            (
                                user,
                                story,
                                match v {
                                    Vote::Up => 1,
                                    Vote::Down => 0,
                                },
                            ),
                        )
                    })
                    .and_then(move |t| {
                        t.drop_exec(
                            &format!(
                                "UPDATE `users` \
                                 SET `karma` = `karma` {} \
                                 WHERE `users`.`id` = ?",
                                match v {
                                    Vote::Up => "+ 1",
                                    Vote::Down => "- 1",
                                }
                            ),
                            (author,),
                        )
                    })
                    .and_then(move |t| {
                        // get all the stuff needed to compute updated hotness
                        t.drop_exec(
                            "SELECT `tags`.* \
                             FROM `tags` \
                             INNER JOIN `taggings` ON `tags`.`id` = `taggings`.`tag_id` \
                             WHERE `taggings`.`story_id` = ?",
                            (story,),
                        )
                    })
                    .and_then(move |t| {
                        t.drop_exec(
                            "SELECT \
                             `comments`.`upvotes`, \
                             `comments`.`downvotes` \
                             FROM `comments` \
                             WHERE `comments`.`story_id` = ? \
                             AND user_id <> ?",
                            (story, author),
                        )
                    })
                    .and_then(move |t| {
                        t.drop_exec(
                            "SELECT `stories`.`id` \
                             FROM `stories` \
                             WHERE `stories`.`merged_story_id` = ?",
                            (story,),
                        )
                    })
                    .and_then(move |t| {
                        // the *actual* algorithm for computing hotness isn't all
                        // that interesting to us. it does affect what's on the
                        // frontpage, but we're okay with using a more basic
                        // upvote/downvote ratio thingy. See Story::calculated_hotness
                        // in the lobsters source for details.
                        t.drop_exec(
                            &format!(
                                "UPDATE stories SET \
                                 upvotes = COALESCE(upvotes, 0) {}, \
                                 downvotes = COALESCE(downvotes, 0) {}, \
                                 hotness = '{}' \
                                 WHERE id = ?",
                                match v {
                                    Vote::Up => "+ 1",
                                    Vote::Down => "+ 0",
                                },
                                match v {
                                    Vote::Up => "+ 0",
                                    Vote::Down => "+ 1",
                                },
                                score + match v {
                                    Vote::Up => 1.0,
                                    Vote::Down => -1.0,
                                }
                            ),
                            (story,),
                        )
                    })
                    .and_then(|t| t.commit())
            })
            .map(|c| (c, false)),
    )
}