extern crate timely;
extern crate differential_dataflow;
extern crate rand;
use serde_json::json;

use differential_dataflow::input::InputSession;
use differential_dataflow::operators::{CountTotal,Count};
use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::operators::join::JoinCore;

// A differential version of item-based collaborative filtering using Jaccard similarity for
// comparing item interaction histories. See Algorithm 1 in https://ssc.io/pdf/amnesia.pdf
// for details.
fn main() {

    timely::execute_from_args(std::env::args(), move |worker| {

        let mut interactions_input = InputSession::new();

        let probe = worker.dataflow(|scope| {

            let interactions = interactions_input.to_collection(scope);

            interactions.inspect(|(data, time, change)| {

                let (user, item) = data;

                let json = json!({
                    "data": "interactions",
                    "user": user,
                    "item": item,
                    "time": time,
                    "change": change
                });

                println!("{}", json);
            });



            let num_interactions_per_item = interactions
                .map(|(_user, item)| item)
                .count_total();

            num_interactions_per_item.inspect(|(data, time, change)| {

                let (item, count) = data;

                let json = json!({
                    "data": "item_interactions_n",
                    "item": item,
                    "count": count,
                    "time": time,
                    "change": change
                });

                println!("{}", json);
            });

            let arranged_remaining_interactions = interactions.arrange_by_key();

            // Compute the number of cooccurrences of each item pair
            let cooccurrences = arranged_remaining_interactions
                .join_core(&arranged_remaining_interactions, |_user, &item_a, &item_b| {
                    if item_a > item_b { Some((item_a, item_b)) } else { None }
                })
                .count();

            cooccurrences.inspect(|(data, time, change)| {

                let ((item_a, item_b), num_cooccurrences) = data;

                let json = json!({
                    "data": "cooccurrences_c",
                    "item_a": item_a,
                    "item_b": item_b,
                    "num_cooccurrences": num_cooccurrences,
                    "time": time,
                    "change": change
                });

                println!("{}", json);
            });


            let arranged_num_interactions_per_item = num_interactions_per_item.arrange_by_key();

            // Compute the jaccard similarity between item pairs (= number of users that interacted
            // with both items / number of users that interacted with at least one of the items)
            let jaccard_similarities = cooccurrences
                // Find the number of interactions for item_a
                .map(|((item_a, item_b), num_cooc)| (item_a, (item_b, num_cooc)))
                .join_core(
                    &arranged_num_interactions_per_item,
                    |&item_a, &(item_b, num_cooc), &occ_a| Some((item_b, (item_a, num_cooc, occ_a)))
                )
                // Find the number of interactions for item_b
                .join_core(
                    &arranged_num_interactions_per_item,
                    |&item_b, &(item_a, num_cooc, occ_a), &occ_b| {
                        Some(((item_a, item_b), (num_cooc, occ_a, occ_b)))
                    },
                )
                // Compute Jaccard similarty, has to be done in a map due to the lack of a
                // total order for f64 (which seems to break the consolidation in join)
                .map(|((item_a, item_b), (num_cooc, occ_a, occ_b))| {
                    let jaccard = num_cooc as f64 / (occ_a + occ_b - num_cooc) as f64;
                    ((item_a, item_b), jaccard)
                });

            jaccard_similarities.inspect(|(data, time, change)| {

                let ((item_a, item_b), similarity) = data;

                let json = json!({
                    "data": "similarities_s",
                    "item_a": item_a,
                    "item_b": item_b,
                    "similarity": similarity,
                    "time": time,
                    "change": change
                });

                println!("{}", json);
            });


            jaccard_similarities.probe()
        });

        let interactions: Vec<(u32, u32)> = vec![
            (0, 0), (0, 1), (0, 2),
            (1, 1), (1, 2),
            (2, 0), (2, 1), (2, 3)
        ];

        for (user, item) in interactions.iter() {
            if *user as usize % worker.peers() == worker.index() {
                interactions_input.insert((*user, *item));
            }
        }

        interactions_input.advance_to(1);
        interactions_input.flush();

        worker.step_while(|| probe.less_than(interactions_input.time()));

        let interactions_to_remove: Vec<(u32, u32)> = vec![(1, 1), (1, 2)];

        for (user, item) in interactions_to_remove.iter() {
            if *user as usize % worker.peers() == worker.index() {
                interactions_input.remove((*user, *item));
            }
        }

        interactions_input.advance_to(2);
        interactions_input.flush();

        worker.step_while(|| probe.less_than(interactions_input.time()));

    }).unwrap();
}
