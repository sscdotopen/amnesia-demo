extern crate kafka;
#[macro_use]
extern crate serde_derive;
extern crate serde;
extern crate serde_json;
extern crate timely;
extern crate differential_dataflow;
extern crate rand;

use serde_json::{json, Result};
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
// use kafka::producer::{Producer, Record, RequiredAcks};

use differential_dataflow::input::InputSession;
use differential_dataflow::operators::{CountTotal,Count};
use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::operators::join::JoinCore;

#[derive(Eq, PartialEq, Debug, Serialize, Deserialize)]
enum Change {
    Add,
    Remove
}

#[derive(Debug, Serialize, Deserialize)]
struct ChangeRequest {
    change: Change,
    interactions: Vec<(u32,u32)>,
}


// A differential version of item-based collaborative filtering using Jaccard similarity for
// comparing item interaction histories. See Algorithm 1 in https://ssc.io/pdf/amnesia.pdf
// for details.
fn main() {

// TODO need to figure out how to safely put this in...
//    let producer =
//        Producer::from_hosts(vec!("localhost:9092".to_owned()))
//            .with_ack_timeout(Duration::from_secs(1))
//            .with_required_acks(RequiredAcks::One)
//            .create()
//            .unwrap();

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

                //producer.send(&Record::from_value("changes", json.to_string().as_bytes())).unwrap();

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

        let mut step: usize = 0;

        // {"change":"Add", "interactions":[[1,1], [1,2], [2,0], [2,1], [2,3], [3,1], [3,3]]}
        // {"change":"Remove", "interactions":[[1,1], [1,2]]}

        let mut consumer =
            Consumer::from_hosts(vec!("localhost:9092".to_owned()))
                .with_topic("interactions".to_owned())
                .with_fallback_offset(FetchOffset::Latest)
                .with_offset_storage(GroupOffsetStorage::Kafka)
                .create()
                .unwrap();

        loop {
            for message_set in consumer.poll().unwrap().iter() {
                for message in message_set.messages() {

                    let parsed_request: Result<ChangeRequest> =
                        serde_json::from_slice(&message.value);

                    match parsed_request {
                        Ok(request) => {

                            step += 1;

                            println!("Received request: {:?}", request);

                            if request.change == Change::Add {
                                for (user, item) in request.interactions.iter() {
                                    interactions_input.insert((*user, *item));
                                }
                            } else {
                                for (user, item) in request.interactions.iter() {
                                    interactions_input.remove((*user, *item));
                                }
                            }

                            interactions_input.advance_to(step);
                            interactions_input.flush();

                            worker.step_while(|| probe.less_than(interactions_input.time()));

                        },
                        Err(_) => println!("Error parsing request..."),
                    }

                }
                consumer.consume_messageset(message_set).unwrap();
            }
            consumer.commit_consumed().unwrap();
        }

    }).unwrap();

}
