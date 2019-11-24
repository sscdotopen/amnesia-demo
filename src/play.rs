extern crate differential_dataflow;
extern crate timely;

use differential_dataflow::input::InputSession;
use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::trace::{Cursor, TraceReader};
use timely::dataflow::operators::Probe;

use std::fmt::Debug;

fn main() {

    timely::execute_from_args(std::env::args(), move |worker| {

        let mut input = InputSession::new();
        let mut probe = timely::dataflow::operators::probe::Handle::new();

        let mut trace = worker.dataflow(|scope| {
            let arranged = input
                .to_collection(scope)
                .map(|n: u32| (n, n * 5))
                .arrange_by_key();

            arranged.stream.probe_with(&mut probe);

            arranged.trace
        });

        input.insert(1_u32);
        input.advance_to(1);
        input.flush();
        worker.step_while(|| probe.less_than(input.time()));

        collect_diffs(&mut trace, 0);

        input.insert(2_u32);
        input.advance_to(2);
        input.flush();
        worker.step_while(|| probe.less_than(input.time()));

        collect_diffs(&mut trace, 1);

        input.remove(1_u32);
        input.advance_to(3);
        input.flush();
        worker.step_while(|| probe.less_than(input.time()));

        collect_diffs(&mut trace, 2);

    }).unwrap();
}

use std::rc::Rc;
use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::trace::implementations::spine_fueled::Spine;
use differential_dataflow::trace::implementations::ord::OrdValBatch;

fn collect_diffs<K, V>(
    trace: &mut TraceAgent<Spine<K, V, i32, isize, Rc<OrdValBatch<K, V, i32, isize>>>>,
    time_of_interest: i32
)
    where V: Clone + Ord + Debug,
        K: Clone + Ord + Debug
{
    let (mut cursor, storage) = trace.cursor();

    while cursor.key_valid(&storage) {
        while cursor.val_valid(&storage) {

            let key = cursor.key(&storage);
            let value = cursor.val(&storage);

            cursor.map_times(&storage, |time, diff| {
                if *time == time_of_interest {
                //    println!("{:?} {:?}", time_of_interest, *time == time_of_interest);
                    println!("key {:?}, val {:?}, time {:?}, diff {:?}", key, value, time, diff);
                }
            });

            cursor.step_val(&storage);
        }
        cursor.step_key(&storage);
    }
}
