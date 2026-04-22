#!/usr/bin/env python3
"""
Patch main.rs to make THROTTLED mechanisms actually throttle.

Before: THROTTLED1MS/10MS/100MS are byte-identical to CLOB because
sort_queue is never called; the `batch_us` is just stuffed into batch_id.

After: THROTTLED variants accumulate orders within a bucket, call
queue_mechanisms::sort_queue to randomize within the bucket, then
run CLOB matching on the shuffled orders. This is the canonical
Budish-et-al throttled-market model.

Also handles the final flush for throttled pending orders.
"""
from pathlib import Path
import sys

f = Path("src/main.rs")
src = f.read_text()

# ─── Patch 1: replace the `} else {` continuous branch ───

OLD1 = '''        } else {
            // Continuous: execute immediately, optional throttle bucket in batch_id
            let seq = match batch_us {
                Some(b) => t_us / b,
                None => batch_id,
            };
            let fills = process_continuous(&mut book, passive, aggressor, mech, seq);
            fill_buffer.extend(fills);
            batch_id = seq;
        }'''

NEW1 = '''        } else if batch_us.is_some() {
            // THROTTLED: accumulate within bucket, randomize at boundary, match CLOB-style.
            // This is the Budish/Cramton/Shim throttled-market model.
            if batch_start_us == 0 { batch_start_us = t_us; }
            let window = batch_us.unwrap();
            if t_us - batch_start_us >= window {
                let mut bucket_orders = std::mem::take(&mut pending);
                queue_mechanisms::sort_queue(&mut bucket_orders, mech);
                for order in bucket_orders.drain(..) {
                    let fills = book.execute_against(&order, mech, batch_id);
                    fill_buffer.extend(fills);
                }
                batch_id += 1;
                batch_start_us = t_us;
            }
            pending.push(passive);
            pending.push(aggressor);
        } else {
            // Pure CLOB: match immediately, no batching
            let fills = process_continuous(&mut book, passive, aggressor, mech, batch_id);
            fill_buffer.extend(fills);
        }'''

if OLD1 not in src:
    print("ERROR: anchor 1 (continuous else branch) not found. File has drifted.")
    sys.exit(1)

src = src.replace(OLD1, NEW1, 1)
print("OK patch 1: continuous branch replaced")

# ─── Patch 2: also drain throttled pending at end-of-stream ───

OLD2 = '''    // Final batch for FBA
    if is_fba && !pending.is_empty() {
        let batch_orders = std::mem::take(&mut pending);
        let fills = clear_fba_batch(&mut book, batch_orders, mech, batch_id);
        fill_buffer.extend(fills);
    }'''

NEW2 = '''    // Final batch for FBA or THROTTLED
    if !pending.is_empty() {
        let batch_orders = std::mem::take(&mut pending);
        let fills = if is_fba {
            clear_fba_batch(&mut book, batch_orders, mech, batch_id)
        } else {
            // throttled: sort then execute in order
            let mut ordered = batch_orders;
            queue_mechanisms::sort_queue(&mut ordered, mech);
            let mut out = Vec::new();
            for o in ordered {
                out.extend(book.execute_against(&o, mech, batch_id));
            }
            out
        };
        fill_buffer.extend(fills);
    }'''

if OLD2 not in src:
    print("ERROR: anchor 2 (final flush) not found. File has drifted.")
    sys.exit(1)

src = src.replace(OLD2, NEW2, 1)
print("OK patch 2: final flush replaced")

f.write_text(src)
print("done — main.rs written")
