#!/usr/bin/env python3
"""Fix: throttled path matches against empty book. Must add_limit
passives before executing aggressors, mirroring FBA's clear_batch."""
from pathlib import Path
import sys

f = Path("src/main.rs")
src = f.read_text()

# ─── Patch 3: throttled in-loop flush — separate passive/aggressor ───
OLD3 = '''            if t_us - batch_start_us >= window {
                let mut bucket_orders = std::mem::take(&mut pending);
                queue_mechanisms::sort_queue(&mut bucket_orders, mech);
                for order in bucket_orders.drain(..) {
                    let fills = book.execute_against(&order, mech, batch_id);
                    fill_buffer.extend(fills);
                }
                batch_id += 1;
                batch_start_us = t_us;
            }'''

NEW3 = '''            if t_us - batch_start_us >= window {
                let mut bucket_orders = std::mem::take(&mut pending);
                queue_mechanisms::sort_queue(&mut bucket_orders, mech);
                // passives rest, then aggressors hit the book in sorted order
                let mut aggressors: Vec<Order> = Vec::new();
                for o in bucket_orders.drain(..) {
                    let is_agg = matches!(o.participant,
                        ParticipantType::Retail | ParticipantType::Institutional);
                    if is_agg { aggressors.push(o); } else { book.add_limit(o); }
                }
                for aggr in aggressors {
                    let fills = book.execute_against(&aggr, mech, batch_id);
                    fill_buffer.extend(fills);
                }
                batch_id += 1;
                batch_start_us = t_us;
            }'''

if OLD3 not in src:
    print("ERROR: anchor 3 not found")
    sys.exit(1)
src = src.replace(OLD3, NEW3, 1)
print("OK patch 3: bucket flush separates passive/aggressor")

# ─── Patch 4: final flush — same fix ───
OLD4 = '''        } else {
            // throttled: sort then execute in order
            let mut ordered = batch_orders;
            queue_mechanisms::sort_queue(&mut ordered, mech);
            let mut out = Vec::new();
            for o in ordered {
                out.extend(book.execute_against(&o, mech, batch_id));
            }
            out
        };'''

NEW4 = '''        } else {
            // throttled final flush: passives rest, aggressors hit
            let mut ordered = batch_orders;
            queue_mechanisms::sort_queue(&mut ordered, mech);
            let mut aggressors: Vec<Order> = Vec::new();
            for o in ordered {
                let is_agg = matches!(o.participant,
                    ParticipantType::Retail | ParticipantType::Institutional);
                if is_agg { aggressors.push(o); } else { book.add_limit(o); }
            }
            let mut out = Vec::new();
            for aggr in aggressors {
                out.extend(book.execute_against(&aggr, mech, batch_id));
            }
            out
        };'''

if OLD4 not in src:
    print("ERROR: anchor 4 not found")
    sys.exit(1)
src = src.replace(OLD4, NEW4, 1)
print("OK patch 4: final flush separates passive/aggressor")

f.write_text(src)
print("done")
