# Operating ori-gateway

## Evidence delivery blocked

The courier delivers each device's artifacts to the evidence authority in two
lanes per device, each oldest first: the device's anchor registrations, and
every other artifact it delivers. A hold, a wait or a back-off in one lane
never delays the device's other lane or another device, so a registration
waiting on commissioning never stalls evidence under an epoch already
confirmed.

When the evidence authority refuses the head of a device's evidence lane for
good, the courier holds it: the artifact is not discarded, the running gateway
does not send it again, and every later artifact **in that lane of that
device** waits behind it. Once the hold has been written to disk, no restart
sends it again either; until then, each restart sends it once, as described
under *The hold survives a restart*. The gateway reports `blocked` only when
every pending artifact is in a held lane; one held lane beside others still
delivering is reported `degraded`.

A registration refused for good is **held** in its lane, like evidence: the
evidence authority's refusal is terminal, and without a retention the authority
has verifiably kept, nothing leaves the gateway's custody
(`evidence-transport/v2`). A held registration blocks later registrations of the
same device until it is resolved, and never the device's evidence lane or
another device. Archiving a refusal the authority retained, and reporting it
as an incident, are not implemented yet.

### Capacity and admission

`evidence.max_items` and `evidence.max_bytes` are shared equally across
`gateway.device_ids`: each device may hold at most `max_items / N` records and
`max_bytes / N` bytes across its two lanes. Every share reserves one item and
the bytes of one maximum-size registration record for registrations only:
evidence is refused `queue_full` before it would take that reserve, so a
device whose evidence is waiting on an unconfirmed epoch can always admit the
registration that confirms it. A device past its own share is refused
`queue_full` at admission, so a held or flooded device cannot take another
device's capacity. A `gateway.device_ids` entry carrying a control character
(NUL among them) is always refused at startup. With the courier enabled, each
entry must also be 1 to 128 Unicode characters with no whitespace, `/`, `+` or
`#`; the configuration is refused at startup otherwise, since every artifact of
such a device would be refused at admission.
The configuration is refused at startup if a share is under two items or the
encoded bytes of one maximum-size registration record plus one maximum-size
evidence record (2,796,604 bytes).
With the defaults (`max_items: 10000`, `max_bytes: 268435456`) and four devices,
each device has 2,500 records and 64 MiB. A queue written while capacity was
gateway-wide opens unchanged even if one device holds more than its new share;
that device's admissions are refused until it drains below it.

An artifact larger than 1,048,576 bytes is refused `malformed` at admission,
before custody, because no evidence authority accepts it. A carriage whose
topic names a device not in `gateway.device_ids` is rejected before admission:
nothing is admitted, no custody is issued, and no acknowledgement is published,
so the runtime keeps the bytes and retries. `queue_full` means only that a
configured device's own share is full. The rejection raises
`device_unconfigured` (below); add the device to `gateway.device_ids` and
restart, and the fault clears on that device's next admitted handoff.

The courier never reads an artifact's declared version (`v`). It reads only
the routing fields — `device_id`, and `local_seq` for a delivery envelope — so
an artifact version it does not know is admitted and delivered, and the
evidence authority alone decides whether it supports it. An artifact is refused
`malformed` at admission only when those routing fields are missing or cannot
be parsed.

### Which refusals block

A refusal is classified in one place in the code, from the refusal policy
table in `ori-specs` `evidence-transport/v2.md`. Each status admits only its own
closed list of reasons. A reason the status does not admit, including none, is
recorded as `unrecognised` and takes that status's fail-closed action, so an
invalid pair never shows as a valid-looking reason. The `retriable` flag the
evidence authority sends is never used to select a well-formed refusal's class.

| Status | Reasons the status admits | Action | Any other reason |
| --- | --- | --- | --- |
| `400` | `malformed` | Hold | Hold, recorded `unrecognised` |
| `401` | `malformed`, `unknown_key`, `bad_authenticator`, `stale`, `replay` | Retry on the device's next handoff only | Same, recorded `unrecognised` |
| `403` | `not_authorized` | Retry on the device's next handoff only | Same, recorded `unrecognised` |
| `409` | `conflict` | Hold | Hold, recorded `unrecognised` |
| `409` | `pending_registration_conflict` | Back off, in the registration lane only | — |
| `422` | `bad_authenticator`, `binding_mismatch`, `commissioning_digest_mismatch` | Hold | Hold, recorded `unrecognised` |
| `422` | `unrecognised_version`, `unknown_key` | Retry on the device's next handoff only | — |
| `429` | `rate_limited`, `pending_registration_limit` | Back off, never sooner than `Retry-After` | Back off, never sooner than a parseable `Retry-After`, recorded `unrecognised` |
| `503` | `unavailable` | Back off | Same, recorded `unrecognised` |

A `refused_retained` response, a `507`, and a `503 retention_capacity_unavailable`
are not handled yet: each is treated as an unrecognised outcome, kept and backed
off, never retired or discarded.

`400 unknown_key`, `409 unknown_key` and `422 malformed` are invalid output
from the evidence authority — malformed bytes are a `400` — so they hold and are
recorded as `unrecognised`. An artifact of an unsupported version is
`422 unrecognised_version`: it waits for the device's next handoff, or a
restart, and is delivered once the evidence authority supports it. `wrong_purpose`, `retired_key`, `unknown_sequence` and
`non_contiguous_range` are reasons on the runtime return path, not on this hop;
at `422` they hold as `unrecognised`.

`409 pending_registration_conflict` means the evidence authority holds a
different registration pending under the same commissioning reference. The
registration is kept and backed off in its own lane until that one is applied
or refused; the device's other artifacts keep flowing. Site health reports the
registration lane `backing_off`, and the stalled line carries the reason.
`429 pending_registration_limit` means the evidence authority already holds as
many pending registrations for the device as it keeps; the registration waits
at least `Retry-After`, and without a positive whole-second `Retry-After` the
response is an unrecognised outcome and backs off.

A registration accepted pending (`200` with no epoch confirmation) is
delivered: its entry retires, and the runtime re-offers the same bytes until a
confirmation reaches it. Each re-offer is a new admission. A `200` with an epoch
confirmation stages it for the runtime. The two are told apart only by the
declared type of the returned authority artifacts.

A handoff is a newly admitted artifact for the same device (for both of its
lanes), a `200` retiring an anchor registration for the same device, with or
without a confirmation (for its
evidence lane), or any start of the courier. A handoff for one device never
retries another device's refusal, and a handoff never brings a back-off
forward.

Anything outside the table is an **unrecognised outcome**: a status the table
does not name (such as `404`, `405`, `413` or `500`), a response whose media
type, body, fields, digest binding or outcome is invalid or absent, a
`429 rate_limited` or `429 pending_registration_limit` without `Retry-After`,
and any `2xx` that is not a clean
`200` acceptance — including a `200` marked `retriable: true` or missing the
receipt an envelope requires. The artifact is kept, recorded `unrecognised`,
reported degraded, and retried after a back-off that doubles from its base up
to its bound. It is
never retired, never held, and never resent immediately.

Once a device declares `gateway-evidence-carriage/v1`, the base and bound are
`evidence.backoff_base_s` (default: the delivery interval, `retry_interval_s`)
and `evidence.backoff_max_s` (default: 300, or the base if that is longer), and
the configuration is refused at startup unless
`backoff_max_s >= backoff_base_s >= retry_interval_s`. A site that declares
nothing does not consume those keys: its base is the delivery interval and its
bound 300 seconds, or the delivery interval if longer. Every back-off waits at
least the delivery interval, and a `Retry-After` is an additional floor.

Back-offs are persisted beside the queue record
(`.ori-evidence-backoff-<queue_record>`), so a restart waits out the rest of a
back-off rather than resending early.

A back-off waits until its deadline has passed on both the wall clock and the
monotonic clock, so a wall clock that steps forward never shortens it. A wall
clock that steps backwards stretches the wait only up to the longest the
back-off could have been: `backoff_max_s`, or its `Retry-After` if longer.
That shortened deadline is written back to the back-off record, so a restart
restores it rather than bounding the original deadline afresh. If the write
fails, the shortened deadline holds in the running gateway and the store
reports `store_unavailable`; a restart before a successful write may then wait
the bound again.
After a restart the wall clock is the only record of the time that passed, so
the rest of the wait it shows is held on both clocks from then on.

A back-off record without a `Retry-After` is written as version 1. One with a
`Retry-After` is written as version 2, which also records it. A gateway that
predates version 2 refuses to open a queue holding a version 2 record. Before
rolling back to such a gateway, stop the gateway, wait until the
`not_before_ms` of every `.ori-evidence-backoff-*` file whose `"v"` is `2` has
passed, then remove those files. Removing one sooner lets the older gateway
retry before the `Retry-After`.

A refusal that does not hold leaves the artifact at the head of its lane. Site
health reports that lane `waiting_handoff` or `backing_off`, and the later
artifacts in that lane wait until it is delivered. A `400` refused before
authentication carries no artifact digest and is classified the same way as one
that does.

`evidence-transport/v2` defines `409 conflict` as a real conflict with state the
evidence authority holds, never its own storage failure, which is
`503 unavailable`. An authority that answers `409` for a failure on its own side
is not conformant. Before quarantining a `409`, confirm with the evidence
authority's operator that the conflict is real; if it was not, use *Retrying a
held artifact once* below.

### Authority artifacts returned to the runtime

Receipts, epoch confirmations and custody acknowledgements the gateway carries
back to a runtime retire only on the runtime's authenticated acknowledgement,
as `gateway-evidence-carriage/v1` defines it. `applied` retires. A refusal for
`malformed`, `bad_authenticator`, `binding_mismatch`, `non_contiguous_range`,
`superseded` or `restatement_conflict` retires. A refusal for
`unrecognised_version`, `wrong_purpose`, `unknown_key`, `retired_key` or
`unknown_sequence` keeps the artifact, and so does a refusal naming any other
reason, which logs `evidence inbound refusal reason unrecognised` with the
reason quoted and cut at 64 bytes. A kept artifact is carried again when the
runtime it is for next hands off a new artifact the gateway admits, or on a
gateway restart, never on `evidence.retry_interval_s`. Another runtime's
handoff, a carriage the gateway refuses, and a runtime carrying again an
artifact the gateway already queued, do not carry it again. An acknowledgement that
does not verify, does not name the queued artifact, or is missing a member or
carries one with the wrong JSON type, such as a null `reason`, is not a
decision: the
artifact is carried again on the retry interval in a newly signed envelope.

The gateway publishes each returned artifact in its canonical JSON form, and
the runtime's acknowledgement names it by the SHA-256 of those canonical bytes.
The gateway stages no authority artifact whose canonical form is over 1,048,576
bytes, the most the inbound topic can carry. Today the evidence channel already
refuses any authority response over 1,048,576 bytes before reading it, so such
an artifact cannot arrive; the staging bound stands behind that. Either way the
attempt is an unrecognised outcome: the outbound entry stays queued and backs
off, artifacts staged before the refusal stay staged, and
`gateway.evidence_delivery` reads degraded. The device's other lane and every
other device keep delivering.

An authority artifact that could not be carried to the runtime without
breaking gateway-api/v2's Payload Member Rule, such as one naming a member
twice, is not staged either. The gateway logs `evidence authority artifact
refused under the payload member rule` with its `artifact_type` and `reason`,
and the attempt is handled exactly as for an oversized artifact: unrecognised,
the outbound entry kept and backed off, artifacts staged before it kept, and
`gateway.evidence_delivery` degraded. An artifact that is malformed or names
another device stays an unrecognised outcome as before.

The return queue has one head for the whole site, so while an artifact is
kept every other runtime's returns wait behind it, custody acknowledgements
included. If the runtime it is for has stopped handing off, for example while
it is offline, restart the gateway once the condition behind the refusal is
repaired; the restart carries the kept artifact again. A restart does not
help when that runtime will never acknowledge again, because it was
decommissioned or now runs under another device ID: its artifact is carried
on every retry interval and the site's returns stay blocked behind it.

### How to see it

The gateway log carries the state whether or not `site_health` is enabled.

| Level | Message | When |
| --- | --- | --- |
| ERROR | `evidence delivery blocked: the evidence authority permanently refused the queue head` | once, when the head is refused |
| ERROR | `evidence delivery blocked: restored the hold on a permanently refused artifact without sending it` | once per start, when the head is held from an earlier refusal |
| ERROR | `evidence delivery still blocked behind a permanently refused artifact` | every 15 minutes while it stays held |
| ERROR | `evidence delivery hold was not persisted: a restart will send the refused artifact once more` | once, when the hold could not be written; the running gateway still holds the artifact |
| WARN | `evidence delivery unblocked: the refused artifact is no longer at the queue head` | once, if the held artifact leaves the head while the process runs |
| WARN | `evidence delivery stalled: the queue head was not delivered` | once, on the first failure after a success, on a new head, or when the head starts waiting for a handoff |
| WARN | `evidence delivery still stalled at the queue head` | at most every 15 minutes while it stays stalled |
| INFO | `evidence delivery resumed` | once, when a stalled head is delivered |

The blocked lines carry:

- `artifact_digest` — `sha256:` over the artifact bytes, the digest the evidence
  authority records;
- `queue_record` — the name of the record in the queue directory, without its
  `.json` suffix;
- `artifact_type`, `device_id` and `lane`;
- `refusal_status` and `reason` — the reason when the refusal's status admits
  it (the table above), otherwise `unrecognised`, so nothing the far end
  chooses reaches the log and an invalid pair never looks valid;
- `first_held_at` — when this artifact was first refused, carried across
  restarts;
- `acknowledged` — always `false`: no acknowledgement command exists yet;
- `hold_persisted` — whether the hold was written beside the record;
- `queued_behind` — how many artifacts are waiting behind it in its lane.

The stalled lines carry `artifact_digest`, `queue_record`, `device_id`, `lane`,
`failure` (the gateway's own closed vocabulary, such as `channel_unavailable`
or `malformed_channel_response`), `reason`, `retry` (`backoff`, or
`next_handoff` for a refusal that waits for the device's next handoff) and
`queued`, that lane's queue depth. Transport error text never reaches the
log.

With `site_health` enabled, `gateway.evidence_delivery` reports the courier per
device and lane, as `gateway-evidence-carriage/v1` defines it:

- `devices` lists one entry for each device and lane whose delivery is not
  clean, with its `device_id`, `lane` (`registration` or `evidence`),
  `pending` (that lane's queued artifacts) and `state` (`held`,
  `waiting_handoff` or `backing_off`); a lane delivering cleanly has no entry;
- a `held` entry carries `held`: the `queue_record`, `artifact_digest`,
  `artifact_type`, `refusal_status`, `reason`, `first_held_at_ms` and
  `acknowledged` (always `false`) of the refused head;
- `faults` lists the courier-level faults active now, empty when none. No
  fault expires on its own:
  - `store_unavailable` while a durable store (outbound or return) cannot be
    read, written or atomically updated. Each store is probed right after it
    opens and then every `evidence.store_probe_interval_s` (300 to 900 seconds,
    default 900) once a device declares `gateway-evidence-carriage/v1`, or every
    900 seconds on a site that declares nothing, even when nothing is being
    delivered, so an idle store that has become unwritable is reported within
    that interval. The probe reads the
    directory and atomically replaces `.ori-evidence-probe` in it; it never
    touches a queue record. A failed probe or a failed store operation raises
    it; only that store's next successful probe clears it, so after the store
    recovers it can stay raised for up to one probe interval, and a
    successful admission or other write never clears it;
  - `admission_failed` while evidence for a configured device could not be
    durably queued, or a delivery envelope's custody acknowledgement could not
    be durably staged. It clears only when a later admission of the same kind
    completes; a probe never clears it. A failure to publish the
    acknowledgement after the artifact is durably queued is not this fault:
    the runtime republishes and receives the same entry;
  - `device_unconfigured` while the gateway has seen outbound carriage for a
    device it is not configured for. The device is not named in site health;
    it is recorded, owner-only, in `.ori-evidence-unconfigured` in the outbound
    queue directory and survives a restart. It clears only when that device is
    configured and a later handoff for it is admitted. Past 256 distinct
    devices the record only counts overflow, which no admission clears; remove
    the file with the gateway stopped once the topic ACLs are corrected;
  - `delivery_impaired` is never raised by this gateway, because it always
    reports `devices` in full;
- `blocked` is true only when every pending artifact is in a listed lane and
  every listed lane is held; a fault never makes it true;
- `degraded` is true whenever `faults` or `devices` is not empty;
- `incidents` and `stopped_custody` are not projected yet.

It also keeps `last_error` and `last_failure_at_ms`, a one-event summary of the
most recent delivery failure on any device. `last_error` is not the fault state:
an empty `last_error` never means the courier is healthy. The gateway heartbeat on `ori/gateway/health` does not carry
evidence delivery state.

### Rolling back

A gateway built before persisted holds refuses to open a queue directory that
contains `.ori-evidence-hold-*`, `.ori-evidence-backoff-*`,
`.ori-evidence-probe` or `.ori-evidence-unconfigured`. With the gateway
stopped, `.ori-evidence-probe` and `.ori-evidence-unconfigured` may be deleted;
neither holds evidence. Moving hold and back-off records out returns their
heads to the older gateway's behaviour, which resends them; the queue records
themselves are unchanged and must stay. This gateway writes no archive record,
and refuses to open a queue directory that contains one.

### The hold survives a restart

When the head is refused, the courier writes a refused-head record beside the
queue record: `.ori-evidence-hold-<queue_record>` in the queue directory,
owner-only and written atomically. It carries the queue record, the artifact
digest and type, the refusal status and the reason recorded for it, the time the
artifact was first refused, and `acknowledged: false`.

A restart reads it and restores the hold without sending the artifact again.
If the refused-head record cannot be read, does not parse, names a queue record
that is not there, or disagrees with that record's bytes, the queue refuses to
open and the gateway does not start; the error names the queue record. Nothing
is deleted. A queue written before refused-head records existed opens as it did
before, and its head is sent once.

The hold is written after the refusal arrives, so there is a short window in
which it is not yet on disk. The bound is per process: each gateway process
that starts without a durable hold — because the one before it stopped inside
that window, through a crash or a power loss, or because its hold write failed,
which the log reports as `evidence delivery hold was not persisted` — sends
the artifact once and is refused again. Once a hold is durably written, no
later start sends it. If the hold write keeps failing, for example on a full or
read-only filesystem, every start sends the artifact once until the write
succeeds. The artifact is never discarded either way. An operator who sees
several refusals of one digest in the evidence authority's audit, each around a
gateway restart, is looking at this window, not at a courier re-sending a held
artifact; repeated `not persisted` lines point at the queue's filesystem.

A crash while the hold is being written leaves at most a temporary file, which
the queue removes when it next opens; it is never read as a hold.

### Retrying a held artifact once

Only do this when the evidence authority's operator has confirmed that the
refusal was a failure on its side, not a verdict on the bytes, and that it is
repaired. With the gateway stopped, move the
refused-head record — not the queue record — into the quarantine directory
described below, then start the gateway. It sends the artifact once; a second
refusal holds it again with a new refused-head record.

```bash
sudo env \
  QUEUE=/var/lib/ori-gateway/evidence-outbound \
  QUARANTINE=/var/lib/ori-gateway/evidence-quarantine \
  RECORD='<queue_record from the log line>' \
  bash -eu -s <<'EOF'
fail() { echo "STOP: $*" >&2; exit 1; }
case "$RECORD" in ''|*[!0-9a-f]*) fail "RECORD must be the queue_record from the log line";; esac
[ "${#RECORD}" -eq 64 ] || fail "RECORD must be 64 hexadecimal characters"
[ -f "$QUEUE/.ori-evidence-queue-v1" ] || fail "QUEUE is not an evidence queue directory"
[ -d "$QUARANTINE" ] || fail "quarantine directory does not exist"
[ "$(stat -c %d "$QUARANTINE")" = "$(stat -c %d "$QUEUE")" ] || fail "quarantine is on another filesystem"
HOLD=".ori-evidence-hold-$RECORD"
[ -f "$QUEUE/$HOLD" ] || fail "no refused-head record for $RECORD"
[ ! -e "$QUARANTINE/$HOLD" ] || fail "quarantine already has $HOLD; move it aside first"
mv -n "$QUEUE/$HOLD" "$QUARANTINE/$HOLD"
[ ! -e "$QUEUE/$HOLD" ] || fail "refused-head record is still in the queue"
[ -f "$QUARANTINE/$HOLD" ] || fail "refused-head record did not reach quarantine"
echo "OK: the next start sends $RECORD once"
EOF
```

### Getting out of blocked

A hold belongs to one device. The steps below release that device only; other
devices are not stopped by the hold and are not touched by the procedure, though
the gateway process as a whole is stopped while the files are moved.

There is no command for this. The gateway has no operator CLI for its queues.
The procedure moves the held record, and its refused-head record, into a
quarantine directory with the gateway stopped. The record is the only copy the
site holds of that artifact — for a checkpoint or anchor registration the
runtime retired its own copy once the gateway queued it — so it is moved, never
deleted, and every check is made by the shell rather than by eye.

The commands are for the Linux hosts the gateway is deployed on (GNU coreutils,
bash and `python3`). The paths are the defaults in `gateway.yaml.example`; use
the value of `evidence.queue_directory` from your configuration. `GATEWAY_USER`
and `GATEWAY_GROUP` are the account the gateway runs as.

1. Decide whether the artifact should be delivered at all. A `409` is a
   conflict with what the evidence authority already holds; resolving it is a
   matter for whoever operates the evidence authority, not for the site, and it
   may be a failure on the authority's side (see above). A deliberately forged
   or corrupted artifact will never be accepted.
2. Stop the gateway.
3. Once per host, create the quarantine directory: owner-only, owned by the
   gateway account, **outside** the queue directory and on the **same
   filesystem**, so every move is a rename and never a copy.

   ```bash
   sudo install -d -m 0700 -o "$GATEWAY_USER" -g "$GATEWAY_GROUP" /var/lib/ori-gateway/evidence-quarantine
   ```

4. Run the move with `RECORD` and `DIGEST` copied from the blocked log line.
   If any check fails it prints `STOP:` and exits non-zero, before moving
   anything wherever the check allows.

   ```bash
   sudo env \
     QUEUE=/var/lib/ori-gateway/evidence-outbound \
     QUARANTINE=/var/lib/ori-gateway/evidence-quarantine \
     RECORD='<queue_record from the log line>' \
     DIGEST='<artifact_digest from the log line>' \
     bash -eu -s <<'EOF'
   fail() { echo "STOP: $*" >&2; exit 1; }
   case "$RECORD" in ''|*[!0-9a-f]*) fail "RECORD must be the queue_record from the log line";; esac
   [ "${#RECORD}" -eq 64 ] || fail "RECORD must be 64 hexadecimal characters"
   case "$DIGEST" in sha256:*) ;; *) fail "DIGEST must be the artifact_digest from the log line";; esac
   [ "${#DIGEST}" -eq 71 ] || fail "DIGEST must be sha256: and 64 hexadecimal characters"
   [ -f "$QUEUE/.ori-evidence-queue-v1" ] || fail "QUEUE is not an evidence queue directory"
   [ -d "$QUARANTINE" ] || fail "quarantine directory does not exist"
   [ "$(stat -c %a "$QUARANTINE")" = 700 ] || fail "quarantine directory is not mode 700"
   [ "$(stat -c %U:%G "$QUARANTINE")" = "$(stat -c %U:%G "$QUEUE")" ] || fail "quarantine directory is not owned like the queue"
   [ "$(stat -c %d "$QUARANTINE")" = "$(stat -c %d "$QUEUE")" ] || fail "quarantine is on another filesystem"
   SRC="$QUEUE/$RECORD.json"
   DST="$QUARANTINE/$RECORD.json"
   SIDECARS=".ori-evidence-hold-$RECORD .ori-evidence-backoff-$RECORD"
   [ -f "$SRC" ] || fail "record $RECORD is not in the queue"
   [ ! -e "$DST" ] || fail "quarantine already has $RECORD.json; move the earlier copy aside first"
   for SIDE in $SIDECARS; do
     [ ! -e "$QUARANTINE/$SIDE" ] || fail "quarantine already has $SIDE; move the earlier copy aside first"
   done
   identity() { python3 -c 'import base64, hashlib, json, sys; r = json.load(open(sys.argv[1])); print("sha256:" + hashlib.sha256(base64.b64decode(r["payload"])).hexdigest(), r["id"])' "$1"; }
   [ "$(identity "$SRC")" = "$DIGEST $RECORD" ] || fail "record in the queue does not match the log line"
   for SIDE in $SIDECARS; do
     if [ -e "$QUEUE/$SIDE" ]; then
       mv -n "$QUEUE/$SIDE" "$QUARANTINE/$SIDE"
       [ ! -e "$QUEUE/$SIDE" ] || fail "$SIDE is still in the queue"
       [ -f "$QUARANTINE/$SIDE" ] || fail "$SIDE did not reach quarantine"
     fi
   done
   mv -n "$SRC" "$DST"
   [ ! -e "$SRC" ] || fail "record is still in the queue"
   [ -f "$DST" ] || fail "record did not reach quarantine"
   [ "$(identity "$DST")" = "$DIGEST $RECORD" ] || fail "quarantined record does not match the log line"
   [ "$(stat -c %a "$DST")" = 600 ] || fail "quarantined record is not mode 600"
   [ "$(stat -c %U:%G "$DST")" = "$(stat -c %U:%G "$QUEUE")" ] || fail "quarantined record is not owned like the queue"
   echo "OK: $RECORD is quarantined; start the gateway"
   EOF
   ```

   The refused-head record, and a back-off record if one exists, move first.
   If the script stops between the moves, the queue record is still queued
   without them and the next start sends it once; it is never left as a hold or
   back-off with no record, which would stop the queue from opening.
5. Start the gateway only after `OK:`. That device's delivery resumes with its
   next artifact, in the original order, and nothing behind the held artifact
   is lost. The new
   process was never blocked, so it writes no unblocked line; the absence of
   blocked lines and a falling queue are the signal.

Do not rename a record, leave a copy inside the queue directory, or leave a
refused-head or back-off record without its queue record. The queue refuses to
open when it finds an entry it does not recognise, a record whose name does not
match its contents, or a refused-head or back-off record that does not match a
queued record.

There is no supported way to send a quarantined artifact again. Moving it back
into the queue directory is not safe: the queue numbers records from the
highest sequence on disk, so a record moved back can collide with a newer one
and stop the queue from opening, and even without a collision it does not
return to its original place. Re-delivery of a quarantined artifact waits for
the quarantine contract.

**Do not delete the queue directory.** It discards every queued artifact, not
only the refused one. Checkpoints and anchor registrations in it are gone for
good, and delivery envelopes the gateway has acknowledged custody of are not
handed over again: the runtime keeps them, awaiting a receipt that can no longer
arrive through this gateway.

Continuing past a refused artifact automatically, acknowledging a hold, or
releasing it with an explicit operator command, is an open decision and not
current behaviour. No acknowledgement command exists yet, so `acknowledged` is
always `false`.

## A runtime node stays critical after it goes silent

A runtime node whose last accepted heartbeat was `critical` keeps that status
when it stops reporting. Silence after `critical` may be the failure itself,
such as lost power, and gateway-api/v2 forbids a projection that folds
`critical` into anything less severe. The site view and `GET /health` show
the node `critical`, with `stale: true` and its last `degradation_reasons`,
and the site status stays `critical`. The gateway log carries one `ERROR`,
`runtime node silent after a critical heartbeat; kept critical until it
reports again`, with the `device_id`, `gateway_seen_ms` and `last_seen_ms`, the
first time the eviction loop finds the node silent, whether or not
`site_health` is enabled. Silent means the gateway has accepted no heartbeat
from the node for three heartbeat intervals, measured on the gateway's own
clock from when it received the last one; the node's `last_seen_ms` plays no
part, so a node whose clock is ahead or behind is not reported silent while it
is still reporting.

A node that goes silent after a `healthy` or `degraded` heartbeat is dropped
from the registry after three heartbeat intervals and shows as unobserved and
`degraded`, as before.

The retention has no time limit, because any time limit would be critical
decaying through silence. It is bounded instead by the configured devices:
the gateway subscribes only to the heartbeat topics of `gateway.device_ids`,
and the handler refuses a heartbeat whose `device_id` differs from its topic,
so at most one retained entry exists per configured device.

It ends in one of three ways:

- **The node reports again.** Its next accepted heartbeat replaces the
  retained one, whatever its status. A node that comes back and goes silent
  after another `critical` is reported again.
- **The node is decommissioned.** Remove its id from `gateway.device_ids` and
  restart the gateway. The node is no longer projected, and nothing more is
  retained for it.
- **The gateway restarts.** The registry is held in memory, so a restart
  clears every retained node, and a node still silent then shows as
  unobserved and `degraded`. Before restarting a gateway for any other reason,
  read `GET /health` or the log for a silent `critical` node and resolve it
  first.

## MQTT payloads refused for size or volume

The gateway refuses a runtime's MQTT payload that is longer than its topic's
maximum in gateway-api/v2 Payload Size before reading any of it, and drops
messages beyond its own rate for each subscription (Payload Volume). A refused
payload is not answered and changes nothing: no reasoning reply, no evidence
custody or acknowledgement, no heartbeat in the site view. The runtime that
sent an outbound evidence carriage keeps its bytes and carries it again.

| Topic family | Maximum (bytes) | Messages per device | Bytes per device |
| --- | --- | --- | --- |
| `ori/{device_id}/runtime/heartbeat` | 65,536 | 1 a second, bursts of 5 | 128 KiB a second, bursts of 256 KiB |
| `ori/{device_id}/reasoning/request` | 262,144 | 10 a second, bursts of 20 | 512 KiB a second, bursts of 1 MiB |
| `ori/{device_id}/tier_c/enrichment/request` | 262,144 | 2 a second, bursts of 10 | 256 KiB a second, bursts of 512 KiB |
| `ori/{device_id}/export/response/{request_id}` | 4,194,304 | 20 a second, bursts of 64 | 4 MiB a second, bursts of 16 MiB |
| `ori/{device_id}/evidence/outbound` | 1,572,864 | 20 a second, bursts of 160 | 2 MiB a second, bursts of 4 MiB |
| `ori/{device_id}/evidence/inbound/ack` | 16,384 | 20 a second, bursts of 64 | 64 KiB a second, bursts of 256 KiB |

A message past either budget is dropped as volume. The byte budget bounds,
per device, the parsing the gateway does before it authenticates a message.
Messages naming a device ID outside `gateway.device_ids` share one more set of
budgets, so a broker client that can publish under any device ID can make the
gateway parse at most the sum of one set per configured device plus that one,
before any of it is authenticated; per-device broker ACLs keep each device to
its own topics. The runtime never resends an export page, so an export page
dropped as volume leaves that request unanswered until its wait bound ends it;
the export budgets are sized so the runtime's four concurrent pages at the
maximum fit.

A rate is kept for each device in `gateway.device_ids`. Messages on the
reasoning and evidence topics that name any other device ID share one rate, so
a sender inventing device IDs cannot grow the gateway's memory or starve a
configured device. A sender that publishes under a configured device's own ID
draws on that device's rate; per-device broker ACLs are what stop that.
Neither the maxima nor the rates have a setting. A broker's own message size
limit should be at most 4,194,304 bytes, so it drops a larger message first.

Each refusal is logged as `WARN`: `mqtt ingress refused payloads over their
topic maximum`, with the `family`, the number `refused`, their `size_classes`
(the least power of two not below each length) and the `maximum`, or `mqtt
ingress dropped messages over the family rate`, with the `family` and the
number `dropped`. A flood writes at most one line a second for each family and
reason, counting every refusal since the last line. No line carries payload
content, a device ID or a topic.

A payload within its size and rate is then held to gateway-api/v2's Payload
Member Rule before anything reads it. A payload that is not valid UTF-8, not
one JSON object, nested deeper than 32, followed by anything but whitespace,
or that names a member twice or spells a member's name in another case (such
as `REQUEST_ID` beside or instead of `request_id`) is refused and not answered.
It is logged as `WARN` `mqtt ingress refused payloads under the payload member
rule`, with the `family`, the `reason` (`invalid_unicode`, `malformed_json`,
`nesting_too_deep`, `trailing_data`, `duplicate_member`, `case_variant_member`
or `not_an_object`) and the number `refused`, paced the same way. These
refusals are not counted in `mqtt_ingress`. A runtime whose messages are
refused this way is producing payloads the contract forbids; the reason names
what to fix.

With `site_health` enabled, `gateway.mqtt_ingress` counts size and volume refusals since the
gateway started, one member for each family above. Abbreviated to one family:

```json
"mqtt_ingress": {
  "ori/{device_id}/reasoning/request": {"payload_too_large": 1, "volume_dropped": 0}
}
```

A non-zero count does not degrade the gateway. A count that keeps rising names
a runtime that is misconfigured or misbehaving; find it from the broker's own
logs, since the gateway never records which device sent a refused message.

## Runtime exports time out or are not verified

The gateway reads runtime data (site posture, weekly reports) through export
requests over MQTT. Each request waits at most 30 seconds, from publishing it,
for a response it can verify, and one export reads at most 100 pages.

- `WARN runtime export request timed out; no verified response arrived`, with
  the `device_id`, `export_type` and `request_id` (or `runtime export response
  subscription timed out`, when the broker did not confirm the subscription in
  time): no verified answer came in time. The runtime may be stopped or unreachable, or, with
  `gateway.auth.enabled`, the gateway and runtime may disagree on the shared
  secret or their clocks may be further apart than the allowed skew: the
  runtime then either leaves the request unanswered or answers in a way the
  gateway cannot verify, so a timeout is the only local sign of either. Check the runtime is running, both sides' secret, and both
  clocks. The request is released; the next posture refresh or report sends a
  new one.
- `WARN runtime export response not verified; dropped`, with the `device_id`
  and `export_type`: a response arrived on the request's topic that did not
  verify. The request keeps waiting for a verified one. Repeated lines while
  exports still complete point to another client publishing on runtime topics;
  check the broker's ACLs.

## SMS webhook bridge refuses requests with 429

The SMS webhook bridge bounds how many provider requests it signs and forwards
to the runtime, after the source check. Each sender (the provider's `from`,
read the runtime's way; a sender in any shape the bridge cannot read that way
shares one allowance with every other such sender) may send `webhook_bridge.sender_per_minute` a
minute, in bursts of `sender_burst` (default 10 and 5, the least allowed,
matching the runtime's own limit on approval replies; a number's remote
commands share the same allowance), and the bridge as a whole forwards at
most `global_per_minute`, in bursts of `global_burst` (default 120 and 20).
A delivery report, which names no sender, counts against the bridge-wide
limit only. A
request past either is answered `429` and not forwarded; the provider may
retry it. The bridge keeps state for at most `max_senders` senders (default
4096), forgetting the least recently seen.

The log carries `WARN webhook bridge refused requests over its rate` with the
`limit` (`sender` or `global`) and the number `refused`, at most once a second
for each. With `site_health` enabled, `gateway.webhook_bridge_refused` counts
`sender` and `global` refusals since the gateway started. Neither names a
sender, a number or message text.

A rising `sender` count is one number texting the site faster than an operator
would. A rising `global` count is many numbers at once, or a burst of delivery
reports: replies from the operator may then be refused at the bridge too until
it eases, without using up the operator's own allowance. The
bridge decides only volume; which senders may approve a proposal is the
runtime's decision.

## Signed messages refused as stale, future or below the replay floor

With `gateway.auth.enabled`, every signed MQTT message the gateway receives
from a runtime is checked for freshness and replay before anything else uses
it, against a skew of 5 minutes. A refusal names one of four reasons:
`stale_timestamp` (signed more than 5 minutes before the gateway's clock),
`future_timestamp` (more than 5 minutes after), `before_replay_floor` (signed
before the gateway's replay floor) or `replay_detected` (a message already
accepted, sent again).

The gateway keeps its replay floor and the messages it has accepted in memory
only. Each start sets the floor to the start time, so a message a runtime
signed just before the gateway started is refused once as
`before_replay_floor`; the runtime's next message is signed afresh and is
accepted. A runtime whose clock runs behind the gateway's has its messages
refused after a start until its clock passes the start time: keep both clocks
synchronised. When a refusal comes from a floor ahead of the clock, the
gateway logs `WARN mqtt auth refused an envelope below the replay floor, which
is ahead of the clock's stale bound` with `floor_ms` and `t_ms`; it clears
once the clock passes the floor.

Every message the gateway signs outside evidence forwarding takes its signing
time from a clock that never issues the same value twice. Its mark is kept in
`gateway.state_directory` (`.ori-signed-at-messages`), at most one second past
the wall clock and written about once a second off the signing path, so a
restart resumes above it even with the clock set back, and a gateway that
restarts over and over never drifts ahead of the wall clock. The state
directory is required when `gateway.auth.enabled` is true. Provision it before
the first start, private to the gateway's user:

```sh
sudo install -d -m 0700 -o "$GATEWAY_USER" -g "$GATEWAY_GROUP" /var/lib/ori-gateway/state
```

or let the gateway create it (0700) under a parent it may write. The gateway
refuses to start when the directory is a symlink, is readable or writable by
group or other users, is owned by another user, is not empty and not marked as
the gateway's (`.ori-gateway-state-v1`), holds a corrupt mark, or cannot be
written. A directory holding only the lock file and staged
`.ori-gateway-state-v1.*` files, as an interrupted first start leaves it, is
adopted. A corrupt marker (`gateway state directory marker is corrupt`) is
not repaired by the gateway, and deleting it does not help: a directory that
already holds the signing mark is then refused as not empty. To recover, stop
the gateway, check that the directory is the gateway's (owned by its user,
holding only `.ori-gateway-state-v1`, `.ori-gateway-state.lock`,
`.ori-signed-at-messages` and staged copies of them), and restore the marker
exactly, as the gateway's user, leaving `.ori-signed-at-messages` in place so
the clock resumes above it:

```sh
sudo -u "$GATEWAY_USER" sh -c 'umask 077; printf "ori-gateway state directory v1\n" > /var/lib/ori-gateway/state/.ori-gateway-state-v1'
```

The state directory must not be, contain, or sit inside an evidence queue
directory, and it
belongs to one gateway: a running gateway holds an exclusive lock on it
(`.ori-gateway-state.lock`), so a second gateway started on the same directory
refuses to start with `gateway state directory is in use by another gateway`.
The lock is released when the gateway exits, however it exits. No file in an
evidence queue directory is read for this clock.

Put the state directory on persistent local storage: a filesystem on the
gateway's own host where `flock` excludes every other process, a rename is
atomic, and a successful sync makes a file durable. NFS and other network or
remote filesystems are not supported, and the gateway does not check this:
there, a gateway on another host could take the lock while a write from this
one is still outstanding and later lower its mark.

A write of the mark that fails, or has not completed within 1,000 ms, opens an
episode: the gateway logs `WARN signing clock mark could not be persisted;
signing goes on uncovered until a write completes` with the cause, once per
episode, reports `degraded` in its heartbeat and in site health, and keeps
signing without waiting on the disk. One write is outstanding at a time and it
is retried no sooner than a second after the previous one started. The episode
ends, logged as `signing clock mark persisted again; every value issued is
covered`, when a write completes that covers every signing time issued. A
restart during an episode can repeat a signing time issued meanwhile, which
runtimes refuse as a replay or, after evicting it, as `before_replay_floor`
until the gateway's clock passes their floor. Outside an episode, a message
whose signing time is past the saved mark waits for the write, no longer than
1,000 ms.

To stop, the gateway first stops signing (a request still being answered then
gets no reply, and its runtime falls back as on any stop) and waits up to 5
seconds for a write of the mark still outstanding. If the write is still
outstanding after that, it logs `ERROR gateway state directory is not
quiescent; ending the process without releasing it` and exits with status 1
without releasing the lock, which ends with the process; restart it as usual
once the disk answers. A start whose first write has not completed within
1,000 ms is refused the same way. On a disk that has stopped answering, the
operating system may not end the process until it does, and the gateway stays
stuck: a second gateway on the same directory is still refused. A mark so
large that the clock would run out (at or above 9223372036854773806) refuses the
start as corrupt or `signing clock exhausted`; it can only be written by hand.

That mark means a clock that once ran fast is not forgotten at a restart: the
gateway keeps signing just above it. While its signing time is a second or
more ahead of the wall clock, every message it signs waits for its mark to be
written to disk first, up to 1,000 ms, so a slow disk slows every signed
message until the wall clock catches up or an episode opens. When it is more than 5 minutes ahead of
the wall clock, runtimes refuse what the gateway signs as `future_timestamp`
(the gateway looks unreachable and Tier 3 falls back), and the gateway logs
`WARN signing clock is ahead of the wall clock by more than receivers' default skew`
with `signed_at_ms` and `wall_ms`. It clears by itself once the wall clock
passes `signed_at_ms`. To clear it sooner, correct the clock, stop the gateway,
delete `.ori-signed-at-messages` from the gateway state directory and start it
again. A message signed at the same time as one before the restart,
with the same content, is then refused once as a replay; and a runtime whose
replay floor was raised past the gateway's earlier signing times refuses the
gateway's messages as `before_replay_floor` until its own clock passes that
floor, at most 5 minutes.

