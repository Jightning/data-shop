//! Shopping Sessions: rules.md pipeline.
//!
//! Stages (each timed to stderr in microseconds; stdout stays empty):
//!   load     manifest, products, campaigns, and read-only CSV mappings
//!   links    parallel parse and hash-partitioned endpoint interning
//!   events   validate copies and scatter by raw user and event ID. Link union and
//!            canonical naming overlap this scan.
//!   clicks   validate and scatter by raw user
//!   dedup    keep the smallest ingest timestamp for each event ID
//!   resolve  assign canonical identities and move records that change partition
//!   persons  group, sessionize, credit, and write user_daily shards
//!   campaign merge per-worker campaign/day counters and write campaign_daily
//! then `_exit(0)`.
//!
//! Short user and click IDs are inline. Event dedup IDs and long keys can
//! reference the read-only input, so mappings stay live through aggregation.

mod csvp;
mod parquet;
mod ts;
mod util;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::sync::Mutex;
use std::time::Instant;

use parquet::{Column, Kind};
use ts::DAY;
use util::{hash, Bs, Key, KeyMap, StrMap};

const WEEK: i64 = 7 * DAY;
const GAP: i64 = 1_800_000;
const PB: usize = 256; // radix partitions (top 8 hash bits)

#[inline(always)]
fn part(h: u64) -> usize {
    (h >> 56) as usize
}

// ------------------------------------------------------------------ records

/// One event copy for global dedup. The ID points into the read-only input.
#[derive(Clone, Copy)]
struct D {
    idh: u64,
    id: *const u8,
    ingest_and_len: u64,
    loc: u64,
}

// Every D points into a leaked mapping or a leaked read buffer. Workers only read the ID bytes.
unsafe impl Send for D {}

impl D {
    const INGEST_BITS: u32 = 40;
    const INGEST_MASK: u64 = (1 << Self::INGEST_BITS) - 1;
    const LONG_ID: u64 = u64::MAX >> Self::INGEST_BITS;

    /// The ID must come from the first field of a well-formed event row. Its
    /// unquoted bytes are followed by either a closing quote or a comma.
    #[inline(always)]
    unsafe fn new(idh: u64, id: Bs, ingest: i64, loc: u64) -> Self {
        // The valid F1 years 0000..=9999 fit in signed 40-bit epoch
        // seconds. Bias them to preserve chronological order as unsigned.
        let ordered_ingest = if ingest == i64::MAX {
            Self::INGEST_MASK
        } else {
            (ingest / 1000 + (1 << (Self::INGEST_BITS - 1))) as u64
        };
        let len = (id.n as u64).min(Self::LONG_ID);
        Self { idh, id: id.p, ingest_and_len: (len << Self::INGEST_BITS) | ordered_ingest, loc }
    }

    #[inline(always)]
    fn ingest_order(&self) -> u64 {
        self.ingest_and_len & Self::INGEST_MASK
    }

    #[inline]
    fn same_id(&self, other: &Self) -> bool {
        let len = self.ingest_and_len >> Self::INGEST_BITS;
        if len != other.ingest_and_len >> Self::INGEST_BITS {
            return false;
        }
        if len < Self::LONG_ID {
            // Both parsed fields supplied the same byte length.
            return unsafe {
                std::slice::from_raw_parts(self.id, len as usize)
                    == std::slice::from_raw_parts(other.id, len as usize)
            };
        }
        self.same_long_id(other)
    }

    #[cold]
    fn same_long_id(&self, other: &Self) -> bool {
        let mut a = self.id;
        let mut b = other.id;
        loop {
            // Each walk stops at its first delimiter. Quoting syntax was
            // validated by the CSV scanner, and neither byte can be in a value.
            let (x, y) = unsafe { (*a, *b) };
            let end_x = matches!(x, b',' | b'"');
            let end_y = matches!(y, b',' | b'"');
            if end_x || end_y {
                return end_x && end_y;
            }
            if x != y {
                return false;
            }
            unsafe {
                a = a.add(1);
                b = b.add(1);
            }
        }
    }
}
const NONE: u64 = u64::MAX;
/// Reserved price for an event eliminated by dedup or moved to another partition.
const DEAD: u32 = u32::MAX;
/// A resolved event whose hash field now holds a canonical name index.
const LINKED: u32 = 1 << 31;
const NO_PERSON: u32 = u32::MAX;
/// Clicks cannot use u32::MAX because an index is reserved for organic.
const MOVED_CLICK: u32 = u32::MAX;

/// Common windows use offsets from a shared origin. Wide windows keep epoch milliseconds.
trait Stamp: Copy + Default + Ord + Send {
    fn encode(ts: i64, origin: i64) -> Self;
    fn decode(self, origin: i64) -> i64;
}

impl Stamp for u32 {
    #[inline(always)]
    fn encode(ts: i64, origin: i64) -> Self {
        (ts - origin) as u32
    }

    #[inline(always)]
    fn decode(self, origin: i64) -> i64 {
        origin + self as i64
    }
}

impl Stamp for i64 {
    #[inline(always)]
    fn encode(ts: i64, _: i64) -> Self {
        ts
    }

    #[inline(always)]
    fn decode(self, _: i64) -> i64 {
        self
    }
}

/// A surviving event keyed by raw user or canonical person. 32 or 40 bytes.
#[derive(Clone, Copy)]
struct PE<T> {
    h: u32,
    key: Key,
    ts: T,
    price: u32,
    cat: u32,
}

/// A valid click keyed by raw user or canonical person. 48 or 56 bytes.
#[derive(Clone, Copy)]
struct PC<T> {
    h: u32,
    key: Key,
    ts: T,
    id: Key,
    camp: u32,
    person: u32,
}

/// Per-person compact event for sorting.
#[derive(Clone, Copy)]
struct SE<T> {
    ts: T,
    price: u32,
    cat: u32,
}

/// Per-person compact click for sorting.
#[derive(Clone, Copy)]
struct SC<T> {
    ts: T,
    id: Key,
    camp: u32,
}

// ------------------------------------------------------------------ context

struct Ctx {
    ws: i64,
    we: i64,
    cws: i64,
    products: StrMap<u32>,
    cat_names: Vec<&'static [u8]>, // index = cat id; last is "unknown"
    cat_rank: Vec<u32>,            // byte-wise rank of each category name
    unknown: u32,
    campaigns: StrMap<u32>,
    camp_ids: Vec<&'static [u8]>, // index = campaign id; last is "organic"
    camp_channels: Vec<&'static [u8]>,
    organic: u32,
    day0: i64,
    ndays: usize,
}

/// A canonical name's stable index within its hash partition.
#[derive(Clone, Copy, Default)]
struct LinkedPerson {
    index: u32,
    partition: u8,
}

/// Link lookup for the IDs whose hash falls in one partition.
struct LinkPart {
    map: KeyMap<u32>,
    persons: Vec<LinkedPerson>,
}

struct Links {
    aliases: Vec<LinkPart>,
    names: Vec<Vec<Key>>,
}

struct LinkEndpoints {
    pair_ids: Vec<u64>,
    base: Vec<usize>,
    keys: Vec<Key>,
    maps: Vec<KeyMap<u32>>,
}

fn run_par<R: Send>(nt: usize, f: impl Fn(usize) -> R + Sync) -> Vec<R> {
    std::thread::scope(|s| {
        let f = &f;
        let hs: Vec<_> = (0..nt).map(|t| s.spawn(move || f(t))).collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    })
}

/// Raw pointer for disjoint parallel writes.
#[derive(Clone, Copy)]
struct SyncPtr<T>(*mut T);
unsafe impl<T> Send for SyncPtr<T> {}
unsafe impl<T> Sync for SyncPtr<T> {}
impl<T> SyncPtr<T> {
    #[inline(always)]
    fn at(self, i: usize) -> *mut T {
        unsafe { self.0.add(i) }
    }
}

fn leak_read(path: &Path) -> &'static [u8] {
    match std::fs::read(path) {
        Ok(v) => Box::leak(v.into_boxed_slice()),
        Err(_) => &[],
    }
}

/// Map a file read-only; first consumers fault in its pages.
fn map_file(path: &Path) -> &'static [u8] {
    use std::os::unix::io::AsRawFd;
    let f = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(_) => return &[],
    };
    let len = f.metadata().map(|m| m.len() as usize).unwrap_or(0);
    if len == 0 {
        return &[];
    }
    unsafe {
        let p = libc::mmap(
            std::ptr::null_mut(),
            len,
            libc::PROT_READ,
            libc::MAP_PRIVATE,
            f.as_raw_fd(),
            0,
        );
        if p == libc::MAP_FAILED {
            return leak_read(path);
        }
        std::slice::from_raw_parts(p as *const u8, len)
    }
}

fn list_csv(dir: &Path) -> Vec<(PathBuf, u64)> {
    let mut files: Vec<_> = match std::fs::read_dir(dir) {
        Ok(entries) => entries
            .filter_map(Result::ok)
            .filter_map(|entry| {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if !name.ends_with(".csv") || name.starts_with('.') {
                    return None;
                }
                let path = entry.path();
                let metadata = std::fs::metadata(&path).ok()?;
                metadata.is_file().then_some((path, metadata.len()))
            })
            .collect(),
        Err(_) => Vec::new(),
    };
    files.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    files
}

/// Splits each file's data (after its header line) into line-aligned chunks of ~`target` bytes.
fn make_chunks(files: &[&'static [u8]], target: usize) -> Vec<&'static [u8]> {
    let mut out = Vec::new();
    for &f in files {
        let start = match memchr::memchr(b'\n', f) {
            Some(i) => i + 1,
            None => continue, // header only (or empty)
        };
        let mut s = start;
        while s < f.len() {
            let mut e = (s + target).min(f.len());
            if e < f.len() {
                e = match memchr::memchr(b'\n', &f[e..]) {
                    Some(i) => e + i + 1,
                    None => f.len(),
                };
            }
            out.push(&f[s..e]);
            s = e;
        }
    }
    out.sort_by_key(|c| std::cmp::Reverse(c.len()));
    out
}

/// Estimated row count of a set of files, from the line density of a sample of the largest.
fn est_rows(files: &[&'static [u8]]) -> usize {
    let total: usize = files.iter().map(|f| f.len()).sum();
    let Some(big) = files.iter().max_by_key(|f| f.len()) else { return 0 };
    let sample = &big[..big.len().min(1 << 20)];
    let lines = memchr::memchr_iter(b'\n', sample).count().max(1);
    total / (sample.len() / lines).max(1) + files.len()
}

fn json_str(s: &str, key: &str) -> Option<String> {
    let pat = format!("\"{key}\"");
    let i = s.find(&pat)?;
    let rest = &s[i + pat.len()..];
    let rest = rest[rest.find(':')? + 1..].trim_start();
    let rest = rest.strip_prefix('"')?;
    Some(rest[..rest.find('"')?].to_string())
}

fn transpose<T>(per_thread: Vec<Vec<Vec<T>>>) -> Vec<Mutex<Vec<Vec<T>>>> {
    let mut out: Vec<Vec<Vec<T>>> = (0..PB).map(|_| Vec::new()).collect();
    for bufs in per_thread {
        for (p, v) in bufs.into_iter().enumerate() {
            if !v.is_empty() {
                out[p].push(v);
            }
        }
    }
    out.into_iter().map(Mutex::new).collect()
}

/// PB scatter buffers, each pre-sized for `rows` spread over `nt` threads (with slack).
/// Large reservations are mmapped lazily by glibc, so unused capacity costs no RSS.
fn new_bufs<T>(rows: usize, nt: usize) -> Vec<Vec<T>> {
    let cap = rows / (nt * PB) * 5 / 4 + 64;
    (0..PB).map(|_| Vec::with_capacity(cap)).collect()
}

// ------------------------------------------------------------------ links

fn parse_links(files: &[&'static [u8]], target: usize, nt: usize) -> LinkEndpoints {
    // A: parse; scatter each ID occurrence by hash partition.
    #[derive(Clone, Copy)]
    struct Occ {
        h: u64,
        key: Key,
        pair: u32, // thread-local pair index
        side: u32,
    }
    let chunks = make_chunks(files, target);
    let rows = est_rows(files);
    let next = AtomicUsize::new(0);
    let per_thread: Vec<(Vec<Vec<Occ>>, u32)> = run_par(nt, |_| {
        let mut bufs: Vec<Vec<Occ>> = new_bufs(rows * 2, nt);
        let mut npairs = 0u32;
        loop {
            let i = next.fetch_add(1, Relaxed);
            if i >= chunks.len() {
                break;
            }
            csvp::for_records::<2>(chunks[i], |f| {
                if !f[0].is_empty() && !f[1].is_empty() {
                    for side in 0..2 {
                        let h = hash(f[side].get());
                        bufs[part(h)].push(Occ { h, key: Key::from_bs(f[side]), pair: npairs, side: side as u32 });
                    }
                    npairs += 1;
                }
            });
        }
        (bufs, npairs)
    });
    let mut pair_base = Vec::with_capacity(nt);
    let mut npairs = 0usize;
    let mut occ_parts: Vec<Vec<Vec<Occ>>> = (0..PB).map(|_| Vec::new()).collect();
    for (t, (bufs, n)) in per_thread.into_iter().enumerate() {
        pair_base.push(npairs);
        npairs += n as usize;
        for (p, v) in bufs.into_iter().enumerate() {
            occ_parts[p].push(v);
            debug_assert_eq!(occ_parts[p].len(), t + 1);
        }
    }
    if npairs == 0 {
        return LinkEndpoints {
            pair_ids: Vec::new(),
            base: vec![0; PB],
            keys: Vec::new(),
            maps: (0..PB).map(|_| KeyMap::with_capacity(0)).collect(),
        };
    }

    // B: intern IDs and record each pair endpoint as (partition, local ID)
    // in the same pass. Each input occurrence owns exactly one output slot.
    let mut pair_ids = vec![0u64; npairs * 2];
    let endpoints = SyncPtr(pair_ids.as_mut_ptr());
    let next = AtomicUsize::new(0);
    let interned: Vec<Mutex<Option<(KeyMap<u32>, Vec<Key>)>>> = (0..PB).map(|_| Mutex::new(None)).collect();
    run_par(nt, |_| loop {
        let p = next.fetch_add(1, Relaxed);
        if p >= PB {
            break;
        }
        let n: usize = occ_parts[p].iter().map(|v| v.len()).sum();
        let mut map: KeyMap<u32> = KeyMap::with_capacity(n / 2 + 8);
        let mut keys = Vec::new();
        let partition_bits = (p as u64) << 32;
        for (t, piece) in occ_parts[p].iter().enumerate() {
            for o in piece {
                let (l, ins) = map.get_or_insert(o.h as u32 as u64, o.key, keys.len() as u32);
                if ins {
                    keys.push(o.key);
                }
                let slot = (pair_base[t] + o.pair as usize) * 2 + o.side as usize;
                unsafe { *endpoints.at(slot) = partition_bits | l as u64 };
            }
        }
        *interned[p].lock().unwrap() = Some((map, keys));
    });
    drop(occ_parts);

    // C: pack the keys into a global array for union-find.
    let interned: Vec<_> = interned.into_iter().map(|m| m.into_inner().unwrap().unwrap()).collect();
    let nids = interned.iter().map(|(_, keys)| keys.len()).sum();
    let mut base = Vec::with_capacity(PB + 1);
    let mut gkeys = Vec::with_capacity(nids);
    let mut maps = Vec::with_capacity(PB);
    for (map, keys) in interned {
        base.push(gkeys.len());
        gkeys.extend(keys);
        maps.push(map);
    }

    LinkEndpoints { pair_ids, base, keys: gkeys, maps }
}

impl LinkEndpoints {
    fn resolve(self) -> Links {
        let Self { pair_ids, mut base, keys, maps } = self;
        let nids = keys.len();
        // Union-find; the root of each set is always its byte-wise smallest ID.
        let mut parent: Vec<u32> = (0..nids as u32).collect();
        fn find(parent: &mut [u32], mut x: u32) -> u32 {
            while parent[x as usize] != x {
                let gp = parent[parent[x as usize] as usize];
                parent[x as usize] = gp;
                x = gp;
            }
            x
        }
        let to_global = |id: u64| (base[(id >> 32) as usize] + id as u32 as usize) as u32;
        for pr in pair_ids.chunks_exact(2) {
            let ra = find(&mut parent, to_global(pr[0]));
            let rb = find(&mut parent, to_global(pr[1]));
            if ra != rb {
                if keys[ra as usize].get() < keys[rb as usize].get() {
                    parent[rb as usize] = ra;
                } else {
                    parent[ra as usize] = rb;
                }
            }
        }
        for i in 0..nids as u32 {
            let r = find(&mut parent, i);
            parent[i as usize] = r;
        }
        drop(pair_ids);

        // A root's original partition already matches its canonical key's
        // hash. Store each root name once and route aliases using its index.
        base.push(nids);
        let mut root_person = vec![LinkedPerson::default(); nids];
        let mut names: Vec<Vec<Key>> = (0..PB).map(|_| Vec::new()).collect();
        for (p, names) in names.iter_mut().enumerate() {
            for i in base[p]..base[p + 1] {
                if parent[i] as usize == i {
                    root_person[i] = LinkedPerson { index: names.len() as u32, partition: p as u8 };
                    names.push(keys[i]);
                }
            }
        }
        let aliases = maps
            .into_iter()
            .enumerate()
            .map(|(p, map)| {
                let persons = parent[base[p]..base[p + 1]].iter().map(|&r| root_person[r as usize]).collect();
                LinkPart { map, persons }
            })
            .collect();
        Links { aliases, names }
    }
}

// ------------------------------------------------------------------ campaign accumulator

struct Acc {
    day0: i64,
    ndays: usize,
    dense: Vec<[i64; 4]>, // [camp * ndays + day] -> sessions, purchasing, revenue, assisted
    sparse: HashMap<(u32, i64), [i64; 4]>,
    use_dense: bool,
}

impl Acc {
    fn new(ctx: &Ctx) -> Self {
        let cells = ctx.camp_ids.len() * ctx.ndays;
        let use_dense = cells > 0 && cells <= 1 << 18;
        Acc {
            day0: ctx.day0,
            ndays: ctx.ndays,
            dense: if use_dense { vec![[0; 4]; cells] } else { Vec::new() },
            sparse: HashMap::new(),
            use_dense,
        }
    }
    #[inline]
    fn at(&mut self, camp: u32, day: i64) -> &mut [i64; 4] {
        if self.use_dense {
            let d = (day - self.day0) as usize;
            &mut self.dense[camp as usize * self.ndays + d]
        } else {
            self.sparse.entry((camp, day)).or_insert([0; 4])
        }
    }
    fn merge(&mut self, o: Acc) {
        if self.use_dense {
            for (a, b) in self.dense.iter_mut().zip(o.dense) {
                for k in 0..4 {
                    a[k] += b[k];
                }
            }
        } else {
            for (k, v) in o.sparse {
                let a = self.sparse.entry(k).or_insert([0; 4]);
                for i in 0..4 {
                    a[i] += v[i];
                }
            }
        }
    }
    fn rows(&self) -> Vec<(u32, i64, [i64; 4])> {
        let mut out = Vec::new();
        if self.use_dense {
            for (i, v) in self.dense.iter().enumerate() {
                if v[0] != 0 || v[3] != 0 {
                    out.push(((i / self.ndays) as u32, self.day0 + (i % self.ndays) as i64, *v));
                }
            }
        } else {
            for (&(c, d), v) in &self.sparse {
                if v[0] != 0 || v[3] != 0 {
                    out.push((c, d, *v));
                }
            }
        }
        out
    }
}

// ------------------------------------------------------------------ person worker

#[derive(Clone, Copy)]
struct Sess {
    start: i64,
    day: i64,
    nev: u32,
    rev: i64,
    purch: bool,
}

#[derive(Default)]
struct PersonRows<T> {
    events: Vec<SE<T>>,
    clicks: Vec<SC<T>>,
}

struct Worker<'a, T> {
    ctx: &'a Ctx,
    acc: Acc,
    // user_daily rows
    u_name: Vec<Key>,
    u_day: Vec<i32>,
    u_sess: Vec<i64>,
    u_ev: Vec<i64>,
    u_rev: Vec<i64>,
    u_top: Vec<u32>,
    // scratch
    names: Vec<Key>,
    linked_pid: Vec<u32>,
    groups: Vec<PersonRows<T>>,
    sess: Vec<Sess>,
    purchases: Vec<(u32, u32, u32)>, // (session idx, cat, price)
    catsum: Vec<i64>,
    touched: Vec<u32>,
    run_of: Vec<u32>,
    run_end: Vec<u32>,
    run_day: Vec<i64>,
    bycamp: Vec<(u32, i64)>,
}

impl<'a, T: Stamp> Worker<'a, T> {
    fn new(ctx: &'a Ctx) -> Self {
        Worker {
            ctx,
            acc: Acc::new(ctx),
            u_name: Vec::new(),
            u_day: Vec::new(),
            u_sess: Vec::new(),
            u_ev: Vec::new(),
            u_rev: Vec::new(),
            u_top: Vec::new(),
            names: Vec::new(),
            linked_pid: Vec::new(),
            groups: Vec::new(),
            sess: Vec::new(),
            purchases: Vec::new(),
            catsum: vec![0; ctx.cat_names.len()],
            touched: Vec::new(),
            run_of: Vec::new(),
            run_end: Vec::new(),
            run_day: Vec::new(),
            bycamp: Vec::new(),
        }
    }

    fn partition(&mut self, linked_names: &[Key], evs: Vec<Vec<PE<T>>>, cls: Vec<Vec<PC<T>>>) {
        let n: usize = evs.iter().map(|v| v.len()).sum();
        if n == 0 {
            return;
        }
        let mut pmap: KeyMap<u32> = KeyMap::with_capacity(n / 16 + 16);
        self.names.clear();
        self.linked_pid.clear();
        self.linked_pid.resize(linked_names.len(), NO_PERSON);
        let mut groups = std::mem::take(&mut self.groups);
        for e in evs.into_iter().flatten() {
            if e.price == DEAD {
                continue;
            }
            let next = self.names.len() as u32;
            let (id, name) = if e.price & LINKED != 0 {
                let index = e.h as usize;
                let pid = &mut self.linked_pid[index];
                if *pid == NO_PERSON {
                    *pid = next;
                    (next, Some(linked_names[index]))
                } else {
                    (*pid, None)
                }
            } else {
                let (id, inserted) = pmap.get_or_insert(e.h as u64, e.key, next);
                (id, inserted.then_some(e.key))
            };
            if let Some(name) = name {
                self.names.push(name);
                if id as usize == groups.len() {
                    groups.push(PersonRows::default());
                } else {
                    let group = &mut groups[id as usize];
                    group.events.clear();
                    group.clicks.clear();
                }
            }
            groups[id as usize].events.push(SE { ts: e.ts, price: e.price & !LINKED, cat: e.cat });
        }
        if self.names.is_empty() {
            self.groups = groups;
            return;
        }
        for c in cls.into_iter().flatten() {
            if c.camp == MOVED_CLICK {
                continue;
            }
            let p = if c.person == NO_PERSON {
                pmap.get(c.h as u64, &c.key).unwrap_or(NO_PERSON)
            } else {
                self.linked_pid[c.person as usize]
            };
            if p != NO_PERSON {
                groups[p as usize].clicks.push(SC { ts: c.ts, id: c.id, camp: c.camp });
            }
        }
        for (p, group) in groups[..self.names.len()].iter_mut().enumerate() {
            group.events.sort_unstable_by_key(|e| e.ts);
            // For equal times, the final click in the range has the smallest ID.
            group.clicks.sort_unstable_by(|a, b| a.ts.cmp(&b.ts).then_with(|| b.id.get().cmp(a.id.get())));
            self.person(self.names[p], &group.events, &group.clicks);
        }
        self.groups = groups;
    }

    fn person(&mut self, name: Key, evs: &[SE<T>], cls: &[SC<T>]) {
        let ctx = self.ctx;
        // Sessionize.
        self.sess.clear();
        self.purchases.clear();
        let mut last = i64::MIN;
        for e in evs {
            let ts = e.ts.decode(ctx.cws);
            if self.sess.is_empty() || ts - last > GAP {
                self.sess.push(Sess { start: ts, day: ts.div_euclid(DAY), nev: 0, rev: 0, purch: false });
            }
            let si = self.sess.len() - 1;
            let s = &mut self.sess[si];
            s.nev += 1;
            if e.price != 0 {
                s.rev += e.price as i64;
                s.purch = true;
                self.purchases.push((si as u32, e.cat, e.price));
            }
            last = ts;
        }

        // user_daily: runs of sessions sharing a start day.
        self.run_of.clear();
        self.run_end.clear();
        self.run_day.clear();
        let ns = self.sess.len();
        let mut a = 0;
        let mut pi = 0;
        while a < ns {
            let day = self.sess[a].day;
            let mut b = a;
            let mut nev = 0i64;
            let mut rev = 0i64;
            while b < ns && self.sess[b].day == day {
                nev += self.sess[b].nev as i64;
                rev += self.sess[b].rev;
                b += 1;
            }
            let r = self.run_end.len() as u32;
            for _ in a..b {
                self.run_of.push(r);
            }
            self.run_end.push(b as u32);
            self.run_day.push(day);
            // top category
            let mut top = u32::MAX;
            while pi < self.purchases.len() && (self.purchases[pi].0 as usize) < b {
                let (_, cat, price) = self.purchases[pi];
                if self.catsum[cat as usize] == 0 {
                    self.touched.push(cat);
                }
                self.catsum[cat as usize] += price as i64;
                pi += 1;
            }
            if !self.touched.is_empty() {
                let mut best = self.touched[0];
                for &c in &self.touched {
                    let (sc, sb) = (self.catsum[c as usize], self.catsum[best as usize]);
                    if sc > sb || (sc == sb && ctx.cat_rank[c as usize] < ctx.cat_rank[best as usize]) {
                        best = c;
                    }
                }
                top = best;
                for &c in &self.touched {
                    self.catsum[c as usize] = 0;
                }
                self.touched.clear();
            }
            self.u_name.push(name);
            self.u_day.push(day as i32);
            self.u_sess.push((b - a) as i64);
            self.u_ev.push(nev);
            self.u_rev.push(rev);
            self.u_top.push(top);
            a = b;
        }

        // Credit: latest click with start - 7d <= ts <= start (ties: smallest click_id).
        let mut j = 0;
        for i in 0..ns {
            let s = self.sess[i];
            while j < cls.len() && cls[j].ts.decode(ctx.cws) <= s.start {
                j += 1;
            }
            let camp = if j > 0 && cls[j - 1].ts.decode(ctx.cws) >= s.start - WEEK { cls[j - 1].camp } else { ctx.organic };
            let v = self.acc.at(camp, s.day);
            v[0] += 1;
            v[1] += s.purch as i64;
            v[2] += s.rev;
        }

        // Assists: per campaign, merged coverage intervals [t, t + 7d] over its click times.
        if cls.is_empty() {
            return;
        }
        self.bycamp.clear();
        self.bycamp.extend(cls.iter().map(|c| (c.camp, c.ts.decode(ctx.cws))));
        self.bycamp.sort_unstable();
        let mut i = 0;
        let nb = self.bycamp.len();
        while i < nb {
            let camp = self.bycamp[i].0;
            let mut lo = self.bycamp[i].1;
            let mut hi = lo + WEEK;
            i += 1;
            loop {
                let more = i < nb && self.bycamp[i].0 == camp;
                if more && self.bycamp[i].1 <= hi + 1 {
                    hi = hi.max(self.bycamp[i].1 + WEEK);
                    i += 1;
                    continue;
                }
                // Sessions with lo <= start <= hi.
                let sa = self.sess.partition_point(|s| s.start < lo);
                let sb = self.sess.partition_point(|s| s.start <= hi);
                let mut k = sa;
                while k < sb {
                    let r = self.run_of[k] as usize;
                    let end = (self.run_end[r] as usize).min(sb);
                    let day = self.run_day[r];
                    self.acc.at(camp, day)[3] += (end - k) as i64;
                    k = end;
                }
                if !more {
                    break;
                }
                lo = self.bycamp[i].1;
                hi = lo + WEEK;
                i += 1;
            }
        }
    }

    fn write_users(&self, path: &Path) -> std::io::Result<()> {
        let ctx = self.ctx;
        let n = self.u_name.len();
        let mut c_user = Column::new("user_id", Kind::Str, false);
        let mut c_day = Column::new("day", Kind::Date, false);
        let mut c_sess = Column::new("sessions", Kind::I64, false);
        let mut c_ev = Column::new("events", Kind::I64, false);
        let mut c_rev = Column::new("revenue_cents", Kind::I64, false);
        let mut c_top = Column::new("top_category", Kind::Str, true);
        for i in 0..n {
            c_user.push_str(Some(self.u_name[i].get()));
            c_day.push_date(self.u_day[i]);
            c_sess.push_i64(self.u_sess[i]);
            c_ev.push_i64(self.u_ev[i]);
            c_rev.push_i64(self.u_rev[i]);
            let t = self.u_top[i];
            c_top.push_str(if t == u32::MAX { None } else { Some(ctx.cat_names[t as usize]) });
        }
        parquet::write_file(path, n, &[c_user, c_day, c_sess, c_ev, c_rev, c_top])
    }
}

fn aggregate<T: Stamp>(
    event_files: &[&'static [u8]],
    click_files: &[&'static [u8]],
    target: usize,
    nt: usize,
    ctx: &Ctx,
    endpoints: LinkEndpoints,
    user_dir: &Path,
    stage: &mut impl FnMut(&str),
) -> Vec<Acc> {
    // ---- events: parse + validate each copy. Valid copies go straight to their user bucket;
    // every copy also leaves a small dedup record, scattered by hash(event_id).
    let ev_chunks = make_chunks(event_files, target);
    let ev_rows = est_rows(event_files);
    let cl_rows = est_rows(click_files);
    let next = AtomicUsize::new(0);
    let parse_events = |tid: usize| {
        let mut db: Vec<Vec<D>> = new_bufs(ev_rows, nt);
        let mut ub: Vec<Vec<PE<T>>> = new_bufs(ev_rows, nt);
        loop {
            let i = next.fetch_add(1, Relaxed);
            if i >= ev_chunks.len() {
                break;
            }
            csvp::for_records::<7>(ev_chunks[i], |f| {
                if f[0].is_empty() {
                    return;
                }
                let ingest = ts::parse_ingest(f[3].get()).unwrap_or(i64::MAX);
                let mut valid = !f[1].is_empty() && !f[5].is_empty();
                let mut t = 0i64;
                let mut purchase = false;
                let mut price = 0u32;
                let mut cat = 0u32;
                if valid {
                    match ts::parse_ts(f[2].get()) {
                        Some(x) if x >= ctx.ws && x < ctx.we => t = x,
                        _ => valid = false,
                    }
                }
                if valid {
                    match f[4].get() {
                        b"view" | b"cart" => {}
                        b"purchase" => purchase = true,
                        _ => valid = false,
                    }
                }
                if valid && purchase {
                    match ts::parse_price(f[6].get()) {
                        Some(p) => {
                            price = p;
                            cat = ctx.products.get(hash(f[5].get()), f[5].get()).unwrap_or(ctx.unknown);
                        }
                        None => valid = false,
                    }
                }
                if valid && ingest.saturating_sub(t) > DAY {
                    valid = false;
                }
                let loc = if valid {
                    let uh = hash(f[1].get());
                    let u = part(uh);
                    let idx = ub[u].len() as u64;
                    ub[u].push(PE { h: uh as u32, key: Key::from_bs(f[1]), ts: T::encode(t, ctx.cws), price, cat });
                    ((tid as u64) << 56) | ((u as u64) << 48) | idx
                } else {
                    NONE
                };
                let idh = hash(f[0].get());
                // The accepted first field is followed by a known delimiter and
                // stays mapped through dedup.
                db[part(idh)].push(unsafe { D::new(idh, f[0], ingest, loc) });
            });
        }
        (db, ub)
    };
    // Raw event parsing does not consult aliases. The caller builds the link
    // components while the other workers parse, then joins the event work.
    let (links, ev_out) = std::thread::scope(|scope| {
        let parse_events = &parse_events;
        let workers: Vec<_> = (0..nt - 1).map(|tid| scope.spawn(move || parse_events(tid))).collect();
        let start = Instant::now();
        let links = endpoints.resolve();
        eprintln!("links union {:>13} us (overlapped)", start.elapsed().as_micros());
        let last = parse_events(nt - 1);
        let mut events: Vec<_> = workers.into_iter().map(|worker| worker.join().unwrap()).collect();
        events.push(last);
        (links, events)
    });
    let (d_bufs, mut ue_bufs): (Vec<_>, Vec<_>) = ev_out.into_iter().unzip();
    stage("events");

    // ---- clicks: parse + validate, scatter by hash(user_id)
    let cl_chunks = make_chunks(click_files, target);
    let next = AtomicUsize::new(0);
    let cl_bufs: Vec<Vec<Vec<PC<T>>>> = run_par(nt, |_| {
        let mut bufs: Vec<Vec<PC<T>>> = new_bufs(cl_rows, nt);
        loop {
            let i = next.fetch_add(1, Relaxed);
            if i >= cl_chunks.len() {
                break;
            }
            csvp::for_records::<4>(cl_chunks[i], |f| {
                if f[0].is_empty() || f[1].is_empty() {
                    return;
                }
                let t = match ts::parse_ts(f[3].get()) {
                    Some(x) if x >= ctx.cws && x < ctx.we => x,
                    _ => return,
                };
                let camp = match ctx.campaigns.get(hash(f[2].get()), f[2].get()) {
                    Some(c) => c,
                    None => return,
                };
                let uh = hash(f[1].get());
                bufs[part(uh)].push(PC { h: uh as u32, key: Key::from_bs(f[1]), ts: T::encode(t, ctx.cws), id: Key::from_bs(f[0]), camp, person: NO_PERSON });
            });
        }
        bufs
    });
    stage("clicks");

    // ---- dedup per event_id partition: for each event_id keep the copy with the smallest
    // ingest_ts; every valid copy that loses is marked DEAD in place in its user bucket.
    let ue_ptr: Vec<Vec<SyncPtr<PE<T>>>> =
        ue_bufs.iter_mut().map(|bufs| bufs.iter_mut().map(|v| SyncPtr(v.as_mut_ptr())).collect()).collect();
    let d_parts = transpose(d_bufs);
    let next = AtomicUsize::new(0);
    run_par(nt, |_| {
        // Each bucket is split 16 ways by more hash bits into `scratch` so that every
        // sub-bucket's records and its u32 index table stay L2-resident.
        const SB: usize = 16;
        let mut scratch: Vec<D> = Vec::new();
        let mut table: Vec<u32> = Vec::new();
        let ue_ptr = &ue_ptr;
        let kill = |d: &D| {
            if d.loc != NONE {
                let t = (d.loc >> 56) as usize;
                let u = ((d.loc >> 48) & 0xff) as usize;
                let i = (d.loc & ((1 << 48) - 1)) as usize;
                // Disjoint: each record belongs to one event_id, handled by one thread.
                unsafe { (*ue_ptr[t][u].at(i)).price = DEAD };
            }
        };
        loop {
            let b = next.fetch_add(1, Relaxed);
            if b >= PB {
                break;
            }
            let pieces = std::mem::take(&mut *d_parts[b].lock().unwrap());
            let n: usize = pieces.iter().map(|v| v.len()).sum();
            if n == 0 {
                continue;
            }
            let sub = |e: &D| ((e.idh >> 52) & (SB as u64 - 1)) as usize;
            let mut start = [0usize; SB + 1];
            for piece in &pieces {
                for e in piece.iter() {
                    start[sub(e) + 1] += 1;
                }
            }
            for i in 0..SB {
                start[i + 1] += start[i];
            }
            let mut pos = start;
            scratch.clear();
            scratch.reserve(n);
            let dst = scratch.as_mut_ptr();
            for piece in &pieces {
                for e in piece.iter() {
                    let k = sub(e);
                    unsafe { dst.add(pos[k]).write(*e) };
                    pos[k] += 1;
                }
            }
            unsafe { scratch.set_len(n) };
            drop(pieces);
            for k in 0..SB {
                let sl = &scratch[start[k]..start[k + 1]];
                if sl.is_empty() {
                    continue;
                }
                let cap = (sl.len() * 2).next_power_of_two();
                let mask = cap - 1;
                table.clear();
                table.resize(cap, u32::MAX);
                for (i, e) in sl.iter().enumerate() {
                    let mut s = (e.idh as usize) & mask;
                    loop {
                        let c = table[s];
                        if c == u32::MAX {
                            table[s] = i as u32;
                            break;
                        }
                        let o = &sl[c as usize];
                        if o.idh == e.idh && o.same_id(e) {
                            // A copy that loses never wins later (the kept ingest_ts only falls), so
                            // it is marked now. Equal ingest_ts: copies are identical; keep the first.
                            if e.ingest_order() < o.ingest_order() {
                                kill(o);
                                table[s] = i as u32;
                            } else {
                                kill(e);
                            }
                            break;
                        }
                        s = (s + 1) & mask;
                    }
                }
            }
        }
    });
    drop(d_parts);
    stage("dedup");

    // ---- resolve locally first. Only cross-partition aliases need a second scatter.
    let pev_parts = transpose(ue_bufs);
    let cl_parts = transpose(cl_bufs);
    let next = AtomicUsize::new(0);
    let moved: Vec<(Vec<Vec<PE<T>>>, Vec<Vec<PC<T>>>)> = run_par(nt, |_| {
        let mut oe: Vec<Vec<PE<T>>> = (0..PB).map(|_| Vec::new()).collect();
        let mut oc: Vec<Vec<PC<T>>> = (0..PB).map(|_| Vec::new()).collect();
        loop {
            let u = next.fetch_add(1, Relaxed);
            if u >= PB {
                break;
            }
            let lp = &links.aliases[u];
            if lp.map.len == 0 {
                continue;
            }
            for piece in &mut *pev_parts[u].lock().unwrap() {
                for e in piece {
                    if e.price == DEAD {
                        continue;
                    }
                    if let Some(i) = lp.map.get(e.h as u64, &e.key) {
                        let person = lp.persons[i as usize];
                        e.h = person.index;
                        e.price |= LINKED;
                        let dest = person.partition as usize;
                        if dest != u {
                            oe[dest].push(*e);
                            e.price = DEAD;
                        }
                    }
                }
            }
            for piece in &mut *cl_parts[u].lock().unwrap() {
                for c in piece {
                    if let Some(i) = lp.map.get(c.h as u64, &c.key) {
                        let person = lp.persons[i as usize];
                        c.person = person.index;
                        let dest = person.partition as usize;
                        if dest != u {
                            oc[dest].push(*c);
                            c.camp = MOVED_CLICK;
                        }
                    }
                }
            }
        }
        (oe, oc)
    });
    for (oe, oc) in moved {
        for (part, buf) in pev_parts.iter().zip(oe) {
            if !buf.is_empty() {
                part.lock().unwrap().push(buf);
            }
        }
        for (part, buf) in cl_parts.iter().zip(oc) {
            if !buf.is_empty() {
                part.lock().unwrap().push(buf);
            }
        }
    }
    stage("resolve");

    // ---- persons
    let mut order: Vec<(usize, usize)> = pev_parts
        .iter()
        .enumerate()
        .map(|(q, m)| (q, m.lock().unwrap().iter().map(|v| v.len()).sum()))
        .collect();
    order.sort_by_key(|&(_, n)| std::cmp::Reverse(n));
    std::fs::create_dir_all(user_dir).expect("create user_daily");
    let next = AtomicUsize::new(0);
    let accs: Vec<Acc> = run_par(nt, |tid| {
        let mut w = Worker::new(ctx);
        loop {
            let k = next.fetch_add(1, Relaxed);
            if k >= PB {
                break;
            }
            let q = order[k].0;
            let evs = std::mem::take(&mut *pev_parts[q].lock().unwrap());
            let cls = std::mem::take(&mut *cl_parts[q].lock().unwrap());
            w.partition(&links.names[q], evs, cls);
        }
        if !w.u_name.is_empty() || tid == 0 {
            w.write_users(&user_dir.join(format!("part-{tid:03}.parquet"))).expect("write user_daily");
        }
        w.acc
    });
    stage("persons");

    accs
}

// ------------------------------------------------------------------ main

fn main() {
    let t_start = Instant::now();
    let mut t_last = t_start;
    let mut stage = |name: &str| {
        let now = Instant::now();
        eprintln!("stage {:<9} {:>10} us", name, (now - t_last).as_micros());
        t_last = now;
    };

    let env = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
    let data = PathBuf::from(env("DATA_DIR", "/data"));
    let output = PathBuf::from(env("OUTPUT_DIR", "/output"));
    let nt: usize = std::env::var("NCPU")
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .filter(|&n: &usize| n > 0)
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(8, usize::from))
        .min(256);

    // ---- load
    let manifest = String::from_utf8_lossy(leak_read(&data.join("manifest.json"))).into_owned();
    let ws = json_str(&manifest, "window_start").and_then(|s| ts::parse_ts(s.as_bytes())).expect("window_start");
    let we = json_str(&manifest, "window_end").and_then(|s| ts::parse_ts(s.as_bytes())).expect("window_end");

    // products.csv
    let pbytes = leak_read(&data.join("products.csv"));
    let mut products: StrMap<u32> = StrMap::with_capacity(1024);
    let mut cat_ids: HashMap<&'static [u8], u32> = HashMap::new();
    let mut cat_names: Vec<&'static [u8]> = Vec::new();
    if let Some(h) = memchr::memchr(b'\n', pbytes) {
        csvp::for_lines(&pbytes[h + 1..], |line| {
            let mut f = [Bs::EMPTY; 2];
            if !csvp::split(line, &mut f) || f[0].is_empty() {
                return;
            }
            let cat = f[1].get();
            let next = cat_names.len() as u32;
            let id = *cat_ids.entry(cat).or_insert_with(|| {
                cat_names.push(cat);
                next
            });
            products.get_or_insert(hash(f[0].get()), f[0], id);
        });
    }
    let unknown = cat_names.len() as u32;
    cat_names.push(b"unknown");
    let mut order: Vec<usize> = (0..cat_names.len()).collect();
    order.sort_by(|&a, &b| cat_names[a].cmp(cat_names[b]));
    let mut cat_rank = vec![0u32; cat_names.len()];
    for (r, &c) in order.iter().enumerate() {
        cat_rank[c] = r as u32;
    }

    // campaigns.csv
    let cbytes = leak_read(&data.join("campaigns.csv"));
    let mut campaigns: StrMap<u32> = StrMap::with_capacity(4096);
    let mut camp_ids: Vec<&'static [u8]> = Vec::new();
    let mut camp_channels: Vec<&'static [u8]> = Vec::new();
    if let Some(h) = memchr::memchr(b'\n', cbytes) {
        csvp::for_lines(&cbytes[h + 1..], |line| {
            let mut f = [Bs::EMPTY; 2];
            if !csvp::split(line, &mut f) || f[0].is_empty() {
                return;
            }
            let (_, ins) = campaigns.get_or_insert(hash(f[0].get()), f[0], camp_ids.len() as u32);
            if ins {
                camp_ids.push(f[0].get());
                camp_channels.push(f[1].get());
            }
        });
    }
    let organic = camp_ids.len() as u32;
    camp_ids.push(b"organic");
    camp_channels.push(b"organic");

    let (day0, ndays) = if we > ws {
        let d0 = ws.div_euclid(DAY);
        let d1 = (we - 1).div_euclid(DAY);
        (d0, (d1 - d0 + 1) as usize)
    } else {
        (0, 0)
    };
    let ctx = Ctx {
        ws,
        we,
        cws: ws.saturating_sub(WEEK),
        products,
        cat_names,
        cat_rank,
        unknown,
        campaigns,
        camp_ids,
        camp_channels,
        organic,
        day0,
        ndays,
    };

    // mmap all CSVs in parallel.
    let mut paths = Vec::new();
    for (k, dir) in [(0u8, "events"), (1, "clicks"), (2, "links")] {
        for (path, len) in list_csv(&data.join(dir)) {
            paths.push((k, path, len));
        }
    }
    paths.sort_by_key(|&(_, _, len)| std::cmp::Reverse(len));
    // Allocator policy. Reusing freed heap memory across stages avoids page faults (fresh zeroed
    // pages) but holds peak RSS near 3x the input size, so only do it when that fits comfortably;
    // otherwise mmap big buffers so freed memory goes straight back to the OS.
    {
        let input: u64 = paths.iter().map(|&(_, _, len)| len).sum();
        let mem_gb: u64 = std::env::var("MEM_GB").ok().and_then(|v| v.trim().parse().ok()).unwrap_or(24);
        let phys = unsafe { libc::sysconf(libc::_SC_PHYS_PAGES) as u64 * libc::sysconf(libc::_SC_PAGESIZE) as u64 };
        let mem = (mem_gb << 30).min(if phys > 0 { phys } else { u64::MAX });
        let reuse = input.saturating_mul(3) <= mem / 4 * 3 && std::env::var_os("DS_OLD_MALLOC").is_none();
        unsafe {
            if reuse {
                libc::mallopt(libc::M_MMAP_THRESHOLD, 32 << 20);
                libc::mallopt(libc::M_TRIM_THRESHOLD, i32::MAX);
                libc::mallopt(libc::M_TOP_PAD, 64 << 20);
            } else {
                libc::mallopt(libc::M_MMAP_THRESHOLD, 128 << 10);
            }
        }
        eprintln!("alloc     {} (input {} MB, budget {} MB)", if reuse { "reuse" } else { "mmap" }, input >> 20, (mem / 4 * 3) >> 20);
    }
    let next = AtomicUsize::new(0);
    let mapped: Vec<Vec<(u8, &'static [u8])>> = run_par(nt, |_| {
        let mut v = Vec::new();
        loop {
            let i = next.fetch_add(1, Relaxed);
            if i >= paths.len() {
                break;
            }
            v.push((paths[i].0, map_file(&paths[i].1)));
        }
        v
    });
    let mut files: [Vec<&'static [u8]>; 3] = [Vec::new(), Vec::new(), Vec::new()];
    for (k, f) in mapped.into_iter().flatten() {
        files[k as usize].push(f);
    }
    let total_bytes: usize = files.iter().flatten().map(|f| f.len()).sum();
    let target = (total_bytes / (nt * 16)).clamp(1 << 16, 8 << 20);
    stage("load");

    // ---- links
    let endpoints = parse_links(&files[2], target, nt);
    stage("links");

    let user_dir = output.join("user_daily");
    let camp_dir = output.join("campaign_daily");
    std::fs::create_dir_all(&camp_dir).expect("create campaign_daily");
    // Clicks have the wider window. Only store offsets when every accepted
    // timestamp can fit, including the earliest qualifying click.
    let accs = if ctx.we.saturating_sub(ctx.cws) <= i64::from(u32::MAX) + 1 {
        aggregate::<u32>(&files[0], &files[1], target, nt, &ctx, endpoints, &user_dir, &mut stage)
    } else {
        aggregate::<i64>(&files[0], &files[1], target, nt, &ctx, endpoints, &user_dir, &mut stage)
    };

    // ---- campaign_daily
    let mut accs = accs.into_iter();
    let mut acc = accs.next().unwrap();
    for a in accs {
        acc.merge(a);
    }
    let rows = acc.rows();
    let mut c_id = Column::new("campaign_id", Kind::Str, false);
    let mut c_ch = Column::new("channel", Kind::Str, false);
    let mut c_day = Column::new("day", Kind::Date, false);
    let mut c_s = Column::new("sessions", Kind::I64, false);
    let mut c_p = Column::new("purchasing_sessions", Kind::I64, false);
    let mut c_r = Column::new("revenue_cents", Kind::I64, false);
    let mut c_a = Column::new("assisted_sessions", Kind::I64, false);
    for &(c, d, v) in &rows {
        c_id.push_str(Some(ctx.camp_ids[c as usize]));
        c_ch.push_str(Some(ctx.camp_channels[c as usize]));
        c_day.push_date(d as i32);
        c_s.push_i64(v[0]);
        c_p.push_i64(v[1]);
        c_r.push_i64(v[2]);
        c_a.push_i64(v[3]);
    }
    parquet::write_file(&camp_dir.join("part-000.parquet"), rows.len(), &[c_id, c_ch, c_day, c_s, c_p, c_r, c_a])
        .expect("write campaign_daily");
    stage("campaign");
    eprintln!("total           {:>10} us", t_start.elapsed().as_micros());

    unsafe { libc::_exit(0) }
}
