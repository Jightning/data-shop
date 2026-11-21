//! Shopping Sessions: rules.md pipeline.
//!
//! Stages (each timed to stderr in microseconds; stdout stays empty):
//!   load     manifest, products, campaigns; mmap every CSV in parallel
//!   links    parallel parse, hash-partitioned ID interning, union-find (root = byte-wise
//!            smallest ID), per-partition lookup tables small enough to stay cache-resident
//!   events   parallel chunked parse + per-copy validation, radix-scattered by hash(event_id)
//!   clicks   parallel chunked parse + validation, scattered by hash(user_id)
//!   dedup    per event_id partition: keep the smallest ingest_ts copy, scatter by hash(user_id)
//!   resolve  per user_id partition: map each ID to its person via that partition's link table,
//!            scatter events and clicks by hash(person)
//!   persons  per person partition (largest first): sessionize, credit, assists, user_daily shard
//!   campaign merge per-worker campaign/day counters, write campaign_daily
//! then `_exit(0)`.
//!
//! IDs up to 15 bytes are stored inline in 16-byte `Key`s, so after parsing no stage touches
//! the input files again; longer IDs keep a pointer into the mmap (exact general fallback).

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

/// One parsed event copy (valid or not), scattered by hash(event_id) for dedup. 64 bytes.
#[derive(Clone, Copy)]
struct Ev {
    idh: u64,
    ingest: i64,
    ts: i64,
    id: Key,
    user: Key,
    price: u32,
    cat: u16,
    kind: u8,
    valid: bool,
}

/// A surviving event keyed by user ID (after dedup) or by person (after resolve). 40 bytes.
#[derive(Clone, Copy)]
struct PE {
    h: u64,
    key: Key,
    ts: i64,
    price: u32,
    cat: u16,
    kind: u8,
}

/// A valid click keyed by user ID (after parse) or by person (after resolve). 56 bytes.
#[derive(Clone, Copy)]
struct PC {
    h: u64,
    key: Key,
    ts: i64,
    id: Key,
    camp: u32,
}

/// Per-person compact event for sorting.
#[derive(Clone, Copy, Default)]
struct SE {
    ts: i64,
    price: u32,
    cat: u16,
    kind: u8,
}

/// Per-person compact click for sorting.
#[derive(Clone, Copy)]
struct SC {
    ts: i64,
    id: Key,
    camp: u32,
}

// ------------------------------------------------------------------ context

struct Ctx {
    ws: i64,
    we: i64,
    cws: i64,
    products: StrMap<u16>,
    cat_names: Vec<&'static [u8]>, // index = cat id; last is "unknown"
    cat_rank: Vec<u32>,            // byte-wise rank of each category name
    unknown: u16,
    campaigns: StrMap<u32>,
    camp_ids: Vec<&'static [u8]>, // index = campaign id; last is "organic"
    camp_channels: Vec<&'static [u8]>,
    organic: u32,
    day0: i64,
    ndays: usize,
}

/// Link lookup for the IDs whose hash falls in one partition.
struct LinkPart {
    map: KeyMap<u32>,
    name: Vec<Key>,
    nh: Vec<u64>,
}

impl LinkPart {
    #[inline(always)]
    fn resolve(&self, h: u64, k: Key) -> (u64, Key) {
        match self.map.get(h, &k) {
            Some(i) => (self.nh[i as usize], self.name[i as usize]),
            None => (h, k),
        }
    }
}

// ------------------------------------------------------------------ helpers

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

/// mmap a file read-only (pages populated by the calling thread); the mapping is never unmapped.
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
            libc::MAP_PRIVATE | libc::MAP_POPULATE,
            f.as_raw_fd(),
            0,
        );
        if p == libc::MAP_FAILED {
            return leak_read(path);
        }
        std::slice::from_raw_parts(p as *const u8, len)
    }
}

fn list_csv(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .filter(|e| {
                let name = e.file_name();
                let name = name.to_string_lossy();
                name.ends_with(".csv") && !name.starts_with('.') && e.path().is_file()
            })
            .map(|e| e.path())
            .collect(),
        Err(_) => Vec::new(),
    };
    v.sort();
    v
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

fn build_links(files: &[&'static [u8]], target: usize, nt: usize) -> Vec<LinkPart> {
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
            csvp::for_lines(chunks[i], |line| {
                let mut f = [Bs::EMPTY; 2];
                if csvp::split(line, &mut f) && !f[0].is_empty() && !f[1].is_empty() {
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
    let empty = || (0..PB).map(|_| LinkPart { map: KeyMap::with_capacity(0), name: Vec::new(), nh: Vec::new() }).collect();
    if npairs == 0 {
        return empty();
    }

    // B: per partition, intern distinct IDs (local index per occurrence).
    let next = AtomicUsize::new(0);
    let interned: Vec<Mutex<Option<(KeyMap<u32>, Vec<Key>, Vec<u32>)>>> = (0..PB).map(|_| Mutex::new(None)).collect();
    run_par(nt, |_| loop {
        let p = next.fetch_add(1, Relaxed);
        if p >= PB {
            break;
        }
        let n: usize = occ_parts[p].iter().map(|v| v.len()).sum();
        let mut map: KeyMap<u32> = KeyMap::with_capacity(n / 2 + 8);
        let mut keys = Vec::new();
        let mut locals = Vec::with_capacity(n);
        for v in &occ_parts[p] {
            for o in v {
                let (l, ins) = map.get_or_insert(o.h, o.key, keys.len() as u32);
                if ins {
                    keys.push(o.key);
                }
                locals.push(l);
            }
        }
        *interned[p].lock().unwrap() = Some((map, keys, locals));
    });
    let interned: Vec<(KeyMap<u32>, Vec<Key>, Vec<u32>)> =
        interned.into_iter().map(|m| m.into_inner().unwrap().unwrap()).collect();
    let mut base = Vec::with_capacity(PB + 1);
    let mut nids = 0usize;
    for (_, keys, _) in &interned {
        base.push(nids);
        nids += keys.len();
    }

    // C: global ID per pair side, global key array.
    let mut pair_gid = vec![0u32; npairs * 2];
    let mut gkeys = vec![Key::EMPTY; nids];
    {
        let pg = SyncPtr(pair_gid.as_mut_ptr());
        let gk = SyncPtr(gkeys.as_mut_ptr());
        let next = AtomicUsize::new(0);
        let (occ_parts, interned, base, pair_base) = (&occ_parts, &interned, &base, &pair_base);
        run_par(nt, move |_| loop {
            let p = next.fetch_add(1, Relaxed);
            if p >= PB {
                break;
            }
            let (_, keys, locals) = &interned[p];
            for (l, k) in keys.iter().enumerate() {
                unsafe { *gk.at(base[p] + l) = *k };
            }
            let mut i = 0;
            for (t, v) in occ_parts[p].iter().enumerate() {
                for o in v {
                    let gid = (base[p] + locals[i] as usize) as u32;
                    i += 1;
                    let slot = (pair_base[t] + o.pair as usize) * 2 + o.side as usize;
                    unsafe { *pg.at(slot) = gid };
                }
            }
        });
    }
    drop(occ_parts);

    // D: union-find; the root of each set is always its byte-wise smallest ID.
    let mut parent: Vec<u32> = (0..nids as u32).collect();
    fn find(parent: &mut [u32], mut x: u32) -> u32 {
        while parent[x as usize] != x {
            let gp = parent[parent[x as usize] as usize];
            parent[x as usize] = gp;
            x = gp;
        }
        x
    }
    for pr in pair_gid.chunks_exact(2) {
        let ra = find(&mut parent, pr[0]);
        let rb = find(&mut parent, pr[1]);
        if ra != rb {
            if gkeys[ra as usize].get() < gkeys[rb as usize].get() {
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
    drop(pair_gid);

    // E: per partition, each ID's person name and its hash.
    let root_hash: Vec<u64> = {
        // Hash only roots (in parallel, by index range).
        let mut rh = vec![0u64; nids];
        let ptr = SyncPtr(rh.as_mut_ptr());
        let per = nids.div_ceil(nt);
        let (parent, gkeys) = (&parent, &gkeys);
        run_par(nt, move |t| {
            for i in t * per..((t + 1) * per).min(nids) {
                if parent[i] as usize == i {
                    unsafe { *ptr.at(i) = hash(gkeys[i].get()) };
                }
            }
        });
        rh
    };
    interned
        .into_iter()
        .enumerate()
        .map(|(p, (map, keys, _))| {
            let mut name = Vec::with_capacity(keys.len());
            let mut nh = Vec::with_capacity(keys.len());
            for l in 0..keys.len() {
                let r = parent[base[p] + l] as usize;
                name.push(gkeys[r]);
                nh.push(root_hash[r]);
            }
            LinkPart { map, name, nh }
        })
        .collect()
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

struct Worker<'a> {
    ctx: &'a Ctx,
    acc: Acc,
    // user_daily rows
    u_name: Vec<Key>,
    u_day: Vec<i32>,
    u_sess: Vec<i64>,
    u_ev: Vec<i64>,
    u_rev: Vec<i64>,
    u_top: Vec<i32>,
    // scratch
    pid: Vec<u32>,
    names: Vec<Key>,
    off: Vec<u32>,
    cur: Vec<u32>,
    se: Vec<SE>,
    coff: Vec<u32>,
    cpid: Vec<u32>,
    sc: Vec<SC>,
    sess: Vec<Sess>,
    purchases: Vec<(u32, u16, u32)>, // (session idx, cat, price)
    catsum: Vec<i64>,
    touched: Vec<u16>,
    run_of: Vec<u32>,
    run_end: Vec<u32>,
    run_day: Vec<i64>,
    bycamp: Vec<(u32, i64)>,
}

impl<'a> Worker<'a> {
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
            pid: Vec::new(),
            names: Vec::new(),
            off: Vec::new(),
            cur: Vec::new(),
            se: Vec::new(),
            coff: Vec::new(),
            cpid: Vec::new(),
            sc: Vec::new(),
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

    fn partition(&mut self, evs: Vec<Vec<PE>>, cls: Vec<Vec<PC>>) {
        let n: usize = evs.iter().map(|v| v.len()).sum();
        if n == 0 {
            return;
        }
        // Group events by person (exact name bytes).
        let mut pmap: KeyMap<u32> = KeyMap::with_capacity(n / 4 + 16);
        self.pid.clear();
        self.names.clear();
        for piece in &evs {
            for e in piece {
                let (id, ins) = pmap.get_or_insert(e.h, e.key, self.names.len() as u32);
                if ins {
                    self.names.push(e.key);
                }
                self.pid.push(id);
            }
        }
        let np = self.names.len();
        self.off.clear();
        self.off.resize(np + 1, 0);
        for &p in &self.pid {
            self.off[p as usize + 1] += 1;
        }
        for i in 0..np {
            self.off[i + 1] += self.off[i];
        }
        self.cur.clear();
        self.cur.extend_from_slice(&self.off[..np]);
        self.se.clear();
        self.se.resize(n, SE::default());
        let mut k = 0;
        for piece in &evs {
            for e in piece {
                let p = self.pid[k] as usize;
                k += 1;
                let at = self.cur[p] as usize;
                self.cur[p] += 1;
                self.se[at] = SE { ts: e.ts, price: e.price, cat: e.cat, kind: e.kind };
            }
        }
        drop(evs);

        // Clicks of people with events.
        self.cpid.clear();
        let mut ccount = vec![0u32; np + 1];
        for piece in &cls {
            for c in piece {
                let p = pmap.get(c.h, &c.key).unwrap_or(u32::MAX);
                if p != u32::MAX {
                    ccount[p as usize + 1] += 1;
                }
                self.cpid.push(p);
            }
        }
        for i in 0..np {
            ccount[i + 1] += ccount[i];
        }
        self.coff.clear();
        self.coff.extend_from_slice(&ccount);
        let ctotal = ccount[np] as usize;
        self.sc.clear();
        self.sc.resize(ctotal, SC { ts: 0, id: Key::EMPTY, camp: 0 });
        let mut k = 0;
        for piece in &cls {
            for c in piece {
                let p = self.cpid[k];
                k += 1;
                if p == u32::MAX {
                    continue;
                }
                let at = ccount[p as usize] as usize;
                ccount[p as usize] += 1;
                self.sc[at] = SC { ts: c.ts, id: c.id, camp: c.camp };
            }
        }
        drop(cls);

        let mut se = std::mem::take(&mut self.se);
        let mut sc = std::mem::take(&mut self.sc);
        for p in 0..np {
            let evs = &mut se[self.off[p] as usize..self.off[p + 1] as usize];
            evs.sort_unstable_by_key(|e| e.ts);
            let cls = &mut sc[self.coff[p] as usize..self.coff[p + 1] as usize];
            // ts ascending; within equal ts, click_id descending so the last one <= start
            // is the latest click with the smallest id.
            cls.sort_unstable_by(|a, b| a.ts.cmp(&b.ts).then_with(|| b.id.get().cmp(a.id.get())));
            self.person(self.names[p], evs, cls);
        }
        self.se = se;
        self.sc = sc;
    }

    fn person(&mut self, name: Key, evs: &[SE], cls: &[SC]) {
        let ctx = self.ctx;
        // Sessionize.
        self.sess.clear();
        self.purchases.clear();
        let mut last = i64::MIN;
        for e in evs {
            if self.sess.is_empty() || e.ts - last > GAP {
                self.sess.push(Sess { start: e.ts, day: e.ts.div_euclid(DAY), nev: 0, rev: 0, purch: false });
            }
            let si = self.sess.len() - 1;
            let s = &mut self.sess[si];
            s.nev += 1;
            if e.kind == 2 {
                s.rev += e.price as i64;
                s.purch = true;
                self.purchases.push((si as u32, e.cat, e.price));
            }
            last = e.ts;
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
            let mut top = -1i32;
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
                top = best as i32;
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
            while j < cls.len() && cls[j].ts <= s.start {
                j += 1;
            }
            let camp = if j > 0 && cls[j - 1].ts >= s.start - WEEK { cls[j - 1].camp } else { ctx.organic };
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
        self.bycamp.extend(cls.iter().map(|c| (c.camp, c.ts)));
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
            c_top.push_str(if t < 0 { None } else { Some(ctx.cat_names[t as usize]) });
        }
        parquet::write_file(path, n, &[c_user, c_day, c_sess, c_ev, c_rev, c_top])
    }
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

    unsafe {
        // Fixed mmap threshold: big buffers are mmapped (lazily faulted, freed to the OS, grown
        // with mremap instead of memcpy); glibc would otherwise keep raising the threshold.
        libc::mallopt(libc::M_MMAP_THRESHOLD, 128 << 10);
    }

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
    let mut products: StrMap<u16> = StrMap::with_capacity(1024);
    let mut cat_ids: HashMap<&'static [u8], u16> = HashMap::new();
    let mut cat_names: Vec<&'static [u8]> = Vec::new();
    if let Some(h) = memchr::memchr(b'\n', pbytes) {
        csvp::for_lines(&pbytes[h + 1..], |line| {
            let mut f = [Bs::EMPTY; 2];
            if !csvp::split(line, &mut f) || f[0].is_empty() {
                return;
            }
            let cat = f[1].get();
            let next = cat_names.len() as u16;
            let id = *cat_ids.entry(cat).or_insert_with(|| {
                cat_names.push(cat);
                next
            });
            products.get_or_insert(hash(f[0].get()), f[0], id);
        });
    }
    let unknown = cat_names.len() as u16;
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
    let mut paths: Vec<(u8, PathBuf)> = Vec::new();
    for (k, dir) in [(0u8, "events"), (1, "clicks"), (2, "links")] {
        for p in list_csv(&data.join(dir)) {
            paths.push((k, p));
        }
    }
    paths.sort_by_key(|(_, p)| std::cmp::Reverse(std::fs::metadata(p).map(|m| m.len()).unwrap_or(0)));
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
    let ev_rows = est_rows(&files[0]);
    let cl_rows = est_rows(&files[1]);
    stage("load");

    // ---- links
    let links = build_links(&files[2], target, nt);
    stage("links");

    // ---- events: parse + validate each copy, scatter by hash(event_id)
    let ev_chunks = make_chunks(&files[0], target);
    let next = AtomicUsize::new(0);
    let ev_bufs: Vec<Vec<Vec<Ev>>> = run_par(nt, |_| {
        let mut bufs: Vec<Vec<Ev>> = new_bufs(ev_rows, nt);
        let ctx = &ctx;
        loop {
            let i = next.fetch_add(1, Relaxed);
            if i >= ev_chunks.len() {
                break;
            }
            csvp::for_lines(ev_chunks[i], |line| {
                let mut f = [Bs::EMPTY; 7];
                if !csvp::split(line, &mut f) || f[0].is_empty() {
                    return;
                }
                let ingest = ts::parse_ts(f[3].get()).unwrap_or(i64::MAX);
                let mut valid = !f[1].is_empty() && !f[5].is_empty();
                let mut t = 0i64;
                let mut kind = 0u8;
                let mut price = 0u32;
                let mut cat = 0u16;
                if valid {
                    match ts::parse_ts(f[2].get()) {
                        Some(x) if x >= ctx.ws && x < ctx.we => t = x,
                        _ => valid = false,
                    }
                }
                if valid {
                    kind = match f[4].get() {
                        b"view" => 0,
                        b"cart" => 1,
                        b"purchase" => 2,
                        _ => {
                            valid = false;
                            0
                        }
                    };
                }
                if valid && kind == 2 {
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
                let idh = hash(f[0].get());
                bufs[part(idh)].push(Ev {
                    idh,
                    ingest,
                    ts: t,
                    id: Key::from_bs(f[0]),
                    user: if valid { Key::from_bs(f[1]) } else { Key::EMPTY },
                    price,
                    cat,
                    kind,
                    valid,
                });
            });
        }
        bufs
    });
    stage("events");

    // ---- clicks: parse + validate, scatter by hash(user_id)
    let cl_chunks = make_chunks(&files[1], target);
    let next = AtomicUsize::new(0);
    let cl_bufs: Vec<Vec<Vec<PC>>> = run_par(nt, |_| {
        let mut bufs: Vec<Vec<PC>> = new_bufs(cl_rows, nt);
        let ctx = &ctx;
        loop {
            let i = next.fetch_add(1, Relaxed);
            if i >= cl_chunks.len() {
                break;
            }
            csvp::for_lines(cl_chunks[i], |line| {
                let mut f = [Bs::EMPTY; 4];
                if !csvp::split(line, &mut f) || f[0].is_empty() || f[1].is_empty() {
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
                bufs[part(uh)].push(PC { h: uh, key: Key::from_bs(f[1]), ts: t, id: Key::from_bs(f[0]), camp });
            });
        }
        bufs
    });
    stage("clicks");

    // ---- dedup per event_id partition, scatter survivors by hash(user_id)
    let ev_parts = transpose(ev_bufs);
    let next = AtomicUsize::new(0);
    let ue_bufs: Vec<Vec<Vec<PE>>> = run_par(nt, |_| {
        let mut out: Vec<Vec<PE>> = new_bufs(ev_rows, nt);
        // (event_id hash, code = piece << 32 | index, +1; 0 = empty)
        let mut table: Vec<(u64, u64)> = Vec::new();
        loop {
            let b = next.fetch_add(1, Relaxed);
            if b >= PB {
                break;
            }
            let pieces = std::mem::take(&mut *ev_parts[b].lock().unwrap());
            let n: usize = pieces.iter().map(|v| v.len()).sum();
            if n == 0 {
                continue;
            }
            let at = |c: u64| &pieces[((c - 1) >> 32) as usize][((c - 1) & 0xffff_ffff) as usize];
            let cap = (n * 2).next_power_of_two();
            let mask = cap - 1;
            table.clear();
            table.resize(cap, (0, 0));
            for (pi, piece) in pieces.iter().enumerate() {
                for (i, e) in piece.iter().enumerate() {
                    let code = (((pi as u64) << 32) | i as u64) + 1;
                    let mut s = (e.idh as usize) & mask;
                    loop {
                        let (h, c) = table[s];
                        if c == 0 {
                            table[s] = (e.idh, code);
                            break;
                        }
                        if h == e.idh {
                            let o = at(c);
                            if o.id.same(&e.id) {
                                if e.ingest < o.ingest {
                                    table[s].1 = code;
                                }
                                break;
                            }
                        }
                        s = (s + 1) & mask;
                    }
                }
            }
            // Emit in input order: a valid copy survives iff it is its event_id's kept copy.
            for (pi, piece) in pieces.iter().enumerate() {
                for (i, e) in piece.iter().enumerate() {
                    if !e.valid {
                        continue;
                    }
                    let code = (((pi as u64) << 32) | i as u64) + 1;
                    let mut s = (e.idh as usize) & mask;
                    let winner = loop {
                        let (h, c) = table[s];
                        if c == code {
                            break true;
                        }
                        if c != 0 && h == e.idh && at(c).id.same(&e.id) {
                            break false;
                        }
                        s = (s + 1) & mask;
                    };
                    if winner {
                        let uh = hash(e.user.get());
                        out[part(uh)].push(PE { h: uh, key: e.user, ts: e.ts, price: e.price, cat: e.cat, kind: e.kind });
                    }
                }
            }
        }
        out
    });
    drop(ev_parts);
    stage("dedup");

    // ---- resolve: user ID -> person, scatter by hash(person)
    let ue_parts = transpose(ue_bufs);
    let uc_parts = transpose(cl_bufs);
    let next = AtomicUsize::new(0);
    let resolved: Vec<(Vec<Vec<PE>>, Vec<Vec<PC>>)> = run_par(nt, |_| {
        let mut oe: Vec<Vec<PE>> = new_bufs(ev_rows, nt);
        let mut oc: Vec<Vec<PC>> = new_bufs(cl_rows, nt);
        loop {
            let u = next.fetch_add(1, Relaxed);
            if u >= PB {
                break;
            }
            let lp = &links[u];
            for piece in std::mem::take(&mut *ue_parts[u].lock().unwrap()) {
                for e in piece {
                    let (nh, name) = lp.resolve(e.h, e.key);
                    oe[part(nh)].push(PE { h: nh, key: name, ..e });
                }
            }
            for piece in std::mem::take(&mut *uc_parts[u].lock().unwrap()) {
                for c in piece {
                    let (nh, name) = lp.resolve(c.h, c.key);
                    oc[part(nh)].push(PC { h: nh, key: name, ..c });
                }
            }
        }
        (oe, oc)
    });
    drop(ue_parts);
    drop(uc_parts);
    let (pe_bufs, pc_bufs): (Vec<_>, Vec<_>) = resolved.into_iter().unzip();
    stage("resolve");

    // ---- persons
    let pev_parts = transpose(pe_bufs);
    let cl_parts = transpose(pc_bufs);
    let mut order: Vec<(usize, usize)> = pev_parts
        .iter()
        .enumerate()
        .map(|(q, m)| (q, m.lock().unwrap().iter().map(|v| v.len()).sum()))
        .collect();
    order.sort_by_key(|&(_, n)| std::cmp::Reverse(n));
    let user_dir = output.join("user_daily");
    let camp_dir = output.join("campaign_daily");
    std::fs::create_dir_all(&user_dir).expect("create user_daily");
    std::fs::create_dir_all(&camp_dir).expect("create campaign_daily");
    let next = AtomicUsize::new(0);
    let accs: Vec<Acc> = run_par(nt, |tid| {
        let mut w = Worker::new(&ctx);
        loop {
            let k = next.fetch_add(1, Relaxed);
            if k >= PB {
                break;
            }
            let q = order[k].0;
            let evs = std::mem::take(&mut *pev_parts[q].lock().unwrap());
            let cls = std::mem::take(&mut *cl_parts[q].lock().unwrap());
            w.partition(evs, cls);
        }
        if !w.u_name.is_empty() || tid == 0 {
            w.write_users(&user_dir.join(format!("part-{tid:03}.parquet"))).expect("write user_daily");
        }
        w.acc
    });
    stage("persons");

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
