//! Minimal hand-written Parquet writer: one row group, one uncompressed PLAIN data page (v1)
//! per column, Thrift compact protocol metadata. Supports required/optional UTF8 strings,
//! required DATE (INT32) and required INT64.

use std::io::{BufWriter, Write};
use std::path::Path;

#[derive(Clone, Copy, PartialEq)]
pub enum Kind {
    Str,
    Date,
    I64,
}

pub struct Column {
    pub name: &'static str,
    pub kind: Kind,
    pub optional: bool,
    data: Vec<u8>,
    defs: Vec<bool>,
    count: usize,
}

impl Column {
    pub fn new(name: &'static str, kind: Kind, optional: bool) -> Self {
        Column { name, kind, optional, data: Vec::new(), defs: Vec::new(), count: 0 }
    }
    #[inline]
    pub fn push_str(&mut self, v: Option<&[u8]>) {
        self.count += 1;
        match v {
            Some(s) => {
                if self.optional {
                    self.defs.push(true);
                }
                self.data.extend_from_slice(&(s.len() as u32).to_le_bytes());
                self.data.extend_from_slice(s);
            }
            None => {
                debug_assert!(self.optional);
                self.defs.push(false);
            }
        }
    }
    #[inline]
    pub fn push_date(&mut self, days: i32) {
        self.count += 1;
        self.data.extend_from_slice(&days.to_le_bytes());
    }
    #[inline]
    pub fn push_i64(&mut self, v: i64) {
        self.count += 1;
        self.data.extend_from_slice(&v.to_le_bytes());
    }
    fn physical(&self) -> i32 {
        match self.kind {
            Kind::Str => 6,  // BYTE_ARRAY
            Kind::Date => 1, // INT32
            Kind::I64 => 2,  // INT64
        }
    }
    fn definition_levels(&self) -> Vec<u8> {
        let mut out = Vec::new();
        if self.optional {
            // RLE/bit-packed hybrid, bit width 1, RLE runs only, 4-byte length prefix.
            let mut lv = Vec::new();
            let mut i = 0;
            while i < self.defs.len() {
                let v = self.defs[i];
                let mut j = i;
                while j < self.defs.len() && self.defs[j] == v {
                    j += 1;
                }
                varint(&mut lv, ((j - i) as u64) << 1);
                lv.push(v as u8);
                i = j;
            }
            out.extend_from_slice(&(lv.len() as u32).to_le_bytes());
            out.extend_from_slice(&lv);
        }
        out
    }
}

fn varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// Thrift compact protocol writer (just what Parquet metadata needs).
struct Tw {
    buf: Vec<u8>,
    last: i16,
    stack: Vec<i16>,
}

const T_I32: u8 = 5;
const T_I64: u8 = 6;
const T_BIN: u8 = 8;
const T_LIST: u8 = 9;
const T_STRUCT: u8 = 12;

impl Tw {
    fn new() -> Self {
        Tw { buf: Vec::new(), last: 0, stack: Vec::new() }
    }
    fn field(&mut self, id: i16, ty: u8) {
        let d = id - self.last;
        if d > 0 && d <= 15 {
            self.buf.push(((d as u8) << 4) | ty);
        } else {
            self.buf.push(ty);
            varint(&mut self.buf, ((id << 1) ^ (id >> 15)) as u16 as u64);
        }
        self.last = id;
    }
    fn i32(&mut self, id: i16, v: i32) {
        self.field(id, T_I32);
        varint(&mut self.buf, ((v << 1) ^ (v >> 31)) as u32 as u64);
    }
    fn i64(&mut self, id: i16, v: i64) {
        self.field(id, T_I64);
        varint(&mut self.buf, ((v << 1) ^ (v >> 63)) as u64);
    }
    fn bin(&mut self, id: i16, b: &[u8]) {
        self.field(id, T_BIN);
        varint(&mut self.buf, b.len() as u64);
        self.buf.extend_from_slice(b);
    }
    fn list(&mut self, id: i16, elem: u8, n: usize) {
        self.field(id, T_LIST);
        if n < 15 {
            self.buf.push(((n as u8) << 4) | elem);
        } else {
            self.buf.push(0xF0 | elem);
            varint(&mut self.buf, n as u64);
        }
    }
    fn list_i32(&mut self, v: i32) {
        varint(&mut self.buf, ((v << 1) ^ (v >> 31)) as u32 as u64);
    }
    fn list_bin(&mut self, b: &[u8]) {
        varint(&mut self.buf, b.len() as u64);
        self.buf.extend_from_slice(b);
    }
    fn begin(&mut self, id: i16) {
        self.field(id, T_STRUCT);
        self.stack.push(self.last);
        self.last = 0;
    }
    /// Start a struct that is a list element (no field header).
    fn begin_elem(&mut self) {
        self.stack.push(self.last);
        self.last = 0;
    }
    fn end(&mut self) {
        self.buf.push(0);
        self.last = self.stack.pop().unwrap();
    }
    fn stop(&mut self) {
        self.buf.push(0);
    }
}

/// Writes `cols` (all with `rows` values) as one Parquet file at `path`.
pub fn write_file(path: &Path, rows: usize, cols: &[Column]) -> std::io::Result<()> {
    let mut file = BufWriter::new(std::fs::File::create(path)?);
    file.write_all(b"PAR1")?;
    let mut offset = 4i64;
    struct Meta {
        offset: i64,
        size: i64,
    }
    let mut metas = Vec::with_capacity(cols.len());
    for c in cols {
        assert_eq!(c.count, rows, "column {} has {} values, expected {}", c.name, c.count, rows);
        let defs = c.definition_levels();
        let body_len = defs.len() + c.data.len();
        let mut h = Tw::new();
        h.i32(1, 0); // DATA_PAGE
        h.i32(2, body_len as i32);
        h.i32(3, body_len as i32);
        h.begin(5); // DataPageHeader
        h.i32(1, rows as i32);
        h.i32(2, 0); // PLAIN
        h.i32(3, 3); // RLE definition levels
        h.i32(4, 3); // RLE repetition levels
        h.end();
        h.stop();
        file.write_all(&h.buf)?;
        file.write_all(&defs)?;
        file.write_all(&c.data)?;
        let size = (h.buf.len() + body_len) as i64;
        metas.push(Meta { offset, size });
        offset += size;
    }

    let mut t = Tw::new();
    t.i32(1, 1); // version
    t.list(2, T_STRUCT, cols.len() + 1);
    t.begin_elem();
    t.bin(4, b"schema");
    t.i32(5, cols.len() as i32);
    t.end();
    for c in cols {
        t.begin_elem();
        t.i32(1, c.physical());
        t.i32(3, if c.optional { 1 } else { 0 }); // OPTIONAL / REQUIRED
        t.bin(4, c.name.as_bytes());
        match c.kind {
            Kind::Str => {
                t.i32(6, 0); // converted_type UTF8
                t.begin(10); // logicalType
                t.begin(1); // STRING
                t.end();
                t.end();
            }
            Kind::Date => {
                t.i32(6, 6); // converted_type DATE
                t.begin(10);
                t.begin(6); // DATE
                t.end();
                t.end();
            }
            Kind::I64 => {}
        }
        t.end();
    }
    t.i64(3, rows as i64);
    t.list(4, T_STRUCT, 1);
    t.begin_elem(); // RowGroup
    t.list(1, T_STRUCT, cols.len());
    let mut total = 0i64;
    for (c, m) in cols.iter().zip(&metas) {
        total += m.size;
        t.begin_elem(); // ColumnChunk
        t.i64(2, m.offset);
        t.begin(3); // ColumnMetaData
        t.i32(1, c.physical());
        t.list(2, T_I32, 2);
        t.list_i32(0); // PLAIN
        t.list_i32(3); // RLE
        t.list(3, T_BIN, 1);
        t.list_bin(c.name.as_bytes());
        t.i32(4, 0); // UNCOMPRESSED
        t.i64(5, rows as i64);
        t.i64(6, m.size);
        t.i64(7, m.size);
        t.i64(9, m.offset);
        t.end();
        t.end();
    }
    t.i64(2, total);
    t.i64(3, rows as i64);
    t.end();
    t.bin(6, b"shopping-sessions");
    t.stop();

    file.write_all(&t.buf)?;
    file.write_all(&(t.buf.len() as u32).to_le_bytes())?;
    file.write_all(b"PAR1")?;
    file.flush()
}
