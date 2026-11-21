//! Byte-level CSV dialect from rules.md: only `\n` ends a line, a quote may only wrap a whole
//! field, no value holds a comma/quote/newline, empty line or wrong field count is malformed.

use crate::util::Bs;

/// Calls `f` for every line of `chunk` (lines end at `\n`; a final unterminated line counts).
#[inline]
pub fn for_lines(chunk: &'static [u8], mut f: impl FnMut(&'static [u8])) {
    let mut pos = 0;
    for nl in memchr::memchr_iter(b'\n', chunk) {
        f(&chunk[pos..nl]);
        pos = nl + 1;
    }
    if pos < chunk.len() {
        f(&chunk[pos..]);
    }
}

#[inline(always)]
fn unquote(s: &'static [u8]) -> Option<Bs> {
    // Called only when the line holds a quote somewhere.
    match memchr::memchr(b'"', s) {
        None => Some(Bs::new(s)),
        Some(0) if s.len() >= 2 && s[s.len() - 1] == b'"' => {
            let inner = &s[1..s.len() - 1];
            if memchr::memchr(b'"', inner).is_some() {
                None
            } else {
                Some(Bs::new(inner))
            }
        }
        _ => None,
    }
}

/// Splits `line` into exactly N fields (unquoted). Returns false if the line is malformed.
#[inline]
pub fn split<const N: usize>(line: &'static [u8], out: &mut [Bs; N]) -> bool {
    if line.is_empty() {
        return false;
    }
    let quoted = memchr::memchr(b'"', line).is_some();
    let mut start = 0usize;
    let mut f = 0usize;
    let bytes = line;
    let mut i = 0usize;
    let n = bytes.len();
    while i < n {
        if bytes[i] == b',' {
            if f + 1 >= N {
                return false;
            }
            let s = &line[start..i];
            out[f] = if quoted {
                match unquote(s) {
                    Some(b) => b,
                    None => return false,
                }
            } else {
                Bs::new(s)
            };
            f += 1;
            start = i + 1;
        }
        i += 1;
    }
    if f + 1 != N {
        return false;
    }
    let s = &line[start..];
    out[f] = if quoted {
        match unquote(s) {
            Some(b) => b,
            None => return false,
        }
    } else {
        Bs::new(s)
    };
    true
}
