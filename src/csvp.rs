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

/// Calls `f` with the N unquoted fields of every well-formed line of `chunk`; malformed lines
/// (empty, wrong field count, a quote not wrapping a whole field) are skipped.
///
/// One structural pass finds every `\n`, `,` and `"` 64 bytes at a time with SSE2 (part of the
/// x86-64 baseline, so no runtime dispatch is needed); lines without quotes are split straight
/// from the comma positions, lines with a quote go through the strict `unquote` check.
#[inline]
pub fn for_records<const N: usize>(chunk: &'static [u8], mut f: impl FnMut(&[Bs; N])) {
    let len = chunk.len();
    let p = chunk.as_ptr();
    let mut line_start = 0usize;
    let mut commas = [0usize; N];
    let mut nc = 0usize;
    let mut quoted = false;
    let mut out = [Bs::EMPTY; N];

    let mut emit = |line_start: usize, end: usize, commas: &[usize; N], nc: usize, quoted: bool| {
        if end == line_start || nc != N - 1 {
            return;
        }
        let mut s = line_start;
        for k in 0..N {
            let e = if k < N - 1 { commas[k] } else { end };
            let field = &chunk[s..e];
            out[k] = if quoted {
                match unquote(field) {
                    Some(b) => b,
                    None => return,
                }
            } else {
                Bs::new(field)
            };
            s = e + 1;
        }
        f(&out);
    };

    let mut handle = |pos: usize, line_start: &mut usize, nc: &mut usize, quoted: &mut bool, commas: &mut [usize; N]| {
        match chunk[pos] {
            b',' => {
                if *nc < N - 1 {
                    commas[*nc] = pos;
                }
                *nc += 1;
            }
            b'"' => *quoted = true,
            _ => {
                // '\n'
                emit(*line_start, pos, commas, *nc, *quoted);
                *line_start = pos + 1;
                *nc = 0;
                *quoted = false;
            }
        }
    };

    let mut i = 0usize;
    #[cfg(target_arch = "x86_64")]
    unsafe {
        use std::arch::x86_64::*;
        let nl = _mm_set1_epi8(b'\n' as i8);
        let cm = _mm_set1_epi8(b',' as i8);
        let qt = _mm_set1_epi8(b'"' as i8);
        let mask16 = |v: __m128i| -> u64 {
            let m = _mm_or_si128(_mm_or_si128(_mm_cmpeq_epi8(v, nl), _mm_cmpeq_epi8(v, cm)), _mm_cmpeq_epi8(v, qt));
            _mm_movemask_epi8(m) as u32 as u64
        };
        while i + 64 <= len {
            let q = p.add(i) as *const __m128i;
            let mut m = mask16(_mm_loadu_si128(q))
                | (mask16(_mm_loadu_si128(q.add(1))) << 16)
                | (mask16(_mm_loadu_si128(q.add(2))) << 32)
                | (mask16(_mm_loadu_si128(q.add(3))) << 48);
            while m != 0 {
                let pos = i + m.trailing_zeros() as usize;
                handle(pos, &mut line_start, &mut nc, &mut quoted, &mut commas);
                m &= m - 1;
            }
            i += 64;
        }
    }
    while i < len {
        let c = chunk[i];
        if c == b'\n' || c == b',' || c == b'"' {
            handle(i, &mut line_start, &mut nc, &mut quoted, &mut commas);
        }
        i += 1;
    }
    if line_start < len {
        emit(line_start, len, &commas, nc, quoted);
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The SIMD scanner must agree with line-by-line `split` on arbitrary byte soup.
    #[test]
    fn scanner_matches_split() {
        let alphabet = b"ab,,\"\"\n\r 9";
        let mut seed: u64 = 0x1234_5678;
        for round in 0..3000 {
            let len = (round % 300) + 1;
            let mut buf = Vec::with_capacity(len);
            for _ in 0..len {
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                buf.push(alphabet[(seed >> 33) as usize % alphabet.len()]);
            }
            let chunk: &'static [u8] = Box::leak(buf.into_boxed_slice());
            fn check<const N: usize>(chunk: &'static [u8]) {
                let mut want: Vec<Vec<Vec<u8>>> = Vec::new();
                for_lines(chunk, |line| {
                    let mut f = [Bs::EMPTY; N];
                    if split(line, &mut f) {
                        want.push(f.iter().map(|b| b.get().to_vec()).collect());
                    }
                });
                let mut got: Vec<Vec<Vec<u8>>> = Vec::new();
                for_records::<N>(chunk, |f| got.push(f.iter().map(|b| b.get().to_vec()).collect()));
                assert_eq!(got, want, "chunk {:?}", String::from_utf8_lossy(chunk));
            }
            check::<2>(chunk);
            check::<4>(chunk);
            check::<7>(chunk);
        }
    }
}
