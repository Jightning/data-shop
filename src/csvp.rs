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
/// Delimiter and quote masks let the scanner count quotes without revisiting their positions.
#[inline]
pub fn for_records<const N: usize>(chunk: &'static [u8], f: impl FnMut(&[Bs; N])) {
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx2") && std::is_x86_feature_detected!("popcnt") {
            // Both instruction sets were checked once for the whole chunk.
            unsafe { return for_records_avx2(chunk, f) };
        }
    }
    for_records_masked::<N, false>(chunk, f);
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,popcnt")]
unsafe fn for_records_avx2<const N: usize>(chunk: &'static [u8], f: impl FnMut(&[Bs; N])) {
    for_records_masked::<N, true>(chunk, f);
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
unsafe fn masks<const AVX2: bool>(p: *const u8) -> (u64, u64, u64) {
    use std::arch::x86_64::*;
    let mut commas = 0u64;
    let mut newlines = 0u64;
    let mut quotes = 0u64;
    if AVX2 {
        for i in [0, 32] {
            let v = _mm256_loadu_si256(p.add(i).cast());
            let mask = |b| _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, _mm256_set1_epi8(b))) as u32 as u64;
            commas |= mask(b',' as i8) << i;
            newlines |= mask(b'\n' as i8) << i;
            quotes |= mask(b'"' as i8) << i;
        }
    } else {
        for i in [0, 16, 32, 48] {
            let v = _mm_loadu_si128(p.add(i).cast());
            let mask = |b| _mm_movemask_epi8(_mm_cmpeq_epi8(v, _mm_set1_epi8(b))) as u64;
            commas |= mask(b',' as i8) << i;
            newlines |= mask(b'\n' as i8) << i;
            quotes |= mask(b'"' as i8) << i;
        }
    }
    (commas, newlines, quotes)
}

#[inline(always)]
fn for_records_masked<const N: usize, const AVX2: bool>(
    chunk: &'static [u8],
    mut f: impl FnMut(&[Bs; N]),
) {
    let mut line_start = 0;
    let mut ends = [0; N];
    let mut nc = 0;
    let mut quote_count = 0;
    let mut out = [Bs::EMPTY; N];

    let mut emit = |start: usize, end, ends: &mut [usize; N], nc, quote_count| {
        if end == start || nc != N - 1 {
            return;
        }
        ends[N - 1] = end;
        let mut pos = start;
        let mut wrapping_quotes = 0;
        for (slot, &end) in out.iter_mut().zip(ends.iter()) {
            let field = &chunk[pos..end];
            *slot = if field.len() >= 2 && field[0] == b'"' && field[field.len() - 1] == b'"' {
                wrapping_quotes += 2;
                Bs::new(&field[1..field.len() - 1])
            } else {
                Bs::new(field)
            };
            pos = end + 1;
        }
        // Every quote must have been accounted for by a distinct pair of field endpoints.
        if wrapping_quotes == quote_count {
            f(&out);
        }
    };

    let mut base = 0;
    #[cfg(target_arch = "x86_64")]
    while base + 64 <= chunk.len() {
        // The loop bounds give both SIMD implementations 64 readable bytes.
        let (commas, newlines, mut quotes) = unsafe { masks::<AVX2>(chunk.as_ptr().add(base)) };
        let mut delimiters = commas | newlines;
        while delimiters != 0 {
            let bit = delimiters.trailing_zeros();
            let pos = base + bit as usize;
            let remaining = delimiters & (delimiters - 1);
            let delimiter = delimiters ^ remaining;
            let lower = delimiter - 1;
            if newlines & delimiter != 0 {
                quote_count += (quotes & lower).count_ones() as usize;
                quotes &= !lower;
                emit(line_start, pos, &mut ends, nc, quote_count);
                line_start = pos + 1;
                nc = 0;
                quote_count = 0;
            } else {
                if nc < N - 1 {
                    ends[nc] = pos;
                }
                nc += 1;
            }
            delimiters = remaining;
        }
        quote_count += quotes.count_ones() as usize;
        base += 64;
    }
    for (i, &byte) in chunk[base..].iter().enumerate() {
        let pos = base + i;
        match byte {
            b',' => {
                if nc < N - 1 {
                    ends[nc] = pos;
                }
                nc += 1;
            }
            b'"' => quote_count += 1,
            b'\n' => {
                emit(line_start, pos, &mut ends, nc, quote_count);
                line_start = pos + 1;
                nc = 0;
                quote_count = 0;
            }
            _ => {}
        }
    }
    if line_start < chunk.len() {
        emit(line_start, chunk.len(), &mut ends, nc, quote_count);
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
                got.clear();
                for_records_masked::<N, false>(chunk, |f| got.push(f.iter().map(|b| b.get().to_vec()).collect()));
                assert_eq!(got, want, "fallback on chunk {:?}", String::from_utf8_lossy(chunk));
            }
            check::<2>(chunk);
            check::<4>(chunk);
            check::<7>(chunk);
        }
    }
}
